"""
Job scheduling class

"""

import logging
import os
import traceback

import pypeliner
import pypeliner.database
import pypeliner.execqueue.base
import pypeliner.graph


class PipelineException(Exception):
    pass


class JobIdMismatchError(Exception):
    pass


class IncompleteJobException(Exception):
    pass


class Scheduler(object):
    """ Job scheduling class for queueing a set of jobs and running
    those jobs according to their dependencies.

    """

    def __init__(self):
        self._logger = logging.getLogger('pypeliner.scheduler')
        self.max_jobs = 1
        self.cleanup = True
        self.temps_dir = './tmp'
        self.workflow_dir = './'
        self.logs_dir = './log'
        self.freeze = True

    def __setattr__(self, attr, value):
        if not attr.startswith('_') and getattr(self, "freeze", False) and not hasattr(self, attr):
            raise AttributeError("Setting new attribute")
        super(Scheduler, self).__setattr__(attr, value)

    @property
    def workflow_dir(self):
        return self._workflow_dir

    @workflow_dir.setter
    def workflow_dir(self, value):
        self._workflow_dir = value

    @property
    def logs_dir(self):
        return self._logs_dir

    @logs_dir.setter
    def logs_dir(self, value):
        self._logs_dir = value

    def run(self, workflow_def, exec_queue, file_storage, runskip):
        """ Run the pipeline

        :param workflow_def: workflow of jobs to be submitted.
        :param exec_queue: queue to which jobs will be submitted.  The queues implemented
                           in :py:mod:`pypeliner.execqueue` should suffice for most purposes
        :param runskip: callable object returning boolean, used to determine whether to run jobs

        Call this function after adding jobs to a workflow using
        :py:func:`pypeliner.scheduler.Scheduler.transform` etc.  Jobs will be run locally or
        remotely using the `exec_queue` provided until completion.  On failure, the function
        will wait for the remaining jobs to finish but will not submit new ones.  The first
        interrupt (control-C) in this function will result in the sessation of new job creation,
        and the second interrupt will attempt to cleanly cancel all jobs.

        """

        self._active_jobs = dict()
        self._job_exc_dirs = set()
        with pypeliner.database.WorkflowDatabaseFactory(
                self.temps_dir, self.workflow_dir, self.logs_dir, file_storage
        ) as db_factory:
            workflow = pypeliner.graph.WorkflowInstance(
                workflow_def, db_factory, runskip, ctx=workflow_def.ctx, cleanup=self.cleanup
            )
            failing = False
            try:
                try:
                    while True:
                        self._add_jobs(exec_queue, workflow)
                        if exec_queue.empty:
                            break
                        self._wait_next_job(exec_queue, workflow)
                except KeyboardInterrupt as e:
                    raise
                except Exception:
                    failing = True
                    self._logger.error('exception\n' + traceback.format_exc())
                while not exec_queue.empty:
                    try:
                        self._wait_next_job(exec_queue, workflow)
                    except KeyboardInterrupt as e:
                        raise
                    except Exception:
                        self._logger.error('exception\n' + traceback.format_exc())
            except KeyboardInterrupt as e:
                self._logger.error('interrupted')
                raise
            if failing:
                self._logger.error('pipeline failed')
                raise PipelineException('pipeline failed')

    def _add_job(self, exec_queue, job):
        sent = job.create_callable()
        exc_dir = job.create_exc_dir()

        self._active_jobs[job.displayname] = job

        if exc_dir in self._job_exc_dirs:
            raise ValueError('duplicate temps directory ' + exc_dir)
        self._job_exc_dirs.add(exc_dir)

        exec_log = [('job_name', job.displayname), ('status', ' executing'), ('context', job.ctx)]

        if hasattr(job.job_def, 'sandbox'):
            if job.job_def.sandbox is not None:
                sandbox_name = os.path.basename(job.job_def.sandbox.prefix)
            else:
                sandbox_name = 'root'
            exec_log.append(('sandbox', sandbox_name))

        pypeliner.helpers.log_event(
            exec_log,
            extras={'task_name': job.id[1], "cmd": sent.displaycommand},
            logger=self._logger
        )

        dirs = [self.temps_dir, self.logs_dir]
        # need these to track the output file and the inputs
        dirs.extend([v.filename for v in job.inputs if isinstance(v, pypeliner.resources.UserResource)])
        dirs.extend([v.filename for v in job.outputs if isinstance(v, pypeliner.resources.UserResource)])
        sent.dirs = dirs
        sent.version = pypeliner.__version__

        exec_queue.send(job.ctx, job.displayname, sent, exc_dir)

    def _retry_job(self, exec_queue, job):
        if not job.retry():
            return False
        pypeliner.helpers.log_event(
            [('job_name', job.displayname), ('retry_count', job.retry_idx)],
            extras={'task_name': job.id[1]},
            logger=self._logger
        )
        self._add_job(exec_queue, job)
        return True

    def _add_jobs(self, exec_queue, workflow):
        while exec_queue.length < self.max_jobs:
            try:
                job = workflow.pop_next_job()
            except pypeliner.graph.NoJobs:
                return
            self._add_job(exec_queue, job)

    def _handle_error(self, job, error, error_str, exec_queue):
        pypeliner.helpers.log_event(
            ['job_name', job.displayname, error_str, error],
            extras={'job_name': job.displayname, 'task_name': job.id[1], "status": 'error'},
            logger=self._logger,
            level='err',
        )
        if not self._retry_job(exec_queue, job):
            raise pypeliner.graph.IncompleteJobException()


    def _wait_next_job(self, exec_queue, workflow):

        name = exec_queue.wait()

        job = self._active_jobs[name]
        del self._active_jobs[name]

        assert job is not None

        try:
            received = exec_queue.receive(name)
            received.collect_logs()
            if not received.finished:
                raise IncompleteJobException()
        except pypeliner.execqueue.base.ReceiveError:
            self._handle_error(job, traceback.format_exc(), "submit error\n", exec_queue)
            return
        except IncompleteJobException:
            self._handle_error(job, received.log_text(), "failed to complete\n", exec_queue)
            pypeliner.helpers.log_event(
                [('job_name', job.displayname), ('->', received.displaycommand)],
                extras={'task_name': job.id[1]},
                logger=self._logger
            )
            return
        except Exception:
            self._handle_error(job, traceback.format_exc(), 'collect logs error\n', exec_queue)
            return

        if job.id != received.id:
            raise JobIdMismatchError('job id {} doesnt match received id {}'.format(job.id, received.id))

        pypeliner.helpers.log_event(
            ['job_name', job.displayname, "completed successfully"],
            extras={'job_name': job.displayname, 'task_name': job.id[1], "status": 'success'},
            logger=self._logger
        )

        pypeliner.helpers.log_event(
            [('job_name', job.displayname), ('->', received.displaycommand)],
            extras={'task_name': job.id[1]},
            logger=self._logger
        )

        pypeliner.helpers.log_event(
            [('job_name', job.displayname), ('time', str(received.duration) + 's')],
            extras={'task_name': job.id[1]},
            logger=self._logger
        )

        pypeliner.helpers.log_event(
            [('job_name', job.displayname), ('memory', str(received.memoryused) + 'G')],
            extras={'task_name': job.id[1]},
            logger=self._logger
        )

        pypeliner.helpers.log_event(
            [('job_name', job.displayname), ('host_name', received.hostname)],
            extras={'task_name': job.id[1]},
            logger=self._logger
        )

        received.finalize(job)
