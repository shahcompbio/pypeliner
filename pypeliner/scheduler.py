"""
Job scheduling class

"""

import os
import logging
import traceback
import fnmatch

import pypeliner.graph
import pypeliner.execqueue.base
import pypeliner.database


class PipelineException(Exception):
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
        with pypeliner.database.WorkflowDatabaseFactory(self.temps_dir, self.workflow_dir, self.logs_dir, file_storage) as db_factory:
            workflow = pypeliner.graph.WorkflowInstance(workflow_def, db_factory, runskip, ctx=workflow_def.ctx, cleanup=self.cleanup)
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

        if hasattr(job.job_def, 'sandbox'):
            if job.job_def.sandbox is not None:
                sandbox_name = os.path.basename(job.job_def.sandbox.prefix)
            else:
                sandbox_name = 'root'

            exec_log_str = 'job ' + job.displayname + ' executing in sandbox ' + sandbox_name

        else:
            exec_log_str = 'job ' + job.displayname + ' executing'

        self._logger.info(exec_log_str,
                          extra={"id": job.displayname, "type":"job", "requested_mem(GB)":job.ctx["mem"], "status":"executing"})
        self._logger.info('job ' + job.displayname + ' executing',
                          extra={"id": job.displayname, "type":"job", "cmd": sent.displaycommand, 'task_name': job.id[1]})
        self._logger.info('job ' + job.displayname + ' context ' + str(job.ctx))

        dirs = [self.temps_dir, self.logs_dir]
        #need these to track the output file and the inputs
        dirs.extend([v.filename for v in job.inputs if isinstance(v, pypeliner.resources.UserResource)])
        dirs.extend([v.filename for v in job.outputs if isinstance(v, pypeliner.resources.UserResource)])
        sent.dirs = dirs

        exec_queue.send(job.ctx, job.displayname, sent, exc_dir)

    def _retry_job(self, exec_queue, job):
        if not job.retry():
            return False
        self._logger.info('job ' + job.displayname + ' retry ' + str(job.retry_idx),
                          extra={"id": job.displayname, "type":"job", "retry_count": job.retry_idx, 'task_name': job.id[1]})
        self._add_job(exec_queue, job)
        return True


    def _add_jobs(self, exec_queue, workflow):
        while exec_queue.length < self.max_jobs:
            try:
                job = workflow.pop_next_job()
            except pypeliner.graph.NoJobs:
                return
            self._add_job(exec_queue, job)

    def _wait_next_job(self, exec_queue, workflow):
        name = exec_queue.wait()

        job = self._active_jobs[name]
        del self._active_jobs[name]

        assert job is not None

        try:
            received = exec_queue.receive(name)
        except pypeliner.execqueue.base.ReceiveError as e:
            self._logger.error('job ' + job.displayname + ' submit error\n' + traceback.format_exc(),
                               extra={"id": job.displayname, "type":"job", "submit_error": traceback.format_exc(), 'task_name': job.id[1]})
            received = None

        if received is not None and job.id != received.id:
            raise Exception('job id {} doenst match received id {}'.format(job.id, received.id))

        if received is not None:
            try:
                received.collect_logs()
            except Exception as e:
                self._logger.error('job ' + job.displayname + ' collect logs error\n' + traceback.format_exc(),
                                   extra={"id": job.displayname, "type":"job", "status":"error", 'task_name': job.id[1]})
            if received.finished:
                self._logger.info('job ' + job.displayname + ' completed successfully',
                                  extra={"id": job.displayname, "type":"job", "status": "success", 'task_name': job.id[1]})
            else:
                self._logger.error('job ' + job.displayname + ' failed to complete\n' + received.log_text(),
                                   extra={"id": job.displayname, "type":"job", "status": "fail", 'task_name': job.id[1]})
            self._logger.info('job ' + job.displayname + ' -> ' + received.displaycommand,
                              extra={"id": job.displayname, "type":"job", "cmd": received.displaycommand, 'task_name': job.id[1]})
            self._logger.info('job ' + job.displayname + ' time ' + str(received.duration) + 's',
                              extra={"id": job.displayname, "type":"job", "time": received.duration, 'task_name': job.id[1]})
            self._logger.info('job ' + job.displayname + ' memory ' + str(received.memoryused) + 'G',
                              extra={"id": job.displayname, "type":"job", "memory": received.memoryused, 'task_name': job.id[1]})
            self._logger.info('job ' + job.displayname + ' host name ' + str(received.hostname) + 's',
                              extra={"id": job.displayname, "type":"job", "hostname": received.hostname, 'task_name': job.id[1]})

            for warning in received.warnings:
                self._logger.warn('job ' + job.displayname + ' warning: ' + warning)

        if received is None or not received.finished:
            if self._retry_job(exec_queue, job):
                return
            else:
                raise pypeliner.graph.IncompleteJobException()

        received.finalize(job)


