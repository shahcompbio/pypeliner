"""
Job scheduling class

"""

import logging
import traceback

import helpers
import graph


class PipelineException(Exception):
    pass

class IncompleteJobException(Exception):
    pass


def _setobj_helper(value):
    return value


class Scheduler(object):
    """ Job scheduling class for queueing a set of jobs and running
    those jobs according to their dependencies.

    """
    def __init__(self):
        self._logger = logging.getLogger('scheduler')
        self.max_jobs = 1
        self.rerun = False
        self.repopulate = False
        self.cleanup = True
        self.prune = True
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
        self._workflow_dir = helpers.abspath(value)

    @property
    def logs_dir(self):
        return self._logs_dir
    @logs_dir.setter
    def logs_dir(self, value):
        self._logs_dir = helpers.abspath(value)

    def run(self, workflow_def, exec_queue):
        """ Run the pipeline

        :param workflow_def: workflow of jobs to be submitted.
        :param exec_queue: queue to which jobs will be submitted.  The queues implemented
                           in :py:mod:`pypeliner.execqueue` should suffice for most purposes

        Call this function after adding jobs to a workflow using
        :py:func:`pypeliner.scheduler.Scheduler.transform` etc.  Jobs will be run locally or
        remotely using the `exec_queue` provided until completion.  On failure, the function
        will wait for the remaining jobs to finish but will not submit new ones.  The first
        interrupt (control-C) in this function will result in the sessation of new job creation,
        and the second interrupt will attempt to cleanly cancel all jobs.

        """
        self._job_temps_dirs = set()
        with helpers.DirectoryLock() as dir_lock:
            workflow = graph.WorkflowInstance(workflow_def, self.workflow_dir, self.logs_dir, dir_lock, prune=self.prune, cleanup=self.cleanup, rerun=self.rerun, repopulate=self.repopulate)
            failing = False
            try:
                try:
                    while True:
                        self._add_jobs(exec_queue, workflow)
                        if exec_queue.empty:
                            break
                        self._wait_next_job(exec_queue, workflow)
                except KeyboardInterrupt as e:
                    raise e
                except Exception:
                    failing = True
                    self._logger.error('exception\n' + traceback.format_exc())
                while not exec_queue.empty:
                    try:
                        self._wait_next_job(exec_queue, workflow)
                    except KeyboardInterrupt as e:
                        raise e
                    except Exception:
                        self._logger.error('exception\n' + traceback.format_exc())
            except KeyboardInterrupt as e:
                self._logger.error('interrupted')
                raise e
            if failing:
                self._logger.error('pipeline failed')
                raise PipelineException('pipeline failed')

    def _add_jobs(self, exec_queue, workflow):
        while exec_queue.length < self.max_jobs:
            try:
                job = workflow.pop_next_job()
            except graph.NoJobs:
                return
            if job.out_of_date or self.rerun or self.repopulate and job.output_missing:
                if job.temps_dir in self._job_temps_dirs:
                    raise ValueError('duplicate temps directory ' + job.temps_dir)
                self._job_temps_dirs.add(job.temps_dir)
                exec_queue.add(job.ctx, job)
                self._logger.info('job ' + job.displayname + ' executing')
                self._logger.info('job ' + job.displayname + ' -> ' + job.displaycommand)
            else:
                job.complete()
                self._logger.info('job ' + job.displayname + ' skipped')
            self._logger.debug('job ' + job.displayname + ' explanation: ' + job.explain())

    def _wait_next_job(self, exec_queue, workflow):
        job, received = exec_queue.wait()
        assert job is not None
        assert job.id == received.id
        if not received.finished:
            self._logger.error('job ' + job.displayname + ' failed to complete\n' + received.log_text())
            raise IncompleteJobException()
        job.finalize(received)
        job.complete()
        self._logger.info('job ' + job.displayname + ' completed successfully')
        self._logger.info('job ' + job.displayname + ' time ' + str(received.duration) + 's')
        self._logger.info('job ' + job.displayname + ' host name ' + str(received.hostname) + 's')

    def pretend(self, workflow_def):
        """ Pretend run the pipeline.

        Print jobs that would be run, but do not actually run them.  May halt before completion of
        the pipeline if some axes have not yet been defined.

        """
        workflow = graph.DependencyGraph()
        with resourcemgr.ResourceManager(self.temps_dir, self.db_dir) as resmgr, self.PipelineLock():
            nodemgr = nodes.NodeManager(self.nodes_dir, self.temps_dir)
            jobs = self._create_jobs(workflow_def, resmgr, nodemgr)
            self._workflow_regenerate(resmgr, nodemgr, jobs, workflow)
            while workflow.jobs_ready:
                job = workflow.next_job()
                if job.out_of_date or self.rerun or self.repopulate and job.output_missing:
                    self._logger.info('job ' + job.displayname + ' executing')
                    if not job.trigger_regenerate:
                        workflow.notify_completed(job)
                else:
                    self._logger.info('job ' + job.displayname + ' skipped')
                    workflow.notify_completed(job)
                self._logger.debug('job ' + job.displayname + ' explanation: ' + job.explain())
        return True

