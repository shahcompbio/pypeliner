"""
Job scheduling class

"""

import os.path
import copy
import pickle
import logging
import shelve
import sys
import traceback
import time
import subprocess
import contextlib
import collections
import itertools

import helpers
import commandline
import graph
import managed
import arguments
import resources
import resourcemgr
import nodes
import jobs


def _setobj_helper(value):
    return value


class Scheduler(object):
    """ Job scheduling class for queueing a set of jobs and running
    those jobs according to their dependencies.

    """
    def __init__(self):
        self._logger = logging.getLogger('scheduler')
        self._abstract_jobs = dict()
        self.max_jobs = 1
        self.rerun = False
        self.repopulate = False
        self.cleanup = True
        self.prune = True
        self.set_pipeline_dir('./')
        self.freeze = True

    def __setattr__(self, attr, value):
        if getattr(self, "freeze", False) and not hasattr(self, attr):
            raise AttributeError("Setting new attribute")
        super(Scheduler, self).__setattr__(attr, value)
 
    def set_pipeline_dir(self, pipeline_dir):
        pipeline_dir = helpers.abspath(pipeline_dir)
        self.db_dir = os.path.join(pipeline_dir, 'db')
        self.temps_dir = os.path.join(pipeline_dir, 'tmp')
        self.logs_dir = os.path.join(pipeline_dir, 'log')
 
    @property
    def db_dir(self):
        return self._db_dir
    @db_dir.setter
    def db_dir(self, value):
        self._db_dir = helpers.abspath(value)

    @property
    def nodes_dir(self):
        return os.path.join(self.db_dir, 'nodes')

    @property
    def temps_dir(self):
        return self._temps_dir
    @temps_dir.setter
    def temps_dir(self, value):
        self._temps_dir = helpers.abspath(value)

    @property
    def logs_dir(self):
        return self._logs_dir
    @logs_dir.setter
    def logs_dir(self, value):
        self._logs_dir = helpers.abspath(value)

    def setobj(self, obj, value):
        """ Set a managed temp object with a specified value.

        :param obj: managed object to be set with a given value
        :type obj: TempOutputObj
        :param value: value to set

        This function is most useful for tracking changes to small objects and parameters.
        Set the object to a given value using this function.  Then use the managed version
        of the object in calls to transform, and pypeliner will only trigger a rerun if the
        value of the object has changed in a subsequent run.

        """
        name = '_'.join(('setobj', obj.name) + obj.axes)
        self.transform(name, (), {'local':True}, _setobj_helper, obj, value)

    def commandline(self, name, axes, ctx, *args):
        """ Add a command line based transform to the pipeline

        This call is equivalent to::

            self.transform(name, axes, ctx, commandline.execute, None, *args)

        See :py:func:`pypeliner.scheduler.transform`

        """
        self.transform(name, axes, ctx, commandline.execute, None, *args)

    def transform(self, name, axes, ctx, func, ret, *args, **kwargs):
        """ Add a transform to the pipeline.  A transform defines a job that uses the
        provided python function ``func`` to take input dependencies and create/update 
        output dependents.

        :param name: unique name of the job, used to identify the job in logs and when
                     submitting instances to the exec queue
        :param axes: axes of the job.  defines the axes on which the job will operate.  A
                     job with an empty list for the axes has a single instance.  A job with
                     one axis in the axes list will have as many instances as were defined
                     for that axis by the split that is responsible for that axis.
        :param ctx: context of the job as a dictionary of key, value pairs.  The context
                    is given to the exec queue and provides a way of communicating jobs
                    specific requirements such as memory and cpu usage.  Setting
                    ``ctx['local'] = True`` will result in the job being run locally on
                    the calling machine even when a cluster is being used.
        :param func: The function to call for this job.
        :param ret: The return value 
        :param args: The list of positional arguments to be used for the function call.
        :param kwargs: The list of keyword arguments to be used for the function call.

        Any value in args or kwargs that is an instance of
        :py:class:`pypeliner.managed.Managed` will be resolved to a pipeline managed
        file or object at runtime.  See :py:mod:`pypeliner.managed`.

        Acceptable values given for ``ret`` are restricted to a subset of
        :py:class:`pypeliner.managed.Managed` derived classes that represent output
        objects.  The return value of ``func`` will be stored and used by the pipelining
        system according to the specific details of the :py:class:`pypeliner.managed.Managed`
        derived class.

        """
        if name in self._abstract_jobs:
            raise ValueError('Job already defined')
        self._abstract_jobs[name] = jobs.AbstractJob(name, axes, ctx, func, jobs.CallSet(ret, args, kwargs), self.logs_dir)

    def changeaxis(self, name, axes, var_name, old_axis, new_axis):
        """ Change the axis for a managed variable.  This acts as a regular jobs with
        input dependencies and output dependents as for jobs created using transform.

        :param name: unique name of the change axis job, used to identify the job in logs
                     and when submitting instances to the exec queue
        :param axes: base axes of the managed object for which the axis change is requested.
                     only the last axis may be changed, thus all previous axes should be
                     given here as a list
        :param var_name: name of the managed object for which the axis change is requested.
        :param old_axis: previous axis on which the managed object is defined.
        :param new_axis: new axis for the new managed object.  The new object will be defined
                         on this axis and will be equivalent to the previous object.

        """
        if name in self._abstract_jobs:
            raise ValueError('Job already defined')
        self._abstract_jobs[name] = jobs.AbstractChangeAxis(name, axes, var_name, old_axis, new_axis)

    def _create_jobs(self, resmgr, nodemgr):
        """ Create concrete jobs from abstract jobs given resource and
        node managers
        """
        jobs = dict()
        for abstract_job in self._abstract_jobs.itervalues():
            for job in abstract_job.create_jobs(resmgr, nodemgr):
                jobs[job.id] = job
        return jobs

    def _depgraph_regenerate(self, resmgr, nodemgr, jobs, depgraph):
        """ regenerate dependency graph based on concrete jobs """
        inputs = set((input for job in jobs.itervalues() for input in job.pipeline_inputs))
        if self.prune:
            outputs = set((output for job in jobs.itervalues() for output in job.pipeline_outputs))
        else:
            outputs = set((output for job in jobs.itervalues() for output in job.outputs))
        inputs = inputs.difference(outputs)
        depgraph.regenerate(inputs, outputs, jobs.values())

    def run(self, exec_queue):
        """ Run the pipeline

        :param exec_queue: queue to which jobs will be submitted.  The queues implemented
                           in :py:mod:`pypeliner.execqueue` should suffice for most purposes

        Call this function after adding jobs to the scheduler using
        :py:func:`pypeliner.scheduler.Scheduler.transform` etc.  Jobs will be run locally or
        remotely using the `exec_queue` provided until completion.  On failure, the function
        will wait for the remaining jobs to finish but will not submit new ones.  The first
        interrupt (control-C) in this function will result in the sessation of new job creation,
        and the second interrupt will attempt to cleanly cancel all jobs.

        """
        helpers.makedirs(self.db_dir)
        helpers.makedirs(self.nodes_dir)
        helpers.makedirs(self.temps_dir)
        helpers.makedirs(self.logs_dir)
        depgraph = graph.DependencyGraph()
        with resourcemgr.ResourceManager(self.temps_dir, self.db_dir) as resmgr, self.PipelineLock():
            nodemgr = nodes.NodeManager(self.nodes_dir, self.temps_dir)
            jobs = self._create_jobs(resmgr, nodemgr)
            self._depgraph_regenerate(resmgr, nodemgr, jobs, depgraph)
            failing = False
            while not depgraph.finished:
                if not failing:
                    try:
                        while exec_queue.length < self.max_jobs and depgraph.jobs_ready:
                            job_id = depgraph.next_job()
                            job = jobs[job_id]
                            if job.out_of_date or self.rerun or self.repopulate and job.output_missing:
                                job_callable = job.create_callable()
                                if job_callable is None:
                                    self._logger.info(job.displayname + ' executing')
                                    job.finalize()
                                    depgraph.notify_completed(job)
                                else:
                                    exec_queue.add(job_callable.ctx, job_callable)
                                    self._logger.info(job_callable.displayname + ' executing')
                                    self._logger.info(job_callable.displayname + ' -> ' + job_callable.displaycommand)
                            else:
                                depgraph.notify_completed(job)
                                self._logger.info(job.displayname + ' skipped')
                            self._logger.debug(job.displayname + ' explanation: ' + job.explain())
                    except helpers.SubmitException as e:
                        failing = True
                    except:
                        failing = True
                        self._logger.error('exception\n' + traceback.format_exc())
                if exec_queue.empty:
                    break
                try:
                    job_id, job_callable = exec_queue.wait()
                    job = jobs[job_id]
                    if job_callable is None:
                        failing = True
                        self._logger.error(job.displayname + ' failed execute')
                        continue
                    assert job_id == job_callable.id
                    if job_callable.finished:
                        job.finalize(job_callable)
                        self._logger.info(job_callable.displayname + ' completed successfully')
                    else:
                        failing = True
                        self._logger.error(job_callable.displayname + ' failed to complete\n' + job_callable.log_text())
                        continue
                    self._logger.info(job_callable.displayname + ' time ' + str(job_callable.duration) + 's')
                    if jobs[job_callable.id].trigger_regenerate:
                        jobs = self._create_jobs(resmgr, nodemgr)
                        self._depgraph_regenerate(resmgr, nodemgr, jobs, depgraph)
                    depgraph.notify_completed(jobs[job_callable.id])
                    if self.cleanup:
                        resmgr.cleanup(depgraph)
                except KeyboardInterrupt as e:
                    if failing:
                        raise e
                    failing = True
                    self._logger.error('exception\n' + traceback.format_exc())
                except:
                    failing = True
                    self._logger.error('exception\n' + traceback.format_exc())
            if failing:
                self._logger.error('pipeline failed')
                raise helpers.PipelineException('pipeline failed')

    @contextlib.contextmanager
    def PipelineLock(self):
        lock_directory = os.path.join(self.db_dir, 'lock')
        try:
            os.mkdir(lock_directory)
        except OSError:
            raise Exception('Pipeline already running, remove {0} to override'.format(lock_directory))
        try:
            yield
        finally:
            os.rmdir(lock_directory)
            
    def pretend(self):
        """ Pretend run the pipeline.

        Print jobs that would be run, but do not actually run them.  May halt before completion of
        the pipeline if some axes have not yet been defined.

        """
        depgraph = graph.DependencyGraph()
        with resourcemgr.ResourceManager(self.temps_dir, self.db_dir) as resmgr, self.PipelineLock():
            nodemgr = nodes.NodeManager(self.nodes_dir, self.temps_dir)
            jobs = self._create_jobs(resmgr, nodemgr)
            self._depgraph_regenerate(resmgr, nodemgr, jobs, depgraph)
            while depgraph.jobs_ready:
                job_id = depgraph.next_job()
                job = jobs[job_id]
                if job.out_of_date or self.rerun or self.repopulate and job.output_missing:
                    self._logger.info(job.displayname + ' executing')
                    if not job.trigger_regenerate:
                        depgraph.notify_completed(job)
                else:
                    self._logger.info(job.displayname + ' skipped')
                    depgraph.notify_completed(job)
                self._logger.debug(job.displayname + ' explanation: ' + job.explain())
        return True

