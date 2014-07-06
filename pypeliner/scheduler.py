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


class Scheduler(object):
    """ Main Pypeline class for queueing a set of jobs and running
    those jobs according to their dependencies """
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
    def ifile(self, name, axes=()):
        """ Create a ManagedInputFile input file representing a managed argument.
        The ManagedInputFile should be given to transform instead of a real argument
        """
        return managed.TempInputFile(name, *axes)
    def ofile(self, name, axes=()):
        """ Create a ManagedOutputFile output file representing a managed argument.
        The ManagedOutputFile should be given to transform instead of a real argument
        """
        return managed.TempOutputFile(name, *axes)
    def tmpfile(self, name, axes=()):
        """ Create a ManagedTempFile representing a managed temporary filename.
        If ManagedTempFile is given to transform in place of an argument, when
        the function is called the ManagedTempFile will be replaced with the a
        valid temporary filename.
        """
        return managed.TempFile(name, *axes)
    def iobj(self, name, axes=()):
        """ Create a ManagedInputObj object representing a managed argument.
        The ManagedInputObj should be given to transform instead of a real argument.
        Can only returned.
        """
        return managed.TempInputObj(name, *axes)
    def oobj(self, name, axes=()):
        """ Create a ManagedOutputObj object representing a managed argument.
        The ManagedOutputObj should be given to transform instead of a real argument.
        Can only be an argument.
        """
        return managed.TempOutputObj(name, *axes)
    def inst(self, axis):
        """ Create a ManagedInstance placeholder for job instances.
        If ManagedInstance is given to transform in place of an argument, when
        the function is called the ManagedInstance will be replaced with the chunk
        for the given axis.
        """
        return managed.Instance(axis)
    def ichunks(self, axes):
        """ Create a ManagedInputChunks placeholder for chunks of a split on an axis.
        If ManagedInputChunks is given to transform in place of an argument, when
        the function is called the ManagedInputChunks will be replaced with the chunk
        for the given axis of the given node.
        """
        return managed.InputChunks(*axes)
    def ochunks(self, axes):
        """ Create an ManagedOutputChunks placeholder for chunks of a split on an axis.
        If ManagedOutputChunks is given as the return value to be used for a call to 
        transform, the list returned by the function will be used as the list of chunks
        for the given axis.
        """
        return managed.OutputChunks(*axes)
    def input(self, filename, axes=()):
        """ Create a ManagedUserInputFile placeholder for an input file.
        If ManagedUserInputFile is given to transform in place of an argument, when
        the function is called the ManagedUserInputFile will be replaced with the input
        filename.
        """
        return managed.InputFile(filename, *axes)
    def output(self, filename, axes=()):
        """ Create a ManagedUserOutputFile placeholder for an output file.
        If ManagedUserOutputFile is given to transform in place of an argument, when
        the function is called the ManagedUserOutputFile will be replaced with a temp
        filename, and after successful completion of the associated job, the
        temp file will be moved to the given output filename.
        """
        return managed.OutputFile(filename, *axes)
    def template(self, template, axes=()):
        """ Create a ManagedTemplate placeholder for a name templated by the given axes.
        If ManagedTemplate is given to transform in place of an argument, when the transform
        function is called, the ManagedTemplate will be resolved to a complete name that will
        replace the argument.
        """
        return managed.Template(template, *axes)
    def commandline(self, name, axes, ctx, *args):
        """ Add a command line based transform to the pipeline """
        if name in self._abstract_jobs:
            raise ValueError('Job already defined')
        self.transform(name, axes, ctx, commandline.execute, None, *args)
    def transform(self, name, axes, ctx, func, ret, *args, **kwargs):
        """ Add a transform to the pipeline """
        if name in self._abstract_jobs:
            raise ValueError('Job already defined')
        self._abstract_jobs[name] = jobs.AbstractJob(name, axes, ctx, func, jobs.CallSet(ret, args, kwargs), self.logs_dir)
    def changeaxis(self, name, axes, var_name, old_axis, new_axis):
        """ Change the axis for a managed variable """
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
        """ Run the pipeline """
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
        """ Pretend run the pipeline """
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

