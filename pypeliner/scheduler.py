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
from collections import *
from itertools import *

import helpers
import commandline


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
        return ManagedInputFile(name, axes)
    def ofile(self, name, axes=()):
        """ Create a ManagedOutputFile output file representing a managed argument.
        The ManagedOutputFile should be given to transform instead of a real argument
        """
        return ManagedOutputFile(name, axes)
    def tmpfile(self, name, axes=()):
        """ Create a ManagedTempFile representing a managed temporary filename.
        If ManagedTempFile is given to transform in place of an argument, when
        the function is called the ManagedTempFile will be replaced with the a
        valid temporary filename.
        """
        return ManagedTempFile(name, axes)
    def iobj(self, name, axes=()):
        """ Create a ManagedInputObj object representing a managed argument.
        The ManagedInputObj should be given to transform instead of a real argument.
        Can only returned.
        """
        return ManagedInputObj(name, axes)
    def oobj(self, name, axes=()):
        """ Create a ManagedOutputObj object representing a managed argument.
        The ManagedOutputObj should be given to transform instead of a real argument.
        Can only be an argument.
        """
        return ManagedOutputObj(name, axes)
    def inst(self, axis):
        """ Create a ManagedInstance placeholder for job instances.
        If ManagedInstance is given to transform in place of an argument, when
        the function is called the ManagedInstance will be replaced with the chunk
        for the given axis.
        """
        return ManagedInstance(axis)
    def ichunks(self, axes):
        """ Create a ManagedInputChunks placeholder for chunks of a split on an axis.
        If ManagedInputChunks is given to transform in place of an argument, when
        the function is called the ManagedInputChunks will be replaced with the chunk
        for the given axis of the given node.
        """
        return ManagedInputChunks(None, axes)
    def ochunks(self, axes):
        """ Create an ManagedOutputChunks placeholder for chunks of a split on an axis.
        If ManagedOutputChunks is given as the return value to be used for a call to 
        transform, the list returned by the function will be used as the list of chunks
        for the given axis.
        """
        return ManagedOutputChunks(None, axes)
    def input(self, filename, axes=()):
        """ Create a ManagedUserInputFile placeholder for an input file.
        If ManagedUserInputFile is given to transform in place of an argument, when
        the function is called the ManagedUserInputFile will be replaced with the input
        filename.
        """
        return ManagedUserInputFile(filename, axes)
    def output(self, filename, axes=()):
        """ Create a ManagedUserOutputFile placeholder for an output file.
        If ManagedUserOutputFile is given to transform in place of an argument, when
        the function is called the ManagedUserOutputFile will be replaced with a temp
        filename, and after successful completion of the associated job, the
        temp file will be moved to the given output filename.
        """
        return ManagedUserOutputFile(filename, axes)
    def template(self, template, axes):
        """ Create a ManagedTemplate placeholder for a name templated by the given axes.
        If ManagedTemplate is given to transform in place of an argument, when the transform
        function is called, the ManagedTemplate will be resolved to a complete name that will
        replace the argument.
        """
        return ManagedTemplate(template, axes)
    def commandline(self, name, axes, ctx, *args):
        """ Add a command line based transform to the pipeline """
        self.transform(name, axes, ctx, commandline.execute, None, *args)
    def transform(self, name, axes, ctx, func, ret, *args, **kwargs):
        """ Add a transform to the pipeline """
        if name in self._abstract_jobs:
            raise ValueError('Job already defined')
        self._abstract_jobs[name] = AbstractJob(name, axes, ctx, func, CallSet(ret, args, kwargs), self.logs_dir)
    def changeaxis(self, name, axes, var_name, old_axis, new_axis):
        """ Change the axis for a managed variable """
        if name in self._abstract_jobs:
            raise ValueError('Job already defined')
        self._abstract_jobs[name] = AbstractChangeAxis(name, axes, var_name, old_axis, new_axis)
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
        depgraph = DependencyGraph()
        with ResourceManager(self.temps_dir, self.db_dir) as resmgr, self.PipelineLock():
            nodemgr = NodeManager(self.nodes_dir, self.temps_dir)
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
        depgraph = DependencyGraph()
        with ResourceManager(self.temps_dir, self.db_dir) as resmgr, self.PipelineLock():
            nodemgr = NodeManager(self.nodes_dir)
            jobs = self._create_jobs(resmgr, nodemgr)
            self._depgraph_regenerate(resmgr, nodemgr, jobs, depgraph)
            while depgraph.jobs_ready:
                job_id = depgraph.next_job()
                job = jobs[job_id]
                if job.out_of_date or self.rerun or self.repopulate and job.output_missing:
                    self._logger.info(job.displayname + ' executing')
                else:
                    self._logger.info(job.displayname + ' skipped')
                self._logger.debug(job.displayname + ' explanation: ' + job.explain())
        return True

class JobArgMismatchException(Exception):
    def __init__(self, name, axes, node):
        self.name = name
        self.axes = axes
        self.node = node
        self.job_name = 'unknown'
    def __str__(self):
        return 'arg {0} with axes {1} does not match job {2} with axes {3}'.format(self.name, self.axes, self.job_name, tuple(a[0] for a in self.node))

class OutputMissingException(Exception):
    def __init__(self, filename):
        self.filename = filename
    def __str__(self):
        return 'expected output {0} missing'.format(self.filename)

class Managed(object):
    """ Interface class used to represent a managed data """
    def __init__(self, name, axes):
        if name is not None and type(name) != str:
            raise ValueError('name of argument must be string')
        if type(axes) != tuple:
            raise ValueError('axes must be a tuple')
        self.name = name
        self.axes = axes
    def _create_arg(self, resmgr, nodemgr, node, normal=None, splitmerge=None, **kwargs):
        common = 0
        for node_axis, axis in zip((a[0] for a in node), self.axes):
            if node_axis != axis:
                break
            common += 1
        axes_specific = self.axes[common:]
        if len(axes_specific) == 0 and normal is not None:
            return normal(resmgr, nodemgr, self.name, node[:common], **kwargs)
        elif len(axes_specific) == 1 and splitmerge is not None:
            return splitmerge(resmgr, nodemgr, self.name, node[:common], axes_specific[0], **kwargs)
        else:
            raise JobArgMismatchException(self.name, self.axes, node)

class ManagedTemplate(Managed):
    """ Represents a name templated by axes """
    def create_arg(self, resmgr, nodemgr, node):
        return self._create_arg(resmgr, nodemgr, node, normal=TemplateArg, splitmerge=MergeTemplateArg)

class ManagedTempFile(Managed):
    """ Represents a temp file the can be written to but is not a dependency """
    def create_arg(self, resmgr, nodemgr, node):
        return self._create_arg(resmgr, nodemgr, node, normal=TempFileArg)

class ManagedUserInputFile(Managed):
    """ Interface class used to represent a user specified managed input file """
    def create_arg(self, resmgr, nodemgr, node):
        return self._create_arg(resmgr, nodemgr, node, normal=InputFileArg, splitmerge=MergeFileArg)

class ManagedUserOutputFile(Managed):
    """ Interface class used to represent a user specified managed output file """
    def create_arg(self, resmgr, nodemgr, node):
        return self._create_arg(resmgr, nodemgr, node, normal=OutputFileArg, splitmerge=SplitFileArg)

class ManagedInputObj(Managed):
    """ Interface class used to represent a managed input object """
    def prop(self, prop_name):
        return ManagedInputObjProp(self.name, self.axes, prop_name)
    def create_arg(self, resmgr, nodemgr, node):
        return self._create_arg(resmgr, nodemgr, node, normal=TempInputObjArg, splitmerge=TempMergeObjArg)

class ManagedOutputObj(Managed):
    """ Interface class used to represent a managed output object """
    def create_arg(self, resmgr, nodemgr, node):
        return self._create_arg(resmgr, nodemgr, node, normal=TempOutputObjArg, splitmerge=TempSplitObjArg)

class ManagedInputObjProp(Managed):
    """ Interface class used to represent a property of a managed
    input object """
    def __init__(self, name, axes, prop_name):
        Managed.__init__(self, name, axes)
        self.prop_name = prop_name
    def create_arg(self, resmgr, nodemgr, node):
        return self._create_arg(resmgr, nodemgr, node, normal=TempInputObjArg, splitmerge=TempMergeObjArg, prop_name=self.prop_name)

class ManagedInputFile(Managed):
    """ Interface class used to represent a managed input file """
    def create_arg(self, resmgr, nodemgr, node):
        return self._create_arg(resmgr, nodemgr, node, normal=TempInputFileArg, splitmerge=TempMergeFileArg)

class ManagedOutputFile(Managed):
    """ Interface class used to represent a managed input file """
    def create_arg(self, resmgr, nodemgr, node):
        return self._create_arg(resmgr, nodemgr, node, normal=TempOutputFileArg, splitmerge=TempSplitFileArg)

class ManagedInstance(Managed):
    """ Interface class used to represent the instance of a job as
    an input parameter """
    def __init__(self, axis):
        self.axis = axis
    def create_arg(self, resmgr, nodemgr, node):
        return InputInstanceArg(resmgr, nodemgr, node, self.axis)

class ManagedInputChunks(Managed):
    """ Interface class used to represent the list of chunks
    from a split along a specific axis """
    def create_arg(self, resmgr, nodemgr, node):
        return self._create_arg(resmgr, nodemgr, node, normal=None, splitmerge=InputChunksArg)

class ManagedOutputChunks(Managed):
    """ Interface class used to represent the list of chunks
    from a split along a specific axis """
    def create_arg(self, resmgr, nodemgr, node):
        return self._create_arg(resmgr, nodemgr, node, normal=None, splitmerge=OutputChunksArg)

class Dependency(object):
    """ An input/output in the dependency graph """
    def __init__(self, axis, node):
        self.axis = axis
        self.node = node
    @property
    def id(self):
        return (self.axis, self.node)

class Resource(Dependency):
    """ Abstract input/output in the dependency graph
    associated with a file tracked using creation time """
    @property
    def exists(self):
        raise NotImplementedError
    @property
    def createtime(self):
        raise NotImplementedError

class UserResource(Resource):
    """ A file resource with filename and creation time if created """
    def __init__(self, filename):
        self.filename = filename
    @property
    def exists(self):
        return os.path.exists(self.filename)
    @property
    def createtime(self):
        if os.path.exists(self.filename):
            return os.path.getmtime(self.filename)
        return None
    @property
    def id(self):
        return (self.filename, ())

class UserFilenameCreator(object):
    """ Function object for creating user filenames from name node pairs """
    def __init__(self, suffix=''):
        self.suffix = suffix
    def __call__(self, name, node):
        return name.format(**dict(node)) + self.suffix
    def __repr__(self):
        return '{0}.{1}({2})'.format(FilenameCreator.__module__, FilenameCreator.__name__, self.suffix)

user_filename_creator = UserFilenameCreator()

class Arg(object):
    @property
    def inputs(self):
        return []
    @property
    def outputs(self):
        return []
    @property
    def is_split(self):
        return False
    def resolve(self):
        return None
    def finalize(self, resolved):
        pass

class TemplateArg(Arg):
    """ Templated name argument """
    def __init__(self, resmgr, nodemgr, name, node):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.name = name
        self.node = node
        self.filename = user_filename_creator(name, node)
    def resolve(self):
        return self.filename

class TempFileArg(Arg):
    """ Temporary file argument """
    def __init__(self, resmgr, nodemgr, name, node):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.name = name
        self.node = node
    def resolve(self):
        return self.resmgr.get_output(self.name, self.node).final_filename

class MergeTemplateArg(Arg):
    """ Temp input files merged along a single axis """
    def __init__(self, resmgr, nodemgr, name, base_node, merge_axis):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.name = name
        self.base_node = base_node
        self.merge_axis = merge_axis
    @property
    def resolve(self):
        resolved = dict()
        for node in self.nodemgr.retrieve_nodes((self.merge_axis,), self.base_node):
            resource = UserResource(user_filename_creator(self.name, node))
            resolved[node[-1][1]] = resource.filename
        return resolved

class InputFileArg(Arg):
    """ Input file argument """
    def __init__(self, resmgr, nodemgr, name, node):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.name = name
        self.node = node
        self.filename = user_filename_creator(name, node)
    @property
    def inputs(self):
        yield UserResource(self.filename)
    def resolve(self):
        return self.filename

class MergeFileArg(Arg):
    """ Temp input files merged along a single axis """
    def __init__(self, resmgr, nodemgr, name, base_node, merge_axis):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.name = name
        self.base_node = base_node
        self.merge_axis = merge_axis
    @property
    def inputs(self):
        for node in self.nodemgr.retrieve_nodes((self.merge_axis,), self.base_node):
            yield UserResource(user_filename_creator(self.name, node))
        yield self.nodemgr.get_input(self.merge_axis, self.base_node)
    def resolve(self):
        resolved = dict()
        for node in self.nodemgr.retrieve_nodes((self.merge_axis,), self.base_node):
            resource = UserResource(user_filename_creator(self.name, node))
            resolved[node[-1][1]] = resource.filename
        return resolved

class OutputFileArg(Arg):
    """ Input file argument """
    def __init__(self, resmgr, nodemgr, name, node):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.name = name
        self.node = node
        self.filename = user_filename_creator(name, node)
        self.temp_filename = self.filename + '.tmp'
    @property
    def outputs(self):
        yield UserResource(self.filename)
    def resolve(self):
        return self.temp_filename
    def finalize(self, resolved):
        try:
            os.rename(self.temp_filename, self.filename)
        except OSError:
            raise OutputMissingException(self.temp_filename)

class SplitFileArg(Arg):
    """ Output file arguments from a split """
    def __init__(self, resmgr, nodemgr, name, base_node, split_axis):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.name = name
        self.base_node = base_node
        self.split_axis = split_axis
    @property
    def outputs(self):
        for node in self.nodemgr.retrieve_nodes((self.split_axis,), self.base_node):
            yield UserResource(user_filename_creator(self.name, node))
        yield self.nodemgr.get_output(self.split_axis, self.base_node)
    @property
    def is_split(self):
        return True
    def resolve(self):
        return FilenameCallback(self.name, self.base_node, self.split_axis, UserFilenameCreator('.tmp'))
    def finalize(self, resolved):
        self.nodemgr.store_chunks(self.split_axis, self.base_node, resolved.chunks)
        for chunk in resolved.chunks:
            node = self.base_node + ((self.split_axis, chunk),)
            filename = user_filename_creator(self.name, node)
            temp_filename = filename + '.tmp'
            try:
                os.rename(temp_filename, filename)
            except OSError:
                raise OutputMissingException(temp_filename)

class TempInputObjArg(Arg):
    """ Temp input object argument """
    def __init__(self, resmgr, nodemgr, name, node, prop_name=None):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.name = name
        self.node = node
        self.prop_name = prop_name
    @property
    def inputs(self):
        yield self.resmgr.get_input(self.name, self.node)
    def resolve(self):
        resource = self.resmgr.get_input(self.name, self.node)
        with open(resource.filename, 'rb') as f:
            if self.prop_name is None:
                return pickle.load(f)
            else:
                return getattr(pickle.load(f), self.prop_name)

class TempMergeObjArg(Arg):
    """ Temp input object arguments merged along single axis """
    def __init__(self, resmgr, nodemgr, name, node, merge_axis, prop_name=None):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.name = name
        self.base_node = node
        self.merge_axis = merge_axis
        self.prop_name = prop_name
    @property
    def inputs(self):
        for node in self.nodemgr.retrieve_nodes((self.merge_axis,), self.base_node):
            yield self.resmgr.get_input(self.name, node)
        yield self.nodemgr.get_input(self.merge_axis, self.base_node)
    def resolve(self):
        resolved = dict()
        for node in self.nodemgr.retrieve_nodes((self.merge_axis,), self.base_node):
            resource = self.resmgr.get_input(self.name, node)
            with open(resource.filename, 'rb') as f:
                if self.prop_name is None:
                    resolved[node[-1][1]] = pickle.load(f)
                else:
                    resolved[node[-1][1]] = getattr(pickle.load(f), self.prop_name)
        return resolved

class TempOutputObjArg(Arg):
    """ Temp output object argument """
    def __init__(self, resmgr, nodemgr, name, node):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.name = name
        self.node = node
    @property
    def outputs(self):
        yield self.resmgr.get_output(self.name, self.node)
    def finalize(self, resolved):
        resource = self.resmgr.get_output(self.name, self.node)
        with open(resource.filename, 'wb') as f:
            pickle.dump(resolved, f)
        self.resmgr.finalize_output(resource)

class TempSplitObjArg(Arg):
    """ Temp output object arguments from a split """
    def __init__(self, resmgr, nodemgr, name, base_node, split_axis):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.name = name
        self.base_node = base_node
        self.split_axis = split_axis
    @property
    def outputs(self):
        for node in self.nodemgr.retrieve_nodes((self.split_axis,), self.base_node):
            yield self.resmgr.get_output(self.name, node)
        yield self.nodemgr.get_output(self.split_axis, self.base_node)
    @property
    def is_split(self):
        return True
    def finalize(self, resolved):
        self.nodemgr.store_chunks(self.split_axis, self.base_node, resolved.keys())
        for chunk, object in resolved.iteritems():
            node = self.base_node + ((self.split_axis, chunk),)
            resource = self.resmgr.get_output(self.name, node)
            with open(resource.filename, 'wb') as f:
                pickle.dump(object, f)
            self.resmgr.finalize_output(resource)

class TempInputFileArg(Arg):
    """ Temp input file argument """
    def __init__(self, resmgr, nodemgr, name, node):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.name = name
        self.node = node
    @property
    def inputs(self):
        yield self.resmgr.get_input(self.name, self.node)
    def resolve(self):
        resource = self.resmgr.get_input(self.name, self.node)
        return resource.filename

class TempMergeFileArg(Arg):
    """ Temp input files merged along a single axis """
    def __init__(self, resmgr, nodemgr, name, base_node, merge_axis):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.name = name
        self.base_node = base_node
        self.merge_axis = merge_axis
    @property
    def inputs(self):
        for node in self.nodemgr.retrieve_nodes((self.merge_axis,), self.base_node):
            yield self.resmgr.get_input(self.name, node)
        yield self.nodemgr.get_input(self.merge_axis, self.base_node)
    def resolve(self):
        resolved = dict()
        for node in self.nodemgr.retrieve_nodes((self.merge_axis,), self.base_node):
            resource = self.resmgr.get_input(self.name, node)
            resolved[node[-1][1]] = resource.filename
        return resolved

class TempOutputFileArg(Arg):
    """ Temp output file argument """
    def __init__(self, resmgr, nodemgr, name, node):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.name = name
        self.node = node
    @property
    def outputs(self):
        yield self.resmgr.get_output(self.name, self.node)
    def resolve(self):
        return self.resmgr.get_output(self.name, self.node).filename
    def finalize(self, resolved):
        resource = self.resmgr.get_output(self.name, self.node)
        self.resmgr.finalize_output(resource)

class FilenameCallback(object):
    """ Argument to split jobs providing callback for filenames
    with a particular instance """
    def __init__(self, name, base_node, split_axis, filename_creator):
        self.name = name
        self.base_node = base_node
        self.split_axis = split_axis
        self.filename_creator = filename_creator
        self.chunks = set()
    def __call__(self, chunk):
        self.chunks.add(chunk)
        filename = self.filename_creator(self.name, self.base_node + ((self.split_axis, chunk),))
        helpers.makedirs(os.path.dirname(filename))
        return filename
    def __repr__(self):
        return '{0}.{1}({2})'.format(FilenameCallback.__module__, FilenameCallback.__name__, ', '.join(repr(a) for a in (self.name, self.base_node, self.split_axis, self.filename_creator)))

class TempSplitFileArg(Arg):
    """ Temp output file arguments from a split """
    def __init__(self, resmgr, nodemgr, name, base_node, split_axis):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.name = name
        self.base_node = base_node
        self.split_axis = split_axis
    @property
    def outputs(self):
        for node in self.nodemgr.retrieve_nodes((self.split_axis,), self.base_node):
            yield self.resmgr.get_output(self.name, node)
        yield self.nodemgr.get_output(self.split_axis, self.base_node)
    @property
    def is_split(self):
        return True
    def resolve(self):
        return FilenameCallback(self.name, self.base_node, self.split_axis, self.resmgr.filename_creator)
    def finalize(self, resolved):
        self.nodemgr.store_chunks(self.split_axis, self.base_node, resolved.chunks)
        for chunk in resolved.chunks:
            resource = self.resmgr.get_output(self.name, self.base_node + ((self.split_axis, chunk),))
            self.resmgr.finalize_output(resource)

class InputInstanceArg(Arg):
    """ Instance of a job as an argument """
    def __init__(self, resmgr, nodemgr, node, axis):
        self.chunk = dict(node)[axis]
    def resolve(self):
        return self.chunk
    def finalize(self, resolved):
        pass

class InputChunksArg(Arg):
    """ Instance list of an axis as an argument """
    def __init__(self, resmgr, nodemgr, name, node, axis):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.node = node
        self.axis = axis
    @property
    def inputs(self):
        yield self.nodemgr.get_input(self.axis, self.node)
    def resolve(self):
        return self.nodemgr.retrieve_chunks(self.axis, self.node)
    def finalize(self, resolved):
        pass

class OutputChunksArg(Arg):
    """ Instance list of a job as an argument """
    def __init__(self, resmgr, nodemgr, name, node, axis):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.node = node
        self.axis = axis
    @property
    def outputs(self):
        yield self.nodemgr.get_output(self.axis, self.node)
    @property
    def is_split(self):
        return True
    def finalize(self, resolved):
        self.nodemgr.store_chunks(self.axis, self.node, resolved)

class CallSet(object):
    """ Set of positional and keyword arguments, and a return value """
    def __init__(self, ret, args, kwargs):
        self.ret = ret
        self.args = args
        self.kwargs = kwargs
    def transform(self, func):
        ret = func(self.ret)
        args = list()
        for arg in self.args:
            args.append(func(arg))
        kwargs = dict()
        for key, arg in sorted(self.kwargs.iteritems()):
            kwargs[key] = func(arg)
        return CallSet(ret, args, kwargs)
    def iteritems(self):
        yield self.ret
        for arg in self.args:
            yield arg
        for key, arg in sorted(self.kwargs.iteritems()):
            yield arg

class ChunksResource(Resource):
    """ A resource representing a list of chunks for an axis """
    def __init__(self, nodemgr, axis, node):
        self.nodemgr = nodemgr
        self.axis = axis
        self.node = node
    @property
    def id(self):
        return (self.axis, self.node)
    @property
    def exists(self):
        return os.path.exists(self.nodemgr.get_chunks_filename(self.axis, self.node))
    @property
    def createtime(self):
        if self.exists:
            return os.path.getmtime(self.nodemgr.get_chunks_filename(self.axis, self.node))

def node_subdir(node):
    if len(node) == 0:
        return ''
    return os.path.join(*([os.path.join(*(str(axis), str(chunk))) for axis, chunk in node]))

def name_node_filename(name, node):
    assert not os.path.isabs(name)
    return os.path.join(node_subdir(node), name)

def name_node_displayname(name, node):
    return name + '<' + ','.join(':'.join((str(axis), str(chunk))) for axis, chunk in node) + '>'

class NodeManager(object):
    """ Manages nodes in the underlying pipeline graph """
    def __init__(self, nodes_dir, temps_dir):
        self.nodes_dir = nodes_dir
        self.temps_dir = temps_dir
        self.cached_chunks = dict()
    def retrieve_nodes(self, axes, base_node=()):
        if len(axes) == 0:
            yield base_node
        else:
            for chunk in self.retrieve_chunks(axes[0], base_node):
                for node in self.retrieve_nodes(axes[1:], base_node + ((axes[0], chunk),)):
                    yield node
    def get_chunks_filename(self, axis, node):
        return os.path.join(self.nodes_dir, node_subdir(node), axis+'_chunks')
    def retrieve_chunks(self, axis, node):
        if (axis, node) not in self.cached_chunks:
            chunks_filename = self.get_chunks_filename(axis, node)
            if not os.path.exists(chunks_filename):
                return (None,)
            else:
                with open(chunks_filename, 'rb') as f:
                    self.cached_chunks[(axis, node)] = pickle.load(f)
        return self.cached_chunks[(axis, node)]
    def store_chunks(self, axis, node, chunks):
        for chunk in chunks:
            helpers.makedirs(os.path.join(self.temps_dir, node_subdir(node + ((axis, chunk),))))
        chunks = sorted(chunks)
        self.cached_chunks[(axis, node)] = chunks
        chunks_filename = self.get_chunks_filename(axis, node)
        helpers.makedirs(os.path.dirname(chunks_filename))
        temp_chunks_filename = chunks_filename + '.tmp'
        with open(temp_chunks_filename, 'wb') as f:
            pickle.dump(chunks, f)
        helpers.overwrite_if_different(temp_chunks_filename, chunks_filename)
    def get_input(self, axis, node):
        return ChunksResource(self, axis, node)
    def get_output(self, axis, node):
        return Dependency(axis, node)
    def get_node_inputs(self, node):
        if len(node) >= 1:
            yield Dependency(node[-1][0], node[:-1])

class TempResource(Resource):
    """ A file resource with filename and creation time if created """
    def __init__(self, resmgr, name, node, temp_filename, final_filename):
        self.resmgr = resmgr
        self.name = name
        self.node = node
        self.temp_filename = temp_filename
        self.final_filename = final_filename
    @property
    def filename(self):
        return self.temp_filename
    @property
    def exists(self):
        return os.path.exists(self.final_filename)
    @property
    def createtime(self):
        return self.resmgr.retrieve_createtime(self.name, self.node)
    @property
    def id(self):
        return (self.name, self.node)

class FilenameCreator(object):
    """ Function object for creating filenames from name node pairs """
    def __init__(self, file_dir='', file_suffix=''):
        self.file_dir = file_dir
        self.file_suffix = file_suffix
    def __call__(self, name, node):
        return os.path.join(self.file_dir, name_node_filename(name, node) + self.file_suffix)
    def __repr__(self):
        return '{0}.{1}({2})'.format(FilenameCreator.__module__, FilenameCreator.__name__, ', '.join(repr(a) for a in (self.file_dir, self.file_suffix)))

class ResourceManager(object):
    """ Manages file resources """
    def __init__(self, temps_dir, db_dir):
        self.temps_dir = temps_dir
        self.db_dir = db_dir
        self.temps_suffix = '.tmp'
        self.aliases = dict()
        self.rev_alias = defaultdict(list)
    def __enter__(self):
        self.createtimes_shelf = shelve.open(os.path.join(self.db_dir, 'createtimes'))
        return self
    def __exit__(self, type, value, traceback):
        self.createtimes_shelf.close()
    def get_input(self, name, node):
        final_filename = self.get_final_filename(name, node)
        return TempResource(self, name, node, final_filename, final_filename)
    def get_output(self, name, node):
        temp_filename = self.get_temp_filename(name, node)
        final_filename = self.get_final_filename(name, node)
        return TempResource(self, name, node, temp_filename, final_filename)
    def get_empty_output(self, name, node):
        return TempResource(self, name, node, None, None)
    def finalize_output(self, resource):
        final_filename = self.get_final_filename(resource.name, resource.node)
        try:
            os.rename(resource.filename, final_filename)
        except OSError:
            raise OutputMissingException(resource.filename)
        self.update_createtime(resource.name, resource.node, os.path.getmtime(final_filename))
    @property
    def filename_creator(self):
        return FilenameCreator(self.temps_dir, self.temps_suffix)
    def retrieve_createtime(self, name, node):
        final_filename = self.get_final_filename(name, node)
        if os.path.exists(final_filename):
            self.createtimes_shelf[str((name, node))] = os.path.getmtime(final_filename)
        return self.createtimes_shelf.get(str((name, node)), None)
    def update_createtime(self, name, node, createtime):
        self.createtimes_shelf[str((name, node))] = createtime
    def get_temp_filename(self, name, node):
        return self.get_final_filename(name, node) + self.temps_suffix
    def get_final_filename(self, name, node):
        if (name, node) in self.aliases:
            return self.get_final_filename(*self.aliases[(name, node)])
        else:
            return os.path.join(self.temps_dir, name_node_filename(name, node))
    def add_alias(self, name, node, alias_name, alias_node):
        self.aliases[(alias_name, alias_node)] = (name, node)
        self.rev_alias[(name, node)].append((alias_name, alias_node))
    def get_aliases(self, name, node):
        for alias_name, alias_node in self.rev_alias[(name, node)]:
            yield (alias_name, alias_node)
            for alias_name_recurse, alias_node_recurse in self.get_aliases(alias_name, alias_node):
                yield (alias_name_recurse, alias_node_recurse)
    def is_temp_file(self, name, node):
        return str((name, node)) in self.createtimes_shelf
    def cleanup(self, depgraph):
        to_remove = set()
        for name, node in depgraph.obsolete:
            if (name, node) in self.aliases:
                continue
            if not self.is_temp_file(name, node):
                continue
            alias_ids = set([(name, node)] + list(self.get_aliases(name, node)))
            if alias_ids.issubset(depgraph.obsolete):
                filename = self.get_final_filename(name, node)
                if os.path.exists(filename):
                    os.remove(filename)
                to_remove.update(alias_ids)
        for name, node in to_remove:
            depgraph.obsolete.remove((name, node))

class AbstractJob(object):
    """ Represents an abstract job including function and arguments """
    def __init__(self, name, axes, ctx, func, callset, logs_dir):
        self.name = name
        self.axes = axes
        self.ctx = ctx
        self.func = func
        self.callset = callset
        self.logs_dir = logs_dir
    def create_jobs(self, resmgr, nodemgr):
        for node in nodemgr.retrieve_nodes(self.axes):
            yield Job(resmgr, nodemgr, self.name, node, self.ctx, self.func, self.callset, self.logs_dir)

def create_arg(resmgr, nodemgr, managed, node):
    """ Translate a managed user argument into an internally used Arg object """
    if isinstance(managed, Managed):
        return managed.create_arg(resmgr, nodemgr, node)
    else:
        return managed

def resolve_arg(arg):
    """ Resolve an Arg object into a concrete argument """
    if isinstance(arg, Arg):
        return arg.resolve()
    else:
        return arg

class InputMissingException(Exception):
    def __init__(self, input, job):
        self.input = input
        self.job = job
    def __str__(self):
        return 'input {0} missing for job {1}'.format(self.input, self.job)

class Job(object):
    """ Represents a job including function and arguments """
    def __init__(self, resmgr, nodemgr, name, node, ctx, func, callset, logs_dir):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.name = name
        self.node = node
        self.ctx = ctx
        self.func = func
        try:
            self.argset = callset.transform(lambda arg: create_arg(resmgr, nodemgr, arg, node))
        except JobArgMismatchException as e:
            e.job_name = name
            raise
        self.logs_dir = os.path.join(logs_dir, node_subdir(node), name)
    @property
    def id(self):
        return (self.name, self.node)
    @property
    def displayname(self):
        return name_node_displayname(self.name, self.node)
    @property
    def _inputs(self):
        for arg in self.argset.iteritems():
            if isinstance(arg, Arg):
                for input in arg.inputs:
                    yield input
        for node_input in self.nodemgr.get_node_inputs(self.node):
            yield node_input
    @property
    def _outputs(self):
        for arg in self.argset.iteritems():
            if isinstance(arg, Arg):
                for output in arg.outputs:
                    yield output
    @property
    def input_resources(self):
        return ifilter(lambda a: isinstance(a, Resource), self._inputs)
    @property
    def output_resources(self):
        return ifilter(lambda a: isinstance(a, Resource), self._outputs)
    @property
    def input_dependencies(self):
        return ifilter(lambda a: isinstance(a, Dependency), self._inputs)
    @property
    def output_dependencies(self):
        return ifilter(lambda a: isinstance(a, Dependency), self._outputs)
    @property
    def input_user_resources(self):
        return ifilter(lambda a: isinstance(a, UserResource), self._inputs)
    @property
    def output_user_resources(self):
        return ifilter(lambda a: isinstance(a, UserResource), self._outputs)
    @property
    def pipeline_inputs(self):
        return (dep.id for dep in self.input_user_resources)
    @property
    def pipeline_outputs(self):
        return (dep.id for dep in self.output_user_resources)
    @property
    def inputs(self):
        return (dep.id for dep in self.input_dependencies)
    @property
    def outputs(self):
        return (dep.id for dep in self.output_dependencies)
    def check_inputs(self):
        for input in self.input_resources:
            if input.createtime is None:
                raise InputMissingException(input.id, self.id)
    @property
    def out_of_date(self):
        self.check_inputs()
        input_dates = [input.createtime for input in self.input_resources]
        output_dates = [output.createtime for output in self.output_resources]
        if len(input_dates) == 0 or len(output_dates) == 0:
            return True
        if None in output_dates:
            return True
        return max(input_dates) > min(output_dates)
    def explain(self):
        self.check_inputs()
        input_dates = [input.createtime for input in self.input_resources]
        output_dates = [output.createtime for output in self.output_resources]
        try:
            newest_input_date = max(input_dates)
        except ValueError:
            newest_input_date = None
        try:
            oldest_output_date = min(output_dates)
        except ValueError:
            oldest_output_date = None
        if len(input_dates) == 0 and len(output_dates) == 0:
            explanation = ['no inputs/outputs: always run']
        elif len(input_dates) == 0:
            explanation = ['no inputs: always run']
        elif len(output_dates) == 0:
            explanation = ['no outputs: always run']
        elif None in output_dates:
            explanation = ['missing outputs']
        elif newest_input_date > oldest_output_date:
            explanation = ['out of date outputs']
        else:
            explanation = ['up to date']
        for input in self.input_resources:
            status = ''
            if oldest_output_date is not None and input.createtime > oldest_output_date:
                status = 'new'
            explanation.append('input {0} {1} {2}'.format(input.id, input.createtime, status))
        for output in self.output_resources:
            status = ''
            if output.createtime is None:
                status = 'missing'
            elif newest_input_date is not None and output.createtime < newest_input_date:
                status = '{0} old'.format(output.createtime)
            else:
                status = '{0}'.format(output.createtime)
            explanation.append('output {0} {1}'.format(output.id, status))
        return '\n'.join(explanation)
    @property
    def output_missing(self):
        return not all([output.exists for output in self.output_resources])
    @property
    def trigger_regenerate(self):
        for arg in self.argset.iteritems():
            if isinstance(arg, Arg):
                if arg.is_split:
                    return True
        return False
    def create_callable(self):
        resolved_args = self.argset.transform(lambda arg: resolve_arg(arg))
        return JobCallable(self.name, self.node, self.ctx, self.func, resolved_args, self.logs_dir)
    def finalize(self, callable):
        for arg, resolved in zip(self.argset.iteritems(), callable.argset.iteritems()):
            if isinstance(arg, Arg):
                arg.finalize(resolved)

class JobTimer(object):
    """ Timer using a context manager """
    def __init__(self):
        self._start = None
        self._finish = None
    def __enter__(self):
        self._start = time.time()
    def __exit__(self, type, value, traceback):
        self._finish = time.time()
    @property
    def duration(self):
        if self._finish is None or self._start is None:
            return None
        return int(self._finish - self._start)

class JobCallable(object):
    """ Callable function and args to be given to exec queue """
    def __init__(self, name, node, ctx, func, argset, logs_dir):
        self.name = name
        self.node = node
        self.ctx = ctx
        self.func = func
        self.argset = argset
        self.finished = False
        helpers.makedirs(logs_dir)
        self.stdout_filename = os.path.join(logs_dir, 'job.out')
        self.stderr_filename = os.path.join(logs_dir, 'job.err')
        self.exc_dir = os.path.join(logs_dir, 'exc')
        helpers.makedirs(self.exc_dir)
        self.job_timer = JobTimer()
    @property
    def id(self):
        return (self.name, self.node)
    @property
    def temps_dir(self):
        return self.exc_dir
    @property
    def displayname(self):
        return name_node_displayname(self.name, self.node)
    @property
    def displaycommand(self):
        if self.func == commandline.execute:
            return '"' + ' '.join(str(arg) for arg in self.argset.args) + '"'
        else:
            return self.func.__name__ + '(' + ', '.join(repr(arg) for arg in self.argset.args) + ', ' + ', '.join(key+'='+repr(arg) for key, arg in self.argset.kwargs.iteritems()) + ')'
    @property
    def duration(self):
        return self.job_timer.duration
    def log_text(self):
        text = '--- stdout ---\n'
        with open(self.stdout_filename, 'r') as job_stdout:
            text += job_stdout.read()
        text += '--- stderr ---\n'
        with open(self.stderr_filename, 'r') as job_stderr:
            text += job_stderr.read()
        return text
    def __call__(self):
        with open(self.stdout_filename, 'w') as stdout_file, open(self.stderr_filename, 'w') as stderr_file:
            old_stdout, old_stderr = sys.stdout, sys.stderr
            sys.stdout, sys.stderr = stdout_file, stderr_file
            try:
                with self.job_timer:
                    self.argset.ret = self.func(*self.argset.args, **self.argset.kwargs)
                self.finished = True
            except:
                sys.stderr.write(traceback.format_exc())
            finally:
                sys.stdout, sys.stderr = old_stdout, old_stderr

class AbstractChangeAxis(object):
    """ Represents an abstract aliasing """
    def __init__(self, name, axes, var_name, old_axis, new_axis):
        self.name = name
        self.axes = axes
        self.var_name = var_name
        self.old_axis = old_axis
        self.new_axis = new_axis
    def create_jobs(self, resmgr, nodemgr):
        for node in nodemgr.retrieve_nodes(self.axes):
            yield ChangeAxis(resmgr, nodemgr, self.name, node, self.var_name, self.old_axis, self.new_axis)

class ChangeAxisException(Exception):
    def __init__(self, old, new):
        self.old = old
        self.new = new
    def __str__(self):
        return 'axis change from {0} to {1}'.format(self.old, self.new)

class ChangeAxis(Job):
    """ Creates an alias of a managed object """
    def __init__(self, resmgr, nodemgr, name, node, var_name, old_axis, new_axis):
        self.resmgr = resmgr
        self.nodemgr = nodemgr
        self.name = name
        self.node = node
        self.var_name = var_name
        self.old_axis = old_axis
        self.new_axis = new_axis
    @property
    def _inputs(self):
        for node in self.nodemgr.retrieve_nodes((self.old_axis,), self.node):
            yield self.resmgr.get_input(self.var_name, node)
        yield self.nodemgr.get_input(self.old_axis, self.node)
        yield self.nodemgr.get_input(self.new_axis, self.node)
        for node_input in self.nodemgr.get_node_inputs(self.node):
            yield node_input
    @property
    def _outputs(self):
        for node in self.nodemgr.retrieve_nodes((self.new_axis,), self.node):
            yield Dependency(self.var_name, node)
    @property
    def out_of_date(self):
        return True
    @property
    def trigger_regenerate(self):
        return False
    def create_callable(self):
        return None
    def finalize(self):
        old_chunks = set(self.nodemgr.retrieve_chunks(self.old_axis, self.node))
        new_chunks = set(self.nodemgr.retrieve_chunks(self.new_axis, self.node))
        if (old_chunks != new_chunks):
            raise ChangeAxisException(old_chunks, new_chunks)
        for chunk in old_chunks:
            old_node = self.node + ((self.old_axis, chunk),)
            new_node = self.node + ((self.new_axis, chunk),)
            self.resmgr.add_alias(self.var_name, old_node, self.var_name, new_node)

class AmbiguousInputException(Exception):
    def __init__(self, id):
        self.id = id
    def __str__(self):
        return 'input {0} not created by any job'.format(self.id)

class AmbiguousOutputException(Exception):
    def __init__(self, output, job1, job2):
        self.output = output
        self.job1 = job1
        self.job2 = job2
    def __str__(self):
        return 'output {0} created by jobs {1} and {2}'.format(self.output, self.job1, self.job2)

class DependencyCycleException(Exception):
    def __init__(self, id):
        self.id = id
    def __str__(self):
        return 'dependency cycle involving output {0}'.format(self.id)

class DependencyGraph:
    """ Graph of dependencies between jobs """
    def __init__(self):
        self.completed = set()
        self.created = set()
        self.ready = set()
        self.running = set()
        self.obsolete = set()
    def regenerate(self, inputs, outputs, jobs):
        self.inputs = inputs
        self.outputs = outputs
        self.created.update(inputs)
        self.forward = defaultdict(set)
        backward = dict()
        for job in jobs:
            for output in job.outputs:
                if output in backward:
                    raise AmbiguousOutputException(output, job.id, backward[output].id)
                backward[output] = job
        self.forward = defaultdict(set)
        tovisit = list(self.outputs)
        paths = list(set() for a in self.outputs)
        visited = set()
        required = set()
        generative = set()
        while len(tovisit) > 0:
            visit = tovisit.pop()
            path = paths.pop()
            if visit in visited:
                continue
            visited.add(visit)
            if visit in self.created:
                required.add(visit)
                continue
            elif visit not in backward:
                raise AmbiguousInputException(visit)
            if len(list(backward[visit].inputs)) == 0:
                generative.add(backward[visit])
                continue
            for input in backward[visit].inputs:
                if backward[visit].id in path:
                    raise DependencyCycleException(visit)
                self.forward[input].add(backward[visit])
                tovisit.append(input)
                paths.append(path.union(set([backward[visit].id])))
        self._notify_created(required)
        for job in generative:
            if job.id not in self.running and job.id not in self.completed:
                self.ready.add(job.id)
    def next_job(self):
        job_id = self.ready.pop()
        self.running.add(job_id)
        return job_id
    def notify_completed(self, job):
        self.running.remove(job.id)
        self.completed.add(job.id)
        for input in job.inputs:
            if all([otherjob.id in self.completed for otherjob in self.forward[input]]):
                self.obsolete.add(input)
        for output in job.outputs:
            if output not in self.forward:
                self.obsolete.add(output)
        self._notify_created([output for output in job.outputs])
    def _notify_created(self, outputs):
        self.created.update(outputs)
        for output in outputs:
            if output not in self.forward:
                continue
            for job in self.forward[output]:
                if all([input in self.created for input in job.inputs]) and job.id not in self.running and job.id not in self.completed:
                    self.ready.add(job.id)
    @property
    def jobs_ready(self):
        return len(self.ready) > 0
    @property
    def finished(self):
        return all([output in self.created for output in self.outputs])
