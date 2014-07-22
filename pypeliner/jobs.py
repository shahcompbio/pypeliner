import os
import sys
import itertools
import time
import traceback

import helpers
import arguments
import commandline
import managed
import nodes
import resources

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
            self.argset = callset.transform(lambda arg: managed.create_arg(resmgr, nodemgr, arg, node))
        except managed.JobArgMismatchException as e:
            e.job_name = name
            raise
        self.logs_dir = os.path.join(logs_dir, nodes.node_subdir(node), name)
    @property
    def id(self):
        return (self.name, self.node)
    @property
    def displayname(self):
        return nodes.name_node_displayname(self.name, self.node)
    @property
    def _inputs(self):
        for arg in self.argset.iteritems():
            if isinstance(arg, arguments.Arg):
                for input in arg.inputs:
                    yield input
        for node_input in self.nodemgr.get_node_inputs(self.node):
            yield node_input
    @property
    def _outputs(self):
        for arg in self.argset.iteritems():
            if isinstance(arg, arguments.Arg):
                for output in arg.outputs:
                    yield output
    @property
    def input_resources(self):
        return itertools.ifilter(lambda a: isinstance(a, resources.Resource), self._inputs)
    @property
    def output_resources(self):
        return itertools.ifilter(lambda a: isinstance(a, resources.Resource), self._outputs)
    @property
    def input_dependencies(self):
        return itertools.ifilter(lambda a: isinstance(a, resources.Dependency), self._inputs)
    @property
    def output_dependencies(self):
        return itertools.ifilter(lambda a: isinstance(a, resources.Dependency), self._outputs)
    @property
    def input_user_resources(self):
        return itertools.ifilter(lambda a: isinstance(a, resources.UserResource), self._inputs)
    @property
    def output_user_resources(self):
        return itertools.ifilter(lambda a: isinstance(a, resources.UserResource), self._outputs)
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
            if isinstance(arg, arguments.Arg):
                if arg.is_split:
                    return True
        return False
    def create_callable(self):
        resolved_args = self.argset.transform(lambda arg: arguments.resolve_arg(arg))
        return JobCallable(self.name, self.node, self.ctx, self.func, resolved_args, self.logs_dir)
    def finalize(self, callable):
        for arg, resolved in zip(self.argset.iteritems(), callable.argset.iteritems()):
            if isinstance(arg, arguments.Arg):
                arg.finalize(resolved)

class JobTimer(object):
    """ Timer using a context manager """
    def __init__(self):
        self._start = None
        self._finish = None
    def __enter__(self):
        self._start = time.time()
    def __exit__(self, exc_type, exc_value, traceback):
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
        return nodes.name_node_displayname(self.name, self.node)
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
            yield resources.Dependency(self.var_name, node)
        yield self.nodemgr.get_merge_input(self.old_axis, self.node)
        yield self.nodemgr.get_merge_input(self.new_axis, self.node)
        for node_input in self.nodemgr.get_node_inputs(self.node):
            yield node_input
    @property
    def _outputs(self):
        for node in self.nodemgr.retrieve_nodes((self.new_axis,), self.node):
            yield resources.Dependency(self.var_name, node)
    @property
    def out_of_date(self):
        return True
    @property
    def trigger_regenerate(self):
        return True
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

