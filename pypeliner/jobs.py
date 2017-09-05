import copy
import os
import sys
import itertools
import time
import traceback
import socket
import datetime

import pypeliner.helpers
import pypeliner.arguments
import pypeliner.commandline
import pypeliner.managed
import pypeliner.resources
import pypeliner.identifiers
import pypeliner.deep


class CallSet(object):
    """ Set of positional and keyword arguments, and a return value """
    def __init__(self, ret=None, args=None, kwargs=None):
        if ret is not None and not isinstance(ret, pypeliner.managed.Managed):
            raise ValueError('ret must be a managed object')
        self.ret = ret
        if args is None:
            self.args = ()
        elif not isinstance(args, (tuple, list)):
            raise ValueError('args must be a list or tuple')
        else:
            self.args = args
        if kwargs is None:
            self.kwargs = {}
        elif not isinstance(kwargs, dict):
            raise ValueError('kwargs must be a dict')
        else:
            self.kwargs = kwargs

class JobDefinition(object):
    """ Represents an abstract job including function and arguments """
    def __init__(self, name, axes, ctx, func, argset):
        self.name = name
        self.axes = axes
        self.ctx = ctx
        self.func = func
        self.argset = argset
    def create_job_instances(self, workflow, db):
        for node in db.nodemgr.retrieve_nodes(self.axes):
            yield JobInstance(self, workflow, db, node)

class InputMissingException(Exception):
    def __init__(self, input, filename, job):
        self.input = input
        self.filename = filename
        self.job = job
    def __str__(self):
        return 'input {0}, filename {1}, missing for job {2}'.format(self.input, self.filename, self.job)

def _pretty_date(ts):
    if ts is None:
        return 'none'
    return datetime.datetime.fromtimestamp(ts).strftime('%Y/%m/%d-%H:%M:%S')

def transform_managed(mg, job):
    if not isinstance(mg, pypeliner.managed.Managed):
        return None, False
    return mg.create_arg(job), True

class JobInstance(object):
    """ Represents a job including function and arguments """
    direct_write = False
    def __init__(self, job_def, workflow, db, node):
        self.job_def = job_def
        self.workflow = workflow
        self.db = db
        self.node = node
        self.args = list()
        try:
            self.argset = pypeliner.deep.deeptransform(
                self.job_def.argset,
                lambda a: transform_managed(a, self))
        except pypeliner.managed.JobArgMismatchException as e:
            e.job_name = self.displayname
            raise
        self.logs_dir = os.path.join(db.logs_dir, self.node.subdir, self.job_def.name)
        pypeliner.helpers.makedirs(self.logs_dir)
        self.retry_idx = 0
        self.ctx = job_def.ctx.copy()
        self.is_required_downstream = False
        self.init_inputs_outputs()
    def init_inputs_outputs(self):
        self.inputs = list()
        for arg in self.args:
            if isinstance(arg, pypeliner.arguments.Arg):
                for input in arg.get_inputs():
                    self.inputs.append(input)
        for node_input in self.db.nodemgr.get_node_inputs(self.node):
            self.inputs.append(node_input)
        self.outputs = list()
        for arg in self.args:
            if isinstance(arg, pypeliner.arguments.Arg):
                for output in arg.get_outputs():
                    self.outputs.append(output)
    @property
    def id(self):
        return (self.node, self.job_def.name)
    @property
    def displayname(self):
        name = '/' + self.job_def.name
        if self.node.displayname != '':
            name = '/' + self.node.displayname + name
        if self.workflow.node.displayname != '':
            name = '/' + self.workflow.node.displayname + name
        return name
    @property
    def input_resources(self):
        return itertools.ifilter(lambda a: isinstance(a, pypeliner.resources.Resource), self.inputs)
    @property
    def output_resources(self):
        return itertools.ifilter(lambda a: isinstance(a, pypeliner.resources.Resource), self.outputs)
    def check_inputs(self):
        for input in self.input_resources:
            if not input.exists:
                raise InputMissingException(input.id, input.filename, self.id)
    def out_of_date(self):
        input_dates = [input.createtime for input in self.input_resources]
        output_dates = [output.createtime for output in self.output_resources]
        if len(input_dates) == 0 or len(output_dates) == 0:
            return True
        if None in output_dates:
            return True
        return max(input_dates) > min(output_dates)
    def explain(self):
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
        elif self.is_required_downstream:
            explanation = ['outputs required by downstream job']
        else:
            explanation = ['up to date']
        for input in self.input_resources:
            status = ''
            if oldest_output_date is not None and input.createtime > oldest_output_date:
                status = 'new'
            text = 'input {0} {1} {2}'.format(
                input.build_displayname_filename(self.workflow.node),
                _pretty_date(input.createtime),
                status)
            explanation.append(text)
        for output in self.output_resources:
            status = ''
            if output.createtime is None:
                status = ''
            elif newest_input_date is not None and output.createtime < newest_input_date:
                status = '{0} old'.format(_pretty_date(output.createtime))
            else:
                status = '{0}'.format(_pretty_date(output.createtime))
            if not output.exists:
                status += ' missing'
            text = 'output {0} {1}'.format(
                output.build_displayname_filename(self.workflow.node),
                status)
            explanation.append(text)
        return '\n'.join(explanation)
    def output_missing(self):
        return not all([output.exists for output in self.output_resources])
    def touch_outputs(self):
        for output in self.output_resources:
            output.touch()
    def check_require_regenerate(self):
        for arg in self.args:
            if isinstance(arg, pypeliner.arguments.Arg):
                if arg.is_split:
                    return True
        return False
    def create_callable(self):
        return JobCallable(self.id, self.job_def.func, self.argset, self.logs_dir)
    def create_exc_dir(self):
        exc_dir = os.path.join(self.logs_dir, 'exc{}'.format(self.retry_idx))
        pypeliner.helpers.makedirs(exc_dir)
        return exc_dir
    def finalize(self, callable):
        callable.finalize(self.db)
        self.init_inputs_outputs()
        if self.check_require_regenerate():
            self.workflow.regenerate()
    def complete(self):
        self.workflow.notify_completed(self.id)
    def retry(self):
        if self.retry_idx >= self.ctx.get('num_retry', 0):
            return False
        self.retry_idx += 1
        updated = False
        for key, value in self.ctx.iteritems():
            if key.endswith('_retry_factor'):
                self.ctx[key[:-len('_retry_factor')]] *= value
                updated = True
            elif key.endswith('_retry_increment'):
                self.ctx[key[:-len('_retry_increment')]] += value
                updated = True
        return updated

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


def resolve_arg(arg, callargs):
    if not isinstance(arg, pypeliner.arguments.Arg):
        return None, False
    arg = copy.copy(arg)
    resolved = arg.resolve()
    arg.sanitize()
    callargs.append(arg)
    return resolved, True


class JobCallable(object):
    """ Callable function and args to be given to exec queue """
    def __init__(self, id, func, argset, logs_dir):
        self.id = id
        self.func = func
        self.argset = argset
        self.finished = False
        self.stdout_filename = os.path.join(logs_dir, 'job.out')
        self.stderr_filename = os.path.join(logs_dir, 'job.err')
        self.job_timer = JobTimer()
        self.hostname = None
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
    @property
    def displaycommand(self):
        if self.func == pypeliner.commandline.execute:
            return '"' + ' '.join(str(arg) for arg in self.argset.args) + '"'
        else:
            return self.func.__module__ + '.' + self.func.__name__ + '(' + ', '.join(repr(arg) for arg in self.argset.args) + ', ' + ', '.join(key+'='+repr(arg) for key, arg in self.argset.kwargs.iteritems()) + ')'
    def __call__(self):
        with open(self.stdout_filename, 'w', 0) as stdout_file, open(self.stderr_filename, 'w', 0) as stderr_file:
            old_stdout, old_stderr = sys.stdout, sys.stderr
            sys.stdout, sys.stderr = stdout_file, stderr_file
            try:
                self.hostname = socket.gethostname()
                with self.job_timer:
                    self.callargs = list()
                    self.callset = pypeliner.deep.deeptransform(
                        self.argset,
                        lambda a: resolve_arg(a, self.callargs))
                    self.ret_value = self.func(*self.callset.args, **self.callset.kwargs)
                    if self.callset.ret is not None:
                        self.callset.ret.value = self.ret_value
                    for arg in self.callargs:
                        arg.finalize()
                self.finished = True
            except:
                sys.stderr.write(traceback.format_exc())
            finally:
                sys.stdout, sys.stderr = old_stdout, old_stderr
    def finalize(self, db):
        for arg in self.callargs:
            arg.updatedb(db)

def _setobj_helper(value):
    return value

class SetObjDefinition(JobDefinition):
    def __init__(self, name, axes, obj, value):
        super(SetObjDefinition, self).__init__(
            name, axes, {}, _setobj_helper,
            CallSet(ret=obj, args=(value,)))
    def create_job_instances(self, workflow, db):
        for node in db.nodemgr.retrieve_nodes(self.axes):
            yield SetObjInstance(self, workflow, db, node)

class SetObjInstance(JobInstance):
    """ Represents a sub workflow. """
    def __init__(self, job_def, workflow, db, node):
        super(SetObjInstance, self).__init__(job_def, workflow, db, node)
        obj_node = pypeliner.identifiers.create_undefined_node(job_def.argset.ret.axes)
        obj_res = pypeliner.resources.Resource(job_def.argset.ret.name, node + obj_node)
        self.obj_displayname = obj_res.build_displayname(workflow.node)

class SubWorkflowDefinition(JobDefinition):
    def __init__(self, name, axes, func, argset):
        self.name = name
        self.axes = axes
        self.ctx = {}
        self.func = func
        self.argset = argset
    def create_job_instances(self, workflow, db):
        for node in db.nodemgr.retrieve_nodes(self.axes):
            yield SubWorkflowInstance(self, workflow, db, node)

class SubWorkflowInstance(JobInstance):
    """ Represents a sub workflow. """
    direct_write = True
    def __init__(self, job_def, workflow, db, node):
        super(SubWorkflowInstance, self).__init__(job_def, workflow, db, node)
