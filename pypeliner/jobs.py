import copy
import os
import sys
import itertools
import time
import traceback
import socket

import helpers
import arguments
import commandline
import managed
import resources
import identifiers

class CallSet(object):
    """ Set of positional and keyword arguments, and a return value """
    def __init__(self, ret=None, args=None, kwargs=None):
        if ret is not None and not isinstance(ret, managed.Managed):
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
    def __init__(self, input, job):
        self.input = input
        self.job = job
    def __str__(self):
        return 'input {0} missing for job {1}'.format(self.input, self.job)

class JobInstance(object):
    """ Represents a job including function and arguments """
    def __init__(self, job_def, workflow, db, node):
        self.job_def = job_def
        self.workflow = workflow
        self.db = db
        self.node = node
        self.args = list()
        try:
            self.argset = copy.deepcopy(self.job_def.argset, {'_job':self})
        except managed.JobArgMismatchException as e:
            e.job_name = self.displayname
            raise
        self.logs_dir = os.path.join(db.logs_dir, self.node.subdir, self.job_def.name)
        helpers.makedirs(self.logs_dir)
        self.is_subworkflow = False
        self.is_immediate = False
        self.retry_idx = 0
        self.ctx = job_def.ctx.copy()
        self.direct_write = False
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
    def _inputs(self):
        for arg in self.args:
            if isinstance(arg, arguments.Arg):
                for input in arg.get_inputs(self.db):
                    yield input
        for node_input in self.db.nodemgr.get_node_inputs(self.node):
            yield node_input
    @property
    def _outputs(self):
        for arg in self.args:
            if isinstance(arg, arguments.Arg):
                for output in arg.get_outputs(self.db):
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
            if input.get_createtime(self.db) is None:
                raise InputMissingException(input.id, self.id)
    @property
    def out_of_date(self):
        self.check_inputs()
        input_dates = [input.get_createtime(self.db) for input in self.input_resources]
        output_dates = [output.get_createtime(self.db) for output in self.output_resources]
        if len(input_dates) == 0 or len(output_dates) == 0:
            return True
        if None in output_dates:
            return True
        return max(input_dates) > min(output_dates)
    def explain(self):
        self.check_inputs()
        input_dates = [input.get_createtime(self.db) for input in self.input_resources]
        output_dates = [output.get_createtime(self.db) for output in self.output_resources]
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
            if oldest_output_date is not None and input.get_createtime(self.db) > oldest_output_date:
                status = 'new'
            explanation.append('input {0} {1} {2}'.format(input.build_displayname_filename(self.db, self.workflow.node), input.get_createtime(self.db), status))
        for output in self.output_resources:
            status = ''
            if output.get_createtime(self.db) is None:
                status = 'missing'
            elif newest_input_date is not None and output.get_createtime(self.db) < newest_input_date:
                status = '{0} old'.format(output.get_createtime(self.db))
            else:
                status = '{0}'.format(output.get_createtime(self.db))
            explanation.append('output {0} {1}'.format(output.build_displayname_filename(self.db, self.workflow.node), status))
        return '\n'.join(explanation)
    @property
    def output_missing(self):
        return not all([output.get_exists(self.db) for output in self.output_resources])
    def check_require_regenerate(self):
        for arg in self.args:
            if isinstance(arg, arguments.Arg):
                if arg.is_split:
                    return True
        return False
    def create_callable(self):
        return JobCallable(self.db, self.id, self.job_def.func, self.argset, self.logs_dir, self.direct_write)
    def create_exc_dir(self):
        exc_dir = os.path.join(self.logs_dir, 'exc{}'.format(self.retry_idx))
        helpers.makedirs(exc_dir)
        return exc_dir
    def finalize(self, callable):
        callable.finalize(self.db)
        if self.check_require_regenerate():
            self.workflow.regenerate()
    def complete(self):
        self.workflow.notify_completed(self)
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

class JobCallable(object):
    """ Callable function and args to be given to exec queue """
    def __init__(self, db, id, func, argset, logs_dir, direct_write=False):
        self.id = id
        self.func = func
        self.callargs = list()
        self.callset = copy.deepcopy(argset, {'_db':db, '_direct_write':direct_write, '_args':self.callargs})
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
        if self.func == commandline.execute:
            return '"' + ' '.join(str(arg) for arg in self.callset.args) + '"'
        else:
            return self.func.__name__ + '(' + ', '.join(repr(arg) for arg in self.callset.args) + ', ' + ', '.join(key+'='+repr(arg) for key, arg in self.callset.kwargs.iteritems()) + ')'
    def __call__(self):
        with open(self.stdout_filename, 'w', 0) as stdout_file, open(self.stderr_filename, 'w', 0) as stderr_file:
            old_stdout, old_stderr = sys.stdout, sys.stderr
            sys.stdout, sys.stderr = stdout_file, stderr_file
            try:
                self.hostname = socket.gethostname()
                with self.job_timer:
                    self.ret_value = self.func(*self.callset.args, **self.callset.kwargs)
                    if self.callset.ret is not None:
                        self.callset.ret.value = self.ret_value
                self.finished = True
            except:
                sys.stderr.write(traceback.format_exc())
            finally:
                sys.stdout, sys.stderr = old_stdout, old_stderr
    def finalize(self, db):
        for arg in self.callargs:
            arg.updatedb(db)
        for arg in self.callargs:
            arg.finalize(db)

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
    def __init__(self, job_def, workflow, db, node):
        super(SubWorkflowInstance, self).__init__(job_def, workflow, db, node)
        self.is_subworkflow = True
        self.is_immediate = False
        self.direct_write = True

class ChangeAxisDefinition(object):
    """ Represents an abstract aliasing """
    def __init__(self, name, axes, var_name, old_axis, new_axis, exact=True):
        self.name = name
        self.axes = axes
        self.var_name = var_name
        self.old_axis = old_axis
        self.new_axis = new_axis
        self.exact = exact
    def create_job_instances(self, workflow, db):
        for node in db.nodemgr.retrieve_nodes(self.axes):
            yield ChangeAxisInstance(self, workflow, db, node, exact=self.exact)

class ChangeAxisException(Exception):
    def __init__(self, old, new):
        self.old = old
        self.new = new
    def __str__(self):
        return 'axis change from {0} to {1}'.format(self.old, self.new)

class ChangeAxisInstance(JobInstance):
    """ Creates an alias of a managed object """
    def __init__(self, job_def, workflow, db, node, exact=True):
        self.job_def = job_def
        self.workflow = workflow
        self.db = db
        self.node = node
        self.exact = exact
        self.is_subworkflow = False
        self.is_immediate = True
        self.trigger_regenerate = False
    @property
    def _inputs(self):
        for node in self.db.nodemgr.retrieve_nodes((self.job_def.old_axis,), self.node):
            yield resources.Dependency(self.job_def.var_name, node)
        for dependency in self.db.nodemgr.get_merge_inputs((self.job_def.old_axis,), self.node):
            yield dependency
        for depenency in self.db.nodemgr.get_merge_inputs((self.job_def.new_axis,), self.node):
            yield dependency
        for node_input in self.db.nodemgr.get_node_inputs(self.node):
            yield node_input
    @property
    def _outputs(self):
        for node in self.db.nodemgr.retrieve_nodes((self.job_def.new_axis,), self.node):
            yield resources.Dependency(self.job_def.var_name, node)
    @property
    def out_of_date(self):
        return True
    def finalize(self):
        old_chunks = set(self.db.nodemgr.retrieve_chunks((self.job_def.old_axis,), self.node))
        new_chunks = set(self.db.nodemgr.retrieve_chunks((self.job_def.new_axis,), self.node))
        if self.exact and new_chunks != old_chunks or not self.exact and not new_chunks.issubset(old_chunks):
            raise ChangeAxisException(old_chunks, new_chunks)
        for chunks in old_chunks:
            if len(chunks) > 1:
                raise NotImplementedError('only single axis changes currently supported')
            old_node = self.node + identifiers.AxisInstance(self.job_def.old_axis, chunks[0])
            new_node = self.node + identifiers.AxisInstance(self.job_def.new_axis, chunks[0])
            self.db.resmgr.add_alias(self.job_def.var_name, old_node, self.job_def.var_name, new_node)
    def complete(self):
        self.workflow.notify_completed(self)

