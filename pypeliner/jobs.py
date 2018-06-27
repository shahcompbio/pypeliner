import copy
import os
import sys
import itertools
import time
import traceback
import socket
import datetime
import signal
import collections


import pypeliner.helpers
import pypeliner.arguments
import pypeliner.commandline
import pypeliner.managed
import pypeliner.resources
import pypeliner.identifiers
import pypeliner.deep
import pypeliner.storage

import resource


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
    def __init__(self, name, axes, ctx, func, argset, origins):
        self.name = name
        self.axes = axes
        self.ctx = ctx
        self.func = func
        self.argset = argset
        self.origins = origins
    def create_job_instances(self, workflow, db):
        for node in db.nodemgr.retrieve_nodes(self.axes):
            yield JobInstance(self, workflow, db, node)


def _pretty_date(ts):
    if ts is None:
        return 'none'
    return datetime.datetime.fromtimestamp(ts).strftime('%Y/%m/%d-%H:%M:%S')


class JobArgMismatchException(Exception):
    def __init__(self, name, axes, node):
        self.name = name
        self.axes = axes
        self.node = node
        self.job_name = 'unknown'
    def __str__(self):
        return 'arg {0} with axes {1} does not match job {2} with axes {3}'.format(self.name, self.axes, self.job_name, tuple(a[0] for a in self.node))


def _is_subseq(seq1, seq2):
    """ Check if seq1 is a subsequence of seq2.
    """
    for i in range(len(seq1)):
        if i >= len(seq2):
            return False
        elif seq1[i] != seq2[i]:
            return False
    return True


def _max_subseq(seq1, seq2):
    """ Find the length of the common prefix between seq1 and seq2.
    """
    common = 0
    for a, b in zip(seq1, seq2):
        if a != b:
            break
        common += 1
    return common


class JobInstance(object):
    """ Represents a job including function and arguments """
    direct_write = False
    def __init__(self, job_def, workflow, db, node):
        self.job_def = job_def
        self.workflow = workflow
        self.db = db
        self.node = node
        self.arglist = list()
        self.inputs = list()
        self.outputs = list()
        self.origins_check = set()
        try:
            self.argset = pypeliner.deep.deeptransform(
                self.job_def.argset,
                self._create_arg)
        except JobArgMismatchException as e:
            e.job_name = self.displayname
            raise
        self.logs_dir = os.path.join(db.logs_dir, self.node.subdir, self.job_def.name)
        pypeliner.helpers.makedirs(self.logs_dir)
        self.retry_idx = 0
        self.ctx = job_def.ctx.copy()
        self.is_required_downstream = False
        for origin in (set(self.job_def.origins) - self.origins_check):
            raise Exception('origin {} not created by any arg for job {}'.format(
                origin, self.displayname))

    def _create_arg(self, mg):
        if not isinstance(mg, pypeliner.managed.Managed):
            return None, False

        # Match managed argument axes to job axes
        if mg.axes is None:
            base_node = self.node
            arg_axes = ()
        else:
            common = _max_subseq(mg.axes, self.node.axes)
            base_node = self.node[:common]
            arg_axes = mg.axes[common:]

        # Nodes on which to split / merge if any
        arg_nodes = list(self.db.nodemgr.retrieve_nodes(arg_axes, base_node))
        is_split_merge = (len(arg_axes) > 0)

        # Check if this argument is an origin for any axes for this job
        arg_origins = []
        for origin in self.job_def.origins:

            # Each origin should have the jobs axes as a prefix
            if not _is_subseq(self.node.axes, origin):
                raise ValueError('invalid origin {} for job {}'.format(
                    origin, self.displayname))
            arg_origin = origin[len(self.node.axes):]

            if _is_subseq(arg_origin, arg_axes):
                arg_origins.append(arg_origin)
                self.origins_check.add(origin)

        # Create an argument for the given managed object
        if len(arg_axes) == 0 and mg.normal is not None:
            arg = mg.normal(self.db, mg.name, base_node,
                direct_write=self.direct_write,
                **mg.kwargs)
        elif len(arg_axes) > 0 and len(arg_origins) > 0 and mg.splitorigin is not None:
            arg = mg.splitorigin(self.db, mg.name, base_node, arg_axes, arg_origins,
                direct_write=self.direct_write,
                **mg.kwargs)
        elif len(arg_axes) > 0 and len(arg_origins) == 0 and mg.splitmerge is not None:
            arg = mg.splitmerge(self.db, mg.name, base_node, arg_axes,
                direct_write=self.direct_write,
                **mg.kwargs)
        else:
            raise JobArgMismatchException(mg.name, mg.axes, self.node)

        self.inputs.extend(arg.get_inputs())
        self.inputs.extend(arg.get_merge_inputs())
        self.outputs.extend(arg.get_outputs())
        self.outputs.extend(arg.get_split_outputs())

        self.arglist.append(arg)
        return arg, True

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
    def already_run(self):
        return self.db.job_shelf.get(self.displayname, False)
    def out_of_date(self):
        input_dates = [input.createtime for input in self.input_resources]
        output_dates = [output.createtime for output in self.output_resources]
        if len(input_dates) == 0 or len(output_dates) == 0:
            return True
        if None in output_dates:
            return True
        return max(input_dates) > min(output_dates)
    def explain_out_of_date(self):
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
    def create_callable(self):
        timeout = self.ctx.get("timeout", None)
        return JobCallable(self.id, self.job_def.func, self.argset, self.arglist, self.db.file_storage, self.logs_dir, timeout)
    def create_exc_dir(self):
        exc_dir = os.path.join(self.logs_dir, 'exc{}'.format(self.retry_idx))
        pypeliner.helpers.makedirs(exc_dir)
        return exc_dir
    def finalize(self, callable):
        regenerate = False
        arg_chunks = collections.defaultdict(dict)
        for arg in callable.arglist:
            for origin, node, chunks in arg.get_origin_chunks():
                arg_chunks[(origin, node)][(arg.name, arg.axes)] = chunks
        for origin, node in arg_chunks.keys():
            origin_chunks = None
            for (arg_name, arg_axes), chunks in arg_chunks[(origin, node)].iteritems():
                if origin_chunks is None:
                    origin_chunks = chunks
                    arg_name_0, arg_axes_0 = arg_name, arg_axes
                    continue
                elif chunks != origin_chunks:
                    raise Exception('chunk mismatch for job {}, node {}, arg {}: {} and arg {}: {}'.format(
                        self.displayname, node,
                        (arg_name, arg_axes), origin_chunks,
                        (arg_name_0, arg_axes_0), chunks))
            self.db.nodemgr.store_axis_chunks(origin[-1], node, origin_chunks)
            regenerate = True
        for arg in callable.arglist:
            arg.finalize(self.db)
        if regenerate:
            self.workflow.regenerate()
    def complete(self):
        self.db.job_shelf[self.displayname] = True
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



class TimeOutError(Exception):
    """Exception Type for throwing timeout errors"""
    pass


class JobTimeOut(object):
    """ TimeOut using a context manager
        set an alarm for the timeout, catch it and throw an Exception.
    """
    def __init__(self, timeout):
        self._timeout_string = timeout

        if not timeout:
            self._timeout = None
            return

        if timeout.endswith("s"):
            multiplier = 1
        elif timeout.endswith("m"):
            multiplier = 60
        elif timeout.endswith("h"):
            multiplier = 60 * 60
        elif timeout.endswith("d"):
            multiplier = 60 * 60 * 24
        else:
            raise ValueError("Invalid timeout interval: {}."\
                             " Timeout should be a number followed by a suffix. "\
                             "SUFFIX may be s for seconds, m for minutes, h for hours "\
                             "or d for days".format(timeout))

        time_interval = int(timeout[:-1])

        self._timeout = time_interval * multiplier

    def handler(self, signum, frame):
        raise TimeOutError("Execution time exceeded the specified timeout of {}.".format(self._timeout_string))

    def __enter__(self):
        if self._timeout:
            signal.signal(signal.SIGALRM, self.handler)
            signal.alarm(self._timeout)

    def __exit__(self, exc_type, exc_value, traceback):
        pass


class JobMemoryTracker(object):
    """ track memory use"""
    def __init__(self):
        self._start = None
        self._finish = None
    def __enter__(self):
        self._start = 0
    def __exit__(self, exc_type, exc_value, traceback):
        # get memory usage for linux systems
        corememusage = float(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) / (1024*1024)
        subprocmemusage = float(resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss) / (1024*1024)
        self._finish = round(corememusage + subprocmemusage, 2)
    @property
    def memoryused(self):
        if self._finish is None or self._start is None:
            return None
        return self._finish


def resolve_arg(arg):
    if not isinstance(arg, pypeliner.arguments.Arg):
        return None, False
    return arg.resolve(), True


class JobCallable(object):
    """ Callable function and args to be given to exec queue """
    def __init__(self, id, func, argset, arglist, storage, logs_dir, timeout):
        self.storage = storage
        self.id = id
        self.func = func
        self.argset = argset
        self.arglist = arglist
        self.finished = False
        self.stdout_filename = os.path.join(logs_dir, 'job.out')
        self.stderr_filename = os.path.join(logs_dir, 'job.err')
        self.stdout_storage = self.storage.create_store(self.stdout_filename)
        self.stderr_storage = self.storage.create_store(self.stderr_filename)
        self.job_timer = JobTimer()
        self.job_mem_tracker = JobMemoryTracker()
        self.job_time_out = JobTimeOut(timeout)
        self.hostname = None
        self.callset = pypeliner.deep.deeptransform(self.argset, resolve_arg)
    @property
    def duration(self):
        return self.job_timer.duration
    @property
    def memoryused(self):
        return self.job_mem_tracker.memoryused
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
            return '"' + ' '.join(str(arg) for arg in self.callset.args) + '"'
        else:
            return self.func.__module__ + '.' + self.func.__name__ + '(' + ', '.join(repr(arg) for arg in self.callset.args) + ', ' + ', '.join(key+'='+repr(arg) for key, arg in self.callset.kwargs.iteritems()) + ')'
    def allocate(self):
        for arg in self.arglist:
            arg.allocate()
    def pull(self):
        for arg in self.arglist:
            arg.pull()
    def push(self):
        for arg in self.arglist:
            arg.push()
    def __call__(self):
        self.stdout_storage.allocate()
        self.stderr_storage.allocate()
        with open(self.stdout_storage.filename, 'w', 0) as stdout_file, open(self.stderr_storage.filename, 'w', 0) as stderr_file:
            old_stdout, old_stderr = sys.stdout, sys.stderr
            sys.stdout, sys.stderr = stdout_file, stderr_file
            try:
                self.hostname = socket.gethostname()
                with self.job_timer, self.job_mem_tracker, self.job_time_out:
                    self.allocate()
                    self.pull()
                    self.ret_value = self.func(*self.callset.args, **self.callset.kwargs)
                    if self.callset.ret is not None:
                        self.callset.ret.value = self.ret_value
                    self.push()
                self.finished = True
            except:
                sys.stderr.write(traceback.format_exc())
            finally:
                sys.stdout, sys.stderr = old_stdout, old_stderr
        self.stdout_storage.push()
        self.stderr_storage.push()
    def collect_logs(self):
        self.stdout_storage.allocate()
        self.stderr_storage.allocate()
        self.stdout_storage.pull()
        self.stderr_storage.pull()

def _setobj_helper(value):
    return value

class SetObjDefinition(JobDefinition):
    def __init__(self, name, axes, obj, value, origins):
        super(SetObjDefinition, self).__init__(
            name, axes, {}, _setobj_helper,
            CallSet(ret=obj, args=(value,)),
            origins)
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
        self.origins = ()
    def create_job_instances(self, workflow, db):
        for node in db.nodemgr.retrieve_nodes(self.axes):
            yield SubWorkflowInstance(self, workflow, db, node)

class SubWorkflowInstance(JobInstance):
    """ Represents a sub workflow. """
    direct_write = True
    def __init__(self, job_def, workflow, db, node):
        super(SubWorkflowInstance, self).__init__(job_def, workflow, db, node)
    def create_callable(self):
        timeout = self.job_def.ctx.get("timeout", None)
        return WorkflowCallable(self.id, self.job_def.func, self.argset, self.arglist, self.db.file_storage, self.logs_dir, timeout)

class WorkflowCallable(JobCallable):
    def allocate(self):
        pass
    def push(self):
        pass
    def pull(self):
        pass
