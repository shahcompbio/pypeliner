import os
import logging
import stat
import shutil
import hashlib
import errno
import json
import time
import random
from functools import wraps
import importlib
from pypeliner import _pypeliner_internal_global_state
from collections import deque


def running_in_docker():
    with open('/proc/self/cgroup', 'r') as procfile:
        for line in procfile:
            fields = line.strip().split('/')
            if 'docker' in fields:
                return True
    return False


class GlobalState(object):
    """
    add the specified variable and value to a pypeliner wide
    key value store
    :param variablename: key name
    :param value: key value
    """
    def __init__(self, variablename, value):
        self.variablename = variablename
        self.value = value
        _pypeliner_internal_global_state[self.variablename] = self.value

    @staticmethod
    def get(variablename):
        return _pypeliner_internal_global_state[variablename]


class Backoff(object):
    """
    wrapper for functions that fail but require retries until it succeeds
    """

    def __init__(self, exception_type=Exception, max_backoff=3600, backoff_time=1, randomize=False,
                 num_retries=None, backoff="exponential", step_size=2):
        self.func = None

        if backoff not in ["exponential", "linear", "fixed"]:
            raise Exception(
                "Currently supports only exponential, linear and fixed backoff")

        self.exception_type = exception_type

        self.max_backoff = max_backoff
        self.backoff_time = backoff_time

        self.randomize = randomize

        self.num_retries = num_retries

        self.backoff = backoff

        self.step_size = step_size

        self.success = False

        self.elapsed_time = 0

    def __call__(self, func):
        self.func = func

        @wraps(func)
        def wrapped(*args, **kwargs):
            return self._run_with_exponential_backoff(*args, **kwargs)

        return wrapped

    def _run_with_exponential_backoff(self, *args, **kwargs):
        """
        keep running the function until we go over the
        max wait time or num retries
        """

        retry_no = 0

        while True:

            if self.elapsed_time >= self.max_backoff:
                break

            if self.num_retries and retry_no > self.num_retries:
                break

            try:
                result = self.func(*args, **kwargs)
            except self.exception_type as exc:
                self._update_backoff_time()
                logging.getLogger("pypeliner.helpers").warn(
                    "error {} caught, retrying after {} seconds".format(
                        exc.message, self.backoff_time)
                )
                retry_no += 1
                time.sleep(self.backoff_time)

            else:
                self.success = True
                break

        # try it one last time
        if not self.success:
            result = self.func(*args, **kwargs)

        return result

    def _update_backoff_time(self):
        """
        update the backoff time
        """

        if self.backoff == "exponential":
            self.backoff_time = self.step_size * (self.backoff_time or 1)

        elif self.backoff == "linear":
            self.backoff_time += self.step_size

        if self.randomize:
            lower_bound = int(0.9 * self.backoff_time)
            upper_bound = int(1.1 * self.backoff_time)
            self.backoff_time = random.randint(lower_bound, upper_bound)

        if self.elapsed_time + self.backoff_time > self.max_backoff:
            self.backoff_time = self.max_backoff - self.elapsed_time

        self.elapsed_time += self.backoff_time

def import_function(import_string):
    module, funcname = import_string.rsplit('.', 1)

    mod = importlib.import_module(module)
    met = getattr(mod, funcname)

    return met


def pop_if(L, pred):
    for idx, item in enumerate(L):
        if pred(item):
            return L.pop(idx)
    raise IndexError()

def abspath(path):
    if path.endswith('/'):
        return os.path.abspath(path) + '/'
    else:
        return os.path.abspath(path)

class MultiLineFormatter(logging.Formatter):
    def format(self, record):
        header = logging.Formatter.format(self, record)
        return header + record.message.rstrip('\n').replace('\n', '\n\t')

class JsonFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps(vars(record))

def which(name):
    if os.environ.get('PATH', None) is not None:
        for p in os.environ.get('PATH', '').split(os.pathsep):
            p = os.path.join(p, name)
            if os.access(p, os.X_OK):
                return p
    raise EnvironmentError('unable to find ' + name + ' in the system path')

def set_executable(filename):
    mode = os.stat(filename).st_mode
    mode |= stat.S_IXUSR
    os.chmod(filename, stat.S_IMODE(mode))

def md5_file(filename, block_size=8192):
    md5 = hashlib.md5()
    with open(filename,'rb') as f: 
        for chunk in iter(lambda: f.read(block_size), b''): 
             md5.update(chunk)
    return md5.digest()

def overwrite_if_different(new_filename, existing_filename):
    do_copy = True
    try:
        do_copy = md5_file(existing_filename) != md5_file(new_filename)
    except IOError:
        pass
    if do_copy:
        os.rename(new_filename, existing_filename)

def makedirs(dirname):
    dirname = abspath(dirname)
    try:
        os.makedirs(dirname)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
    assert os.path.isdir(dirname)

def saferemove(filename):
    try:
        os.remove(filename)
    except OSError:
        pass

def symlink(source, link_name):
    source = os.path.abspath(source)
    try:
        os.remove(link_name)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise
    os.symlink(source, link_name)

def touch(filename, times=None):
    with open(filename, 'a'):
        os.utime(filename, times)

def removefiledir(filename):
    saferemove(filename)
    shutil.rmtree(filename, ignore_errors=True)


class RemoteLogHandler(logging.Handler):
    def __init__(self, logs):
        logging.Handler.__init__(self)
        self.logs = logs

    def emit(self, log_record):
        self.logs.append(log_record)


class RemoteLogger(object):
    def __init__(self):
        self._logs = deque(maxlen=1000)
        self._handler = RemoteLogHandler(self._logs)

    @property
    def log_records(self):
        return self._logs

    @property
    def log_handler(self):
        return self._handler
