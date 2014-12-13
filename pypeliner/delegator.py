import inspect
import logging
import os
import pickle
import sys
import traceback
import time
import tempfile
import shutil
import subprocess


class delegator(object):
    def __init__(self, job, prefix, modules):
        self.job = job
        self.before_filename = prefix + ".before"
        self.after_filename = prefix + ".after"
        self.syspaths = [os.path.dirname(os.path.abspath(module.__file__)) for module in modules]
    def cleanup(self):
        self._saferemove(self.before_filename)
        self._saferemove(self.after_filename)
    def _saferemove(self, filename):
        try: os.remove(filename)
        except OSError: pass
    def _waitfile(self, filename):
        waittime = 1
        while waittime < 100:
            if os.path.exists(filename):
                return
            if waittime >= 4:
                logging.getLogger('delegator').warn('waiting {0}s for {1} to appear'.format(waittime, filename))
            time.sleep(waittime)
            waittime *= 2
    def initialize(self):
        self.cleanup()
        with open(self.before_filename, 'wb') as before:
            pickle.dump(self.job, before)
        command = [sys.executable, inspect.getabsfile(type(self)), self.before_filename, self.after_filename] + self.syspaths
        return command
    def finalize(self):
        self._waitfile(self.after_filename)
        if not os.path.exists(self.after_filename):
            return None
        self.job = None
        with open(self.after_filename, 'rb') as after:
            self.job = pickle.load(after)
        self.cleanup()
        return self.job

def call_external(obj):
    try:
        tmp_dir = tempfile.mkdtemp()
        dgt = delegator(obj, tmp_dir, [sys.modules[obj.__module__]])
        subprocess.check_call(dgt.initialize())
        return dgt.finalize()
    finally:
        try:
            shutil.rmtree(tmp_dir)
        except OSError as exc:
            if exc.errno != 2:
                raise

if __name__ == "__main__":
    before_filename = sys.argv[1]
    after_filename = sys.argv[2]
    def not_pypeliner_path(a):
        if os.path.exists(a) and os.path.samefile(a, os.path.dirname(__file__)):
            return False
        return True
    sys.path = filter(not_pypeliner_path, sys.path)
    sys.path.extend(sys.argv[3:])
    with open(before_filename, 'rb') as before:
        job = pickle.load(before)
    if job is None:
        raise Exception('no job data in ' + before_filename)
    job()
    with open(after_filename, 'wb') as after:
        pickle.dump(job, after)

