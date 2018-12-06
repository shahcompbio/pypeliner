import inspect
import logging
import os
import dill as pickle
import sys
import time
import tempfile
import shutil
import subprocess
import traceback

import pypeliner.helpers


class Delegator(object):
    def __init__(self, job, prefix, modules):
        self.job = job
        self.before_filename = prefix + ".before"
        self.after_filename = prefix + ".after"
        self.syspaths = [os.path.dirname(os.path.abspath(module.__file__)) for module in modules]
    def cleanup(self):
        pypeliner.helpers.saferemove(self.before_filename)
        pypeliner.helpers.saferemove(self.after_filename)
    def _waitfile(self, filename):
        waittime = 1
        while waittime < 100:
            if os.path.exists(filename):
                return
            if waittime >= 4:
                logging.getLogger('pypeliner.delegator').warn('waiting {0}s for {1} to appear'.format(waittime, filename))
            time.sleep(waittime)
            waittime *= 2
    def initialize(self):
        self.cleanup()
        self.job.version = pypeliner.__version__
        with open(self.before_filename, 'wb') as before:
            pickle.dump(self.job, before)
        command = ['pypeliner_delegate', self.before_filename, self.after_filename] + self.syspaths
        command = pypeliner.commandline.dockerize_args(*command, **self.job.ctx)

        context_config = pypeliner.helpers.GlobalState.get("context_config")
        if context_config is not None:
            if 'docker' in context_config:
                command = pypeliner.commandline.dockerize_args(*command, **self.job.ctx)
            elif 'singularity' in context_config:
                command = pypeliner.commandline.singularity_args(*command, **self.job.ctx)
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
        dgt = Delegator(obj, tmp_dir, [sys.modules[obj.__module__]])
        subprocess.check_call(dgt.initialize())
        return dgt.finalize()
    finally:
        try:
            shutil.rmtree(tmp_dir)
        except OSError as exc:
            if exc.errno != 2:
                raise

def main():
    try:
        before_filename = sys.argv[1]
        after_filename = sys.argv[2]
        def not_pypeliner_path(a):
            if os.path.exists(a) and os.path.samefile(a, os.path.dirname(__file__)):
                return False
            return True
        sys.path = filter(not_pypeliner_path, sys.path)
        sys.path.extend(sys.argv[3:])
        job = None
        with open(before_filename, 'rb') as before:
            job = pickle.load(before)
        if job is None:
            raise ValueError('no job data in ' + before_filename)
        if not job.version == pypeliner.__version__:
            logging.getLogger('pypeliner.delegator').warn(
                'mismatching pypeliner versions, '
                'running {} on head node and {} on compute node'.format(job.version, pypeliner.__version__))
        job()
    except:
        sys.stderr.write(traceback.format_exc())
    finally:
        with open(after_filename, 'wb') as after:
            pickle.dump(job, after)


if __name__ == "__main__":
    main()
