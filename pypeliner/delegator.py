import logging
import os
import shutil
import subprocess
import sys
import tempfile
import time
import traceback

import dill as pickle
import pypeliner.helpers
import pypeliner.containerize


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
                logging.getLogger('pypeliner.delegator').warn(
                    'waiting {0}s for {1} to appear'.format(waittime, filename))
            time.sleep(waittime)
            waittime *= 2

    def initialize(self):
        self.cleanup()
        with open(self.before_filename, 'wb') as before:
            pickle.dump(self.job, before)
        command = ['pypeliner_delegate', self.before_filename, self.after_filename] + self.syspaths
        # local tasks like setobj need to run without docker args,
        # since they might already be running inside docker
        if not self.job.ctx.get('local'):
            command = pypeliner.containerize.containerize_args(*command)
        return command

    def finalize(self):
        self._waitfile(self.after_filename)
        if not os.path.exists(self.after_filename):
            return None
        self.job = None
        with open(self.after_filename, 'rb') as after:
            self.job = pickle.load(after)
        self.cleanup()
        if self.job:
            for logrecord in self.job.log_records:
                logging.getLogger().handle(logrecord)
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
    job_logger = None

    try:
        job = None
        before_filename = sys.argv[1]
        after_filename = sys.argv[2]

        delegator_dir = os.path.dirname(before_filename)

        if not os.path.exists(delegator_dir) and pypeliner.helpers.running_in_docker():
            if delegator_dir:
                raise Exception(
                    "Did you forget to add your working directory to docker mounts?"
                    " {} is not available inside docker, delegator will "
                    "fail.".format(delegator_dir)
                )

        def not_pypeliner_path(a):
            if os.path.exists(a) and os.path.samefile(a, os.path.dirname(__file__)):
                return False
            return True

        sys.path = list(filter(not_pypeliner_path, sys.path))
        sys.path.extend(sys.argv[3:])

        with open(before_filename, 'rb') as before:
            job = pickle.load(before)
        if job is None:
            raise ValueError('no job data in ' + before_filename)
        if not job.version == pypeliner.__version__:
            logging.getLogger('pypeliner.delegator').warn(
                'mismatching pypeliner versions, '
                'running {} on head node and {} on compute node'.format(job.version, pypeliner.__version__))

        job_logger = pypeliner.helpers.RemoteLogger()
        logging.getLogger().addHandler(job_logger.log_handler)
        job()
    except:
        sys.stderr.write(traceback.format_exc())
    finally:
        if job_logger:
            job.log_records = job_logger.log_records
        with open(after_filename, 'wb') as after:
            pickle.dump(job, after)


if __name__ == "__main__":
    main()
