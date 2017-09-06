import os
import errno

import pypeliner.execqueue.base


class SubProcessJobQueue(pypeliner.execqueue.base.JobQueue):
    """ Abstract class for a queue of jobs run using subprocesses.  Maintains
    a list of running jobs, with the ability to wait for jobs and return
    completed jobs.  Requires override of the create method.
    """
    def __init__(self, modules=None, **kwargs):
        self.modules = modules
        self.jobs = dict()
        self.pid_names = dict()
        self.pid_returncodes = dict()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def create(self, ctx, name, sent, temps_dir):
        raise NotImplementedError()

    def send(self, ctx, name, sent, temps_dir):
        submitted = self.create(ctx, name, sent, temps_dir)
        self.jobs[name] = submitted
        self.pid_names[submitted.process.pid] = name

    def wait(self, immediate=False):
        while True:
            if immediate:
                process_id, returncode = os.waitpid(-1, os.WNOHANG)
                if process_id is None:
                    return None
            else:
                try:
                    process_id, returncode = os.wait()
                except OSError as e:
                    if e.errno == errno.EINTR:
                        continue
                    else:
                        raise
            try:
                name = self.pid_names.pop(process_id)
            except KeyError:
                continue
            self.pid_returncodes[name] = returncode
            return name

    def receive(self, name):
        job = self.jobs.pop(name)
        job.finalize(self.pid_returncodes.pop(name))
        return job.received

    @property
    def length(self):
        return len(self.jobs)

    @property
    def empty(self):
        return self.length == 0

