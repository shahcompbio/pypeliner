import logging
import os
import subprocess

import pypeliner.delegator
import pypeliner.execqueue.base
import pypeliner.execqueue.subproc
import pypeliner.execqueue.utils
import pypeliner

class LocalJob(object):
    """ Encapsulate a running job called locally by subprocess """
    def __init__(self, ctx, name, sent, temps_dir, modules):
        self.name = name
        self.logger = logging.getLogger('pypeliner.execqueue')
        self.delegated = pypeliner.delegator.Delegator(sent, os.path.join(temps_dir, 'job.dgt'), modules)
        self.command = self.delegated.initialize()
        self.debug_filenames = dict()
        self.debug_filenames['job stdout'] = os.path.join(temps_dir, 'job.out')
        self.debug_filenames['job stderr'] = os.path.join(temps_dir, 'job.err')
        self.debug_files = []
        try:
            self.debug_files.append(open(self.debug_filenames['job stdout'], 'w'))
            self.debug_files.append(open(self.debug_filenames['job stderr'], 'w'))
            self.process = subprocess.Popen(self.command, stdout=self.debug_files[0], stderr=self.debug_files[1])
        except OSError as e:
            self.close_debug_files()
            error_text = self.name + ' submit failed\n'
            error_text += '-' * 10 + ' delegator command ' + '-' * 10 + '\n'
            error_text += ' '.join(self.command) + '\n'
            error_text += str(e) + '\n'
            error_text += pypeliner.execqueue.utils.log_text(self.debug_filenames)
            self.logger.error(error_text)
            raise pypeliner.execqueue.base.SubmitError()

    def close_debug_files(self):
        for file in self.debug_files:
            file.close()
            
    def finalize(self, returncode):
        self.close_debug_files()
        self.received = self.delegated.finalize()
        if returncode != 0 or self.received is None or not self.received.started:
            error_text = self.name + ' failed to complete\n'
            error_text += '-' * 10 + ' delegator command ' + '-' * 10 + '\n'
            error_text += ' '.join(self.command) + '\n'
            error_text += 'return code {0}\n'.format(returncode)
            error_text += pypeliner.execqueue.utils.log_text(self.debug_filenames)
            self.logger.error(error_text)
            raise pypeliner.execqueue.base.ReceiveError()


class LocalJobQueue(pypeliner.execqueue.subproc.SubProcessJobQueue):
    """ Queue of local jobs """
    def create(self, ctx, name, sent, temps_dir):
        return LocalJob(ctx, name, sent, temps_dir, self.modules)


class LocalRemoteQueue(pypeliner.execqueue.base.JobQueue):
    """ Class for a combined remote and local queue.
    """
    def __init__(self, remote_queue, modules=None):
        self.name_islocal = dict()
        self.local_queue = pypeliner.execqueue.local.LocalJobQueue(modules)
        self.remote_queue = remote_queue

    def __enter__(self):
        self.local_queue.__enter__()
        self.remote_queue.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.local_queue.__exit__(exc_type, exc_value, traceback)
        self.remote_queue.__exit__(exc_type, exc_value, traceback)

    def send(self, ctx, name, sent, temps_dir):
        if not ctx.get('local', False):
            self.remote_queue.send(ctx, name, sent, temps_dir)
        else:
            self.local_queue.send(ctx, name, sent, temps_dir)

    def wait(self):
        while True:
            if not self.local_queue.empty:
                name = self.local_queue.wait(immediate=True)
                if name is not None:
                    self.name_islocal[name] = True
                    return name
            return self.remote_queue.wait()

    def receive(self, name):
        if not self.name_islocal.pop(name, False):
            return self.remote_queue.receive(name)
        else:
            return self.local_queue.receive(name)

    @property
    def length(self):
        return self.local_queue.length + self.remote_queue.length

    @property
    def empty(self):
        return self.length == 0

