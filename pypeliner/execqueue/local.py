import logging
import os
import subprocess

import pypeliner.delegator
import pypeliner.execqueue.subproc


class LocalJob(object):
    """ Encapsulate a running job called locally by subprocess """
    def __init__(self, ctx, name, sent, temps_dir, modules):
        self.name = name
        self.logger = logging.getLogger('execqueue')
        self.delegated = pypeliner.delegator.delegator(sent, os.path.join(temps_dir, 'job.dgt'), modules)
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
        if returncode != 0 or self.received is None:
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

