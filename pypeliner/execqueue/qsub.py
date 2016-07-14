import os
import logging
import subprocess
import time

import pypeliner.helpers
import pypeliner.execqueue.local
import pypeliner.execqueue.qcmd
import pypeliner.execqueue.utils
import pypeliner.delegator


class QsubJob(object):
    """ Encapsulate a running job created using a queueing system's
    qsub submit command called using subprocess
    """
    def __init__(self, ctx, name, sent, temps_dir, modules, qsub_bin, native_spec):
        self.name = name
        self.logger = logging.getLogger('execqueue')
        self.delegated = pypeliner.delegator.delegator(sent, os.path.join(temps_dir, 'job.dgt'), modules)
        self.command = self.delegated.initialize()
        self.debug_filenames = dict()
        self.debug_filenames['job stdout'] = os.path.join(temps_dir, 'job.out')
        self.debug_filenames['job stderr'] = os.path.join(temps_dir, 'job.err')
        self.debug_filenames['submit stdout'] = os.path.join(temps_dir, 'submit.out')
        self.debug_filenames['submit stderr'] = os.path.join(temps_dir, 'submit.err')
        self.debug_files = []
        self.script_filename = os.path.join(temps_dir, 'submit.sh')
        with open(self.script_filename, 'w') as script_file:
            script_file.write(' '.join(self.command) + '\n')
        pypeliner.helpers.set_executable(self.script_filename)
        self.submit_command = self.create_submit_command(ctx, self.name, self.script_filename, qsub_bin, native_spec, self.debug_filenames['job stdout'], self.debug_filenames['job stderr'])
        try:
            self.debug_files.append(open(self.debug_filenames['submit stdout'], 'w'))
            self.debug_files.append(open(self.debug_filenames['submit stderr'], 'w'))
            self.process = subprocess.Popen(self.submit_command, stdout=self.debug_files[0], stderr=self.debug_files[1])
        except OSError as e:
            self.close_debug_files()
            error_text = self.name + ' submit failed\n'
            error_text += '-' * 10 + ' submit command ' + '-' * 10 + '\n'
            error_text += ' '.join(self.submit_command) + '\n'
            if type(e) == OSError and e.errno == 2:
                error_text += str(e) + ', ' + self.submit_command[0] + ' not found\n'
            else:
                error_text += str(e) + '\n'
            error_text += pypeliner.execqueue.utils.log_text(self.debug_filenames)
            self.logger.error(error_text)
            raise pypeliner.execqueue.base.SubmitError()

    def create_submit_command(self, ctx, name, script_filename, qsub_bin, native_spec, stdout_filename, stderr_filename):
        qsub = [qsub_bin]
        qsub += ['-sync', 'y', '-b', 'y']
        qsub += native_spec.format(**ctx).split()
        qsub += ['-N', pypeliner.execqueue.utils.qsub_format_name(name)]
        qsub += ['-o', stdout_filename]
        qsub += ['-e', stderr_filename]
        qsub += [script_filename]
        return qsub

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
            error_text += '-' * 10 + ' submit command ' + '-' * 10 + '\n'
            error_text += ' '.join(self.submit_command) + '\n'
            error_text += 'return code {0}\n'.format(returncode)
            error_text += pypeliner.execqueue.utils.log_text(self.debug_filenames)
            self.logger.error(error_text)
            raise pypeliner.execqueue.base.ReceiveError()


class QsubJobQueue(pypeliner.execqueue.subproc.SubProcessJobQueue):
    """ Queue of qsub jobs """
    def __init__(self, modules, native_spec):
        super(QsubJobQueue, self).__init__(modules)
        self.qsub_bin = pypeliner.helpers.which('qsub')
        self.native_spec = native_spec
        self.local_queue = pypeliner.execqueue.local.LocalJobQueue(modules)

    def create(self, ctx, name, sent, temps_dir):
        if ctx.get('local', False):
            return pypeliner.execqueue.local.LocalJob(ctx, name, sent, temps_dir, self.modules)
        else:
            return QsubJob(ctx, name, sent, temps_dir, self.modules, self.qsub_bin, self.native_spec)


class AsyncQsubJob(object):
    """ Encapsulate a running job created using a queueing system's
    qsub submit command called using subprocess, and polled using qstat
    """
    def __init__(self, ctx, name, sent, temps_dir, modules, qenv, native_spec, qstat_job_status):
        self.name = name
        self.qenv = qenv
        self.qstat_job_status = qstat_job_status
        self.qsub_job_id = None
        self.logger = logging.getLogger('execqueue')

        self.delegated = pypeliner.delegator.delegator(sent, os.path.join(temps_dir, 'job.dgt'), modules)
        self.command = self.delegated.initialize()

        self.debug_filenames = dict()
        self.debug_filenames['job stdout'] = os.path.join(temps_dir, 'job.out')
        self.debug_filenames['job stderr'] = os.path.join(temps_dir, 'job.err')
        self.debug_filenames['submit stdout'] = os.path.join(temps_dir, 'submit.out')
        self.debug_filenames['submit stderr'] = os.path.join(temps_dir, 'submit.err')
        self.debug_filenames['qacct stdout'] = os.path.join(temps_dir, 'qacct.out')
        self.debug_filenames['qacct stderr'] = os.path.join(temps_dir, 'qacct.err')
        for filename in self.debug_filenames.itervalues():
            pypeliner.helpers.saferemove(filename)

        self.script_filename = os.path.join(temps_dir, 'submit.sh')
        with open(self.script_filename, 'w') as script_file:
            script_file.write(' '.join(self.command) + '\n')
        pypeliner.helpers.set_executable(self.script_filename)

        self.submit_command = self.create_submit_command(ctx, name, self.script_filename, self.qenv.qsub_bin, native_spec, self.debug_filenames['job stdout'], self.debug_filenames['job stderr'])

        try:
            with open(self.debug_filenames['submit stdout'], 'w') as submit_stdout, open(self.debug_filenames['submit stderr'], 'w') as submit_stderr:
                subprocess.check_call(self.submit_command, stdout=submit_stdout, stderr=submit_stderr)
        except Exception as e:
            raise pypeliner.execqueue.base.SubmitError(self.create_error_text('submit error ' + str(e)))

        with open(self.debug_filenames['submit stdout'], 'r') as submit_stdout:
            self.qsub_job_id = submit_stdout.readline().rstrip().replace('Your job ', '').split(' ')[0]

        self.qsub_time = time.time()

        self.qacct = pypeliner.execqueue.qcmd.QacctWrapper(
            self.qenv, self.qsub_job_id,
            self.debug_filenames['qacct stdout'],
            self.debug_filenames['qacct stderr'],
        )

    def create_submit_command(self, ctx, name, script_filename, qsub_bin, native_spec, stdout_filename, stderr_filename):
        qsub = [qsub_bin]
        qsub += native_spec.format(**ctx).split()
        qsub += ['-N', pypeliner.execqueue.utils.qsub_format_name(name)]
        qsub += ['-o', stdout_filename]
        qsub += ['-e', stderr_filename]
        qsub += [script_filename]
        return qsub

    @property
    def finished(self):
        """ Get job finished boolean.
        """
        if not self.qstat_job_status.finished(self.qsub_job_id, self.qsub_time) and not self.qstat_job_status.errors(self.qsub_job_id):
            return False

        if self.qacct.results is None:
            try:
                self.qacct.check()
            except pypeliner.execqueue.qcmd.QacctError:
                return True
        
        return self.qacct.results is not None

    def finalize(self):
        """ Finalize a job after remote run.
        """
        assert self.finished

        if self.qstat_job_status.errors(self.qsub_job_id):
            self.delete()
            raise pypeliner.execqueue.base.ReceiveError(self.create_error_text('job error'))

        if self.qacct.results is None:
            raise pypeliner.execqueue.base.ReceiveError(self.create_error_text('qacct error'))

        if self.qacct.results['exit_status'] != '0':
            raise pypeliner.execqueue.base.ReceiveError(self.create_error_text('qsub error'))

        self.received = self.delegated.finalize()

        if self.received is None:
            raise pypeliner.execqueue.base.ReceiveError(self.create_error_text('receive error'))

    def delete(self):
        """ Delete job from queue.
        """
        try:
            subprocess.check_call([self.qenv.qdel_bin, self.qsub_job_id])
        except:
            self.logger.exception('Unable to delete {}'.format(self.qsub_job_id))

    def create_error_text(self, desc):
        """ Create error text string.

        Args:
            desc (str): error description

        Returns:
            str: multi-line error text
        """

        error_text = list()

        error_text += [desc + ' for job: ' + self.name]

        if self.qsub_job_id is not None:
            error_text += ['qsub id: ' + self.qsub_job_id]

        error_text += ['delegator command: ' + ' '.join(self.command)]
        error_text += ['submit command: ' + ' '.join(self.submit_command)]

        if self.qacct.results is not None:
            error_text += ['memory consumed: ' + self.qacct.results['maxvmem']]
            error_text += ['job exit status: ' + self.qacct.results['exit_status']]

        for debug_type, debug_filename in self.debug_filenames.iteritems():
            if not os.path.exists(debug_filename):
                error_text += [debug_type + ': missing']
                continue
            with open(debug_filename, 'r') as debug_file:
                error_text += [debug_type + ':']
                for line in debug_file:
                    error_text += ['\t' + line.rstrip()]

        return '\n'.join(error_text)


class AsyncQsubJobQueue(pypeliner.execqueue.base.JobQueue):
    """ Class for a queue of jobs run using subprocesses.  Maintains
    a list of running jobs, with the ability to wait for jobs and return
    completed jobs.  Requires override of the create method.
    """
    def __init__(self, modules, **kwargs):
        self.modules = modules
        self.qenv = pypeliner.execqueue.qcmd.QEnv()
        self.native_spec = kwargs['native_spec']
        self.qstat = pypeliner.execqueue.qcmd.QstatJobStatus(self.qenv)
        self.jobs = dict()
        self.name_islocal = dict()
        self.local_queue = pypeliner.execqueue.local.LocalJobQueue(modules)

    def __enter__(self):
        self.local_queue.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.local_queue.__exit__(exc_type, exc_value, traceback)
        for job in self.jobs.itervalues():
            job.delete()

    def create(self, ctx, name, sent, temps_dir):
        return AsyncQsubJob(ctx, name, sent, temps_dir, self.modules, self.qenv, self.native_spec, self.qstat)

    def send(self, ctx, name, sent, temps_dir):
        if ctx.get('local', False):
            self.local_queue.send(ctx, name, sent, temps_dir)
        else:
            self.jobs[name] = self.create(ctx, name, sent, temps_dir)

    def wait(self):
        while True:
            if not self.local_queue.empty:
                name = self.local_queue.wait(immediate=True)
                if name is not None:
                    self.name_islocal[name] = True
                    return name
            for name, job in self.jobs.iteritems():
                if job.finished:
                    return name
            self.qstat.update()

    def receive(self, name):
        if self.name_islocal.pop(name, False):
            return self.local_queue.receive(name)
        job = self.jobs.pop(name)
        job.finalize()
        return job.received

    @property
    def length(self):
        return len(self.jobs) + self.local_queue.length

    @property
    def empty(self):
        return self.length == 0


class PbsQstatJobStatus(pypeliner.execqueue.qcmd.QstatJobStatus):
    """ Statuses of jobs on a pbs cluster """
    def finished(self, job_id):
        return 'c' in self.cached_job_status.get(job_id, 'c')

    def errors(self, job_id):
        return False


class PbsJobQueue(AsyncQsubJobQueue):
    """ Queue of jobs running on a pbs cluster """
    def __init__(self, modules, **kwargs):
        super(PbsJobQueue, self).__init__(modules, **kwargs)
        self.qstat = PbsQstatJobStatus(self.qenv)
