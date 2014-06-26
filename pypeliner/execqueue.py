import os
import subprocess
import time
import logging

import helpers
import delegator


def log_text(debug_filenames):
    text = ''
    for debug_type, debug_filename in debug_filenames.iteritems():
        preamble = '-' * 10 + ' ' + debug_type + ' ' + '-' * 10
        if not os.path.exists(debug_filename):
            text += preamble + ' (missing)\n'
            continue
        else:
            text += preamble + '\n'
        with open(debug_filename, 'r') as debug_file:
            text += debug_file.read()
    return text

def qsub_format_name(name):
    return name.replace('<','_').replace('>', '').replace(':', '.').rstrip('_')

class LocalJob(object):
    """ Encapsulate a running job called locally by subprocess """
    def __init__(self, ctx, job, modules):
        self.job = job
        self.logger = logging.getLogger('execqueue')
        self.delegated = delegator.delegator(job, os.path.join(job.temps_dir, 'job.dgt'), modules)
        self.command = self.delegated.initialize()
        self.debug_filenames = dict()
        self.debug_filenames['job stdout'] = os.path.join(job.temps_dir, 'job.out')
        self.debug_filenames['job stderr'] = os.path.join(job.temps_dir, 'job.err')
        self.debug_files = []
        try:
            self.debug_files.append(open(self.debug_filenames['job stdout'], 'w'))
            self.debug_files.append(open(self.debug_filenames['job stderr'], 'w'))
            self.process = subprocess.Popen(self.command, stdout=self.debug_files[0], stderr=self.debug_files[1])
        except OSError as e:
            self.close_debug_files()
            error_text = self.job.displayname + ' submit failed\n'
            error_text += '-' * 10 + ' delegator command ' + '-' * 10 + '\n'
            error_text += ' '.join(self.command) + '\n'
            error_text += str(e) + '\n'
            error_text += log_text(self.debug_filenames)
            self.logger.error(error_text)
            raise helpers.SubmitException()
    def close_debug_files(self):
        for file in self.debug_files:
            file.close()
    def finalize(self, returncode):
        self.close_debug_files()
        self.received = self.delegated.finalize()
        if returncode != 0 or self.received is None:
            error_text = self.job.displayname + ' failed to complete\n'
            error_text += '-' * 10 + ' delegator command ' + '-' * 10 + '\n'
            error_text += ' '.join(self.command) + '\n'
            error_text += 'return code {0}\n'.format(returncode)
            error_text += log_text(self.debug_filenames)
            self.logger.error(error_text)

class QsubJob(object):
    """ Encapsulate a running job created using a queueing system's
    qsub submit command called using subprocess
    """
    def __init__(self, ctx, job, modules, qsub_bin, native_spec):
        self.job = job
        self.logger = logging.getLogger('execqueue')
        self.delegated = delegator.delegator(job, os.path.join(job.temps_dir, 'job.dgt'), modules)
        self.command = self.delegated.initialize()
        self.debug_filenames = dict()
        self.debug_filenames['job stdout'] = os.path.join(job.temps_dir, 'job.out')
        self.debug_filenames['job stderr'] = os.path.join(job.temps_dir, 'job.err')
        self.debug_filenames['submit stdout'] = os.path.join(job.temps_dir, 'submit.out')
        self.debug_filenames['submit stderr'] = os.path.join(job.temps_dir, 'submit.err')
        self.debug_files = []
        self.script_filename = os.path.join(job.temps_dir, 'submit.sh')
        with open(self.script_filename, 'w') as script_file:
            script_file.write(' '.join(self.command) + '\n')
        helpers.set_executable(self.script_filename)
        self.submit_command = self.create_submit_command(ctx, job.displayname, self.script_filename, qsub_bin, native_spec, self.debug_filenames['job stdout'], self.debug_filenames['job stderr'])
        try:
            self.debug_files.append(open(self.debug_filenames['submit stdout'], 'w'))
            self.debug_files.append(open(self.debug_filenames['submit stderr'], 'w'))
            self.process = subprocess.Popen(self.submit_command, stdout=self.debug_files[0], stderr=self.debug_files[1])
        except OSError as e:
            self.close_debug_files()
            error_text = self.job.displayname + ' submit failed\n'
            error_text += '-' * 10 + ' submit command ' + '-' * 10 + '\n'
            error_text += ' '.join(self.submit_command) + '\n'
            if type(e) == OSError and e.errno == 2:
                error_text += str(e) + ', ' + self.submit_command[0] + ' not found\n'
            else:
                error_text += str(e) + '\n'
            error_text += log_text(self.debug_filenames)
            self.logger.error(error_text)
            raise helpers.SubmitException()
    def create_submit_command(self, ctx, name, script_filename, qsub_bin, native_spec, stdout_filename, stderr_filename):
        qsub = [qsub_bin]
        qsub += ['-sync', 'y', '-b', 'y']
        qsub += native_spec.format(**ctx).split()
        qsub += ['-N', qsub_format_name(name)]
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
            error_text = self.job.displayname + ' failed to complete\n'
            error_text += '-' * 10 + ' delegator command ' + '-' * 10 + '\n'
            error_text += ' '.join(self.command) + '\n'
            error_text += '-' * 10 + ' submit command ' + '-' * 10 + '\n'
            error_text += ' '.join(self.submit_command) + '\n'
            error_text += 'return code {0}\n'.format(returncode)
            error_text += log_text(self.debug_filenames)
            self.logger.error(error_text)

class SubProcessJobQueue(object):
    """ Abstract class for a queue of jobs run using subprocesses.  Maintains
    a list of running jobs, with the ability to wait for jobs and return
    completed jobs.  Requires override of the create method.
    """
    def __init__(self, modules):
        self.modules = modules
        self.jobs = dict()
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_value, traceback):
        pass
    def create(self, ctx, job):
        raise NotImplementedError()
    def add(self, ctx, job):
        submitted = self.create(ctx, job)
        self.jobs[submitted.process.pid] = submitted
    def wait(self, immediate=False):
        while True:
            if immediate:
                process_id, returncode = os.waitpid(-1, os.WNOHANG)
                if process_id is None:
                    return None, None
            else:
                process_id, returncode = os.wait()
            if process_id in self.jobs:
                submitted = self.jobs[process_id]
                del self.jobs[process_id]
                submitted.finalize(returncode)
                return submitted.job.id, submitted.received
    @property
    def length(self):
        return len(self.jobs)
    @property
    def empty(self):
        return self.length == 0

class LocalJobQueue(SubProcessJobQueue):
    """ Queue of local jobs """
    def create(self, ctx, job):
        return LocalJob(ctx, job, self.modules)

class QsubJobQueue(SubProcessJobQueue):
    """ Queue of qsub jobs """
    def __init__(self, modules, native_spec):
        super(QsubJobQueue, self).__init__(modules)
        self.qsub_bin = helpers.which('qsub')
        self.native_spec = native_spec
        self.local_queue = LocalJobQueue(modules)
    def create(self, ctx, job):
        if ctx.get('local', False):
            return LocalJob(ctx, job, self.modules)
        else:
            return QsubJob(ctx, job, self.modules, self.qsub_bin, self.native_spec)

class AsyncQsubJob(object):
    """ Encapsulate a running job created using a queueing system's
    qsub submit command called using subprocess, and polled using qstat
    """
    def __init__(self, ctx, job, modules, qsub_bin, native_spec):
        self.job = job
        self.logger = logging.getLogger('execqueue')
        self.delegated = delegator.delegator(job, os.path.join(job.temps_dir, 'job.dgt'), modules)
        self.command = self.delegated.initialize()
        self.debug_filenames = dict()
        self.debug_filenames['job stdout'] = os.path.join(job.temps_dir, 'job.out')
        self.debug_filenames['job stderr'] = os.path.join(job.temps_dir, 'job.err')
        self.debug_filenames['submit stdout'] = os.path.join(job.temps_dir, 'submit.out')
        self.debug_filenames['submit stderr'] = os.path.join(job.temps_dir, 'submit.err')
        self.script_filename = os.path.join(job.temps_dir, 'submit.sh')
        with open(self.script_filename, 'w') as script_file:
            script_file.write(' '.join(self.command) + '\n')
        helpers.set_executable(self.script_filename)
        self.submit_command = self.create_submit_command(ctx, job.displayname, self.script_filename, qsub_bin, native_spec, self.debug_filenames['job stdout'], self.debug_filenames['job stderr'])
        try:
            with open(self.debug_filenames['submit stdout'], 'w') as submit_stdout, open(self.debug_filenames['submit stderr'], 'w') as submit_stderr:
                subprocess.check_call(self.submit_command, stdout=submit_stdout, stderr=submit_stderr)
        except Exception as e:
            error_text = self.job.displayname + ' submit failed\n'
            error_text += '-' * 10 + ' submit command ' + '-' * 10 + '\n'
            error_text += ' '.join(self.submit_command) + '\n'
            if type(e) == OSError and e.errno == 2:
                error_text += str(e) + ', ' + self.submit_command[0] + ' not found\n'
            else:
                error_text += str(e) + '\n'
            error_text += log_text(self.debug_filenames)
            self.logger.error(error_text)
            raise helpers.SubmitException()
        with open(self.debug_filenames['submit stdout'], 'r') as submit_stdout:
            self.qsub_job_id = submit_stdout.readline().rstrip().replace('Your job ', '').split(' ')[0]
    def create_submit_command(self, ctx, name, script_filename, qsub_bin, native_spec, stdout_filename, stderr_filename):
        qsub = [qsub_bin]
        qsub += native_spec.format(**ctx).split()
        qsub += ['-N', qsub_format_name(name)]
        qsub += ['-o', stdout_filename]
        qsub += ['-e', stderr_filename]
        qsub += [script_filename]
        return qsub
    def finalize(self):
        self.received = self.delegated.finalize()
        if self.received is None:
            error_text = self.job.displayname + ' failed to complete\n'
            error_text += '-' * 10 + ' delegator command ' + '-' * 10 + '\n'
            error_text += ' '.join(self.command) + '\n'
            error_text += '-' * 10 + ' submit command ' + '-' * 10 + '\n'
            error_text += ' '.join(self.submit_command) + '\n'
            error_text += log_text(self.debug_filenames)
            self.logger.error(error_text)

class QstatJobStatus(object):
    """ Class representing statuses retrieved using qstat """
    def __init__(self, poll_time, max_qstat_failures):
        self.poll_time = poll_time
        self.max_qstat_failures = max_qstat_failures
        self.qstat_bin = helpers.which('qstat')
        self.last_qstat_time = None
        self.qstat_failures = 0
    def update(self):
        self.update_status_cache()
        return self.last_qstat_time is not None
    def invalidate(self):
        self.last_qstat_time = None
    def finished(self, job_id):
        assert self.last_qstat_time is not None
        return job_id not in self.cached_job_status
    def errors(self, job_id):
        assert self.last_qstat_time is not None
        return 'e' in self.cached_job_status.get(job_id, '')
    def update_status_cache(self):
        if self.last_qstat_time is None or time.time() - self.last_qstat_time > self.poll_time:
            try:
                self.cached_job_status = dict(self.qstat_job_status())
                self.last_qstat_time = time.time()
                self.qstat_failures = 0
            except:
                self.qstat_failures += 1
            if self.qstat_failures >= self.max_qstat_failures:
                raise Exception('too many qstat failures')
    def qstat_job_status(self):
        for line in subprocess.check_output([self.qstat_bin]).split('\n'):
            row = line.split()
            try:
                job_id = row[0]
                status = row[4].lower()
                yield job_id, status
            except Exception:
                continue

class AsyncQsubJobQueue(object):
    """ Class for a queue of jobs run using subprocesses.  Maintains
    a list of running jobs, with the ability to wait for jobs and return
    completed jobs.  Requires override of the create method.
    """
    def __init__(self, modules, native_spec, poll_time):
        self.modules = modules
        self.qsub_bin = helpers.which('qsub')
        self.native_spec = native_spec
        self.poll_time = poll_time
        self.qstat = QstatJobStatus(poll_time, 10)
        self.jobs = dict()
        self.local_queue = LocalJobQueue(modules)
    def __enter__(self):
        self.local_queue.__enter__()
        return self
    def __exit__(self, exc_type, exc_value, traceback):
        self.local_queue.__exit__(exc_type, exc_value, traceback)
        for qsub_job_id in self.jobs.iterkeys():
            self.delete_job(qsub_job_id)
    def create(self, ctx, job):
        return AsyncQsubJob(ctx, job, self.modules, self.qsub_bin, self.native_spec)
    def add(self, ctx, job):
        if ctx.get('local', False):
            self.local_queue.add(ctx, job)
        else:
            submitted = self.create(ctx, job)
            self.jobs[submitted.qsub_job_id] = submitted
            self.qstat.invalidate()
    def wait(self):
        while True:
            if not self.local_queue.empty:
                job_id, job = self.local_queue.wait(immediate=True)
                if job_id is not None:
                    return job_id, job
            if len(self.jobs) == 0:
                return None
            try:
                updated = self.qstat.update()
            except Exception as e:
                for qsub_job_id in self.jobs.iterkeys():
                    self.delete_job(qsub_job_id)
                self.jobs.clear()
                raise e
            if updated:
                for qsub_job_id in self.jobs.iterkeys():
                    if self.qstat.errors(qsub_job_id):
                        del self.jobs[qsub_job_id]
                        raise Exception('job ' + str(qsub_job_id) + ' entered error state')
                    if self.qstat.finished(qsub_job_id):
                        submitted = self.jobs[qsub_job_id]
                        del self.jobs[qsub_job_id]
                        submitted.finalize()
                        return submitted.job.id, submitted.received
            time.sleep(self.poll_time)
    def delete_job(self, qsub_job_id):
        try:
            subprocess.call(['qdel', qsub_job_id])
        except:
            pass
    @property
    def length(self):
        return len(self.jobs) + self.local_queue.length
    @property
    def empty(self):
        return self.length == 0

class PbsQstatJobStatus(QstatJobStatus):
    """ Statuses of jobs on a pbs cluster """
    def finished(self, job_id):
        return 'c' in self.cached_job_status.get(job_id, 'c')
    def errors(self, job_id):
        return False

class PbsJobQueue(AsyncQsubJobQueue):
    """ Queue of jobs running on a pbs cluster """
    def __init__(self, modules, native_spec, poll_time):
        super(PbsJobQueue, self).__init__(modules, native_spec, poll_time)
        self.qstat = PbsQstatJobStatus(poll_time, 10)
