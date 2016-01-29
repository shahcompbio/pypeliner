import os
import subprocess
import time
import logging

import helpers
import delegator


class JobQueue(object):
    """ Abstract class for a queue of jobs.
    """
    def send(self, ctx, name, sent, temps_dir):
        """ Add a job to the queue.

        Args:
            ctx (dict): context of job, mem etc.
            name (str): unique name for the job
            sent (callable): callable object
            temps_dir (str): unique path for strong job temps

        """
        raise NotImplementedError()

    def wait(self, immediate=False):
        """ Wait for a job to finish.

        KwArgs:
            immediate (bool): do not wait if no job has finished

        Returns:
            str: job name

        """
        raise NotImplementedError()

    def receive(self, name):
        """ Receive finished job.

        Args:
            name (str): name of job to retrieve

        Returns:
            object: received object

        """
        raise NotImplementedError()

    @property
    def length(self):
        """ Number of jobs in the queue. """
        raise NotImplementedError()

    @property
    def empty(self):
        """ Queue is empty. """
        raise NotImplementedError()


class SubmitError(Exception):
    pass

class ReceiveError(Exception):
    pass

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
    return name.strip('/').rstrip('/').replace('/','.').replace(':','_')

class LocalJob(object):
    """ Encapsulate a running job called locally by subprocess """
    def __init__(self, ctx, name, sent, temps_dir, modules):
        self.name = name
        self.logger = logging.getLogger('execqueue')
        self.delegated = delegator.delegator(sent, os.path.join(temps_dir, 'job.dgt'), modules)
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
            error_text += log_text(self.debug_filenames)
            self.logger.error(error_text)
            raise SubmitError()
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
            error_text += log_text(self.debug_filenames)
            self.logger.error(error_text)
            raise ReceiveError()

class QsubJob(object):
    """ Encapsulate a running job created using a queueing system's
    qsub submit command called using subprocess
    """
    def __init__(self, ctx, name, sent, temps_dir, modules, qsub_bin, native_spec):
        self.name = name
        self.logger = logging.getLogger('execqueue')
        self.delegated = delegator.delegator(sent, os.path.join(temps_dir, 'job.dgt'), modules)
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
        helpers.set_executable(self.script_filename)
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
            error_text += log_text(self.debug_filenames)
            self.logger.error(error_text)
            raise SubmitError()
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
            error_text = self.name + ' failed to complete\n'
            error_text += '-' * 10 + ' delegator command ' + '-' * 10 + '\n'
            error_text += ' '.join(self.command) + '\n'
            error_text += '-' * 10 + ' submit command ' + '-' * 10 + '\n'
            error_text += ' '.join(self.submit_command) + '\n'
            error_text += 'return code {0}\n'.format(returncode)
            error_text += log_text(self.debug_filenames)
            self.logger.error(error_text)
            raise ReceiveError()

class SubProcessJobQueue(JobQueue):
    """ Abstract class for a queue of jobs run using subprocesses.  Maintains
    a list of running jobs, with the ability to wait for jobs and return
    completed jobs.  Requires override of the create method.
    """
    def __init__(self, modules):
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
                process_id, returncode = os.wait()
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

class LocalJobQueue(SubProcessJobQueue):
    """ Queue of local jobs """
    def create(self, ctx, name, sent, temps_dir):
        return LocalJob(ctx, name, sent, temps_dir, self.modules)

class QsubJobQueue(SubProcessJobQueue):
    """ Queue of qsub jobs """
    def __init__(self, modules, native_spec):
        super(QsubJobQueue, self).__init__(modules)
        self.qsub_bin = helpers.which('qsub')
        self.native_spec = native_spec
        self.local_queue = LocalJobQueue(modules)
    def create(self, ctx, name, sent, temps_dir):
        if ctx.get('local', False):
            return LocalJob(ctx, name, sent, temps_dir, self.modules)
        else:
            return QsubJob(ctx, name, sent, temps_dir, self.modules, self.qsub_bin, self.native_spec)

class QacctError(Exception):
    pass

class AsyncQsubJob(object):
    """ Encapsulate a running job created using a queueing system's
    qsub submit command called using subprocess, and polled using qstat
    """
    def __init__(self, ctx, name, sent, temps_dir, modules, qenv, native_spec, qstat_job_status, max_qacct_failures=100):
        self.name = name
        self.qenv = qenv
        self.qstat_job_status = qstat_job_status
        self.max_qacct_failures = max_qacct_failures
        self.qsub_job_id = None
        self.qacct_results = None
        self.qacct_failures = 0
        self.logger = logging.getLogger('execqueue')

        self.delegated = delegator.delegator(sent, os.path.join(temps_dir, 'job.dgt'), modules)
        self.command = self.delegated.initialize()

        self.debug_filenames = dict()
        self.debug_filenames['job stdout'] = os.path.join(temps_dir, 'job.out')
        self.debug_filenames['job stderr'] = os.path.join(temps_dir, 'job.err')
        self.debug_filenames['submit stdout'] = os.path.join(temps_dir, 'submit.out')
        self.debug_filenames['submit stderr'] = os.path.join(temps_dir, 'submit.err')
        self.debug_filenames['qacct stdout'] = os.path.join(temps_dir, 'qacct.out')
        self.debug_filenames['qacct stderr'] = os.path.join(temps_dir, 'qacct.err')
        for filename in self.debug_filenames.itervalues():
            helpers.saferemove(filename)

        self.script_filename = os.path.join(temps_dir, 'submit.sh')
        with open(self.script_filename, 'w') as script_file:
            script_file.write(' '.join(self.command) + '\n')
        helpers.set_executable(self.script_filename)

        self.submit_command = self.create_submit_command(ctx, name, self.script_filename, self.qenv.qsub_bin, native_spec, self.debug_filenames['job stdout'], self.debug_filenames['job stderr'])

        try:
            with open(self.debug_filenames['submit stdout'], 'w') as submit_stdout, open(self.debug_filenames['submit stderr'], 'w') as submit_stderr:
                subprocess.check_call(self.submit_command, stdout=submit_stdout, stderr=submit_stderr)
        except Exception as e:
            raise SubmitError(self.create_error_text('submit error ' + str(e)))

        with open(self.debug_filenames['submit stdout'], 'r') as submit_stdout:
            self.qsub_job_id = submit_stdout.readline().rstrip().replace('Your job ', '').split(' ')[0]

        self.qsub_time = time.time()

    def create_submit_command(self, ctx, name, script_filename, qsub_bin, native_spec, stdout_filename, stderr_filename):
        qsub = [qsub_bin]
        qsub += native_spec.format(**ctx).split()
        qsub += ['-N', qsub_format_name(name)]
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

        if self.qacct_results is None:
            try:
                self.qacct_results = self.get_qacct_results()
            except QacctError:
                return True
        
        return self.qacct_results is not None

    def finalize(self):
        """ Finalize a job after remote run.
        """
        assert self.finished

        if self.qstat_job_status.errors(self.qsub_job_id):
            self.delete()
            raise ReceiveError(self.create_error_text('job error'))

        if self.qacct_results is None:
            raise ReceiveError(self.create_error_text('qacct error'))

        if self.qacct_results['exit_status'] != '0':
            raise ReceiveError(self.create_error_text('qsub error'))

        self.received = self.delegated.finalize()

        if self.received is None:
            raise ReceiveError(self.create_error_text('receive error'))

    def get_qacct_results(self):
        """ Run qacct to obtain finished job info.

        Returns:
            dict: key value mapping of job info
        """
        try:
            with open(self.debug_filenames['qacct stdout'], 'w') as qacct_stdout, open(self.debug_filenames['qacct stderr'], 'w') as qacct_stderr:
                subprocess.check_call([self.qenv.qacct_bin, '-j', self.qsub_job_id], stdout=qacct_stdout, stderr=qacct_stderr)
        except subprocess.CalledProcessError:
            self.qacct_failures += 1
            if self.qacct_failures > self.max_qacct_failures:
                raise QacctError()
            else:
                return None

        qacct_results = dict()

        with open(self.debug_filenames['qacct stdout'], 'r') as qacct_file:
            for line in qacct_file:
                try:
                    key, value = line.split()
                except ValueError:
                    continue
                qacct_results[key] = value

        return qacct_results

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

        if self.qacct_results is not None:
            error_text += ['memory consumed: ' + self.qacct_results['maxvmem']]
            error_text += ['job exit status: ' + self.qacct_results['exit_status']]

        for debug_type, debug_filename in self.debug_filenames.iteritems():
            if not os.path.exists(debug_filename):
                error_text += [debug_type + ': missing']
                continue
            with open(debug_filename, 'r') as debug_file:
                error_text += [debug_type + ':']
                for line in debug_file:
                    error_text += ['\t' + line.rstrip()]

        return '\n'.join(error_text)


class QstatError(Exception):
    pass

class QstatJobStatus(object):
    """ Class representing statuses retrieved using qstat """
    def __init__(self, qenv, qstat_period, max_qstat_failures=10):
        self.qenv = qenv
        self.qstat_period = qstat_period
        self.max_qstat_failures = max_qstat_failures
        self.qstat_attempt_time = None
        self.cached_job_status = None
        self.qstat_failures = 0
        self.qstat_time = None
        self.logger = logging.getLogger('execqueue')

    def update(self):
        """ Update cached job status after sleeping for remainder of polling time.
        """
        if self.qstat_attempt_time is not None:
            time_since_attempt = time.time() - self.qstat_attempt_time
            sleep_time = max(0, self.qstat_period - time_since_attempt)
            time.sleep(sleep_time)
        self.qstat_attempt_time = time.time()
        try:
            self.cached_job_status = self.get_qstat_job_status()
            self.qstat_failures = 0
            self.qstat_time = time.time()
        except:
            self.logger.exception('Unable to qstat')
            self.qstat_failures += 1
        if self.qstat_failures >= self.max_qstat_failures:
            raise QstatError('too many consecutive qstat failures')

    def finished(self, qsub_job_id, qsub_time):
        """ Query whether job is finished.

        Args:
            qsub_job_id (str): job id returned by qsub
            qsub_time (float): time job was submitted

        Returns:
            bool: job finished, None for no information
        """
        if self.cached_job_status is None:
            return False

        if qsub_time >= self.qstat_time:
            return False

        return qsub_job_id not in self.cached_job_status

    def errors(self, qsub_job_id):
        """ Query whether job is in error state.

        Returns:
            bool: job finished, None for no information
        """
        if self.cached_job_status is None:
            return None

        return 'e' in self.cached_job_status.get(qsub_job_id, '').lower()

    def get_qstat_job_status(self):
        """ Run qstat to obtain job statues.

        Returns:
            dict: job id to job status dictionary
        """
        job_status = dict()

        for line in subprocess.check_output([self.qenv.qstat_bin]).split('\n'):
            row = line.split()
            try:
                qsub_job_id = row[0]
                status = row[4].lower()
                job_status[qsub_job_id] = status
            except IndexError:
                continue

        return job_status

class QEnv(object):
    """ Paths to qsub, qstat, qdel """
    def __init__(self):
        self.qsub_bin = helpers.which('qsub')
        self.qstat_bin = helpers.which('qstat')
        self.qacct_bin = helpers.which('qacct')
        self.qdel_bin = helpers.which('qdel')

class AsyncQsubJobQueue(JobQueue):
    """ Class for a queue of jobs run using subprocesses.  Maintains
    a list of running jobs, with the ability to wait for jobs and return
    completed jobs.  Requires override of the create method.
    """
    def __init__(self, modules, native_spec, qstat_period):
        self.modules = modules
        self.qenv = QEnv()
        self.native_spec = native_spec
        self.qstat = QstatJobStatus(self.qenv, qstat_period)
        self.jobs = dict()
        self.name_islocal = dict()
        self.local_queue = LocalJobQueue(modules)

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
        
class PbsQstatJobStatus(QstatJobStatus):
    """ Statuses of jobs on a pbs cluster """
    def finished(self, job_id):
        return 'c' in self.cached_job_status.get(job_id, 'c')
    def errors(self, job_id):
        return False

class PbsJobQueue(AsyncQsubJobQueue):
    """ Queue of jobs running on a pbs cluster """
    def __init__(self, modules, native_spec, qstat_period):
        super(PbsJobQueue, self).__init__(modules, native_spec, qstat_period)
        self.qstat = PbsQstatJobStatus(qstat_period, 10)
