import logging
import subprocess

import pypeliner.helpers
import time


class QEnv(object):
    """ Paths to qsub, qstat, qdel """

    def __init__(self):
        self.qsub_bin = pypeliner.helpers.which('qsub')
        self.qstat_bin = pypeliner.helpers.which('qstat')
        self.qacct_bin = pypeliner.helpers.which('qacct')
        self.qdel_bin = pypeliner.helpers.which('qdel')


class LsfEnv(object):
    """ Paths to qsub, qstat, qdel in LSF"""

    def __init__(self):
        self.qsub_bin = pypeliner.helpers.which('bsub')
        self.qstat_bin = pypeliner.helpers.which('bjobs')
        self.qacct_bin = pypeliner.helpers.which('bhist')
        self.qdel_bin = pypeliner.helpers.which('bkill')

class SlurmEnv(object):
    """ Paths to qsub, qstat, qdel in LSF"""

    def __init__(self):
        self.qsub_bin = pypeliner.helpers.which('sbatch')
        self.qstat_bin = pypeliner.helpers.which('squeue')
        self.qacct_bin = pypeliner.helpers.which('sacct')
        self.qdel_bin = pypeliner.helpers.which('scancel')


class QstatError(Exception):
    pass


class QstatJobStatus(object):
    """ Class representing statuses retrieved using qstat """

    def __init__(self, qenv, qstat_period=30, max_qstat_failures=10):
        self.qenv = qenv
        self.qstat_period = qstat_period
        self.max_qstat_failures = max_qstat_failures
        self.qstat_attempt_time = None
        self.cached_job_status = None
        self.qstat_failures = 0
        self.qstat_time = None
        self.logger = logging.getLogger('pypeliner.execqueue')

    def update(self, jobs):
        """ Update cached job status after sleeping for remainder of polling time.
        """
        if self.qstat_attempt_time is not None:
            time_since_attempt = time.time() - self.qstat_attempt_time
            sleep_time = max(0, self.qstat_period - time_since_attempt)
            time.sleep(sleep_time)
        self.qstat_attempt_time = time.time()
        try:
            self.cached_job_status = self.get_qstat_job_status(jobs)
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

    def get_qstat_job_status(self, jobs):
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


class QacctError(Exception):
    pass


class QacctWrapper(object):
    def __init__(self, qenv, job_id, qacct_stdout_filename, qacct_stderr_filename, max_qacct_failures=100):
        self.qenv = qenv
        self.job_id = job_id
        self.qacct_stdout_filename = qacct_stdout_filename
        self.qacct_stderr_filename = qacct_stderr_filename
        self.max_qacct_failures = max_qacct_failures
        self.qacct_failures = 0

        self.results = None

    def parse_qacct(self):
        """parse job completion status report"""
        qacct_results = dict()

        with open(self.qacct_stdout_filename, 'r') as qacct_file:
            for line in qacct_file:
                try:
                    key, value = line.split()
                except ValueError:
                    continue
                qacct_results[key] = value
        return qacct_results

    def check(self):
        """ Run qacct to obtain finished job info.
        """
        try:
            with open(self.qacct_stdout_filename, 'w') as qacct_stdout, \
                    open(self.qacct_stderr_filename, 'w') as qacct_stderr:
                subprocess.check_call([self.qenv.qacct_bin, '-j', self.job_id], stdout=qacct_stdout,
                                      stderr=qacct_stderr)
        except subprocess.CalledProcessError:
            self.qacct_failures += 1
            if self.qacct_failures > self.max_qacct_failures:
                raise QacctError()
            else:
                return None

        self.results = self.parse_qacct()


class LsfacctWrapper(QacctWrapper):
    """class to check job status after completion in LSF"""

    def parse_qacct(self):
        """parse job completion status report"""
        qacct_results = dict()

        with open(self.qacct_stdout_filename, 'r') as qacct_file:
            for line in qacct_file:
                line = line.strip()
                if 'Done successfully' in line and 'Post job process' not in line:
                    qacct_results['exit_code'] = 0
                if 'Exited with exit code' in line:
                    line = line[line.find('Exited with exit code'):].split('.')[0]
                    line = line.replace('Exited with exit code', '')
                    qacct_results['exit_code'] = line
                if 'MAX MEM' in line:
                    line = line.split(';')[0].split(':')
                    assert line[0] == 'MAX MEM'
                    qacct_results['maxvmem'] = line[1]
        return qacct_results


class SlurmacctWrapper(QacctWrapper):
    """class to check job status after completion in LSF"""

    def parse_qacct(self):
        """parse job completion status report"""
        qacct_results = dict()

        with open(self.qacct_stdout_filename, 'r') as qacct_file:
            lines = [line.strip().split('|') for line in qacct_file]
            assert len(lines) == 2
            header = {v: i for i, v in enumerate(lines[0])}
            exit_code = lines[1][header['ExitCode']]
            status = lines[1][header['State']]
            qacct_results[exit_code] = exit_code
            assert status == 'COMPLETED' or status == 'FAILED'
        return qacct_results


class QsubWrapper(object):
    """class to submit jobs in SGE"""

    def __init__(self, qenv, ctx, name, script_filename, native_spec, job_stdout_filename,
                 job_stderr_filename, submit_stdout_filename, submit_stderr_filename):
        self.qsub_job_id = None
        self.qenv = qenv
        self.ctx = ctx
        self.name = name
        self.script_filename = script_filename
        self.native_spec = native_spec
        self.job_stdout_filename = job_stdout_filename
        self.job_stderr_filename = job_stderr_filename
        self.submit_stdout_filename = submit_stdout_filename
        self.submit_stderr_filename = submit_stderr_filename
        self.submit_command = self.create_submit_command()

    def create_submit_command(self):
        """generate the job submission command"""
        qsub = [self.qenv.qsub_bin]
        qsub += self.native_spec.format(**self.ctx).split()
        qsub += ['-N', pypeliner.execqueue.utils.qsub_format_name(self.name)]
        qsub += ['-o', self.job_stdout_filename]
        qsub += ['-e', self.job_stderr_filename]
        qsub += [self.script_filename]
        return qsub

    def extract_job_id(self):
        """parse the job id from job submission command output"""
        with open(self.submit_stdout_filename, 'r') as submit_stdout:
            return submit_stdout.readline().rstrip().replace('Your job ', '').split(' ')[0]

    def submit_job(self):
        """submit job and return job id"""
        try:
            with open(self.submit_stdout_filename, 'w') as submit_stdout, \
                    open(self.submit_stderr_filename, 'w') as submit_stderr:
                subprocess.check_call(self.submit_command, stdout=submit_stdout, stderr=submit_stderr)
        except Exception as e:
            raise pypeliner.execqueue.base.SubmitError('submit error ' + str(e))

        return self.extract_job_id()


class LsfsubWrapper(QsubWrapper):
    """class to submit jobs in LSF"""

    def create_submit_command(self):
        """generate the job submission command"""
        qsub = [self.qenv.qsub_bin]
        qsub += self.native_spec.format(**self.ctx).split()
        qsub += ['-J', pypeliner.execqueue.utils.qsub_format_name(self.name)]
        qsub += ['-o', self.job_stdout_filename]
        qsub += ['-eo', self.job_stderr_filename]
        qsub += [self.script_filename]
        return qsub

    def extract_job_id(self):
        """parse the job id from job submission command output"""
        with open(self.submit_stdout_filename, 'r') as submit_stdout:
            return submit_stdout.readline().rstrip().replace("<", "\t").replace(">", '\t').split('\t')[1]


class SlurmsubWrapper(QsubWrapper):
    """class to submit jobs in LSF"""

    def create_submit_command(self):
        """generate the job submission command"""
        qsub = [self.qenv.qsub_bin]
        qsub += self.native_spec.format(**self.ctx).split()
        qsub += ['-J', pypeliner.execqueue.utils.qsub_format_name(self.name)]
        qsub += ['-o', self.job_stdout_filename]
        qsub += ['-e', self.job_stderr_filename]
        qsub += [self.script_filename]
        return qsub

    def extract_job_id(self):
        """parse the job id from job submission command output"""
        with open(self.submit_stdout_filename, 'r') as submit_stdout:
            jobid = submit_stdout.readline().rstrip().replace("Submitted batch job", "")
            return int(jobid)
