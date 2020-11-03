import logging
import subprocess
from random import randint

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


class QstatError(Exception):
    pass


class QstatJobStatus(object):
    """ Class representing statuses retrieved using qstat """

    def __init__(self, qenv, qstat_period=20, max_qstat_failures=10):
        self.qenv = qenv
        self.qstat_period = qstat_period
        self.max_qstat_failures = max_qstat_failures
        self.qstat_attempt_time = None
        self.running_job_status = set()
        self.queued_job_status = set()
        self.error_job_status = set()
        self.qstat_failures = 0
        self.qstat_time = None
        self.logger = logging.getLogger('pypeliner.execqueue')

    def update(self):
        """ Update cached job status after sleeping for remainder of polling time.
        """
        if self.qstat_attempt_time is not None:
            time_since_attempt = time.time() - self.qstat_attempt_time
            sleep_time = max(0.0, self.qstat_period - time_since_attempt)
            time.sleep(sleep_time)
        self.qstat_attempt_time = time.time()
        try:
            self.running_job_status, self.queued_job_status, self.error_job_status = self.get_qstat_job_status(
                self.queued_job_status, self.error_job_status
            )
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
        if self.qstat_time is None:
            return False

        if qsub_time >= self.qstat_time:
            return False

        return qsub_job_id not in self.running_job_status and qsub_job_id not in self.queued_job_status

    def errors(self, qsub_job_id):
        """ Query whether job is in error state.

        Returns:
            bool: job finished, None for no information
        """
        return qsub_job_id in self.error_job_status

    def parse_job_status(self, cmd=None):
        if not cmd:
            cmd = [self.qenv.qstat_bin]

        for line in subprocess.check_output(cmd).split('\n'):
            row = line.split()
            try:
                qsub_job_id = row[0]
                status = row[4].lower()
                yield qsub_job_id, status
            except IndexError:
                continue

    @staticmethod
    def job_errored_out(err_str):
        return 'e' in err_str.lower()

    @property
    def cmd_list_running_jobs(self):
        return [self.qenv.qstat_bin, '-s', 'r']

    def get_qstat_job_status(self, queued_job_status, error_job_status):
        """ Run qstat to obtain job status.

        Returns:
            dict: job id to job status dictionary
        """

        running_job_status = set()

        # 33% chance of running full bjobs to update queued dict
        # lowers load in case too many jobs are in queue
        if randint(0, 2) == 2:
            queued_job_status = set()
            error_job_status = set()
            for qsub_job_id, status_code in self.parse_job_status():
                if status_code == 'pend':
                    queued_job_status.add(qsub_job_id)
                elif self.job_errored_out(status_code):
                    error_job_status.add(qsub_job_id)
                    if qsub_job_id in queued_job_status:
                        queued_job_status.remove(qsub_job_id)
                    if qsub_job_id in running_job_status:
                        running_job_status.remove(qsub_job_id)
                else:
                    running_job_status.add(qsub_job_id)
                    if qsub_job_id in queued_job_status:
                        queued_job_status.remove(qsub_job_id)
        else:
            for qsub_job_id, status_code in self.parse_job_status(cmd=self.cmd_list_running_jobs):
                running_job_status.add(qsub_job_id)
                if qsub_job_id in queued_job_status:
                    queued_job_status.remove(qsub_job_id)

        return running_job_status, queued_job_status, error_job_status


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
            with open(self.qacct_stdout_filename, 'w') as qacct_stdout, open(self.qacct_stderr_filename,
                                                                             'w') as qacct_stderr:
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


class QsubWrapper(object):
    """class to submit jobs in SGE"""

    def __init__(self, qenv, ctx, name, script_filename, native_spec, job_stdout_filename,
                 job_stderr_filename, submit_stdout_filename, submit_stderr_filename, ):
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
            with open(self.submit_stdout_filename, 'w') as submit_stdout, open(self.submit_stderr_filename,
                                                                               'w') as submit_stderr:
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
