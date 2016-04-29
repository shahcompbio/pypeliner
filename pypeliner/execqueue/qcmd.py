import logging
import subprocess
import time

import pypeliner.helpers


class QEnv(object):
    """ Paths to qsub, qstat, qdel """
    def __init__(self):
        self.qsub_bin = pypeliner.helpers.which('qsub')
        self.qstat_bin = pypeliner.helpers.which('qstat')
        self.qacct_bin = pypeliner.helpers.which('qacct')
        self.qdel_bin = pypeliner.helpers.which('qdel')


class QstatError(Exception):
    pass


class QstatJobStatus(object):
    """ Class representing statuses retrieved using qstat """
    def __init__(self, qenv, qstat_period=20, max_qstat_failures=10):
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

    def check(self):
        """ Run qacct to obtain finished job info.
        """
        try:
            with open(self.qacct_stdout_filename, 'w') as qacct_stdout, open(self.qacct_stderr_filename, 'w') as qacct_stderr:
                subprocess.check_call([self.qenv.qacct_bin, '-j', self.job_id], stdout=qacct_stdout, stderr=qacct_stderr)
        except subprocess.CalledProcessError:
            self.qacct_failures += 1
            if self.qacct_failures > self.max_qacct_failures:
                raise QacctError()
            else:
                return None

        qacct_results = dict()

        with open(self.qacct_stdout_filename, 'r') as qacct_file:
            for line in qacct_file:
                try:
                    key, value = line.split()
                except ValueError:
                    continue
                qacct_results[key] = value

        self.results = qacct_results


