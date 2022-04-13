from __future__ import absolute_import, print_function

import os
import dill as pickle
import logging
import yaml
import time
import pypeliner.execqueue.base
import pypeliner.tests.jobs
import pypeliner.helpers
import pypeliner.contrib.aws.helpers as aws_helpers

from .objectstorage import AwsSimpleStorageService

from .aws_batch import AwsBatch

class AwsJobQueue(object):
    """ Azure batch job queue.
    """
    debug_filenames = {
        'job stderr': 'stderr.txt',
        'job stdout': 'stdout.txt',
    }

    def __init__(self, config_filename=None, **kwargs):
        aws_helpers.set_aws_logging_filters()

        self.run_id = aws_helpers.random_string(8)

        with open(config_filename) as f:
            self.config = yaml.safe_load(f)

        self.logger = logging.getLogger('pypeliner.execqueue.aws_batch')

        self.logger.info('creating blob client')

        self.s3client = AwsSimpleStorageService()

        self.batch_client = AwsBatch()

        self.logger.info('creating task container')

        self.job_storage_bucket = self.config['storage_bucket']
        self.s3client.create_bucket(self.job_storage_bucket)

        self.job_names = {}
        self.job_ids = {}
        self.job_temps_dir = {}

        self.job_blobname_prefix = {}
        self.running_job_ids = set()
        self.completed_job_ids = set()
        self.job_names = {}
        self.job_definitions = {}

    def __enter__(self):
        """
        Create the compute environments and job queues if needed
        """
        self.batch_client.create_compute_environments(self.config)
        # need to add a little bit of delay, to let AWS create env
        time.sleep(5)
        self.batch_client.create_job_queues(self.config, self.run_id)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        kill all running jobs
        """
        reason = '{}:{}'.format(str(exc_type), str(exc_value))
        for job_id in self.running_job_ids:
            self.batch_client.terminate_job(job_id, reason)

        self.batch_client.delete_job_queues(self.config, self.run_id)
        self.batch_client.delete_compute_environments(self.config)

    def _create_error_text(self, job_temp_dir, hostname=None):
        """
        create error if the delegator fails and doesnt generate
        after pickle
        :param job_temp_dir: temp dir used for saving before and after pickles
        :type job_temp_dir: str
        :param hostname: machine hostname where the job was executed
        :type hostname: str
        :return: error explanation
        :rtype: str
        """
        error_text = 'failed to execute'

        if hostname:
            error_text += ' on node {}'.format(hostname)

        error_text = [error_text]

        for debug_type, debug_filename in self.debug_filenames.items():
            debug_filename = os.path.join(job_temp_dir, debug_filename)

            if not os.path.exists(debug_filename):
                error_text += [debug_type + ': missing']

                continue

            with open(debug_filename, 'r') as debug_file:
                error_text += [debug_type + ':']

                for line in debug_file:
                    error_text += ['\t' + line.rstrip()]

        return '\n'.join(error_text)

    def send(self, ctx, name, sent, temps_dir):
        """
        Add a job to the queue.
        :param ctx: context of job, mem etc.
        :type ctx: dict
        :param name: unique name for the job
        :type name: str
        :param sent: callable object
        :type sent: callable
        :param temps_dir: unique path for job temps
        :type temps_dir: str
        """
        self.job_temps_dir[name] = temps_dir

        job_name = self.batch_client.batch_compatible_format(name)
        # sanity check, remove this later
        jobs = [job['jobName'] for job in self.batch_client.list_all_jobs(self.config, self.run_id)]
        if job_name in jobs:
            raise Exception()

        self.job_blobname_prefix[name] = 'output_' + job_name

        # create pickle
        job_before_filename = os.path.join(temps_dir, 'job.pickle')
        with open(job_before_filename, 'wb') as before:
            pickle.dump(sent, before)

        # upload pickle to s3
        job_before_blobname = os.path.join(
            self.job_blobname_prefix[name],
            'job.pickle')
        self.s3client.upload_from_file(
            job_before_filename, job_before_blobname, self.job_storage_bucket
        )

        workdir = os.path.join(self.config['workdir'], job_name)
        command = ['aws_fetch_run', self.job_storage_bucket, job_before_blobname, workdir]

        context_config = pypeliner.helpers.GlobalState.get("context_config")

        defname = self.job_definitions.get(ctx.get("image"))
        if not defname:
            defname = self.batch_client.get_matching_job_defn(self.config, ctx, context_config)
            self.job_definitions[ctx.get("image")] = defname

        self.logger.debug('job {} submitted with job name: {}'.format(name, job_name))

        job_id = self.batch_client.submit_job(job_name, defname, ctx, command, self.config, self.run_id)

        self.running_job_ids.add(job_id)
        self.job_names[job_id] = name
        self.job_ids[name] = job_id

    def wait(self, immediate=False):
        """
        Wait for a job to finish.
        :param immediate: do not wait if no job has finished
        :type immediate: bool
        :return: job name
        :rtype: str
        """
        while True:
            jobs = self.batch_client.list_all_jobs(
                self.config, self.run_id, status=["FAILED", "SUCCEEDED"]
            )

            for jobsummary in jobs:
                jobid = jobsummary['jobId']
                self.completed_job_ids.add(jobid)

            for job_id in self.running_job_ids.intersection(
                    self.completed_job_ids):
                return self.job_names[job_id]

    def receive(self, name):
        """
        receive finished job
        :param name: name of the job to retrieve
        :type name: str
        :return: received object
        :rtype: object
        """
        blobname_prefix = self.job_blobname_prefix.pop(name)
        job_id = self.job_ids.pop(name)
        self.job_names.pop(job_id)
        temps_dir = self.job_temps_dir.pop(name)
        self.running_job_ids.remove(job_id)

        # check aws batch for exitcode
        exitcode = self.batch_client.get_job_exitcode(job_id)
        if not exitcode == 0:
            cloudwatch_log = self.batch_client.get_cloudwatch_error(job_id)
            if cloudwatch_log:
                raise pypeliner.execqueue.base.ReceiveError(
                    cloudwatch_log)
            else:
                raise pypeliner.execqueue.base.ReceiveError(
                    self._create_error_text(temps_dir))

        job_after_filename = os.path.join(temps_dir, 'job.pickle.after')

        try:
            self.s3client.download_to_path(
                os.path.join(blobname_prefix, 'job.pickle.after'),
                self.config['storage_bucket'],
                os.path.join(temps_dir, 'job.pickle.after'))
        except:
            raise pypeliner.execqueue.base.ReceiveError(
                self._create_error_text(temps_dir))

        with open(job_after_filename, 'rb') as job_after_file:
            received = pickle.load(job_after_file)

        if received is None or not received.started:
            raise pypeliner.execqueue.base.ReceiveError(
                self._create_error_text(temps_dir)
            )

        received.hostname = 'aws_ecs'

        return received

    @property
    def length(self):
        """
        :return: Number of jobs in the queue.
        :rtype: int
        """
        return len(self.job_names)

    @property
    def empty(self):
        """
        :return: True if queue is empty
        :rtype: bool
        """
        return self.length == 0


if __name__ == "__main__":
    exc_dir = 'tempazure'

    logging.basicConfig(level=logging.DEBUG)

    with AwsJobQueue() as queue:
        job = pypeliner.tests.jobs.TestJob()
        pypeliner.helpers.makedirs(exc_dir)

        queue.send({}, 'testjob1', job, exc_dir)

        finished_name = queue.wait()

        job2 = queue.receive(finished_name)
