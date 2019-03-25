from __future__ import print_function
import datetime
import os
import time
import pickle
import logging
import yaml

import pypeliner.execqueue.base

import azure.storage.blob as azureblob
import azure.batch.batch_service_client as batch
import azure.batch.models as batchmodels

from azure.profiles import KnownProfiles

from azure.common.credentials import ServicePrincipalCredentials

KnownProfiles.default.use(KnownProfiles.latest)

import pypeliner.contrib.azure.helpers as azure_helpers

import pypeliner.tests.jobs
import pypeliner.helpers


class AzureJobQueue(object):
    """ Azure batch job queue.
    """

    def __init__(self, config_filename=None, **kwargs):
        azure_helpers.set_azure_logging_filters()
        batch_account_url = os.environ['AZURE_BATCH_URL']
        client_id = os.environ['CLIENT_ID']
        tenant_id = os.environ['TENANT_ID']
        secret_key = os.environ['SECRET_KEY']
        keyvault_account = os.environ['AZURE_KEYVAULT_ACCOUNT']

        with open(config_filename) as f:
            self.config = yaml.load(f)

        storage_account_name = self.config['pypeliner_storage_account']
        storage_account_key = azure_helpers.get_storage_account_key(
            storage_account_name, client_id, secret_key,
            tenant_id, keyvault_account)

        self.logger = logging.getLogger('pypeliner.execqueue.azure_batch')

        if 'run_id' in self.config:
            self.run_id = self.config['run_id']
        else:
            self.run_id = azure_helpers.random_string(8)

        self.logger.info('creating blob client')

        self.blob_client = azureblob.BlockBlobService(
            account_name=storage_account_name,
            account_key=storage_account_key)

        self.credentials = ServicePrincipalCredentials(client_id=client_id,
                                                       secret=secret_key,
                                                       tenant=tenant_id,
                                                       resource="https://batch.core.windows.net/")

        self.logger.info('creating batch client')

        self.batch_client = batch.BatchServiceClient(
            self.credentials,
            batch_account_url
        )

        self.logger.info('creating task container')

        self.container_name = self.config['storage_container_name']
        self.blob_client.create_container(self.container_name)

        self.compute_start_commands = {id: pool['compute_start_commands'] for id,pool in self.config['pools'].items()}
        self.compute_finish_commands = {id: pool['compute_finish_commands'] for id,pool in self.config['pools'].items()}

        self.no_delete_pool = self.config.get('no_delete_pool', False)
        self.no_delete_job = self.config.get('no_delete_job', False)

        self.job_names = {}
        self.job_task_ids = {}
        self.job_temps_dir = {}
        self.job_blobname_prefix = {}

        self.mapping_from_tasks_to_job = {}

        self.most_recent_transition_time = None
        self.completed_task_ids = set()
        self.running_task_ids = set()

        self.active_pools = set()
        self.active_jobs = set()

    debug_filenames = {
        'job stderr': 'stderr.txt',
        'job stdout': 'stdout.txt',
        'upload err': 'fileuploaderr.txt',
        'upload out': 'fileuploadout.txt',
    }


    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.logger.info('tear down')

        for pool_id in self.active_pools:

            if not self.no_delete_pool:
                self.logger.info("deleting pool {}".format(pool_id))
                try:
                    self.batch_client.pool.delete(pool_id)
                except Exception as e:
                    print(e)

        for job_id in self.active_jobs:
            if not self.no_delete_job:
                self.logger.info("deleting job {}".format(job_id))
                try:
                    self.batch_client.job.delete(job_id)
                except Exception as e:
                    print(e)

        for job_id in self.active_jobs:
            if not self.no_delete_job:
                azure_helpers.wait_for_job_deletion(self.batch_client, job_id, self.logger)

    def get_jobid(self, pool_id):
        return 'pypeliner_{}_{}'.format(pool_id, self.run_id)

    def prep_pools_and_jobs(self, pool_id, job_id):
        """
        start a pool and job for the task if it hasnt been initialized already
        :param str pool_id: azure batch pool id
        :param str job_id: azure batch job id
        """

        if pool_id not in self.active_pools and \
                not self.batch_client.pool.exists(pool_id):
            pool_params = self.config['pools'][pool_id]
            self.logger.info("creating pool {}".format(pool_id))
            azure_helpers.create_pool(
                self.batch_client,
                self.blob_client,
                pool_id,
                pool_params)
            self.active_pools.add(pool_id)

        job_ids = set([job.id for job in self.batch_client.job.list()])
        if job_id not in self.active_jobs and job_id not in job_ids:
            self.logger.info("creating job {}".format(job_id))
            azure_helpers.create_job(
                self.batch_client,
                job_id,
                pool_id)
            self.active_jobs.add(job_id)

            for task in self.batch_client.task.list(job_id):
                self.batch_client.task.delete(job_id, task.id)

        return job_id

    def send(self, ctx, name, sent, temps_dir):
        """ Add a job to the queue.

        Args:
            ctx (dict): context of job, mem etc.
            name (str): unique name for the job
            sent (callable): callable object
            temps_dir (str): unique path for strong job temps

        """
        pool_id = azure_helpers.find_pool(self.config['pools'], ctx)

        job_id = self.get_jobid(pool_id)

        self.logger.debug(
            'assigning job {} to pool {}'.format(name, pool_id)
        )

        self.prep_pools_and_jobs(pool_id, job_id)

        task_id = azure_helpers.get_task_id(name)

        self.logger.info(
            'assigning task_id {} to job {}'.format(
                task_id,
                name))

        self.mapping_from_tasks_to_job[task_id] = job_id
        self.job_names[task_id] = name
        self.job_task_ids[name] = task_id
        self.job_temps_dir[name] = temps_dir
        self.job_blobname_prefix[name] = 'output_' + task_id

        job_before_file_path = 'job.pickle'
        job_before_filename = os.path.join(temps_dir, job_before_file_path)
        job_before_blobname = os.path.join(
            self.job_blobname_prefix[name],
            job_before_file_path)

        sent.version = pypeliner.__version__

        with open(job_before_filename, 'wb') as before:
            pickle.dump(sent, before)

        job_before_file = azure_helpers.create_blob_batch_resource(
            self.blob_client, self.container_name, job_before_filename, job_before_blobname, job_before_file_path)

        job_after_file_path = 'job_result.pickle'
        job_after_filename = os.path.join(temps_dir, job_after_file_path)
        job_after_blobname = os.path.join(
            self.job_blobname_prefix[name],
            job_after_file_path)

        # Delete any previous job result file locally and in blob
        azure_helpers.delete_from_container(
            self.blob_client, self.container_name, job_after_blobname)
        try:
            os.remove(job_after_filename)
        except OSError:
            pass

        # Delete any previous log files locally
        for debug_type, debug_filename in self.debug_filenames.iteritems():
            debug_filename = os.path.join(temps_dir, debug_filename)
            try:
                os.remove(debug_filename)
            except OSError:
                pass

        # Create a bash script for running the job
        run_script_file_path = 'job.sh'
        run_script_filename = os.path.join(temps_dir, run_script_file_path)
        run_script_blobname = os.path.join(
            self.job_blobname_prefix[name],
            run_script_file_path)

        command = azure_helpers.get_run_command(ctx)
        command = command.format(
            input_filename=job_before_file_path,
            output_filename=job_after_file_path)

        with open(run_script_filename, 'w') as f:
            f.write('set -e\n')
            f.write('set -o pipefail\n\n')
            f.write(self.compute_start_commands[pool_id] + '\n')
            f.write(command + '\n')
            f.write(self.compute_finish_commands[pool_id] + '\n')
            f.write('wait')

        run_script_file = azure_helpers.create_blob_batch_resource(
            self.blob_client, self.container_name, run_script_filename, run_script_blobname, run_script_file_path)

        # Obtain a shared access signature that provides write access to the output
        # container to which the tasks will upload their output.
        container_sas_token = azure_helpers.get_container_sas_token(
            self.blob_client,
            self.container_name,
            azureblob.BlobPermissions.CREATE | azureblob.BlobPermissions.WRITE)

        # Add the task to the job
        azure_helpers.add_task(
            self.batch_client, job_id, task_id,
            job_before_file, run_script_file,
            self.container_name, container_sas_token,
            self.job_blobname_prefix[name], self.blob_client)

        self.running_task_ids.add(task_id)

    def wait(self, immediate=False):
        """ Wait for a job to finish.

        KwArgs:
            immediate (bool): do not wait if no job has finished

        Returns:
            str: job name

        """

        timeout = datetime.timedelta(minutes=60)

        while True:
            timeout_expiration = datetime.datetime.now() + timeout

            while datetime.datetime.now() < timeout_expiration:
                for task_id in self.running_task_ids.intersection(
                        self.completed_task_ids):
                    return self.job_names[task_id]

                if not self._update_task_state():
                    time.sleep(20)

            if not self.running_task_ids:
                self.logger.debug('something is not right. please try restarting the pipeline again')

            for task in self.running_task_ids:
                name = self.job_names[task]
                azurejob = self.mapping_from_tasks_to_job[task]
                self.logger.debug(
                    "waiting for task {} with name {} running under job {}".format(task, name, azurejob))

            self.logger.warn(
                "Tasks did not reach 'Completed' state within timeout period of " +
                str(timeout))

            self.logger.info(
                "Most recent transition: {}".format(
                    self.most_recent_transition_time))
            task_filter = "state eq 'completed'"
            list_options = batchmodels.TaskListOptions(filter=task_filter)

            for pool_id in self.active_pools:
                azure_helpers.check_pool_for_failed_nodes(
                    self.batch_client,
                    pool_id,
                    self.logger)

            for job_id in self.active_jobs:
                tasks = list(
                    self.batch_client.task.list(
                        job_id,
                        task_list_options=list_options))
                self.logger.info("Received total {} tasks".format(len(tasks)))
                for task in tasks:
                    if task.id not in self.completed_task_ids:
                        self.logger.info(
                            "Missed completed task: {}".format(
                                task.serialize()))
                        self.completed_task_ids.add(task.id)

    def _update_task_state(self, latest_transition_time=None):
        """ Query azure and update task state. """

        task_filter = "state eq 'completed'"

        if self.most_recent_transition_time is not None:
            assert self.most_recent_transition_time.tzname() == 'UTC'
            filter_time_string = self.most_recent_transition_time.strftime(
                "%Y-%m-%dT%H:%M:%SZ")
            task_filter += " and stateTransitionTime gt DateTime'{}'".format(
                filter_time_string)

        if latest_transition_time is not None:
            assert latest_transition_time.tzname() == 'UTC'
            filter_time_string = latest_transition_time.strftime(
                "%Y-%m-%dT%H:%M:%SZ")
            task_filter += " and stateTransitionTime lt DateTime'{}'".format(
                filter_time_string)

        list_max_results = 1000
        list_options = batchmodels.TaskListOptions(
            filter=task_filter,
            max_results=list_max_results)

        tasks = []
        for job_id in self.active_jobs:
            tasks += list(self.batch_client.task.list(job_id,
                                                      task_list_options=list_options))
        sorted_transition_times = sorted(
            [task.state_transition_time for task in tasks])

        if len(tasks) >= list_max_results:
            mid_transition_time = sorted_transition_times[
                len(sorted_transition_times) /
                2]
            return self._update_task_state(
                latest_transition_time=mid_transition_time)

        elif len(tasks) == 0:
            return False

        self.most_recent_transition_time = sorted_transition_times[-1]

        num_completed_before = len(self.completed_task_ids)
        self.completed_task_ids.update([task.id for task in tasks])
        return len(self.completed_task_ids) > num_completed_before

    def _create_error_text(self, job_temp_dir, hostname=None):
        error_text = 'failed to execute'

        if hostname:
            error_text += ' on node {}'.format(hostname)

        error_text = [error_text]

        for debug_type, debug_filename in self.debug_filenames.iteritems():
            debug_filename = os.path.join(job_temp_dir, debug_filename)

            if not os.path.exists(debug_filename):
                error_text += [debug_type + ': missing']

                continue

            with open(debug_filename, 'r') as debug_file:
                error_text += [debug_type + ':']

                for line in debug_file:
                    error_text += ['\t' + line.rstrip()]

        return '\n'.join(error_text)

    def receive(self, name):
        """ Receive finished job.

        Args:
            name (str): name of job to retrieve

        Returns:
            object: received object

        """

        task_id = self.job_task_ids.pop(name)
        self.job_names.pop(task_id)
        temps_dir = self.job_temps_dir.pop(name)
        blobname_prefix = self.job_blobname_prefix.pop(name)
        self.running_task_ids.remove(task_id)

        job_id = self.mapping_from_tasks_to_job[task_id]

        # hostname on the node doesnt match id in azure ui(affinity id in API)
        # overrides the hostname collected from node with affinity id.
        hostname = self.batch_client.task.get(
            job_id,
            task_id).node_info.node_id

        azure_helpers.download_blobs_from_container(
            self.blob_client,
            self.container_name,
            temps_dir,
            blobname_prefix)

        job_after_filename = os.path.join(temps_dir, 'job_result.pickle')

        if not os.path.exists(job_after_filename):
            raise pypeliner.execqueue.base.ReceiveError(
                self._create_error_text(temps_dir, hostname=hostname))

        with open(job_after_filename, 'rb') as job_after_file:
            received = pickle.load(job_after_file)

        if received is None:
            raise pypeliner.execqueue.base.ReceiveError(
                self._create_error_text(temps_dir, hostname=hostname))

        received.hostname = hostname

        # batch keeps scheduling tasks on nodes in failed state, need to
        # explicitly disable failed nodes.
        if not received.finished or not received.started:
            pool_id = azure_helpers.get_pool_id_from_job(self.batch_client, job_id)

            azure_helpers.verify_node_status(
                hostname,
                pool_id,
                self.batch_client,
                self.logger)

        return received

    @property
    def length(self):
        """ Number of jobs in the queue. """
        return len(self.job_names)

    @property
    def empty(self):
        """ Queue is empty. """
        return self.length == 0


if __name__ == "__main__":
    exc_dir = 'tempazure'

    logging.basicConfig(level=logging.DEBUG)

    with AzureJobQueue() as queue:
        job = pypeliner.tests.jobs.TestJob()
        pypeliner.helpers.makedirs(exc_dir)

        queue.send({}, 'testjob1', job, exc_dir)

        finished_name = queue.wait()

        job2 = queue.receive(finished_name)
