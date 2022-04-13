from __future__ import print_function

import logging
import os

import datetime
import dill as pickle
import pypeliner.execqueue.base
import pypeliner.helpers
import pypeliner.tests.jobs
import time
import yaml

from .batchclient import BatchClient


class AzureJobQueue(object):
    """ Azure batch job queue.
    """

    def __init__(self, config_filename=None, **kwargs):
        batch_account_url = os.environ['AZURE_BATCH_URL']
        client_id = os.environ['CLIENT_ID']
        tenant_id = os.environ['TENANT_ID']
        secret_key = os.environ['SECRET_KEY']

        with open(config_filename) as f:
            self.config = yaml.safe_load(f)

        storage_account_name = self.config['pypeliner_storage_account']
        storage_keys = pypeliner.helpers.GlobalState.get('azure_storage_keys', {})
        account_token = storage_keys.get(storage_account_name)

        self.batch_client = BatchClient(
            client_id, tenant_id, secret_key, batch_account_url,
            self.config, storage_account_token=account_token
        )

        self.logger = self.batch_client.logger
        self.run_id = self.batch_client.run_id

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
        if not self.no_delete_job:
            self.batch_client.delete_jobs(ignore_exception=True)
        if not self.no_delete_pool:
            self.batch_client.delete_pools(ignore_exception=True)

    def get_jobid(self, pool_id):
        return 'pypeliner_{}_{}'.format(pool_id, self.run_id)

    def cleanup_old_job_files(self, pickle_blob_path, pickle_local_path, temps_dir):
        # Delete any previous job result file locally and in blob
        self.batch_client.blob_client.delete_blob(
            container_name=self.batch_client.container_name, blob_name=pickle_blob_path
        )

        if os.path.exists(pickle_local_path):
            os.remove(pickle_local_path)

        # Delete any previous log files locally
        for debug_type, debug_filename in self.debug_filenames.items():
            debug_filename = os.path.join(temps_dir, debug_filename)
            if os.path.exists(debug_filename):
                os.remove(debug_filename)

    def create_run_script(self, command, script_path, pool_id):
        with open(script_path, 'w') as f:
            f.write('set -e\n')
            f.write('set -o pipefail\n\n')
            f.write(self.batch_client.compute_start_commands[pool_id] + '\n')
            f.write(command + '\n')
            f.write(self.batch_client.compute_finish_commands[pool_id] + '\n')
            f.write('wait')

    def send(self, ctx, name, sent, temps_dir):
        """ Add a job to the queue.

        Args:
            ctx (dict): context of job, mem etc.
            name (str): unique name for the job
            sent (callable): callable object
            temps_dir (str): unique path for strong job temps

        """
        pool_id = self.batch_client.find_pool(ctx)
        job_id = self.get_jobid(pool_id)
        task_id = self.batch_client.get_task_id(name)

        self.logger.debug(
            'assigning job {} to pool {}'.format(name, pool_id)
        )
        self.logger.info(
            'assigning task_id {} to job {}'.format(
                task_id,
                name))

        self.batch_client.create_pool(pool_id)
        self.batch_client.create_job(pool_id, job_id)

        self.mapping_from_tasks_to_job[task_id] = job_id
        self.job_names[task_id] = name
        self.job_task_ids[name] = task_id
        self.job_temps_dir[name] = temps_dir
        self.job_blobname_prefix[name] = 'output_' + task_id

        after_remote_path = 'job_result.pickle'
        after_local_path = os.path.join(temps_dir, after_remote_path)
        after_blob_path = os.path.join(self.job_blobname_prefix[name], after_remote_path)

        self.cleanup_old_job_files(after_blob_path, after_local_path, temps_dir)

        before_remote_path = 'job.pickle'
        before_local_path = os.path.join(temps_dir, before_remote_path)
        before_blob_path = os.path.join(self.job_blobname_prefix[name], before_remote_path)

        with open(before_local_path, 'wb') as before:
            pickle.dump(sent, before)

        before_file_resource = self.batch_client.create_blob_resource(
            before_local_path, before_blob_path, before_remote_path
        )

        # Create a bash script for running the job
        script_remote_path = 'job.sh'
        script_local_path = os.path.join(temps_dir, script_remote_path)
        script_blob_path = os.path.join(
            self.job_blobname_prefix[name], script_remote_path
        )

        command = ['pypeliner_delegate', '$AZ_BATCH_TASK_WORKING_DIR/' + before_remote_path,
                   '$AZ_BATCH_TASK_WORKING_DIR/' + after_remote_path]

        # local tasks like setobj need to run without docker args,
        # since they might already be running inside docker
        if not ctx.get('local'):
            command = pypeliner.containerize.containerize_args(*command)

        command = ' '.join(command)

        self.create_run_script(command, script_local_path, pool_id)

        run_script_file = self.batch_client.create_blob_resource(
            script_local_path, script_blob_path, script_remote_path)

        # Add the task to the job
        self.batch_client.add_task(
            job_id, task_id, before_file_resource,
            run_script_file, self.job_blobname_prefix[name]
        )

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
                self.batch_client.check_for_missing_node(azurejob, task)

            self.logger.warn(
                "Tasks did not reach 'Completed' state within timeout period of " +
                str(timeout))

            self.logger.info(
                "Most recent transition: {}".format(
                    self.most_recent_transition_time))
            task_filter = "state eq 'completed'"

            for pool_id in self.batch_client.active_pools:
                self.batch_client.check_pool_for_failed_nodes(pool_id)

            for job_id in self.batch_client.active_jobs:
                tasks = list(
                    self.batch_client.get_task_list(
                        job_id,
                        task_filter=task_filter)
                )
                self.logger.info("Received total {} tasks".format(len(tasks)))
                for task in tasks:
                    if task.id not in self.completed_task_ids:
                        self.logger.info(
                            "Missed completed task: {}".format(
                                task.serialize()))
                        self.completed_task_ids.add(task.id)

    def _update_task_state(self, latest_transition_time=None):
        """ Query azure and update task state. """

        list_max_results = 1000

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

        tasks = []
        for job_id in self.batch_client.active_jobs:
            tasks += list(self.batch_client.get_task_list(
                job_id, task_filter=task_filter, list_max_results=list_max_results
            )
            )
        sorted_transition_times = sorted(
            [task.state_transition_time for task in tasks])

        if len(tasks) >= list_max_results:
            mid_transition_time = sorted_transition_times[
                int(len(sorted_transition_times) / 2)]
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
        hostname = self.batch_client.get_node_id_for_task(job_id, task_id)

        self.batch_client.download_blobs_from_container(
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

        # just in case a new storage key was pulled from storage in job
        # generally this doesnt happen as key gets pulled on head node to
        # get the create time for all resources anyway prior to running the job
        pypeliner.helpers.GlobalState.update_all(received.pypeliner_globals)

        received.hostname = hostname

        # batch keeps scheduling tasks on nodes in failed state, need to
        # explicitly disable failed nodes.
        if not received.finished or not received.started:
            pool_id = self.batch_client.get_pool_id_from_job(job_id)
            self.batch_client.verify_node_status(hostname, pool_id)

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
