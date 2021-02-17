from __future__ import absolute_import

import logging
import os
import random
import re
import string

import azure.batch.models as batchmodels
import datetime
import time
import uuid
from azure.batch import BatchServiceClient
from azure.common.credentials import ServicePrincipalCredentials
from azure.profiles import KnownProfiles

from .blobclient import BlobStorageClient

KnownProfiles.default.use(KnownProfiles.latest)


class AzureBatchPoolError(Exception):
    pass


class AzureLoggingFilter(logging.Filter):
    def __init__(self):
        self.filter_keywords = [
            'azure', 'adal-python', 'urllib3', 'msrest', 'msal'
        ]

    def filter(self, rec):
        logname = rec.name.split('.')[0]
        if logname in self.filter_keywords:
            return False
        return True


class BatchClient(object):
    def __init__(
            self, client_id, tenant_id, secret_key, batch_account_url,
            config, storage_account_token=None
    ):
        """
        abstraction around all batch related methods
        :param client_id: Azure AD client id (application id)
        :type client_id: str
        :param tenant_id: Azure AD tenant id
        :type tenant_id: str
        :param secret_key: secret key for the app
        :type secret_key: str
        :param batch_account_url: azure batch account URL
        :type batch_account_url: str
        :param config: configuration file data from submit config
        :type config: dict
        :param storage_account_token: storage account key
        :type storage_account_token:  str
        """
        self.config = config

        self.run_id = self.__random_string(8)

        self.logger = self.__get_logger(add_azure_filter=True)

        self.logger.info('creating blob client')
        self.blob_client = BlobStorageClient(
            config['pypeliner_storage_account'],
            client_id, tenant_id, secret_key,
            storage_account_token=storage_account_token,
        )

        self.logger.info('creating batch client')
        self.credentials = ServicePrincipalCredentials(
            client_id=client_id,
            secret=secret_key,
            tenant=tenant_id,
            resource="https://batch.core.windows.net/"
        )
        self.batch_client = BatchServiceClient(
            self.credentials,
            batch_account_url
        )

        self.container_name = self.config['storage_container_name']
        self.blob_client.create_container(container_name=self.container_name)

        self.active_pools = set()
        self.active_jobs = set()

    def __get_vm_image_and_node_agent_sku(self, pool_config):
        """Select the latest verified image that Azure Batch supports given
        a publisher, offer and sku (starts with filter).
        Get the node agent SKU and image reference for the virtual machine
        configuration.
        For more information about the virtual machine configuration, see:
        https://azure.microsoft.com/documentation/articles/batch-linux-nodes/
        :param dict pool_config: vm configuration
        :rtype: tuple
        :return: (node agent sku id to use, vm image ref to use)
        """
        if pool_config.get("node_resource_id", None):
            image_ref_to_use = batchmodels.ImageReference(
                virtual_machine_image_id=pool_config["node_resource_id"])
            sku_to_use = pool_config['node_os_sku']
        else:
            publisher = pool_config['node_os_publisher']
            offer = pool_config['node_os_offer']
            sku_starts_with = pool_config['node_os_sku']

            # get verified vm image list and node agent sku ids from service
            node_agent_skus = self.batch_client.account.list_node_agent_skus()

            # pick the latest supported sku
            for sku in node_agent_skus:
                for image_ref in sorted(sku.verified_image_references, key=lambda item: item.sku):
                    if not image_ref.publisher.lower() == publisher.lower():
                        continue
                    if not image_ref.offer.lower() == offer.lower():
                        continue
                    if not image_ref.sku.startswith(sku_starts_with):
                        continue

                    sku_to_use = sku.id
                    image_ref_to_use = image_ref
                    break

        return sku_to_use, image_ref_to_use

    def __print_batch_exception(self, batch_exception):
        """
        Prints the contents of the specified Batch exception.

        :param batch_exception:
        """
        print('-------------------------------------------')
        print('Exception encountered:')
        if (batch_exception.error and batch_exception.error.message and
                batch_exception.error.message.value):
            print(batch_exception.error.message.value)
            if batch_exception.error.values:
                print()
                for msg in batch_exception.error.values:
                    print('{}:\t{}'.format(msg.key, msg.value))
        print('-------------------------------------------')

    def __create_commands(self, commands_str):
        return [a for a in commands_str.split('\n') if len(a) > 0]

    def __wrap_commands_in_shell(self, ostype, commands):
        """Wrap commands in a shell

        :param list commands: list of commands to wrap
        :param str ostype: OS type, linux or windows
        :rtype: str
        :return: a shell wrapping commands
        """
        if not commands:
            commands = []
        if ostype.lower() == 'linux':
            return '/bin/bash -c \'set -e; set -o pipefail; {}; wait\''.format(
                '; '.join(commands))
        elif ostype.lower() == 'windows':
            return 'cmd.exe /c "{}"'.format('&'.join(commands))
        else:
            raise ValueError('unknown ostype: {}'.format(ostype))

    def pool_exists(self, pool_id):
        """
        check if pool exists already
        :param pool_id: pool name
        :type pool_id: str
        :return: True if exists, False otherwise
        :rtype: bool
        """
        return self.batch_client.pool.exists(pool_id)

    def get_job_list(self):
        """
        list all jobs in batch account
        :return: list of jobs
        :rtype: list
        """
        job_ids = set([job.id for job in self.batch_client.job.list()])
        return job_ids

    def get_task_list(self, job_id, task_filter=None, list_max_results=None):
        """
        return tasks, optionally can filter them as well
        :param job_id: lists tasks under this Job Id
        :type job_id: str
        :param task_filter: filter string for list query
        :type task_filter:  str
        :param list_max_results: number of results to return
        :type list_max_results: int or None
        :return: list of tasks
        :rtype: list
        """
        if task_filter:
            list_options = batchmodels.TaskListOptions(filter=task_filter)
        else:
            list_options = None

        return self.batch_client.task.list(
            job_id, task_list_options=list_options, list_max_results=list_max_results
        )

    def create_job(self, pool_id, job_id):
        """
        Creates a job with the specified ID, associated with the specified pool.

        :param str job_id: The ID for the job.
        :param str pool_id: The ID for the pool.
        """

        if job_id in self.active_jobs or job_id in self.get_job_list():
            return

        self.logger.info("creating job {}".format(job_id))

        job = batchmodels.JobAddParameter(
            id=job_id,
            pool_info=batchmodels.PoolInformation(pool_id=pool_id))

        try:
            self.batch_client.job.add(job)
        except batchmodels.BatchErrorException as err:
            self.__print_batch_exception(err)
            raise

        for task in self.get_task_list(job_id):
            self.batch_client.task.delete(job_id, task.id)

        self.active_jobs.add(job_id)

    def create_pool(self, pool_id):
        """
        Creates a pool of compute nodes with the specified OS settings.

        :param str pool_id: An ID for the new pool.
        :param dict config: Configuration details.
        """
        if pool_id in self.active_pools or self.pool_exists(pool_id):
            return

        self.logger.info("creating pool {}".format(pool_id))

        pool_config = self.config['pools'][pool_id]

        sku_to_use, image_ref_to_use = self.__get_vm_image_and_node_agent_sku(
            pool_config
        )

        start_vm_commands = None
        if pool_config.get('create_vm_commands', None):
            start_vm_commands = self.__create_commands(pool_config['create_vm_commands'])

        user = batchmodels.AutoUserSpecification(
            scope=batchmodels.AutoUserScope.pool,
            elevation_level=batchmodels.ElevationLevel.admin)

        vm_configuration = batchmodels.VirtualMachineConfiguration(
            image_reference=image_ref_to_use,
            node_agent_sku_id=sku_to_use,
        )

        vm_start_task = batchmodels.StartTask(
            command_line=self.__wrap_commands_in_shell('linux', start_vm_commands),
            user_identity=batchmodels.UserIdentity(auto_user=user),
            wait_for_success=True)

        new_pool = batchmodels.PoolAddParameter(
            id=pool_id,
            virtual_machine_configuration=vm_configuration,
            vm_size=pool_config['pool_vm_size'],
            enable_auto_scale=True,
            auto_scale_formula=pool_config['auto_scale_formula'],
            auto_scale_evaluation_interval=datetime.timedelta(minutes=5),
            start_task=vm_start_task,
            max_tasks_per_node=pool_config['max_tasks_per_node'],
        )

        try:
            self.batch_client.pool.add(new_pool)
        except batchmodels.BatchErrorException as err:
            self.__print_batch_exception(err)
            raise

        self.active_pools.add(pool_id)

    def __get_logger(self, add_azure_filter=False):
        """
        generate a logger, optionally add filter to remove all azure sdk logs
        :param add_azure_filter: set the filter if set
        :type add_azure_filter: bool
        :return: python logger object
        :rtype: logging.Logger
        """
        logger = logging.getLogger('pypeliner.execqueue.azure_batch')
        if add_azure_filter:
            for handler in logging.root.handlers:
                handler.addFilter(AzureLoggingFilter())
        return logger

    def __random_string(self, length):
        return ''.join(random.choice(string.ascii_lowercase) for _ in range(length))

    def __wait_for_job_deletion(self, job_id):
        """
        Wait until the job is deleted. avoids issues when running
        multiple pipelines with same run id serially.
        :param str job_id: job identifier
        """

        while True:
            try:
                self.batch_client.job.get(job_id)
            except batchmodels.BatchErrorException:
                break
            time.sleep(30)

    def __wait_for_pool_deletion(self, pool_id):
        """
        Wait until the job is deleted. avoids issues when running
        multiple pipelines with same run id serially.
        :param str pool_id: job identifier
        """

        while True:
            try:
                self.batch_client.pool.get(pool_id)
            except batchmodels.BatchErrorException:
                break
            time.sleep(30)

    def __get_biggest_pool(self):
        """
        try to find the pool with the biggest node size
        :return: pool name
        :rtype: str
        """
        biggestpool = None

        for poolid, poolinfo in self.config['pools'].items():
            memory = poolinfo['mem_per_task']
            cpus = poolinfo['cpus_per_task']
            disk = poolinfo['disk_per_task']
            score = memory + (cpus * 8) + disk / 10

            if not biggestpool:
                biggestpool = (score, poolid)
            else:
                if score > biggestpool[0]:
                    biggestpool = (score, poolid)

        return biggestpool[1]

    def find_pool(self, ctx):
        """
        choose the best pool based on job context
        :param ctx: dict, job context dict
        :return: str, name of pool that fits job context
        """
        if ctx.get('pool_id', None):
            return ctx['pool_id']
        memory_req = ctx.get('mem', 4)
        cpus_req = ctx.get('ncpus', 1)
        disk_req = ctx.get('disk', 8)
        dedicated_req = ctx.get('dedicated', False)

        # at the ctx level, memory is normally specified per cpu
        # in batch yaml memory_per_task is less confusing than memory_per_cpu
        memory_req_per_task = memory_req * cpus_req

        pools = []

        for poolid, poolinfo in self.config['pools'].items():
            memory = poolinfo['mem_per_task']
            cpus = poolinfo['cpus_per_task']
            disk = poolinfo['disk_per_task']
            dedicated = poolinfo.get('dedicated', None)

            # dont submit dedicated jobs to low priority nodes
            if dedicated_req and not dedicated:
                continue

            if memory_req_per_task <= memory and cpus_req <= cpus and disk_req <= disk:
                # giving a bigger weight to cpu since cpu number is always smaller
                # smaller weight to disk. score is an approximation of amount of wasted
                # resources
                distance = (memory - memory_req_per_task) + ((cpus - cpus_req) * 8) + (disk - disk_req) / 10
                pools.append((distance, poolid))

        if not pools:
            biggestpool = self.__get_biggest_pool()
            warn_str = \
                "Could not find a pool to satisfy job requirements. " \
                "requested mem:{} cpus:{} dedicated: {}." \
                " submitting to {}".format(
                    memory_req_per_task, cpus_req, dedicated_req, biggestpool)
            self.logger.warn(warn_str)
            return biggestpool

        # choose best fit
        pools = sorted(pools, key=lambda tup: tup[0])

        return pools[0][1]

    @property
    def compute_start_commands(self):
        compute_start_commands = {
            id: pool['compute_start_commands'] for id, pool in self.config['pools'].items()
        }
        return compute_start_commands

    @property
    def compute_finish_commands(self):
        compute_finish_commands = {
            id: pool['compute_finish_commands'] for id, pool in self.config['pools'].items()
        }
        return compute_finish_commands

    def get_node_id_for_task(self, job_id, task_id):
        """
        find the node that task was scheduled on
        :param job_id: batch job id
        :type job_id: str
        :param task_id: batch task id
        :type task_id: str
        :return: name of node (azure affinity id)
        :rtype: str
        """
        node_info = self.batch_client.task.get(
            job_id,
            task_id).node_info
        if not node_info:
            return
        hostname = node_info.node_id
        return hostname

    def get_job_id(self, pool_id):
        return 'pypeliner_{}_{}'.format(pool_id, self.run_id)

    def delete_pool(self, pool_id, ignore_exception=False):
        """
        try to delete a pool
        :param pool_id: batch pool id
        :type pool_id: str
        :param ignore_exception: dont raise any errors if set
        :type ignore_exception: bool
        """
        try:
            self.batch_client.pool.delete(pool_id)
        except Exception as e:
            if ignore_exception:
                print(e)
            else:
                raise e

    def delete_job(self, job_id, ignore_exception=False):
        """
        try to delete a job
        :param job_id:
        :type job_id: str
        :param ignore_exception: dont raise any errors if set
        :type ignore_exception: bool
        """
        try:
            self.batch_client.job.delete(job_id)
        except Exception as e:
            if ignore_exception:
                print(e)
            else:
                raise e

    def delete_jobs(self, ignore_exception=False, wait=False):
        """
        delete all jobs that were created by this instance
        :param ignore_exception: dont raise any errors if set
        :type ignore_exception: bool
        :param wait: wait until all jobs are deleted
        :type wait: bool
        """
        for job_id in self.active_jobs:
            self.delete_job(job_id, ignore_exception=ignore_exception)

        if wait:
            for job_id in self.active_jobs:
                self.__wait_for_job_deletion(job_id)

    def delete_pools(self, ignore_exception=False, wait=False):
        """
        delete all pools that were created by this instance
        :param ignore_exception: dont raise any errors if set
        :type ignore_exception: bool
        :param wait: wait until all jobs are deleted
        :type wait: bool
        """
        for pool_id in self.active_pools:
            self.delete_pool(pool_id, ignore_exception=ignore_exception)

        if wait:
            for pool_id in self.active_pools:
                self.__wait_for_pool_deletion(pool_id)

    @property
    def healthy_states(self):
        healthy_state_list = [
            "idle", "running", "preempted",
            "waitingforstarttask", "leavingpool", 'starting'
        ]
        return healthy_state_list

    def verify_node_status(self, node_id, pool_id):
        """
        check if the node is in working state, if not disable it.
        :param str node_id: compute node id
        :param str pool_id: batch pool id
        """

        status = self.__get_node_status(pool_id, node_id)

        # dont send jobs to failed nodes
        if status not in self.healthy_states and not status == "offline":
            self.logger.warning(
                'node {} is in {} state and will be disabled'.format(
                    node_id, status)
            )
            self.__disable_node(pool_id, node_id)

    def __disable_node(self, pool_id, node_id):
        self.batch_client.compute_node.disable_scheduling(pool_id, node_id)

    def __list_compute_nodes_in_pool(self, pool_id):
        for node in self.batch_client.compute_node.list(pool_id):
            yield node.id

    def __get_node_status(self, pool_id, node_id):
        try:
            status = self.batch_client.compute_node.get(
                pool_id,
                node_id).state.value
        except batchmodels.BatchErrorException:
            self.logger.warning("Couldn't get status for node {} ".format(node_id))
            status = 'error'
        return status

    def check_pool_for_failed_nodes(self, pool_id):
        """
        Collects node state information from the batch pool
        :param batch_client: batch service client
        :param str pool_id: ID of pool to monitor
        :param logger: logger object to issue warnings
        """

        healthy_states = ["idle", "running", "preempted",
                          "waitingforstarttask", "leavingpool", 'starting']

        for node_id in self.__list_compute_nodes_in_pool(pool_id):
            status = self.__get_node_status(pool_id, node_id)

            # node states are inconsistent with lower and upper cases
            # stop looking when at least one working node exists
            if status.lower() in healthy_states:
                return

        self.logger.warning(
            "Couldn't find any working nodes in pool. Please verify pool status")

    def download_blobs_from_container(
            self, directory_path, prefix
    ):

        """
        Downloads all blobs from the specified Azure Blob storage container.
        :param directory_path: The local directory to which to download the files.
        :type directory_path: str
        :param prefix: removes this prefix from blob name
        :type prefix:str
        """
        container_blobs = self.blob_client.list_blobs(container_name=self.container_name, prefix=prefix)

        for blob in container_blobs:
            destination_filename = blob.name

            # Remove prefix
            assert destination_filename.startswith(prefix)
            destination_filename = destination_filename[(len(prefix)):]
            destination_filename = destination_filename.strip('/')

            destination_file_path = os.path.join(
                directory_path,
                destination_filename)

            try:
                os.makedirs(os.path.dirname(destination_file_path))
            except:
                pass

            self.blob_client.download_to_path(
                destination_file_path, container_name=self.container_name, blob_name=blob.name
            )

    def create_blob_resource(
            self, file_path, blob_name, job_file_path):
        """
        Uploads a local file to an Azure Blob storage container for use as
        an azure batch resource.

        :param file_path: The local path to the file.
        :type file_path: str
        :param blob_name: path to upload to
        :type blob_name: str
        :param job_file_path: filepath for batch resource
        :type job_file_path: str
        :rtype: `azure.batch.models.ResourceFile`
        :return: A ResourceFile initialized with a SAS URL appropriate for Batch
        """

        self.blob_client.upload_from_file(
            file_path, container_name=self.container_name, blob_name=blob_name
        )

        sas_url = self.blob_client.generate_blob_url(
            container_name=self.container_name, blob_name=blob_name, add_sas=True
        )

        return batchmodels.ResourceFile(
            file_path=job_file_path,
            http_url=sas_url
        )

    def add_task(
            self, job_id, task_id, input_file, job_script_file,
            output_blob_prefix
    ):
        """
        Adds a task for each input file in the collection to the specified job.

        :param job_id: The ID of the job to which to add the tasks.
        :type job_id: str
        :param task_id: The ID of the task.
        :type task_id: str
        :param input_file: delegator input file
        :type input_file: str
        :param job_script_file: job.sh file
        :type job_script_file: str
        :param output_blob_prefix: prefix where output files will be uploaded to
        :type output_blob_prefix: str
        """
        commands = ['/bin/bash job.sh']

        resource_files = [input_file, job_script_file]

        # Obtain a shared access signature that provides write access to the output
        # container to which the tasks will upload their output.
        output_sas_url = self.blob_client.generate_container_url(
            container_name=self.container_name, add_sas=True, sas_permissions='create|write')

        def create_output_file(filename, blobname):
            container = batchmodels.OutputFileBlobContainerDestination(
                container_url=output_sas_url, path=blobname)
            destination = batchmodels.OutputFileDestination(container=container)
            upload_options = batchmodels.OutputFileUploadOptions(upload_condition='taskCompletion')
            return batchmodels.OutputFile(file_pattern=filename, destination=destination, upload_options=upload_options)

        task = batchmodels.TaskAddParameter(
            id=task_id,
            command_line=self.__wrap_commands_in_shell('linux', commands),
            resource_files=resource_files,
            output_files=[
                create_output_file(
                    '../stdout.txt',
                    os.path.join(
                        output_blob_prefix,
                        'stdout.txt')),
                create_output_file(
                    '../stderr.txt',
                    os.path.join(
                        output_blob_prefix,
                        'stderr.txt')),
                create_output_file(
                    'job_result.pickle',
                    os.path.join(
                        output_blob_prefix,
                        'job_result.pickle')),
            ]
        )

        self.batch_client.task.add(job_id, task)

    def get_task_id(self, name, azure_task_name_max_len=64):
        """
        generate uniq string, fill in the leftover space
        with job information
        :param name: str, pypeliner's internal task name
        :param azure_task_name_max_len: azure maximum allowed task name length
        :return str, unique azure compatible task id
        """

        uniq_string = str(uuid.uuid1())

        leftover_space = azure_task_name_max_len - len(uniq_string) - 1

        # last few characters are more useful for identifying tasks
        name = name[-leftover_space:]

        name = '-'.join(re.split('[:/?]', name))

        name = "{}-{}".format(uniq_string, name)

        return name

    def get_pool_id_from_job(self, job_id):
        """
        get pool id from job id
        :param str job_id: azure batch job id
        :return str: azure batch pool id
        """
        return self.batch_client.job.get(job_id).pool_info.pool_id

    def check_for_missing_node(self, job_id, task_id):
        node = self.get_node_id_for_task(job_id, task_id)
        pool = self.get_pool_id_from_job(job_id)

        if not node:
            self.logger.debug(
                'task {} under job {} and in pool {} '
                'has not been scheduled on a node yet!'.format(
                    task_id, job_id, pool
                )
            )
        else:
            status = self.__get_node_status(pool, node)

            if status not in self.healthy_states:
                logging.debug(
                    'task {} under job {} and in pool {} was scheduled '
                    'on node {} which is now in {} state'.format(
                        task_id, job_id, pool, node, status
                    )
                )
