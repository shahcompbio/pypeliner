import os
import re
import uuid
import time
import random
import string
import datetime
import logging

import pypeliner
from pypeliner.helpers import Backoff
from rabbitmq import RabbitMqSemaphore

import azure.batch.batch_service_client as batch
import azure.storage.blob as azureblob
import azure.batch.models as batchmodels
from azure.common import AzureHttpError
from azure.common.credentials import ServicePrincipalCredentials
from azure.keyvault import KeyVaultClient, KeyVaultAuthentication
from azure.keyvault.models import KeyVaultErrorException

class UnconfiguredStorageAccountError(Exception):
    pass

class AzureLoggingFilter(logging.Filter):
    def __init__(self):
        self.filter_keywords = [
            'azure', 'adal-python', 'urllib3', 'msrest'
        ]

    def filter(self, rec):
        logname = rec.name.split('.')[0]
        if logname in self.filter_keywords:
            return False
        return True

def set_azure_logging_filters():
    for handler in logging.root.handlers:
        handler.addFilter(AzureLoggingFilter())


def random_string(length):
    return ''.join(random.choice(string.lowercase) for _ in range(length))


def get_storage_account_key(
        accountname, client_id, secret_key, tenant_id, keyvault_account
):
    """
    Uses the azure management package and the active directory
    credentials to fetch the authentication key for a storage
    account from azure key vault. The key must be stored in azure
    keyvault for this to work.
    :param str accountname: storage account name
    """
    def auth_callback(server, resource, scope):
        credentials = ServicePrincipalCredentials(
            client_id=client_id,
            secret=secret_key,
            tenant=tenant_id,
            resource="https://vault.azure.net"
        )
        token = credentials.token
        return token['token_type'], token['access_token']

    client = KeyVaultClient(KeyVaultAuthentication(auth_callback))
    keyvault = "https://{}.vault.azure.net/".format(keyvault_account)
    # passing in empty string for version returns latest key
    try:
        secret_bundle = client.get_secret(keyvault, accountname, "")
    except KeyVaultErrorException:
        err_str = "The pipeline is not setup to use the {} account".format(accountname)
        err_str += "please add the storage key for the account to {}".format(keyvault_account)
        err_str += "as a secret. All input/output paths should start with accountname"
        raise UnconfiguredStorageAccountError(err_str)
    return secret_bundle.value


def get_blob_name(filename):
    """
    blob storage backend expects path that includes
    storage account name, container name and blob name
    without a leading slash. remove leading slash if input
    starts with one.
    :param filename: input filename
    :return: blob backend compatible blob name
    """
    return filename.strip('/')


@Backoff(exception_type=AzureHttpError, max_backoff=1800, randomize=True)
def download_from_blob_to_path(
        blob_client, container_name, blob_name, destination_file_path,
        account_name=None, username=None, password=None, ipaddress=None,
        vhost=None):
    """
    download data from blob storage
    :param blob_client: azure.storage.blob.BlockBlobService instance
    :param container_name: blob container name
    :param blob_name: blob path
    :param destination_file_path: path to download the file to
    :param account_name: storage account name
    :param username: username for rabbitmq instance
    :param password: password for rabbitmq instance
    :param ipaddress: ip for rabbitmq instance
    :param vhost: rabbitmq vhost
    :return: azure.storage.blob.baseblobservice.Blob instance with content properties and metadata
    """
    if username:
        queue = "{}_pull".format(account_name)

        semaphore = RabbitMqSemaphore(
            username, password, ipaddress, queue, vhost,
            blob_client.get_blob_to_path,
            [container_name, blob_name, destination_file_path],
            {}
        )

        blob = semaphore.run()

    else:
        blob = blob_client.get_blob_to_path(container_name,
                                            blob_name,
                                            destination_file_path)

    return blob


def find_pool(poolinfos,  ctx):
    """
    choose the best pool based on job context
    :param poolinfos: dict, configuration for pools
    :param ctx: dict, job context dict
    :return: str, name of pool that fits job context
    """
    if ctx.get('pool_id', None):
        return ctx['pool_id']
    memory_req = ctx.get('mem', None)
    cpus_req = ctx.get('ncpus', None)
    dedicated_req = ctx.get('dedicated', None)

    pools = []

    for poolid, poolinfo in poolinfos.iteritems():
        memory = poolinfo['mem_per_task']
        cpus = poolinfo['cpus_per_task']
        dedicated = poolinfo.get('dedicated', None)

        # total available mem (to submit jobs that require high mem but 1 core)
        memory = memory * cpus

        # dont submit dedicated jobs to low priority nodes
        if dedicated_req and not dedicated:
            continue

        if memory_req <= memory and cpus_req <= cpus:
            # giving a bigger weight to cpu since cpu number is always smaller
            distance = abs(memory_req - memory) + abs((cpus_req - cpus)*5)
            pools.append((distance, poolid))

    if not pools:
        error_str = "Could not find a pool to satisfy job requirements. " \
                    "requested mem:{} cpus:{} dedicated: {}".format(memory_req, cpus_req, dedicated)
        raise Exception(error_str)

    # choose best fit
    pools = sorted(pools, key=lambda tup: tup[0])
    return pools[0][1]


def get_run_command(ctx):
    """
    generate pypeliner delegator command with docker args
    Azure batch starts a process on each node during startup
    this process runs the entire time VM is up (speculation?). running sudo gasswd
    adds our user to the docker group in node startup. but all user/group
    permission updates require restarting the bash process (normally by
    running newgrp or by logout-login). Since we cannot do that with batch
    and since the start task and compute task run under same process, we need
    to explicitly specify the user group when running docker commands. sg in
    linux can be used to set group when executing commands
    :param ctx: dict, job context
    :return: str, delegator command
    """
    command = ['pypeliner_delegate',
               '$AZ_BATCH_TASK_WORKING_DIR/{input_filename}',
               '$AZ_BATCH_TASK_WORKING_DIR/{output_filename}',
               ]
    command = ' '.join(command)

    context_config = pypeliner.helpers.GlobalState.get("context_config")

    if "docker" in context_config:
        image = ctx.get("docker_image")
        if not image:
            return command
        ctx = context_config["docker"]
        mount_string = ['-v {}:{}'.format(mount, mount) for mount in ctx.get("mounts").values()]
        mount_string += ['-v /mnt:/mnt']
        mount_string = ' '.join(mount_string)
        image = ctx.get("server") + '/' + image
        command = ['docker run -w $PWD',
                   mount_string,
                   '-v /var/run/docker.sock:/var/run/docker.sock',
                   '-v /usr/bin/docker:/usr/bin/docker',
                   image, command
                   ]
        command = ' '.join(command)
        # wrap it up as docker group command
        command = 'sg docker -c "{}"'.format(command)

        login_command = [
            'echo', ctx.get('password'), '|', 'docker', 'login',
            ctx.get('server'), '-u', ctx.get('username'),
            '--password-stdin', '>', '/dev/null'
        ]
        login_command = ' '.join(login_command)
        login_command = 'sg docker -c "{}"'.format(login_command)

        pull_command = 'docker pull {} > /dev/null'.format(image)
        pull_command = 'sg docker -c "{}"'.format(pull_command)

        command = '\n'.join([login_command, pull_command, command])

    return command


def get_task_id(name, azure_task_name_max_len=64):
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

    name = '-'.join(re.split('[:/]', name))

    name = "{}-{}".format(uniq_string, name)

    return name


def get_pool_id_from_job(batch_client, job_id):
    """
    get pool id from job id
    :param batch_client: azure batch service client
    :param str job_id: azure batch job id
    :return str: azure batch pool id
    """
    return batch_client.job.get(job_id).pool_info.pool_id


def get_container_and_blob_path(file_path):
    """
    pypeliner assumes blob paths start with container name followed
    by the blob path. This function extracts the container name and blob path
    from the input paths.
    :param str file_path: the composite filepath with container and blob paths
    """
    if file_path.startswith('/'):
        file_path = file_path[1:]

    file_path = file_path.split('/')
    container_name = file_path[0]
    blobpath = '/'.join(file_path[1:])

    return container_name, blobpath


def print_batch_exception(batch_exception):
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


def wrap_commands_in_shell(ostype, commands):
    """Wrap commands in a shell

    :param list commands: list of commands to wrap
    :param str ostype: OS type, linux or windows
    :rtype: str
    :return: a shell wrapping commands
    """
    if ostype.lower() == 'linux':
        return '/bin/bash -c \'set -e; set -o pipefail; {}; wait\''.format(
            '; '.join(commands))
    elif ostype.lower() == 'windows':
        return 'cmd.exe /c "{}"'.format('&'.join(commands))
    else:
        raise ValueError('unknown ostype: {}'.format(ostype))


def get_vm_image_and_node_agent_sku(
        batch_client, node_config):
    """Select the latest verified image that Azure Batch supports given
    a publisher, offer and sku (starts with filter).
    Get the node agent SKU and image reference for the virtual machine
    configuration.
    For more information about the virtual machine configuration, see:
    https://azure.microsoft.com/documentation/articles/batch-linux-nodes/
    :param batch_client: The batch client to use.
    :type batch_client: `batchserviceclient.BatchServiceClient`
    :param dict node_config: vm configuration
    :rtype: tuple
    :return: (node agent sku id to use, vm image ref to use)
    """
    if node_config.get("node_resource_id", None):
        image_ref_to_use = batchmodels.ImageReference(
            virtual_machine_image_id=node_config["node_resource_id"])
        sku_to_use = node_config['node_os_sku']
    else:
        publisher = node_config['node_os_publisher']
        offer = node_config['node_os_offer']
        sku_starts_with = node_config['node_os_sku']

        # get verified vm image list and node agent sku ids from service
        node_agent_skus = batch_client.account.list_node_agent_skus()
        # pick the latest supported sku
        skus_to_use = [
            (sku, image_ref) for sku in node_agent_skus for image_ref in sorted(
                sku.verified_image_references, key=lambda item: item.sku)
            if image_ref.publisher.lower() == publisher.lower() and
            image_ref.offer.lower() == offer.lower() and
            image_ref.sku.startswith(sku_starts_with)
        ]
        # skus are listed in reverse order, pick first for latest
        sku_to_use, image_ref_to_use = skus_to_use[0]
        sku_to_use = sku_to_use.id

    return sku_to_use, image_ref_to_use


def create_commands(commands_str):
    return filter(lambda a: len(a) > 0, commands_str.split('\n'))


def generate_blob_url(blob_client, container_name, blob_name):
    return blob_client.make_blob_url(container_name, blob_name)


def create_pool(batch_service_client, blob_client, pool_id, config):
    """
    Creates a pool of compute nodes with the specified OS settings.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param blob_client: A blob service client
    :type blob_client: `azure.storage.blob.BlockBlobService`
    :param str pool_id: An ID for the new pool.
    :param dict config: Configuration details.
    """

    sku_to_use, image_ref_to_use = get_vm_image_and_node_agent_sku(
        batch_service_client, config)

    account_name = os.environ['AZURE_STORAGE_ACCOUNT']
    account_key = get_storage_account_key(
        account_name, os.environ['CLIENT_ID'], os.environ['SECRET_KEY'],
        os.environ['TENANT_ID'], os.environ['AZURE_KEYVAULT_ACCOUNT']
    )

    start_vm_commands = None
    if config.get('create_vm_commands', None):
        start_vm_commands = create_commands(config['create_vm_commands'])
        start_vm_commands = [command.format(accountname=account_name, accountkey=account_key)
                             for command in start_vm_commands]

    data_disks = None
    if config['data_disk_sizes']:
        data_disks = []
        for i, disk_size in config['data_disk_sizes'].iteritems():
            data_disks.append(batchmodels.DataDisk(i, disk_size))

    resource_files = []

    if config['start_resources']:
        for vm_file_name, blob_path in config['start_resources'].iteritems():
            container_name, blob_name = get_container_and_blob_path(blob_path)
            res_url = generate_blob_url(blob_client, container_name, blob_name)
            resource_files.append(
                batch.models.ResourceFile(
                    res_url,
                    vm_file_name))

    user = batchmodels.AutoUserSpecification(
        scope=batchmodels.AutoUserScope.pool,
        elevation_level=batchmodels.ElevationLevel.admin)

    vm_configuration = batchmodels.VirtualMachineConfiguration(
        image_reference=image_ref_to_use,
        node_agent_sku_id=sku_to_use,
        data_disks=data_disks)

    vm_start_task = batch.models.StartTask(
        command_line=wrap_commands_in_shell('linux', start_vm_commands),
        user_identity=batchmodels.UserIdentity(auto_user=user),
        resource_files=resource_files,
        wait_for_success=True)

    new_pool = batch.models.PoolAddParameter(
        id=pool_id,
        virtual_machine_configuration=vm_configuration,
        vm_size=config['pool_vm_size'],
        enable_auto_scale=True,
        auto_scale_formula=config['auto_scale_formula'],
        auto_scale_evaluation_interval=datetime.timedelta(minutes=5),
        start_task=vm_start_task,
        max_tasks_per_node=config['max_tasks_per_node'],
    )

    try:
        batch_service_client.pool.add(new_pool)
    except batchmodels.BatchErrorException as err:
        print_batch_exception(err)
        raise


def create_job(batch_service_client, job_id, pool_id):
    """
    Creates a job with the specified ID, associated with the specified pool.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id: The ID for the job.
    :param str pool_id: The ID for the pool.
    """

    job = batch.models.JobAddParameter(
        id=job_id,
        pool_info=batch.models.PoolInformation(pool_id=pool_id))

    try:
        batch_service_client.job.add(job)
    except batchmodels.BatchErrorException as err:
        print_batch_exception(err)
        raise


def create_blob_batch_resource(
        block_blob_client, container_name, file_path, blob_name, job_file_path):
    """
    Uploads a local file to an Azure Blob storage container for use as
    an azure batch resource.

    :param block_blob_client: A blob service client.
    :type block_blob_client: `azure.storage.blob.BlockBlobService`
    :param str container_name: The name of the Azure Blob storage container.
    :param str file_path: The local path to the file.
    :rtype: `azure.batch.models.ResourceFile`
    :return: A ResourceFile initialized with a SAS URL appropriate for Batch
    tasks.
    """

    block_blob_client.create_blob_from_path(container_name,
                                            blob_name,
                                            file_path)

    sas_token = block_blob_client.generate_blob_shared_access_signature(
        container_name,
        blob_name,
        permission=azureblob.BlobPermissions.READ,
        expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=120))

    sas_url = block_blob_client.make_blob_url(container_name,
                                              blob_name,
                                              sas_token=sas_token)

    return batchmodels.ResourceFile(file_path=job_file_path,
                                    http_url=sas_url)


def delete_from_container(block_blob_client, container_name, blob_name):
    """
    Deletes a blob from an Azure Blob storage container.

    :param block_blob_client: A blob service client.
    :type block_blob_client: `azure.storage.blob.BlockBlobService`
    :param str container_name: The name of the Azure Blob storage container.
    :param str file_path: The local path to the file.
    """
    if block_blob_client.exists(container_name, blob_name):
        block_blob_client.delete_blob(container_name, blob_name)


def get_container_sas_token(block_blob_client,
                            container_name, blob_permissions):
    """
    Obtains a shared access signature granting the specified permissions to the
    container.

    :param block_blob_client: A blob service client.
    :type block_blob_client: `azure.storage.blob.BlockBlobService`
    :param str container_name: The name of the Azure Blob storage container.
    :param BlobPermissions blob_permissions:
    :rtype: str
    :return: A SAS token granting the specified permissions to the container.
    """
    # Obtain the SAS token for the container, setting the expiry time and
    # permissions. In this case, no start time is specified, so the shared
    # access signature becomes valid immediately.
    container_sas_token = \
        block_blob_client.generate_container_shared_access_signature(
            container_name,
            permission=blob_permissions,
            expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=120))

    return container_sas_token


def add_task(batch_service_client, job_id, task_id, input_file,
             job_script_file,
             output_container_name, output_container_sas_token,
             output_blob_prefix, blob_client):
    """
    Adds a task for each input file in the collection to the specified job.

    :param batch_service_client: A Batch service client.
    :type batch_service_client: `azure.batch.BatchServiceClient`
    :param str job_id: The ID of the job to which to add the tasks.
    :param list input_files: A collection of input files. One task will be
     created for each input file.
    :param output_container_name: The ID of an Azure Blob storage container to
    which the tasks will upload their results.
    :param output_container_sas_token: A SAS token granting write access to
    the specified Azure Blob storage container.
    """

    commands = ['/bin/bash job.sh']

    # WORKAROUND: for some reason there is no function to create an url for a
    # container
    output_sas_url = blob_client.make_blob_url(
        output_container_name, 'null',
        sas_token=output_container_sas_token).replace('/null', '')

    def create_output_file(filename, blobname):
        container = batchmodels.OutputFileBlobContainerDestination(
            container_url=output_sas_url, path=blobname)
        destination = batchmodels.OutputFileDestination(container=container)
        upload_options = batchmodels.OutputFileUploadOptions(upload_condition='taskCompletion')
        return batchmodels.OutputFile(file_pattern=filename, destination=destination, upload_options=upload_options)

    task = batch.models.TaskAddParameter(
        id=task_id,
        command_line=wrap_commands_in_shell('linux', commands),
        resource_files=[
            input_file,
            job_script_file],
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

    batch_service_client.task.add(job_id, task)


def download_blobs_from_container(block_blob_client,
                                  container_name,
                                  directory_path,
                                  prefix):
    """
    Downloads all blobs from the specified Azure Blob storage container.

    :param block_blob_client: A blob service client.
    :type block_blob_client: `azure.storage.blob.BlockBlobService`
    :param container_name: The Azure Blob storage container from which to
     download files.
    :param directory_path: The local directory to which to download the files.
    """
    container_blobs = block_blob_client.list_blobs(
        container_name,
        prefix=prefix)

    for blob in container_blobs.items:
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

        download_from_blob_to_path(block_blob_client,
                                   container_name,
                                   blob.name,
                                   destination_file_path)


def verify_node_status(node_id, pool_id, batch_client, logger):
    '''
    check if the node is in working state, if not disable it.
    :param str node_id: compute node id
    :param str pool_id: batch pool id
    :param batch_client: azure batch service client
    :param logger: pypeliner logger object
    '''

    healthy_states = ["idle", "running", "preempted",
                      "waitingforstarttask", "leavingpool", 'starting']

    try:
        status = batch_client.compute_node.get(pool_id, node_id).state.value
    except batchmodels.BatchErrorException:
        logger.warning("Couldn't get status for node {} ".format(node_id))
        return

    # dont send jobs to failed nodes
    if status not in healthy_states and not status == "offline":
        logger.warning(
            'node {} is in {} state and will be disabled'.format(
                node_id,
                status))
        batch_client.compute_node.disable_scheduling(pool_id, node_id)


def check_pool_for_failed_nodes(batch_client, pool_id, logger):
    """
    Collects node state information from the batch pool
    :param batch_client: batch service client
    :param str pool_id: ID of pool to monitor
    :param logger: logger object to issue warnings
    """

    healthy_states = ["idle", "running", "preempted",
                      "waitingforstarttask", "leavingpool", 'starting']

    for node in batch_client.compute_node.list(pool_id):
        try:
            status = batch_client.compute_node.get(
                pool_id,
                node.id).state.value
        except batchmodels.BatchErrorException:
            logger.warning("Couldn't get status for node {} ".format(node.id))
            continue

        # node states are inconsistent with lower and upper cases
        # stop looking when atleast one working node exists
        if status.lower() in healthy_states:
            return

    logger.warning(
        "Couldn't find any working nodes in pool. Please verify pool status")


def wait_for_job_deletion(batch_client, job_id, logger):
    """
    Wait until the job is deleted. avoids issues when running
    multiple pipelines with same run id serially.
    :param batch_client: batch service client
    :param str job_id: job identifier
    :param logger: logging object for issuing warnings
    """

    while True:
        try:
            batch_client.job.get(job_id)
        except batchmodels.BatchErrorException:
            break

        time.sleep(30)
