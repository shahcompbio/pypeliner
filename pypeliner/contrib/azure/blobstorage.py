import os
import datetime
import time

import pypeliner.helpers
import pypeliner.storage
import pypeliner.flyweight
from pypeliner.helpers import Backoff

from rabbitmq import RabbitMqSemaphore

import azure.storage.blob
import azure.common
from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.storage import StorageManagementClient
from azure.common import AzureHttpError



def _get_blob_key(accountname, client_id, secret_key, tenant_id, subscription_id, resource_group):
    blob_credentials = ServicePrincipalCredentials(client_id=client_id,
                                                   secret=secret_key,
                                                   tenant=tenant_id)

    storage_client = StorageManagementClient(blob_credentials, subscription_id)
    keys = storage_client.storage_accounts.list_keys(resource_group,
                                                     accountname)
    keys = {v.key_name: v.value for v in keys.keys}

    return keys["key1"]


def _get_blob_name(filename):
    return filename.strip('/')


# @Backoff(exception_type=AzureHttpError, max_backoff=1800, randomize=True)
def download_from_blob_to_path(blob_client, account_name, container_name, blob_name,
                               destination_file_path, username, password,
                               ipaddress, vhost):
    blob=None

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


class BlobMissing(Exception):
    pass


class AzureBlob(object):
    def __init__(self, storage, filename, blob_name, **kwargs):
        self.storage = storage
        if filename.startswith('/'):
            filename = filename[1:]
        self.blob_name = blob_name
        self.createtime_cache = storage.create_createtime_cache(blob_name)
        store_dir = kwargs.get("store_dir")
        if kwargs.get("direct_write") or not store_dir:
            self.filename = filename
            self.write_filename = filename
        else:
            self.filename = os.path.join(store_dir, filename)
            self.write_filename = os.path.join(store_dir, filename)
    def allocate(self):
        pypeliner.helpers.makedirs(os.path.dirname(self.filename))
    def push(self):
        createtime = datetime.datetime.fromtimestamp(os.path.getmtime(self.filename)).strftime('%Y/%m/%d-%H:%M:%S')
        self.storage.push(self.blob_name, self.filename, createtime)
        self.createtime_cache.set(createtime)
    def pull(self):
        self.storage.pull(self.blob_name, self.filename)
    def get_exists(self):
        createtime = self.get_createtime()
        return createtime is not None and createtime != 'missing'
    def get_createtime(self):
        createtime = self.createtime_cache.get()
        if createtime is None:
            try:
                createtime = self.storage.retrieve_blob_createtime(self.blob_name)
            except BlobMissing:
                createtime = 'missing'
            self.createtime_cache.set(createtime)
        if createtime == 'missing':
            return None
        createtime = datetime.datetime.strptime(createtime, '%Y/%m/%d-%H:%M:%S')
        return time.mktime(createtime.timetuple())
    def touch(self):
        createtime = datetime.datetime.now().strftime('%Y/%m/%d-%H:%M:%S')
        self.storage.update_blob_createtime(self.blob_name, createtime)
        self.createtime_cache.set(createtime)
    def delete(self):
        self.storage.delete_blob(self.blob_name)
        self.createtime_cache.set(None)


class AzureBlobStorage(object):
    def __init__(self, **kwargs):
        self.rabbitmq_username = os.environ.get("RABBITMQ_USERNAME", None)
        self.rabbitmq_password = os.environ.get("RABBITMQ_PASSWORD", None)
        self.rabbitmq_ipaddress = os.environ.get("RABBITMQ_IP", None)
        self.rabbitmq_vhost = os.environ.get("RABBITMQ_VHOST", None)
        self.client_id=os.environ["CLIENT_ID"]
        self.secret_key=os.environ["SECRET_KEY"]
        self.tenant_id=os.environ["TENANT_ID"]
        self.subscription_id = os.environ["SUBSCRIPTION_ID"]
        self.resource_group = os.environ["RESOURCE_GROUP"]
        self.cached_createtimes = pypeliner.flyweight.FlyweightState()
        self.blob_client = None
    def connect(self, storage_account_name):
        #dont need to regenerate client if already exists
        client = getattr(self, "blob_client", None)
        if client and client.account_name == storage_account_name:
            return
        storage_account_key = _get_blob_key(storage_account_name, self.client_id,
                                            self.secret_key, self.tenant_id,
                                            self.subscription_id, self.resource_group)
        self.blob_client = azure.storage.blob.BlockBlobService(
            account_name=storage_account_name,
            account_key=storage_account_key)
    def create_createtime_cache(self, blob_name):
        return self.cached_createtimes.create_flyweight(blob_name)
    def __enter__(self):
        self.cached_createtimes.__enter__()
        return self
    def __exit__(self, exc_type, exc_value, traceback):
        self.cached_createtimes.__exit__(exc_type, exc_value, traceback)
    def __getstate__(self):
        return (self.cached_createtimes, self.rabbitmq_username, self.rabbitmq_password,
                self.rabbitmq_ipaddress, self.rabbitmq_vhost, self.client_id,
                self.secret_key, self.tenant_id, self.subscription_id,
                self.resource_group)
    def __setstate__(self, state):
        self.cached_createtimes, self.rabbitmq_username, self.rabbitmq_password,\
        self.rabbitmq_ipaddress, self.rabbitmq_vhost, self.client_id,\
        self.secret_key, self.tenant_id, self.subscription_id,\
        self.resource_group = state
    def create_store(self, filename, extension=None, **kwargs):
        if extension is not None:
            filename = filename + extension
        blob_name = _get_blob_name(filename)
        return AzureBlob(self, filename, blob_name, **kwargs)
    def unpack_path(self, filename):
        if filename.startswith('/'):
            filename = filename[1:]
        filename = filename.split('/')
        storage_account = filename[0]
        container_name = filename[1]
        filename = '/'.join(filename[2:])
        return storage_account, container_name, filename
    def push(self, blob_name, filename, createtime):
        storage_account, container_name, blob_name = self.unpack_path(blob_name)
        self.connect(storage_account)
        self.blob_client.create_container(container_name)
        self.blob_client.create_blob_from_path(
            container_name,
            blob_name,
            filename,
            metadata={'create_time': createtime})
    def pull(self, blob_name, filename):
        try:
            storage_account, container_name, blob_name = self.unpack_path(blob_name)
            self.connect(storage_account)
            blob = self.blob_client.get_blob_properties(
                container_name,
                blob_name)
            blob_size = blob.properties.content_length
            blob = download_from_blob_to_path(
                self.blob_client,
                storage_account,
                container_name,
                blob_name,
                filename,
                self.rabbitmq_username,
                self.rabbitmq_password,
                self.rabbitmq_ipaddress,
                self.rabbitmq_vhost)
        except azure.common.AzureMissingResourceHttpError:
            print blob_name, filename
            raise pypeliner.storage.InputMissingException(blob_name)
        filesize = os.path.getsize(filename)
        assert blob_size == blob.properties.content_length
        if filesize != blob.properties.content_length:
            raise Exception('file size mismatch for {}:{} compared to {}:{}'.format(
                blob_name, blob.properties.content_length, filename, filesize))
    def retrieve_blob_createtime(self, blob_name):
        storage_account, container_name, blob_name = self.unpack_path(blob_name)
        try:
            self.connect(storage_account)
            blob = self.blob_client.get_blob_properties(
                container_name,
                blob_name)
        except azure.common.AzureMissingResourceHttpError:
            raise BlobMissing((container_name, blob_name))
        if 'create_time' in blob.metadata:
            return blob.metadata['create_time']
        return blob.properties.last_modified.strftime('%Y/%m/%d-%H:%M:%S')
    def update_blob_createtime(self, blob_name, createtime):
        storage_account, container_name, blob_name = self.unpack_path(blob_name)
        self.connect(storage_account)
        self.blob_client.set_blob_metadata(
            container_name,
            blob_name,
            {'create_time': createtime})
    def delete_blob(self, blob_name):
        storage_account, container_name, blob_name = self.unpack_path(blob_name)
        self.connect(storage_account)
        self.blob_client.delete_blob(
            container_name,
            blob_name)

