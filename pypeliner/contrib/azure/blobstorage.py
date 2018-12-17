import os
import datetime
import time

import pypeliner.helpers
import pypeliner.storage
import pypeliner.flyweight
import azure.storage.blob
import azure.common

import pypeliner.contrib.azure.helpers as azure_helpers


class BlobMissing(Exception):
    pass


class AzureBlob(object):
    def __init__(self, storage, filename, **kwargs):
        self.storage = storage
        if filename.startswith('/'):
            filename = filename[1:]
        store_dir = kwargs.get("store_dir")
        extension = kwargs.get("extension")
        direct_write = kwargs.get("direct_write", False)

        self.blob_name = helpers.get_blob_name(filename)
        if extension:
            self.blob_name += extension
        self.write_filename = filename + ('.tmp', '')[direct_write]
        self.filename = filename

        self.createtime_cache = storage.create_createtime_cache(self.blob_name)

        if extension is not None:
            self.filename = filename + extension
            self.write_filename = self.write_filename + extension
        if store_dir and not direct_write:
            self.filename = os.path.join(store_dir, self.filename)
            self.write_filename = os.path.join(store_dir, self.write_filename)

    def allocate(self):
        pypeliner.helpers.makedirs(os.path.dirname(self.filename))

    def push(self):
        try:
            os.rename(self.write_filename, self.filename)
        except OSError:
            raise pypeliner.storage.OutputMissingException(self.write_filename)
        create_time = datetime.datetime.fromtimestamp(os.path.getmtime(self.filename)).strftime('%Y/%m/%d-%H:%M:%S')
        self.storage.push(self.blob_name, self.filename, create_time)
        self.createtime_cache.set(create_time)

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
        azure_helpers.set_azure_logging_filters()
        self.rabbitmq_username = os.environ.get("RABBITMQ_USERNAME", None)
        self.rabbitmq_password = os.environ.get("RABBITMQ_PASSWORD", None)
        self.rabbitmq_ipaddress = os.environ.get("RABBITMQ_IP", None)
        self.rabbitmq_vhost = os.environ.get("RABBITMQ_VHOST", None)
        self.client_id = os.environ["CLIENT_ID"]
        self.secret_key = os.environ["SECRET_KEY"]
        self.tenant_id = os.environ["TENANT_ID"]
        self.subscription_id = os.environ["SUBSCRIPTION_ID"]
        self.resource_group = os.environ["RESOURCE_GROUP"]
        self.keyvault_account = os.environ['AZURE_KEYVAULT_ACCOUNT']
        self.cached_createtimes = pypeliner.flyweight.FlyweightState()
        self.blob_client = None

    def connect(self, storage_account_name):
        # dont need to regenerate client if already exists
        client = getattr(self, "blob_client", None)
        if client and client.account_name == storage_account_name:
            return
        storage_account_key = azure_helpers.get_storage_account_key(
            storage_account_name, self.client_id, self.secret_key,
            self.tenant_id, self.keyvault_account)
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
                self.resource_group, self.keyvault_account)

    def __setstate__(self, state):
        [self.cached_createtimes, self.rabbitmq_username, self.rabbitmq_password,
         self.rabbitmq_ipaddress, self.rabbitmq_vhost, self.client_id,
         self.secret_key, self.tenant_id, self.subscription_id,
         self.resource_group, self.keyvault_account] = state

    def create_store(self, filename, **kwargs):
        return AzureBlob(self, filename, **kwargs)

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
            container_name, blob_name, filename,
            metadata={'create_time': createtime}
        )

    def pull(self, blob_name, filename):
        try:
            storage_account, container_name, blob_name = self.unpack_path(blob_name)
            self.connect(storage_account)
            blob = self.blob_client.get_blob_properties(
                container_name,
                blob_name)
            blob_size = blob.properties.content_length
            blob = azure_helpers.download_from_blob_to_path(
                self.blob_client,
                container_name,
                blob_name,
                filename,
                account_name=storage_account,
                username=self.rabbitmq_username,
                password=self.rabbitmq_password,
                ipaddress=self.rabbitmq_ipaddress,
                vhost=self.rabbitmq_vhost)
        except azure.common.AzureMissingResourceHttpError:
            raise pypeliner.storage.InputMissingException(blob_name)
        file_size = os.path.getsize(filename)
        assert blob_size == blob.properties.content_length
        if file_size != blob.properties.content_length:
            raise Exception('file size mismatch for {}:{} compared to {}:{}'.format(
                blob_name, blob.properties.content_length, filename, file_size))

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
