import datetime
import os
import time

import pypeliner.flyweight
import pypeliner.helpers
import pypeliner.storage

from .blobclient import BlobStorageClient, BlobMissing


class AzureBlob(object):
    def __init__(self, storage, filename, **kwargs):
        self.storage = storage
        store_dir = kwargs.get("store_dir")
        extension = kwargs.get("extension")
        direct_write = kwargs.get("direct_write", False)

        self.filename = filename
        self.blob_name = filename
        self.write_filename = filename + ('.tmp', '')[direct_write]

        if extension:
            self.filename += extension
            self.blob_name += extension
            self.write_filename += extension

        if store_dir and not direct_write:
            self.filename = os.path.join(store_dir, self.filename)
            self.write_filename = os.path.join(store_dir, self.write_filename)

        self.createtime_cache = storage.create_createtime_cache(self.blob_name)

    def allocate(self):
        pypeliner.helpers.makedirs(os.path.dirname(self.filename))

    def push(self):
        try:
            os.rename(self.write_filename, self.filename)
        except OSError:
            raise pypeliner.storage.OutputMissingException(self.write_filename)

        create_time = datetime.datetime.fromtimestamp(
            os.path.getmtime(self.filename)
        )
        create_time = create_time.strftime('%Y/%m/%d-%H:%M:%S')

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
        self.rabbitmq_username = os.environ.get("RABBITMQ_USERNAME", None)
        self.rabbitmq_password = os.environ.get("RABBITMQ_PASSWORD", None)
        self.rabbitmq_ipaddress = os.environ.get("RABBITMQ_IP", None)
        self.rabbitmq_vhost = os.environ.get("RABBITMQ_VHOST", None)
        self.client_id = os.environ["CLIENT_ID"]
        self.secret_key = os.environ["SECRET_KEY"]
        self.tenant_id = os.environ["TENANT_ID"]
        self.subscription_id = os.environ["SUBSCRIPTION_ID"]
        self.keyvault_account = os.environ['AZURE_KEYVAULT_ACCOUNT']
        self.cached_createtimes = pypeliner.flyweight.FlyweightState()
        self.blob_client = None

    def connect(self, storage_account_name):
        # dont need to regenerate client if already exists
        client = getattr(self, "blob_client", None)
        if client and client.account_name == storage_account_name:
            return

        storage_keys = pypeliner.helpers.GlobalState.get('azure_storage_keys', {})
        account_key = storage_keys.get(storage_account_name)

        self.blob_client = BlobStorageClient(
            storage_account_name, self.client_id, self.tenant_id,
            self.secret_key, self.keyvault_account,
            storage_account_key=account_key,
            mq_username=self.rabbitmq_username,
            mq_password=self.rabbitmq_password,
            mq_ip=self.rabbitmq_ipaddress,
            mq_vhost=self.rabbitmq_vhost
        )

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
                self.keyvault_account)

    def __setstate__(self, state):
        [self.cached_createtimes, self.rabbitmq_username, self.rabbitmq_password,
         self.rabbitmq_ipaddress, self.rabbitmq_vhost, self.client_id,
         self.secret_key, self.tenant_id, self.subscription_id,
         self.keyvault_account] = state

    def create_store(self, filename, **kwargs):
        return AzureBlob(self, filename, **kwargs)

    def get_storage_account(self, filename):
        filename = filename.strip('/')
        filename = filename.split('/')
        storage_account = filename[0]
        return storage_account

    def push(self, blob_name, filename, createtime):
        self.connect(self.get_storage_account(blob_name))
        self.blob_client.upload_from_file(
            filename,
            metadata={'create_time': createtime},
            blob_uri=blob_name
        )

    def pull(self, blob_name, filename):
        self.connect(self.get_storage_account(blob_name))
        self.blob_client.download_to_path(
            filename,
            blob_uri=blob_name
        )

    def retrieve_blob_createtime(self, blob_name):
        self.connect(self.get_storage_account(blob_name))
        createtime = self.blob_client.get_metadata(
            'create_time', blob_uri=blob_name
        )
        if not createtime:
            return self.blob_client.get_last_modified(blob_uri=blob_name)
        else:
            return createtime

    def update_blob_createtime(self, blob_name, createtime):
        self.connect(self.get_storage_account(blob_name))
        self.blob_client.set_metadata(
            'create_time', createtime, blob_uri=blob_name
        )

    def delete_blob(self, blob_name):
        self.connect(self.get_storage_account(blob_name))
        self.blob_client.delete_blob(blob_uri=blob_name)