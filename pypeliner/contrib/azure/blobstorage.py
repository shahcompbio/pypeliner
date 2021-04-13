import os

import datetime
import pypeliner.flyweight
import pypeliner.helpers
import pypeliner.storage
import time
from pypeliner.sqlitedb import SqliteDb

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

        self.createtime_cache = kwargs.get('createtime_cache')
        self.createtime_save = kwargs.get('createtime_save')
        self.exists_cache = kwargs.get('exists_cache')

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
        self.createtime_save.set(create_time)
        self.exists_cache.set(True)

    def pull(self):
        self.storage.pull(self.blob_name, self.filename)

    def get_exists(self):
        exists = self.exists_cache.get()
        if exists is None:
            createtime = self.get_createtime()

            if createtime is not None and createtime != 'missing':
                exists = True
            else:
                exists = False
            self.exists_cache.set(exists)
        return exists

    def get_createtime(self):
        sentinel_only = pypeliner.helpers.GlobalState.get('sentinel_only')
        if sentinel_only:
            createtime = self.createtime_save.get()
        else:
            createtime = self.createtime_cache.get()

        if createtime is None:
            try:
                createtime = self.storage.retrieve_blob_createtime(self.blob_name)
            except BlobMissing:
                createtime = 'missing'
            self.createtime_cache.set(createtime)
            self.createtime_save.set(createtime)
        if createtime == 'missing':
            return None
        createtime = datetime.datetime.strptime(createtime, '%Y/%m/%d-%H:%M:%S')
        return time.mktime(createtime.timetuple())

    def touch(self):
        createtime = datetime.datetime.now().strftime('%Y/%m/%d-%H:%M:%S')
        self.exists_cache.set(True)
        self.storage.update_blob_createtime(self.blob_name, createtime)
        self.createtime_cache.set(createtime)
        self.createtime_save.set(createtime)

    def delete(self):
        self.storage.delete_blob(self.blob_name)
        self.createtime_cache.set(None)


class AzureBlobStorage(object):
    def __init__(self, **kwargs):
        self.client_id = os.environ["CLIENT_ID"]
        self.secret_key = os.environ["SECRET_KEY"]
        self.tenant_id = os.environ["TENANT_ID"]
        self.subscription_id = os.environ["SUBSCRIPTION_ID"]
        self.blob_client = None

        metadata_prefix = kwargs.get('metadata_prefix')
        createtime_shelf_filename = metadata_prefix + 'createtimes.db'
        self.cached_createtimes = pypeliner.flyweight.FlyweightState()
        pypeliner.helpers.makedirs(os.path.dirname(createtime_shelf_filename))
        self.saved_createtimes = pypeliner.flyweight.FlyweightState(
            state_container=SqliteDb(createtime_shelf_filename))
        self.cached_exists = pypeliner.flyweight.FlyweightState()

    def connect(self, storage_account_name):
        # dont need to regenerate client if already exists
        client = getattr(self, "blob_client", None)
        if client and client.account_name == storage_account_name:
            return

        storage_keys = pypeliner.helpers.GlobalState.get('azure_storage_keys', {})
        account_token = storage_keys.get(storage_account_name)

        self.blob_client = BlobStorageClient(
            storage_account_name, self.client_id, self.tenant_id,
            self.secret_key,
            storage_account_token=account_token,
        )

    def create_createtime_cache(self, blob_name):
        return self.cached_createtimes.create_flyweight(blob_name)

    def __enter__(self):
        self.cached_exists.__enter__()
        self.cached_createtimes.__enter__()
        self.saved_createtimes.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.cached_exists.__exit__(exc_type, exc_value, traceback)
        self.cached_createtimes.__exit__(exc_type, exc_value, traceback)
        self.saved_createtimes.__exit__(exc_type, exc_value, traceback)

    def __getstate__(self):
        return (
            self.cached_createtimes, self.saved_createtimes, self.cached_exists,
            self.client_id, self.secret_key, self.tenant_id, self.subscription_id
        )

    def __setstate__(self, state):
        [self.cached_createtimes, self.saved_createtimes, self.cached_exists,
         self.client_id, self.secret_key, self.tenant_id, self.subscription_id] = state

    def create_store(self, filename, **kwargs):
        kwargs['createtime_cache'] = self.cached_createtimes.create_flyweight(filename)
        kwargs['createtime_save'] = self.saved_createtimes.create_flyweight(filename)
        kwargs['exists_cache'] = self.cached_exists.create_flyweight(filename)

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
