import importlib

from pypeliner.sqlitedb import SqliteDb
import os

import pypeliner.flyweight
import pypeliner.helpers


class InputMissingException(Exception):
    def __init__(self, filename):
        self.filename = filename

    def __str__(self):
        return 'expected input {} missing'.format(self.filename)


class OutputMissingException(Exception):
    def __init__(self, filename):
        self.filename = filename

    def __str__(self):
        return 'expected output {0} missing'.format(self.filename)


class RegularFile(object):
    def __init__(
            self, filename, exists_cache, createtime_cache, createtime_save,
            extension=None, direct_write=True, **kwargs
    ):
        self.filename = filename
        self.exists_cache = exists_cache
        self.createtime_cache = createtime_cache
        self.createtime_save = createtime_save
        self.write_filename = filename + ('.tmp', '')[direct_write]
        if extension is not None:
            self.filename = filename + extension
            self.write_filename = self.write_filename + extension

    def allocate(self):
        if not os.path.exists(self.filename):
            pypeliner.helpers.makedirs(os.path.dirname(self.filename))

    def push(self):
        try:
            os.rename(self.write_filename, self.filename)
        except OSError:
            raise OutputMissingException(self.write_filename)
        self.exists_cache.set(True)
        createtime = os.path.getmtime(self.filename)
        self.createtime_cache.set(createtime)
        self.createtime_save.set(createtime)

    def pull(self):
        if not self.get_exists():
            raise InputMissingException(self.filename)

    def get_exists(self):
        exists = self.exists_cache.get()
        if exists is None:
            exists = os.path.exists(self.filename)
            self.exists_cache.set(exists)
        return exists

    def get_createtime(self):
        # if sentinal only flag is set then pull create time from database too
        # to avoid hitting filesystem. this will cause issues if files
        # are deleted after pipeline run and before rerun
        sentinel_only = pypeliner.helpers.GlobalState.get('sentinel_only')
        if sentinel_only:
            createtime = self.createtime_save.get()
        else:
            createtime = self.createtime_cache.get()
        if createtime is None:
            if not self.get_exists():
                return None
            createtime = os.path.getmtime(self.filename)
            self.createtime_cache.set(createtime)
            self.createtime_save.set(createtime)
        if not isinstance(createtime, float):
            createtime = float(createtime)
        return createtime

    def touch(self):
        pypeliner.helpers.touch(self.filename)
        self.exists_cache.set(True)
        createtime = os.path.getmtime(self.filename)
        self.createtime_cache.set(createtime)
        self.createtime_save.set(createtime)

    def delete(self):
        raise Exception('cannot delete non-temporary files')


class RegularTempFile(RegularFile):
    def get_createtime(self):
        if super(RegularTempFile, self).get_exists():
            return float(super(RegularTempFile, self).get_createtime())
        createtime = self.createtime_save.get()
        if createtime and not isinstance(createtime, float):
            createtime = float(createtime)
        return createtime

    def delete(self):
        pypeliner.helpers.saferemove(self.filename)


class FileStorage(object):
    def __init__(self, metadata_prefix=None, **kwargs):
        createtime_shelf_filename = metadata_prefix + 'createtimes.db'
        pypeliner.helpers.makedirs(os.path.dirname(createtime_shelf_filename))
        self.cached_exists = pypeliner.flyweight.FlyweightState()
        self.cached_createtimes = pypeliner.flyweight.FlyweightState()
        self.saved_createtimes = pypeliner.flyweight.FlyweightState(
            state_container=SqliteDb(createtime_shelf_filename))

    def __enter__(self):
        self.cached_exists.__enter__()
        self.cached_createtimes.__enter__()
        self.saved_createtimes.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.cached_exists.__exit__(exc_type, exc_value, traceback)
        self.cached_createtimes.__exit__(exc_type, exc_value, traceback)
        self.saved_createtimes.__exit__(exc_type, exc_value, traceback)

    def _create_store(self, filename, factory, **kwargs):
        exists_cache = self.cached_exists.create_flyweight(filename)
        createtime_cache = self.cached_createtimes.create_flyweight(filename)
        createtime_save = self.saved_createtimes.create_flyweight(filename)
        return factory(filename, exists_cache, createtime_cache, createtime_save, **kwargs)

    def create_store(self, filename, is_temp=False, **kwargs):
        if is_temp:
            return self._create_store(filename, RegularTempFile, **kwargs)
        else:
            return self._create_store(filename, RegularFile, **kwargs)


def create(requested_storage, workflow_dir=None):
    if requested_storage is None:
        raise Exception('No storage specified')
    elif requested_storage == 'local':
        storage_name = 'pypeliner.storage.FileStorage'
    elif requested_storage == 'azureblob':
        storage_name = 'pypeliner.contrib.azure.blobstorage.AzureBlobStorage'
    elif requested_storage == 'awss3':
        storage_name = 'pypeliner.contrib.aws.objectstorage.AwsStorage'
    else:
        storage_name = requested_storage

    storage_class_name = storage_name.split('.')[-1]
    storage_module_name = storage_name[:-len(storage_class_name) - 1]

    storage_module = importlib.import_module(storage_module_name)
    storage_class = vars(storage_module)[storage_class_name]

    file_storage_prefix = os.path.join(workflow_dir, 'files_')

    storage = storage_class(metadata_prefix=file_storage_prefix)

    return storage
