import os
import datetime
import time
import shutil
import shelve

import pypeliner.helpers
import pypeliner.flyweight


class InputMissingException(Exception):
    def __init__(self, filename):
        self.filename
    def __str__(self):
        return 'expected input {} missing'.format(self.filename)


class OutputMissingException(Exception):
    def __init__(self, filename):
        self.filename = filename
    def __str__(self):
        return 'expected output {0} missing'.format(self.filename)


class RegularFile(object):
    def __init__(self, filename, exists_cache, createtime_cache, createtime_save, direct_write=True):
        self.filename = filename
        self.exists_cache = exists_cache
        self.createtime_cache = createtime_cache
        self.createtime_save = createtime_save
        self.write_filename = filename + ('.tmp', '')[direct_write]
    def allocate(self):
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
        if not self.get_exists():
            return None
        createtime = self.createtime_cache.get()
        if createtime is None:
            createtime = os.path.getmtime(self.filename)
            self.createtime_cache.set(createtime)
            self.createtime_save.set(createtime)
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
            return super(RegularTempFile, self).get_createtime()
        return self.createtime_save.get()
    def delete(self):
        pypeliner.helpers.saferemove(self.filename)


class FileStorage(object):
    def __init__(self, createtime_shelf_filename):
        self.cached_exists = pypeliner.flyweight.FlyweightState(
            'FileStorage.cached_exists', dict)
        self.cached_createtimes = pypeliner.flyweight.FlyweightState(
            'FileStorage.cached_createtimes', dict)
        self.saved_createtimes = pypeliner.flyweight.FlyweightState(
            'FileStorage.saved_createtimes', lambda: shelve.open(createtime_shelf_filename))
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


def _get_obj_key(filename):
    return 'obj:' + filename


def _get_createtime_key(filename):
    return 'createtime:' + filename


class ShelveObjectStorage(object):
    catalog = {}
    def __init__(self, shelf_filename):
        self.shelf_filename = shelf_filename
    def __enter__(self):
        self.shelf = shelve.open(self.shelf_filename)
        self.catalog[self.shelf_filename] = self
        return self
    def __exit__(self, exc_type, exc_value, traceback):
        del self.catalog[self.shelf_filename]
        self.shelf.close()
    def create_store(self, filename):
        return ShelveObject(self, self.shelf_filename, filename)
    def put(self, filename, obj):
        self.shelf[_get_obj_key(filename)] = obj
        self.touch(filename)
    def get(self, filename):
        return self.shelf[_get_obj_key(filename)]
    def get_exists(self, filename):
        return _get_obj_key(filename) in self.shelf
    def get_createtime(self, filename):
        return self.shelf.get(_get_createtime_key(filename), None)
    def touch(self, filename):
        createtime = time.mktime(datetime.datetime.now().timetuple())
        self.shelf[_get_createtime_key(filename)] = createtime


class ShelveObject(object):
    def __init__(self, storage, storage_id, filename):
        self.storage = storage
        self.storage_id = storage_id
        self.filename = filename
    def __getstate__(self):
        return (self.storage_id, self.filename)
    def __setstate__(self, state):
        self.storage_id, self.filename = state
        self.storage = ShelveObjectStorage.catalog.get(self.storage_id)
    def put(self, obj):
        self.storage.put(self.filename, obj)
    def get(self):
        return self.storage.get(self.filename)
    def get_exists(self):
        return self.storage.get_exists(self.filename)
    def get_createtime(self):
        return self.storage.get_createtime(self.filename)
    def touch(self):
        return self.storage.touch(self.filename)


