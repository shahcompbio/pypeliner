import os
import datetime
import time
import shutil
import shelve

import pypeliner.helpers
import pypeliner.flyweight


class OutputMissingException(Exception):
    def __init__(self, filename):
        self.filename = filename
    def __str__(self):
        return 'expected output {0} missing'.format(self.filename)


class RegularFile(object):
    def __init__(self, filename, direct_write=False):
        self.filename = filename
        self.direct_write = direct_write
    def allocate_input(self):
        self.allocated_filename = self.filename
        pypeliner.helpers.makedirs(os.path.dirname(self.allocated_filename))
        return self.allocated_filename
    def allocate_output(self):
        suffix = ('.tmp', '')[self.direct_write]
        self.allocated_filename = self.filename + suffix
        pypeliner.helpers.makedirs(os.path.dirname(self.allocated_filename))
        return self.allocated_filename
    def push(self):
        try:
            os.rename(self.allocated_filename, self.filename)
        except OSError:
            raise OutputMissingException(self.allocated_filename)
    def pull(self):
        pass
    def get_exists(self):
        return os.path.exists(self.filename)
    def get_createtime(self):
        if os.path.exists(self.filename):
            return os.path.getmtime(self.filename)
    def touch(self):
        pypeliner.helpers.touch(self.filename)
    def delete(self):
        raise Exception('cannot delete non-temporary files')


class FileStorage(object):
    def __init__(self):
        pass
    def create_store(self, filename, **kwargs):
        return RegularFile(filename, **kwargs)


class RegularTempFile(RegularFile):
    def __init__(self, filename, createtime, direct_write=False):
        super(RegularTempFile, self).__init__(filename, direct_write=direct_write)
        self.createtime = createtime
    def _save_createtime(self):
        self.createtime.set(os.path.getmtime(self.filename))
    def push(self):
        super(RegularTempFile, self).push()
        self._save_createtime()
    def get_createtime(self):
        if os.path.exists(self.filename):
            return os.path.getmtime(self.filename)
        return self.createtime.get()
    def touch(self):
        super(RegularTempFile, self).touch(self.filename)
        self._save_createtime()
    def delete(self):
        pypeliner.helpers.saferemove(self.filename)


class ShelvedState(object):
    def __init__(self, shelf_filename):
        self.shelf = shelve.open(shelf_filename)
    def __del__(self):
        self.shelf.close()
    def get(self, key):
        return self.shelf.get(key)
    def set(self, key, value):
        self.shelf[key] = value


class TempFileStorage(object):
    def __init__(self, shelf_filename):
        def create_shelved_state():
            return ShelvedState(shelf_filename)
        self.createtime_state = pypeliner.flyweight.FlyweightState(
            'TempFileStorage.createtime',
            create_shelved_state)
    def __enter__(self):
        self.createtime_state.__enter__()
        return self
    def __exit__(self, exc_type, exc_value, traceback):
        self.createtime_state.__exit__(exc_type, exc_value, traceback)
    def create_store(self, filename, **kwargs):
        createtime = self.createtime_state.create_flyweight(filename)
        return RegularTempFile(filename, createtime, **kwargs)


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


