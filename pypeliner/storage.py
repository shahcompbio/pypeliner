import os
import datetime
import time
import shutil

import pypeliner.helpers


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


class RegularTempFile(RegularFile):
    def __init__(self, filename, direct_write=False):
        super(RegularTempFile, self).__init__(filename, direct_write=direct_write)
        self.placeholder_filename = self.filename + '._placeholder'
    def _save_createtime(self):
        pypeliner.helpers.saferemove(self.placeholder_filename)
        pypeliner.helpers.touch(self.placeholder_filename)
        shutil.copystat(self.filename, self.placeholder_filename)
    def push(self):
        super(RegularTempFile, self).push()
        self._save_createtime()
    def get_createtime(self):
        if os.path.exists(self.filename):
            return os.path.getmtime(self.filename)
        if os.path.exists(self.placeholder_filename):
            return os.path.getmtime(self.placeholder_filename)
    def touch(self):
        super(RegularTempFile, self).touch(self.filename)
        self._save_createtime()
    def delete(self):
        pypeliner.helpers.saferemove(self.filename)


class FileStorage(object):
    def __init__(self):
        pass
    def create_store(self, filename, is_temp=False, **kwargs):
        if is_temp:
            return RegularTempFile(filename, **kwargs)
        else:
            return RegularFile(filename, **kwargs)


