from __future__ import absolute_import

import os
import datetime
import time

import pypeliner.helpers
import pypeliner.storage
import pypeliner.flyweight

from .aws_storage import AwsSimpleStorageService

import pypeliner.contrib.aws.helpers as aws_helpers


class AwsObjectStorage(object):
    """
    :param storage: storage object
    :type storage: object
    :param filename: filename for the storage
    :type filename: str
    :param kwargs: store_dir, extension and direct_write
    :type kwargs: dict
    """
    def __init__(self, storage, filename, **kwargs):
        self.storage = storage
        if filename.startswith('/'):
            filename = filename[1:]
        store_dir = kwargs.get("store_dir")
        extension = kwargs.get("extension")
        direct_write = kwargs.get("direct_write", False)

        self.object_name = filename
        if self.object_name.startswith('/'):
            self.object_name = self.object_name[1:]
        if extension:
            self.object_name += extension
        self.write_filename = filename + ('.tmp', '')[direct_write]
        self.filename = filename

        self.createtime_cache = storage.create_createtime_cache(self.object_name)

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
        self.storage.push(self.object_name, self.filename, create_time)
        self.createtime_cache.set(create_time)

    def pull(self):
        self.storage.pull(self.object_name, self.filename)

    def get_exists(self):
        createtime = self.get_createtime()
        return createtime is not None and createtime != 'missing'

    def get_createtime(self):
        createtime = self.createtime_cache.get()
        if createtime is None:
            if not self.storage.exists(self.object_name):
                createtime = 'missing'
            else:
                createtime = self.storage.retrieve_object_createtime(self.object_name)
            self.createtime_cache.set(createtime)
        if createtime == 'missing':
            return None
        createtime = datetime.datetime.strptime(createtime, '%Y/%m/%d-%H:%M:%S')
        return time.mktime(createtime.timetuple())

    def touch(self):
        createtime = datetime.datetime.now().strftime('%Y/%m/%d-%H:%M:%S')
        self.storage.update_object_createtime(self.object_name, createtime)
        self.createtime_cache.set(createtime)

    def delete(self):
        self.storage.delete_object(self.object_name)
        self.createtime_cache.set(None)


class AwsStorage(object):
    def __init__(self, **kwargs):
        aws_helpers.set_aws_logging_filters()
        self.cached_createtimes = pypeliner.flyweight.FlyweightState()
        self.object_client = AwsSimpleStorageService()

    def connect(self):
        """
        generate the s3 client if needed.
        :return:
        :rtype:
        """
        # dont need to regenerate client if already exists
        client = getattr(self, "object_client", None)
        if client:
            return
        self.object_client = AwsSimpleStorageService()

    def exists(self, object_name):
        """
        check if specified object exists
        :param object_name: name of object, should start with bucket name
        :type object_name: str
        :return: True if exists in S3
        :rtype: bool
        """
        bucket, object_name = self.unpack_path(object_name)
        self.connect()
        if not self.object_client.bucket_exists(bucket):
            return False
        if not self.object_client.key_exists(bucket, object_name):
            return False
        return True

    def create_createtime_cache(self, object_name):
        """
        cache the create time
        :param object_name: name of object, should start with bucket name
        :type object_name: str
        :return: flyweight cache object
        :rtype: pypeliner.flyweight.ReattachableFlyweight
        """
        return self.cached_createtimes.create_flyweight(object_name)

    def __enter__(self):
        self.cached_createtimes.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.cached_createtimes.__exit__(exc_type, exc_value, traceback)

    def __getstate__(self):
        return self.cached_createtimes

    def __setstate__(self, state):
        self.cached_createtimes = state

    def create_store(self, filename, **kwargs):
        """
        create a storage object for the file
        :param filename: name of object, should start with bucket name
        :type filename: str
        :param kwargs: keyword args for storage
        :type kwargs: dict
        :return: storage object
        :rtype: pypeliner.contrib.aws.objectstorage.AwsObjectStorage
        """
        return AwsObjectStorage(self, filename, **kwargs)

    def unpack_path(self, filename):
        """
        extract bucket and object from path
        :param filename: file path
        :type filename: str
        :return: bucket and object names
        :rtype: list of str
        """
        if filename.startswith('/'):
            filename = filename[1:]
        filename = filename.split('/')
        bucket = filename[0]
        filename = '/'.join(filename[1:])
        return bucket, filename

    def push(self, object_name, filename, createtime):
        """
        push data to s3
        :param object_name: name of object, should start with bucket name
        :type object_name: str
        :param filename: path on local storage
        :type filename: str
        :param createtime: time string
        :type createtime: str
        """
        bucket, object_name = self.unpack_path(object_name)
        self.connect()
        self.object_client.create_bucket(bucket)
        self.object_client.upload_from_file(
            filename, object_name, bucket)
        self.object_client.set_metadata(bucket, object_name, 'create_time', createtime)

    def pull(self, object_name, filename):
        """
        pull data from S3
        :param object_name: name of object, should start with bucket name
        :type object_name: str
        :param filename: path on local storage
        :type filename: str
        """
        bucket, object_name = self.unpack_path(object_name)
        self.connect()
        self.object_client.download_to_path(object_name, bucket, filename)
        object_size = self.object_client.get_file_size(bucket, object_name)
        file_size = os.path.getsize(filename)
        if file_size != object_size:
            raise Exception('file size mismatch for {}:{} compared to {}:{}'.format(
                object_name, object_size, filename, file_size))

    def retrieve_object_createtime(self, object_name):
        """
        get the create time from object metadata
        :param object_name: name of object, should start with bucket name
        :type object_name: str
        :return: time str
        :rtype: str
        """
        bucket, object_name = self.unpack_path(object_name)
        self.connect()
        createtime = self.object_client.get_metadata(bucket, object_name, 'create_time')
        if not createtime:
            createtime = self.object_client.get_last_modified(bucket, object_name)
        return createtime

    def update_object_createtime(self, object_name, createtime):
        """
        update createtime in S3 metadata
        :param object_name: name of object, should start with bucket name
        :type object_name: str
        :param createtime: time string
        :type createtime: str
        """
        bucket, object_name = self.unpack_path(object_name)
        self.connect()
        self.object_client.set_metadata(
            bucket,
            object_name,
            'create_time',
            createtime)

    def delete_object(self, object_name):
        """
        delete an object from S3
        :param object_name: name of object, should start with bucket name
        :type object_name: str
        """
        bucket, object_name = self.unpack_path(object_name)
        self.connect()
        self.object_client.delete_key(
            bucket,
            object_name)
