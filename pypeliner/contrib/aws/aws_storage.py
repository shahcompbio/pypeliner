import boto3
import botocore

class AwsSimpleStorageService(object):
    """
    :param access_id: AWS IAM access id (optional)
    :type access_id: str
    :param access_key: AWS IAM access key (optional)
    :type access_key: str
    :param location: AWS datacenter location (optional)
    :type location: str
    :param multipart_threshold: The transfer size threshold for which multipart
    uploads, downloads, and copies will automatically be triggered.
    :type multipart_threshold: int
    :param num_download_attempts: number of retries
    :type num_download_attempts: int
    :param max_concurrency: number of concurrent downloads(threads)
    :type max_concurrency: int
    :param multipart_chunksize: The partition size of each part for a multipart transfer.
    :type multipart_chunksize: int
    :param max_io_queue: The maximum amount of read parts that can be queued in memory to be written for a download
    :type max_io_queue: int
    :param io_chunksize: The max size of each chunk in the io queue
    :type io_chunksize: int
    """

    def __init__(
            self,
            access_id=None,
            access_key=None,
            location=None,
            multipart_threshold=512*1024*1024,
            num_download_attempts=5,
            max_concurrency=1,
            multipart_chunksize=512*1024*1024,
            max_io_queue=100,
            io_chunksize=1024*1024
    ):
        self.client = boto3.client('s3', aws_access_key_id=access_id, aws_secret_access_key=access_key)
        self.resource = boto3.resource('s3', aws_access_key_id=access_id, aws_secret_access_key=access_key)
        self.location = location

        transfer_config = boto3.s3.transfer.TransferConfig(
            multipart_threshold=multipart_threshold,
            num_download_attempts=num_download_attempts,
            max_concurrency=max_concurrency,
            multipart_chunksize=multipart_chunksize,
            max_io_queue=max_io_queue,
            io_chunksize=io_chunksize
        )
        self.transfer = boto3.s3.transfer.S3Transfer(
            client=self.client,
            config=transfer_config
        )

    def create_bucket(self, bucketname):
        """
        create a new S3 bucket
        :param bucketname: name of bucket
        :type bucketname: str
        """
        if self.bucket_exists(bucketname):
            return
        response = self.client.create_bucket(
            ACL='private',
            Bucket=bucketname,
            CreateBucketConfiguration={
                'LocationConstraint': self.location
            },
            ObjectLockEnabledForBucket=True,
        )

    def create_signed_url(self, bucket, key):
        url = self.client.generate_presigned_url(
            ClientMethod='get_object',
            Params={
                'Bucket': bucket,
                'Key': key
            },
            ExpiresIn=604800
        )

        return url


    def bucket_exists(self, bucket):
        """
        check if a bucket exists
        :param bucket: bucket name
        :type bucket: str
        :return: True if exists
        :rtype: bool
        """
        bucket = self.resource.Bucket(bucket)
        if bucket.creation_date:
            return True
        else:
            return False

    def key_exists(self, bucket, key):
        """
        check if an object exists
        :param bucket: bucket name
        :type bucket: str
        :param key: object name
        :type key: str
        :return: True if exists
        :rtype: bool
        """
        bucket = self.resource.Bucket(bucket)
        s3object = bucket.Object(key)
        try:
            s3object.load()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                # The object does not exist.
                return False
            else:
                # Something else has gone wrong.
                raise
        else:
            return True

    def upload_from_file(self, source, destination, bucket):
        """
        upload file to S3. uses multipart for big files
        :param source: filename on filesystem
        :type source: str
        :param destination: key in S3
        :type destination: str
        :param bucket: bucket in S3
        :type bucket: str
        """
        self.transfer.upload_file(source, bucket, destination)

    def get_file_size(self, bucket, filename):
        """
        get size of object in S3
        :param bucket: bucket name
        :type bucket: str
        :param filename: object name
        :type filename: str
        :return: size
        :rtype: int
        """
        if not self.bucket_exists(bucket):
            raise Exception()
        if not self.key_exists(bucket, filename):
            raise Exception()

        bucket = self.resource.Bucket(bucket)
        s3object = bucket.Object(filename)
        return s3object.content_length

    def delete_key(self, bucket, key):
        """
        delete an object
        :param bucket: bucket name
        :type bucket: str
        :param key: object name
        :type key: str
        """
        self.resource.Object(bucket, key).delete()

    def download_to_path(self, source, bucket, destination):
        """
        download from S3. uses multipart for big files
        :param source: object name in S3
        :type source: str
        :param bucket: bucket name in S3
        :type bucket: str
        :param destination: file path in local filesystem
        :type destination: str
        """

        try:
            self.transfer.download_file(bucket, source, destination)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print (source,bucket, destination)
                raise Exception("The object does not exist.")
            else:
                raise

    def set_metadata(self, bucket, key, metadata_key, metadata_value):
        """
        set metadata key value pair for an object in S3
        :param bucket: bucket name
        :type bucket: str
        :param key: object name
        :type key: str
        :param metadata_key: key to store the metadata under
        :type metadata_key:  str
        :param metadata_value: value for the key
        :type metadata_value: str
        """
        copy_source = {
            'Bucket': bucket,
            'Key': key
        }
        self.client.copy(
            copy_source, bucket, key,
            ExtraArgs={
                "Metadata": {
                    metadata_key: metadata_value
                },
                "MetadataDirective": "REPLACE"
            }
        )

    def get_metadata(self, bucket, key, metadata_key):
        """
        get value for provided key in object metadata in S3
        :param bucket: bucket name
        :type bucket: str
        :param key: object name
        :type key: str
        :param metadata_key: key
        :type metadata_key: str
        :return: value for key
        :rtype:  str
        """
        key_metadata = self.client.head_object(Bucket=bucket, Key=key)
        custom_metadata = key_metadata.get('Metadata')
        if not custom_metadata:
            return None
        createtime = custom_metadata.get(metadata_key)
        return createtime

    def get_last_modified(self, bucket, key):
        """
        get time the object was last modified
        :param bucket: bucket name
        :type bucket: str
        :param key: object name
        :type key: str
        :return: time of last change(format: %Y/%m/%d-%H:%M:%S)
        :rtype: str
        """
        timeobj = self.resource.Bucket(bucket).Object(key).last_modified
        return timeobj.strftime('%Y/%m/%d-%H:%M:%S')
