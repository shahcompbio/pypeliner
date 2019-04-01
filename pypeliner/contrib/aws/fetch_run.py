from __future__ import absolute_import

import os
import sys
import shutil
import subprocess
import pypeliner
from .objectstorage import AwsSimpleStorageService


def init_storage():
    """
    init S3 handling code
    :return: pypeliner S3 class
    :rtype: pypeliner.contrib.aws.aws_storage.SimpleStorageService
    """
    client = AwsSimpleStorageService()
    return client


def run_delegator(before, after, stdout, stderr, workdir):
    """
    run pypeliner delegator
    :param before: path to job callable pickle
    :type before: str
    :param after: path to output pickle
    :type after: str
    :param stdout: file with stdout from pypeliner_delegate
    :type stdout: str
    :param stderr: file with stderr from pypeliner_delegate
    :type stderr: str
    :param workdir: dir where intermediate files will be generated
    :type workdir: str
    """
    before = os.path.abspath(before)
    after = os.path.abspath(after)
    stdout = os.path.abspath(stdout)
    stderr = os.path.abspath(stderr)

    current_path = os.getcwd()

    os.chdir(workdir)

    command = ["pypeliner_delegate", before, after]

    with open(stdout, 'w') as stdout_stream,open(stderr, 'w') as stderr_stream:
        p = subprocess.Popen(command, stdout=stdout_stream, stderr=stderr_stream)
        p.communicate()

    os.chdir(current_path)

def upload_data(client, key, bucket):
    """
    upload file to S3
    :param client: S3 handler
    :type client: pypeliner.contrib.aws.aws_storage.SimpleStorageService
    :param key: object name
    :type key: str
    :param bucket: bucket name
    :type bucket: str
    """

    after_pickle = key + '.after'
    client.upload_from_file(after_pickle, after_pickle, bucket)

    stdout = key + ".stdout"
    client.upload_from_file(stdout, stdout, bucket)

    stderr = key + ".stderr"
    client.upload_from_file(stderr, stderr, bucket)

def cleanup(directories, files):
    """
    remove files
    :param directories: directories to remove
    :type directories: list of str
    :param files: files to remove
    :type files: list of str
    """
    for dirname in directories:
        shutil.rmtree(dirname)

    for filename in files:
        os.remove(filename)


def main():
    """
    download the pickle
    cd into workdir and run delegator
    upload after pickle and logs
    remove all intermediate files
    """
    bucket = sys.argv[1]
    key = sys.argv[2]
    workdir = sys.argv[3]

    pypeliner.helpers.makedirs(workdir)

    client = init_storage()

    if key.startswith('/'):
        key = key[1:]

    # assume we're downloading into current dir
    pypeliner.helpers.makedirs(os.path.dirname(key))

    client.download_to_path(key, bucket, key)

    after_pickle = key + '.after'
    stdout = key + ".stdout"
    stderr = key + ".stderr"
    run_delegator(key, after_pickle, stdout, stderr, workdir)

    upload_data(client, key, bucket)

    cleanup([workdir], [key, after_pickle, stdout, stderr])


if __name__ == '__main__':
    main()