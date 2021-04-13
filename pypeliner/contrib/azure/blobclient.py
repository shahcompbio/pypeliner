import logging
import os

import azure.common
import azure.core.exceptions
import azure.storage.blob as azureblob
import azure.storage.blob._shared_access_signature as blob_sas
import datetime
import pypeliner
from azure.common import AzureHttpError
from azure.core.exceptions import ResourceNotFoundError
from azure.identity import ClientSecretCredential
from pypeliner.helpers import Backoff


class UnconfiguredStorageAccountError(Exception):
    pass


class BlobMissing(Exception):
    pass


class UnknownBlobPermission(Exception):
    pass


class WrongStorageClient(Exception):
    pass


class MissingBlobPath(Exception):
    pass


class MissingContainerName(Exception):
    pass


class IncorrectContainerUri(Exception):
    pass


class AzureLoggingFilter(logging.Filter):
    def __init__(self):
        self.filter_keywords = [
            'azure', 'adal-python', 'urllib3', 'msrest', 'msal'
        ]

    def filter(self, rec):
        logname = rec.name.split('.')[0]
        if logname in self.filter_keywords:
            return False
        return True


class BlobStorageClient(object):
    def __init__(
            self, storage_account_name, client_id, tenant_id, secret_key,
            storage_account_token=None,
    ):
        """
        abstraction around all azure blob related methods
        :param storage_account_name:
        :type storage_account_name:
        :param client_id: Azure AD client id (application id)
        :type client_id: str
        :param tenant_id: Azure AD tenant id
        :type tenant_id: str
        :param secret_key: secret key for the app
        :type secret_key: str
        """
        self.account_name = storage_account_name

        self.logger = self.__get_logger(add_azure_filter=True)

        if not storage_account_token:
            storage_account_token = self.__get_storage_account_token(
                client_id, secret_key, tenant_id
            )

        storage_account_url = self.__get_account_url(storage_account_name)

        self.blob_client = azureblob.BlobServiceClient(
            storage_account_url,
            storage_account_token)

    @staticmethod
    def __get_account_url(account_name):
        """
        generate and return oauth account url
        :param account_name: storage account_name
        :type str
        :return: storage account oauth url
        :rtype: str
        """
        oauth_url = "https://{}.blob.core.windows.net".format(
            account_name
        )

        return oauth_url

    def __get_logger(self, add_azure_filter=False):
        """
        generate a logger, optionally add filter to remove all azure sdk logs
        :param add_azure_filter: set the filter if set
        :type add_azure_filter: bool
        :return: python logger object
        :rtype: logging.Logger
        """
        logger = logging.getLogger('pypeliner.execqueue.azure_batch')
        if add_azure_filter:
            for handler in logging.root.handlers:
                handler.addFilter(AzureLoggingFilter())
        return logger

    def __unpack_blob_uri(self, blob_uri):
        blob_uri = self.__format_blob_name(blob_uri)

        blob_uri = blob_uri.split('/')
        storage_account = blob_uri[0]
        container_name = blob_uri[1]
        blob_name = '/'.join(blob_uri[2:])
        blob_name = self.__format_blob_name(blob_name)

        return storage_account, container_name, blob_name

    def __unpack_container_uri(self, container_uri):
        container_uri = self.__format_blob_name(container_uri)

        container_uri_split = container_uri.split('/')

        if not len(container_uri_split) == 2:
            raise IncorrectContainerUri('could not parse container uri: {}'.format(container_uri))

        storage_account = container_uri_split[0]
        container_name = container_uri_split[1]

        return storage_account, container_name

    def __parse_blob_args(self, blob_uri, container_name, blob_name):
        if blob_uri:
            storage_account, container_name, blob_name = self.__unpack_blob_uri(blob_uri)
            if not storage_account == self.account_name:
                err_str = "The storage client intended for {} used for account {}".format(
                    self.account_name, storage_account
                )
                raise WrongStorageClient(err_str)
        else:
            if container_name is None:
                raise MissingContainerName("Container name not specified")
            elif blob_name is None:
                raise MissingBlobPath("Blob Path not specified")

        blob_name = self.__format_blob_name(blob_name)
        return container_name, blob_name

    def __parse_container_args(self, container_uri, container_name):
        if container_uri:
            storage_account, container_name = self.__unpack_container_uri(container_uri)
            if not storage_account == self.account_name:
                err_str = "The storage client intended for {} used for account {}".format(
                    self.account_name, storage_account
                )
                raise WrongStorageClient(err_str)
        else:
            if container_name is None:
                raise MissingContainerName("Container name not specified")

        return container_name

    def __format_blob_name(self, filename):
        """
        blob storage backend expects path that includes
        storage account name, container name and blob name
        without a leading slash. remove leading slash if input
        starts with one.
        :param filename: input filename
        :return: blob backend compatible blob name
        """
        return filename.strip('/')

    def __get_storage_account_token(
            self, client_id, secret_key, tenant_id
    ):
        """
        Uses the azure management package and the active directory
        credentials to fetch the authentication key for a storage
        account from azure key vault. The key must be stored in azure
        keyvault for this to work.
        :param str accountname: storage account name
        """

        token_credential = ClientSecretCredential(tenant_id, client_id, secret_key)

        return token_credential

    @Backoff(exception_type=AzureHttpError, max_backoff=1800, randomize=True)
    def __get_blob_to_path(
            self, container_name, blob_name, destination_file_path,
    ):
        """
        download data from blob storage
        :param container_name: blob container name
        :param blob_name: blob path
        :param destination_file_path: path to download the file to
        :return: azure.storage.blob.baseblobservice.Blob instance with content properties and metadata
        """
        blob_name = self.__format_blob_name(blob_name)
        try:
            blob_client = self.blob_client.get_blob_client(container_name, blob_name)
            with open(destination_file_path, "wb") as my_blob:
                download_stream = blob_client.download_blob()
                download_stream.readinto(my_blob)
            blob = blob_client.get_blob_properties()
        except Exception as exc:
            self.logger.exception("Error downloading {} from {}".format(blob_name, container_name))
            raise exc

        if not blob:
            self.logger.exception('Blob SDK returned None, retrying')
            raise AzureHttpError('Blob download failure', 1)

        return blob

    def __get_blob_permission_obj(self, permissions):
        permissions = permissions.split('|')
        permissions = {val: True for val in permissions}
        return azure.storage.blob.BlobSasPermissions(**permissions)

    def __get_blob_properties(self, container_name, blob_name):
        blob_name = self.__format_blob_name(blob_name)
        try:
            blob_client = self.blob_client.get_blob_client(container_name, blob_name)
            blob = blob_client.get_blob_properties()
        except azure.common.AzureMissingResourceHttpError:
            raise BlobMissing((container_name, blob_name))
        except:
            raise BlobMissing((container_name, blob_name))

        return blob

    def download_to_path(
            self, destination_file_path, container_name=None, blob_name=None, blob_uri=None,
    ):
        """
        download blob to a path on the node
        :param destination_file_path: path to local file
        :type destination_file_path: str
        :param container_name: name of container
        :type container_name: str
        :param blob_name: blob path
        :type blob_name: str
        :param blob_uri: unique blob identifier. generated by joining
        storage account name, container name and blob path
        :type blob_uri: str
        """
        container_name, blob_name = self.__parse_blob_args(blob_uri, container_name, blob_name)

        try:
            blob = self.__get_blob_to_path(
                container_name, blob_name, destination_file_path,
            )
        except azure.common.AzureMissingResourceHttpError:
            raise pypeliner.storage.InputMissingException(blob_name)

        blob_size = self.get_blob_size(container_name, blob_name)
        assert blob_size == blob.size

        file_size = os.path.getsize(destination_file_path)
        if file_size != blob.size:
            raise Exception('file size mismatch for {}:{} compared to {}:{}'.format(
                blob_name, blob.properties.content_length, destination_file_path, file_size))

    def upload_from_file(
            self, file_path, metadata=None, container_name=None, blob_name=None, blob_uri=None
    ):
        """
        :param file_path: path of file to upload
        :type file_path: str
        :param metadata: metadata to attach to blob
        :type metadata: str
        :param container_name: name of container
        :type container_name: str
        :param blob_name: blob path
        :type blob_name: str
        :param blob_uri: unique blob identifier. generated by joining
        storage account name, container name and blob path
        :type blob_uri: str
        """
        container_name, blob_name = self.__parse_blob_args(blob_uri, container_name, blob_name)

        self.create_container(container_name=container_name)

        blob_client = self.blob_client.get_blob_client(container_name, blob_name)

        if os.stat(file_path).st_size > 1024 * 1024 * 1024:
            blob_client.MAX_BLOCK_SIZE = 100 * 1024 * 1024
            with open(file_path, "rb") as stream:
                blob_client.upload_blob(stream, overwrite=True)
        else:
            blob_client.MAX_BLOCK_SIZE = 4 * 1024 * 1024
            with open(file_path, "rb") as stream:
                blob_client.upload_blob(stream, overwrite=True)

        blob_client.set_blob_metadata(metadata=metadata)

    def blob_exists(self, container_name=None, blob_name=None, blob_uri=None):
        """
        check if blob exists
        :param container_name: name of container
        :type container_name: str
        :param blob_name: blob path
        :type blob_name: str
        :param blob_uri: unique blob identifier. generated by joining
        storage account name, container name and blob path
        :type blob_uri: str
        :return: True if blob exists
        :rtype: bool
        """
        container_name, blob_name = self.__parse_blob_args(blob_uri, container_name, blob_name)

        blob_client = self.blob_client.get_blob_client(container_name, blob_name)

        try:
            blob_client.get_blob_properties()
            return True
        except:
            return False

    def get_blob_size(self, container_name=None, blob_name=None, blob_uri=None):
        """
        get blob size in bytes
        :param container_name: name of container
        :type container_name: str
        :param blob_name: blob path
        :type blob_name: str
        :param blob_uri: unique blob identifier. generated by joining
        storage account name, container name and blob path
        :type blob_uri: str
        :return: blob length
        :rtype: int
        """
        container_name, blob_name = self.__parse_blob_args(blob_uri, container_name, blob_name)
        blob = self.__get_blob_properties(container_name, blob_name)
        return blob.size

    def set_metadata(
            self, metadata_key, metadata_value, container_name=None, blob_name=None, blob_uri=None,
    ):
        """
        set a metadata key value pair for a blob
        :param metadata_key: key for metadata pair
        :type metadata_key: str
        :param metadata_value: value for the metadata pair
        :type metadata_value: Any
        :param container_name: name of container
        :type container_name: str
        :param blob_name: blob path
        :type blob_name: str
        :param blob_uri: unique blob identifier. generated by joining
        storage account name, container name and blob path
        :type blob_uri: str
        """
        container_name, blob_name = self.__parse_blob_args(blob_uri, container_name, blob_name)
        self.blob_client.set_blob_metadata(
            container_name,
            blob_name,
            {metadata_key: metadata_value})

    def get_metadata(self, metadata_key, container_name=None, blob_name=None, blob_uri=None):
        """
        pull value for provided from blob metadata
        :param metadata_key: key to look up
        :type metadata_key: any
        :param container_name: name of container
        :type container_name: str
        :param blob_name: blob path
        :type blob_name: str
        :param blob_uri: unique blob identifier. generated by joining
        storage account name, container name and blob path
        :type blob_uri: str
        :return: value of provided key in metadata
        :rtype: any
        """
        container_name, blob_name = self.__parse_blob_args(blob_uri, container_name, blob_name)
        blob = self.__get_blob_properties(container_name, blob_name)
        return blob.metadata.get(metadata_key)

    def get_last_modified(self, container_name=None, blob_name=None, blob_uri=None):
        """
        get time blob was last modified
        :param container_name: name of container
        :type container_name: str
        :param blob_name: blob path
        :type blob_name: str
        :param blob_uri: unique blob identifier. generated by joining
        storage account name, container name and blob path
        :type blob_uri: str
        :return: time formatted as string
        :rtype: str
        """
        container_name, blob_name = self.__parse_blob_args(blob_uri, container_name, blob_name)
        blob = self.__get_blob_properties(container_name, blob_name)
        return blob.last_modified.strftime('%Y/%m/%d-%H:%M:%S')

    def delete_blob(self, container_name=None, blob_name=None, blob_uri=None):
        """
        delete a blob
        :param container_name: name of container
        :type container_name: str
        :param blob_name: blob path
        :type blob_name: str
        :param blob_uri: unique blob identifier. generated by joining
        storage account name, container name and blob path
        :type blob_uri: str
        """
        container_name, blob_name = self.__parse_blob_args(blob_uri, container_name, blob_name)
        if self.blob_exists(container_name=container_name, blob_name=blob_name):
            self.blob_client.delete_blob(container_name, blob_name)

    def generate_blob_url(
            self, container_name=None, blob_name=None, blob_uri=None,
            add_sas=False, sas_lifetime_hours=120,
            sas_permissions='read'
    ):
        """
        generate url for blob. can also add sas if required
        :param container_name: name of container
        :type container_name: str
        :param blob_name: blob path
        :type blob_name: str
        :param blob_uri: unique blob identifier. generated by joining
        storage account name, container name and blob path
        :type blob_uri: str
        :param add_sas: adds SAS to url if set
        :type add_sas: bool
        :param sas_lifetime_hours: SAS lifetime in hours
        :type sas_lifetime_hours: int
        :param sas_permissions: permissions for SAS token
        :type sas_permissions: str
        :return: blob url
        :rtype: str
        """
        container_name, blob_name = self.__parse_blob_args(blob_uri, container_name, blob_name)
        sas_token = None
        if add_sas:
            sas_token = self.get_blob_sas_token(
                container_name=container_name,
                blob_name=blob_name,
                blob_permissions=sas_permissions,
                lifetime_hours=sas_lifetime_hours
            )

        return self.make_blob_url(container_name, blob_name, sas_token=sas_token)

    def get_blob_sas_token(
            self, container_name=None, blob_name=None, blob_uri=None,
            blob_permissions='read', lifetime_hours=120
    ):
        """
        generate a SAS token
        :param container_name: name of container
        :type container_name: str
        :param blob_name: blob path
        :type blob_name: str
        :param blob_uri: unique blob identifier. generated by joining
        storage account name, container name and blob path
        :type blob_uri: str
        :param blob_permissions: permissions for SAS token
        :type blob_permissions: str
        :param lifetime_hours: lifetime of token in hours
        :type lifetime_hours: int
        :return: sas token
        :rtype: str
        """
        container_name, blob_name = self.__parse_blob_args(blob_uri, container_name, blob_name)
        start_time = datetime.datetime.utcnow()
        expiry_time = datetime.datetime.utcnow() + datetime.timedelta(hours=lifetime_hours)

        token = self.blob_client.get_user_delegation_key(
            key_start_time=start_time,
            key_expiry_time=expiry_time
        )

        sas_token = blob_sas.generate_blob_sas(
            self.account_name,
            container_name,
            blob_name,
            user_delegation_key=token,
            permission=self.__get_blob_permission_obj(blob_permissions),
            start=start_time,
            expiry=expiry_time,
        )

        return sas_token

    def get_container_sas_token(self, container_uri=None, container_name=None, blob_permissions='read',
                                lifetime_hours=120):

        """
        Obtains a shared access signature granting the specified permissions to the
        container.
        :param container_name: name of container
        :type container_name: str
        :param container_uri: unique blob identifier. generated by joining
        storage account name, container name and blob path
        :type container_uri: str
        :param blob_permissions: permissions for SAS token
        :type blob_permissions: str
        :param lifetime_hours: lifetime of token in hours
        :type lifetime_hours: int
        :return: A SAS token granting the specified permissions to the container.
        :rtype: str
        """
        container_name = self.__parse_container_args(container_uri, container_name)

        start_time = datetime.datetime.utcnow()
        expiry_time = datetime.datetime.utcnow() + datetime.timedelta(hours=lifetime_hours)

        token = self.blob_client.get_user_delegation_key(
            key_start_time=start_time,
            key_expiry_time=expiry_time
        )

        sas_token = blob_sas.generate_container_sas(
            self.account_name,
            container_name,
            user_delegation_key=token,
            permission=self.__get_blob_permission_obj(blob_permissions),
            start=start_time,
            expiry=expiry_time,
        )

        return sas_token

    def list_blobs(self, container_uri=None, container_name=None, prefix=None):
        """
        list all blobs in a container
        :param container_name: name of container
        :type container_name: str
        :param container_uri: unique blob identifier. generated by joining
        storage account name, container name and blob path
        :type container_uri: str
        :param prefix: list blobs that start with this prefix only
        :type prefix: str
        :return: generator to list the blobs
        :rtype: generator
        """
        container_name = self.__parse_container_args(container_uri, container_name)
        blob_client = self.blob_client.get_container_client(container_name)
        container_blobs = blob_client.list_blobs(name_starts_with=prefix)

        for container in container_blobs:
            yield container

    def create_container(self, container_uri=None, container_name=None):
        """
        create a container if it doesnt exist
        :param container_name: name of container
        :type container_name: str
        :param container_uri: unique blob identifier. generated by joining
        storage account name, container name and blob path
        :type container_uri: str
        """
        container_name = self.__parse_container_args(container_uri, container_name)

        if not self.container_exists(container_name=container_name):
            container_client = self.blob_client.get_container_client(container_name)
            container_client.create_container()

    def container_exists(self, container_uri=None, container_name=None):
        """
        check if a container exists
        :param container_uri: unique blob identifier. generated by joining
        storage account name, container name and blob path
        :type container_uri: str
        :param container_name: name of container
        :type container_name: str
        """
        container_name = self.__parse_container_args(container_uri, container_name)
        container_client = self.blob_client.get_container_client(container_name)
        try:
            container_client.get_container_properties()
            return True
        except ResourceNotFoundError:
            raise

    def make_blob_url(self, container_name, blob_name, sas_token=None):
        protocol = "https"
        primary_endpoint = "{}.blob.core.windows.net".format(self.account_name)

        url = '{}://{}/{}/{}'.format(
            protocol,
            primary_endpoint,
            container_name,
            blob_name,
        )

        if sas_token:
            url += '?' + sas_token
        return url

    def generate_container_url(
            self, container_uri=None, container_name=None, add_sas=True,
            sas_lifetime_hours=120, sas_permissions='read'
    ):
        """
        generate url for the container, adds SAS if required
        :param container_uri: unique blob identifier. generated by joining
        storage account name, container name and blob path
        :type container_uri: str
        :param container_name: name of container
        :type container_name: str
        :param add_sas: adds SAS to url if set
        :type add_sas: bool
        :param sas_lifetime_hours: SAS lifetime in hours
        :type sas_lifetime_hours: int
        :param sas_permissions: permissions for SAS token
        :type sas_permissions: str
        :return: blob url
        :rtype: str
        """
        container_name = self.__parse_container_args(container_uri, container_name)
        sas_token = None
        if add_sas:
            sas_token = self.get_container_sas_token(
                container_name=container_name,
                blob_permissions=sas_permissions,
                lifetime_hours=sas_lifetime_hours
            )

        # WORKAROUND: for some reason there is no function to create an url for a
        # container
        output_sas_url = self.make_blob_url(
            container_name, 'null',
            sas_token=sas_token).replace('/null', '')

        return output_sas_url
