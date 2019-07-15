import datetime
import os

import azure.common
import azure.storage.blob as azureblob
import pypeliner
from azure.common import AzureHttpError
from azure.common.credentials import ServicePrincipalCredentials
from azure.keyvault import KeyVaultClient, KeyVaultAuthentication
from azure.keyvault.models import KeyVaultErrorException
from pypeliner.helpers import Backoff

from .rabbitmq import RabbitMqSemaphore


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


class BlobStorageClient(object):
    def __init__(
            self, storage_account_name, client_id, tenant_id, secret_key, keyvault_account,
            storage_account_key=None, mq_username=None, mq_password=None, mq_ip=None, mq_vhost=None
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
        :param keyvault_account: keyvault account for pulling storage keys
        :type keyvault_account:  str
        :param storage_account_key: storage account key
        :type storage_account_key:  str
        :param mq_username: rabbitmq username
        :type mq_username: str
        :param mq_password: rabbitmq password
        :type mq_password: str
        :param mq_ip: rabbitmq IP address
        :type mq_ip: str
        :param mq_vhost: rabbitmq vhost
        :type mq_vhost: str
        """
        self.account_name = storage_account_name
        self.mq_username = mq_username
        self.mq_password = mq_password
        self.mq_ip = mq_ip
        self.mq_vhost = mq_vhost

        if not storage_account_key:
            storage_account_key = self.__get_storage_account_key(
                storage_account_name, client_id, secret_key, tenant_id, keyvault_account
            )

        self.blob_client = azureblob.BlockBlobService(
            account_name=storage_account_name,
            account_key=storage_account_key)

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

    def __get_storage_account_key(
            self, accountname, client_id, secret_key, tenant_id, keyvault_account
    ):
        """
        Uses the azure management package and the active directory
        credentials to fetch the authentication key for a storage
        account from azure key vault. The key must be stored in azure
        keyvault for this to work.
        :param str accountname: storage account name
        """

        def auth_callback(server, resource, scope):
            credentials = ServicePrincipalCredentials(
                client_id=client_id,
                secret=secret_key,
                tenant=tenant_id,
                resource="https://vault.azure.net"
            )
            token = credentials.token
            return token['token_type'], token['access_token']

        client = KeyVaultClient(KeyVaultAuthentication(auth_callback))
        keyvault = "https://{}.vault.azure.net/".format(keyvault_account)
        # passing in empty string for version returns latest key
        try:
            secret_bundle = client.get_secret(keyvault, accountname, "")
        except KeyVaultErrorException:
            err_str = "The pipeline is not setup to use the {} account. ".format(accountname)
            err_str += "please add the storage key for the account to {} ".format(keyvault_account)
            err_str += "as a secret. All input/output paths should start with accountname"
            raise UnconfiguredStorageAccountError(err_str)
        account_key = secret_bundle.value

        return account_key

    @Backoff(exception_type=AzureHttpError, max_backoff=1800, randomize=True)
    def __get_blob_to_path(
            self, container_name, blob_name, destination_file_path,
            username=None, password=None, ipaddress=None,
            vhost=None):
        """
        download data from blob storage
        :param container_name: blob container name
        :param blob_name: blob path
        :param destination_file_path: path to download the file to
        :param username: username for rabbitmq instance
        :param password: password for rabbitmq instance
        :param ipaddress: ip for rabbitmq instance
        :param vhost: rabbitmq vhost
        :return: azure.storage.blob.baseblobservice.Blob instance with content properties and metadata
        """
        blob_name = self.__format_blob_name(blob_name)
        if username:
            queue = "{}_pull".format(self.account_name)

            semaphore = RabbitMqSemaphore(
                username, password, ipaddress, queue, vhost,
                self.blob_client.get_blob_to_path,
                [container_name, blob_name, destination_file_path],
                {}
            )

            blob = semaphore.run()

        else:
            blob = self.blob_client.get_blob_to_path(container_name,
                                                     blob_name,
                                                     destination_file_path)

        return blob

    def __get_blob_permission_obj(self, permissions):
        permissions = permissions.split('|')
        permissions = {val: True for val in permissions}
        return azureblob.BlobPermissions(**permissions)

    def __get_blob_properties(self, container_name, blob_name):
        blob_name = self.__format_blob_name(blob_name)
        try:
            blob = self.blob_client.get_blob_properties(
                container_name, blob_name
            )
        except azure.common.AzureMissingResourceHttpError:
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
                username=self.mq_username, password=self.mq_username,
                ipaddress=self.mq_ip, vhost=self.mq_vhost
            )
        except azure.common.AzureMissingResourceHttpError:
            raise pypeliner.storage.InputMissingException(blob_name)

        blob_size = self.get_blob_size(container_name, blob_name)
        assert blob_size == blob.properties.content_length

        file_size = os.path.getsize(destination_file_path)
        if file_size != blob.properties.content_length:
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

        if not self.container_exists(container_name=container_name):
            self.create_container(container_name=container_name)

        self.blob_client.create_blob_from_path(
            container_name, blob_name, file_path, metadata=metadata
        )

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
        return self.blob_client.exists(container_name, blob_name)

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
        blob_size = blob.properties.content_length
        return blob_size

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
        return blob.properties.last_modified.strftime('%Y/%m/%d-%H:%M:%S')

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

        return self.blob_client.make_blob_url(container_name, blob_name, sas_token=sas_token)

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
        blob_sas_token = self.blob_client.generate_blob_shared_access_signature(
            container_name,
            blob_name,
            permission=self.__get_blob_permission_obj(blob_permissions),
            expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=lifetime_hours))

        return blob_sas_token

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
        # Obtain the SAS token for the container, setting the expiry time and
        # permissions. In this case, no start time is specified, so the shared
        # access signature becomes valid immediately.
        container_sas_token = self.blob_client.generate_container_shared_access_signature(
            container_name,
            permission=self.__get_blob_permission_obj(blob_permissions),
            expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=lifetime_hours))

        return container_sas_token

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
        :return: list of blobs
        :rtype: list
        """
        container_name = self.__parse_container_args(container_uri, container_name)
        container_blobs = self.blob_client.list_blobs(
            container_name,
            prefix=prefix)
        return container_blobs

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
            self.blob_client.create_container(container_name)

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
        return self.blob_client.exists(container_name)

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
        output_sas_url = self.blob_client.make_blob_url(
            container_name, 'null',
            sas_token=sas_token).replace('/null', '')

        return output_sas_url
