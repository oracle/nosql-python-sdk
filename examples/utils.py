#
# Copyright (C) 2018, 2019 Oracle and/or its affiliates. All rights reserved.
#
# This software is licensed with the Universal Permissive License (UPL) version 1.0
#
# Please see LICENSE.txt file included in the top-level directory of the
# appropriate download for a copy of the license and additional information.
#

from borneo import NoSQLHandle, NoSQLHandleConfig
from borneo.idcs import (
    AccessTokenProvider, CredentialsProvider, DefaultAccessTokenProvider,
    IDCSCredentials, PropertiesCredentialsProvider)

from parameters import (
    credentials_file, endpoint, entitlement_id, idcs_url,
    use_properties_credentials, using_cloud_sim)


class NoSecurityAccessTokenProvider(AccessTokenProvider):
    """
    This class is used as an AccessTokenProvider when using the Cloud Simulator,
    which has no security configuration.
    """

    def __init__(self, tenant_id):
        super(NoSecurityAccessTokenProvider, self).__init__()
        self.__tenant_id = tenant_id

    def get_account_access_token(self):
        return self.__tenant_id

    def get_service_access_token(self):
        return self.__tenant_id


def create_access_token_provider(tenant_id):
    # Creates an AccessTokenProvider instance based on the environment.
    if using_cloud_sim:
        return NoSecurityAccessTokenProvider(tenant_id)

    provider = DefaultAccessTokenProvider(
        idcs_url=idcs_url, entitlement_id=entitlement_id)
    if use_properties_credentials:
        provider.set_credentials_provider(
            PropertiesCredentialsProvider()
            .set_properties_file(credentials_file))
    else:
        provider.set_credentials_provider(MyCredentialsProvider())
    return provider


def get_handle(tenant_id):
    """
    Constructs a NoSQLHandle. Additional configuration options can be added
    here.
    """
    config = NoSQLHandleConfig(endpoint).set_authorization_provider(
        create_access_token_provider(tenant_id))
    return NoSQLHandle(config)

class MyCredentialsProvider(CredentialsProvider):
    """
    A credentials provider that returns credentials as defined locally
    in this instance. The credential values must be changed here based
    on the identity used.

    Using a class that implements CredentialsProvider is more secure
    than putting credentials in a file in the local file system. Editing
    this class is not secure as it puts credentials in this file, but
    it serves as an example of how an application can secure its
    credentials.
    """
    def get_oauth_client_credentials(self):
        return IDCSCredentials('your_idcs_client_id',
                            'your_client_secret')

    def get_user_credentials(self):
        #
        # password must be URL-encoded. This can be done using
        # urllib.parse.quote
        #
        return IDCSCredentials('your_oracle_cloud_user_name',
                            'your_oracle_cloud_password')
