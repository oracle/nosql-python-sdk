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
    AccessTokenProvider, DefaultAccessTokenProvider,
    PropertiesCredentialsProvider)

from parameters import (
    credentials_file, entitlement_id, host, idcs_url, port, protocol,
    using_cloud_sim)


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
        entitlement_id=entitlement_id, idcs_url=idcs_url,
        use_refresh_token=False)
    provider.set_credentials_provider(
        PropertiesCredentialsProvider()
        .set_properties_file(credentials_file))
    return provider


def get_handle(tenant_id):
    """
    Constructs a NoSQLHandle. Additional configuration options can be added
    here.
    """
    config = NoSQLHandleConfig(protocol, host, port).set_authorization_provider(
        create_access_token_provider(tenant_id))
    return NoSQLHandle(config)
