#
# Copyright (C) 2018, 2019 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
#
# Please see LICENSE.txt file included in the top-level directory of the
# appropriate download for a copy of the license and additional information.
#

from borneo import (
    AuthorizationProvider, IllegalArgumentException, NoSQLHandle,
    NoSQLHandleConfig)
from borneo.iam import SignatureProvider
from borneo.kv import StoreAccessTokenProvider

from parameters import (
    credentials_file, endpoint, password, principal, user_name, using_cloud_sim,
    using_on_prem, using_service)


class ExampleAuthorizationProvider(AuthorizationProvider):
    """
    This class is used as an AuthorizationProvider when using the Cloud
    Simulator, which has no security configuration.
    """

    def __init__(self, tenant_id):
        super(ExampleAuthorizationProvider, self).__init__()
        self._tenant_id = tenant_id

    def close(self):
        pass

    def get_authorization_string(self, request=None):
        return 'Bearer ' + self._tenant_id


def create_access_token_provider(tenant_id):
    # Creates an AuthorizationProvider instance based on the environment.
    if using_cloud_sim:
        provider = ExampleAuthorizationProvider(tenant_id)
    elif using_service:
        if principal == 'user principal':
            if credentials_file is None:
                raise IllegalArgumentException(
                    'Must specify the credentials file path.')
            provider = SignatureProvider(config_file=credentials_file)
        elif principal == 'instance principal':
            provider = SignatureProvider.create_with_instance_principal()
        else:
            raise IllegalArgumentException('Must specify the principal.')
    elif using_on_prem:
        if user_name is None and password is None:
            provider = StoreAccessTokenProvider()
        else:
            if user_name is None or password is None:
                raise IllegalArgumentException(
                    'Please set both the user_name and password.')
            provider = StoreAccessTokenProvider(user_name, password)
    else:
        raise IllegalArgumentException('Please set the test server.')
    return provider


def get_handle(tenant_id):
    """
    Constructs a NoSQLHandle. Additional configuration options can be added
    here.
    """
    config = NoSQLHandleConfig(endpoint).set_authorization_provider(
        create_access_token_provider(tenant_id))
    return NoSQLHandle(config)
