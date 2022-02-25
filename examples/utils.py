#
# Copyright (c) 2018, 2022 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#

from borneo import (
    AuthorizationProvider, IllegalArgumentException, NoSQLHandle,
    NoSQLHandleConfig, Regions)
from borneo.iam import SignatureProvider
from borneo.kv import StoreAccessTokenProvider

from parameters import (
    ca_certs, credentials_file, endpoint, password, principal, user_name,
    using_cloud_sim, using_on_prem, using_service)


class ExampleAuthorizationProvider(AuthorizationProvider):
    """
    This class is used as an AuthorizationProvider when using the Cloud
    Simulator, which has no security configuration. It accepts a string tenant
    id that is used as a simple namespace for tables.
    """

    def __init__(self, tenant_id):
        super(ExampleAuthorizationProvider, self).__init__()
        self._tenant_id = tenant_id

    def close(self):
        pass

    def get_authorization_string(self, request=None):
        return 'Bearer ' + self._tenant_id


def generate_authorization_provider(tenant_id):
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
            if isinstance(endpoint, str):
                region = Regions.from_region_id(endpoint)
            else:
                region = endpoint
            provider = SignatureProvider.create_with_instance_principal(
                region=region)
        elif principal == 'resource principal':
            provider = SignatureProvider.create_with_resource_principal()
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
    here. Use the tenant_id as the default compartment for all operations. This
    puts tables in the root compartment of the tenancy.
    """
    return NoSQLHandle(get_handle_config(tenant_id))


def get_handle_config(tenant_id):
    config = NoSQLHandleConfig(
        endpoint, generate_authorization_provider(
            tenant_id)).set_default_compartment(tenant_id)
    if ca_certs is not None:
        config.set_ssl_ca_certs(ca_certs)
    return config
