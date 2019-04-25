#
# Copyright (C) 2018, 2019 Oracle and/or its affiliates. All rights reserved.
#
# This software is licensed with the Universal Permissive License (UPL) version 1.0
#
# Please see LICENSE.txt file included in the top-level directory of the
# appropriate download for a copy of the license and additional information.
#

from base64 import urlsafe_b64encode
from json import dumps
from logging import FileHandler, Logger
from os import mkdir, path, remove, sep
from requests import codes, delete, post
from rsa import PrivateKey, sign
from sys import argv
from time import sleep

from borneo import (
    DefaultRetryHandler, IllegalArgumentException, IllegalStateException,
    NoSQLHandle, NoSQLHandleConfig)
from borneo.idcs import (
    AccessTokenProvider, DefaultAccessTokenProvider,
    PropertiesCredentialsProvider)
from parameters import (
    consistency, endpoint, entitlement_id, idcs_url, is_cloudsim, is_dev_pod,
    is_minicloud, is_prod_pod, logger_level, pool_connections, pool_maxsize,
    table_prefix, table_request_timeout, timeout)

# The sc endpoint port for setting the tier.
sc_endpoint = 'localhost:13600'
sc_url_base = ('http://' + sc_endpoint + '/V0/service/')
sc_tier_base = sc_url_base + 'tier/'
sc_nd_tenant_base = sc_url_base + 'tenant/nondefault/'
tier_name = 'test_tier'

logger = None
retry_handler = DefaultRetryHandler(10, 5)
# The timeout for waiting security information is available.
sec_info_timeout = 20000

andc_client_id = 'test-user'
andc_client_secret = 'test-client-secret'
andc_username = 'test-user'
andc_user_pwd = 'test-user-pwd%%'

testdir = path.abspath(path.dirname(argv[0])) + sep

credentials_file = testdir + 'credentials'
fake_credentials_file = testdir + 'testcreds'
properties_file = testdir + 'testprops'
keystore = testdir + 'tenant.pem'

#
# HTTP proxy settings are generally not required. If the server used for
# testing is running behind an HTTP proxy server they may be needed.
#

# The proxy host.
proxy_host = None
# The proxy port.
proxy_port = 0
# The proxy username.
proxy_username = None
# The proxy password.
proxy_password = None


def get_handle_config(tenant_id):
    # Creates a NoSQLHandleConfig
    get_logger()
    config = NoSQLHandleConfig(endpoint).set_timeout(
        timeout).set_consistency(consistency).set_pool_connections(
        pool_connections).set_pool_maxsize(pool_maxsize).set_retry_handler(
        retry_handler).set_logger(logger).set_table_request_timeout(
        table_request_timeout).set_sec_info_timeout(sec_info_timeout)
    if proxy_host is not None:
        config.set_proxy_host(proxy_host)
    if proxy_port != 0:
        config.set_proxy_port(proxy_port)
    if proxy_username is not None:
        config.set_proxy_username(proxy_username)
    if proxy_password is not None:
        config.set_proxy_password(proxy_password)
    set_access_token_provider(config, tenant_id)
    return config


def get_simple_handle_config(tenant_id, ep=endpoint):
    # Creates a simple NoSQLHandleConfig
    get_logger()
    config = NoSQLHandleConfig(ep).set_logger(
        logger)
    set_access_token_provider(config, tenant_id)
    return config


def get_handle(tenant_id):
    # Returns a connection to the server
    config = get_handle_config(tenant_id)
    if config.get_protocol() == 'https':
        # sleep a while to avoid the OperationThrottlingException
        sleep(60)
    return NoSQLHandle(config)


def set_access_token_provider(config, tenant_id):
    if is_cloudsim():
        config.set_authorization_provider(
            NoSecurityAccessTokenProvider(tenant_id))
    elif is_dev_pod() or is_minicloud():
        config.set_authorization_provider(
            KeystoreAccessTokenProvider().set_tenant(tenant_id))
    elif is_prod_pod():
        if credentials_file is None:
            raise IllegalArgumentException(
                'Must specify the credentials file path.')
        creds_provider = PropertiesCredentialsProvider().set_properties_file(
            credentials_file)
        authorization_provider = DefaultAccessTokenProvider(
            idcs_url=idcs_url(), entitlement_id=entitlement_id,
            creds_provider=creds_provider, timeout_ms=timeout)
        config.set_authorization_provider(authorization_provider)
    else:
        raise IllegalArgumentException('Please set the test server.')


def add_test_tier_tenant(tenant_id):
    add_tier()
    add_tenant(tenant_id)


def delete_test_tier_tenant(tenant_id):
    delete_tenant(tenant_id)
    delete_tier()


def add_tier():
    if is_minicloud():
        tier_url = sc_tier_base + tier_name
        limits = {"version": 1, "numTables": 10, "tenantSize": 5000,
                  "tenantReadUnits": 100000, "tenantWriteUnits": 40000,
                  "tableSize": 5000, "tableReadUnits": 40000,
                  "tableWriteUnits": 20000, "indexesPerTable": 5,
                  "columnsPerTable": 20, "ddlRequestsRate": 400,
                  "tableLimitReductionsRate": 4, "schemaEvolutions": 6}
        response = post(tier_url, json=limits)
        if response.status_code != codes.ok:
            raise IllegalStateException('Add tier failed.')


def delete_tier():
    if is_minicloud():
        tier_url = sc_tier_base + tier_name
        response = delete(tier_url, data=None)
        # allow 404 -- not found -- in this path
        if (response.status_code != codes.ok and
                response.status_code != codes.not_found):
            raise IllegalStateException('Delete tier failed.')


def add_tenant(tenant_id):
    if is_minicloud():
        tenant_url = sc_nd_tenant_base + tenant_id + '/' + tier_name
        response = post(tenant_url, data=None)
        if response.status_code != codes.ok:
            raise IllegalStateException('Add tenant failed.')


def delete_tenant(tenant_id):
    if is_minicloud():
        tenant_url = sc_nd_tenant_base + tenant_id
        response = delete(tenant_url, data=None)
        # allow 404 -- not found -- in this path
        if (response.status_code != codes.ok and
                response.status_code != codes.not_found):
            raise IllegalStateException('Delete tenant failed.')


def generate_credentials_file():
    # Generate credentials file
    if path.exists(fake_credentials_file):
        remove(fake_credentials_file)

    with open(fake_credentials_file, 'w') as cred_file:
        cred_file.write('andc_client_id=' + andc_client_id + '\n')
        cred_file.write('andc_client_secret=' + andc_client_secret + '\n')
        cred_file.write('andc_username=' + andc_username + '\n')
        cred_file.write('andc_user_pwd=' + andc_user_pwd + '\n')


def generate_properties_file(test_idcs_url, test_credentials_file):
    # Generate properties file
    if path.exists(properties_file):
        remove(properties_file)

    with open(properties_file, 'w') as prop_file:
        prop_file.write('idcs_url=' + test_idcs_url + '\n')
        prop_file.write('creds_file=' + test_credentials_file + '\n')
        if entitlement_id is not None:
            prop_file.write('entitlement_id=' + entitlement_id + '\n')


def get_logger():
    global logger
    if logger is None:
        logger = Logger('unittest')
        logger.setLevel(logger_level)
        log_dir = (path.abspath(path.dirname(argv[0])) + sep + 'logs')
        if not path.exists(log_dir):
            mkdir(log_dir)
        logger.addHandler(FileHandler(log_dir + sep + 'unittest.log'))


def make_table_name(name):
    return table_prefix + name


class KeystoreAccessTokenProvider(AccessTokenProvider):
    # Static fields used to build AT
    IDCS_SUPPORTED_ALGORITHM = 'RS256'
    ALGORITHM_NAME = 'alg'
    AUDIENCE_CLAIM_NAME = 'aud'
    EXPIRATION_CLAIM_NAME = 'exp'
    SCOPE_CLAIM_NAME = 'scope'
    CLIENT_ID_CLAIM_NAME = 'client_id'
    USER_ID_CLAIM_NAME = 'user_id'
    TENANT_CLAIM_NAME = 'tenant'
    SIG_ALGORITHM_DEFAULT = 'SHA256withRSA'

    # PSM AT audience and scope
    PSM_AUD = 'https://psmtest'
    PSM_SCOPE = '/paas/api/*'

    def __init__(self):
        super(KeystoreAccessTokenProvider, self).__init__()
        self.__ks_user_id = 'TestUser'
        self.__ks_client_id = 'TestClient'
        self.__ks_tenant_id = 'TestTenant'
        self.__ks_entitlement_id = 'TestEntitlement'
        self.__ks_expires_in = 3471321600
        if keystore is None or not path.exists(keystore):
            raise IllegalArgumentException('Missing keystore')

    def set_tenant(self, ks_tenant_id):
        self.__ks_tenant_id = ks_tenant_id
        return self

    def get_account_access_token(self):
        try:
            at = self.__build_psm_access_token(
                self.__ks_tenant_id, self.__ks_user_id, self.__ks_client_id,
                self.__ks_expires_in)
            return at
        except Exception as e:
            raise IllegalStateException(
                'Error getting PSM access token: ', str(e))

    def get_service_access_token(self):
        try:
            at = self.__build_andc_access_token(
                self.__ks_tenant_id, self.__ks_user_id, self.__ks_client_id,
                self.__ks_entitlement_id, self.__ks_expires_in)
            return at
        except Exception as e:
            raise IllegalStateException(
                'Error getting ANDC access token: ', str(e))

    def __build_andc_access_token(self, ks_tenant_id, ks_user_id, ks_client_id,
                                  ks_entitlement_id, ks_expires_in):
        return self.__build_access_token(
            ks_tenant_id, ks_user_id, ks_client_id,
            AccessTokenProvider.ANDC_AUD_PREFIX + ks_entitlement_id,
            AccessTokenProvider.SCOPE, ks_expires_in)

    def __build_psm_access_token(self, ks_tenant_id, ks_user_id, ks_client_id,
                                 ks_expires_in):
        return self.__build_access_token(
            ks_tenant_id, ks_user_id, ks_client_id,
            KeystoreAccessTokenProvider.PSM_AUD,
            KeystoreAccessTokenProvider.PSM_SCOPE, ks_expires_in)

    def __build_access_token(self, ks_tenant_id, ks_user_id, ks_client_id,
                             audience, scope, ks_expires_in):
        header = dict()
        header[KeystoreAccessTokenProvider.ALGORITHM_NAME] = (
            KeystoreAccessTokenProvider.IDCS_SUPPORTED_ALGORITHM)
        header = ''.join(dumps(header).split())

        payload = dict()
        payload[KeystoreAccessTokenProvider.SCOPE_CLAIM_NAME] = scope
        payload[KeystoreAccessTokenProvider.EXPIRATION_CLAIM_NAME] = (
            ks_expires_in)
        payload[KeystoreAccessTokenProvider.TENANT_CLAIM_NAME] = ks_tenant_id
        payload[KeystoreAccessTokenProvider.CLIENT_ID_CLAIM_NAME] = ks_client_id
        if ks_user_id is not None:
            payload[KeystoreAccessTokenProvider.USER_ID_CLAIM_NAME] = ks_user_id
        payload[KeystoreAccessTokenProvider.AUDIENCE_CLAIM_NAME] = [audience]
        payload = ''.join(dumps(payload).split())
        try:
            signing_content = (urlsafe_b64encode(header) + '.' +
                               urlsafe_b64encode(payload))
        except TypeError:
            signing_content = (
                urlsafe_b64encode(header.encode()).decode() + '.' +
                urlsafe_b64encode(payload.encode()).decode())
        signature = self.__sign(signing_content)
        return signing_content + '.' + signature

    def __sign(self, content):
        if keystore is None or not path.exists(keystore):
            raise IllegalArgumentException(
                'Unable to find the keystore: ' + keystore)
        with open(keystore, 'r') as key_file:
            private_key = PrivateKey.load_pkcs1(key_file.read().encode())
        try:
            signature = sign(content, private_key, 'SHA-256')
            return urlsafe_b64encode(signature)
        except TypeError:
            signature = sign(content.encode(), private_key, 'SHA-256')
            return urlsafe_b64encode(signature).decode()


class NoSecurityAccessTokenProvider(AccessTokenProvider):
    def __init__(self, ns_tenant_id):
        super(NoSecurityAccessTokenProvider, self).__init__()
        self.__ns_tenant_id = ns_tenant_id

    def get_account_access_token(self):
        return self.__ns_tenant_id

    def get_service_access_token(self):
        return self.__ns_tenant_id
