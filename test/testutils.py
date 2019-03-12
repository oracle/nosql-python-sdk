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

from borneo import (
    IllegalArgumentException, IllegalStateException, NoSQLHandle, NoSQLHandleConfig)
from borneo.idcs import (
    AccessTokenProvider, DefaultAccessTokenProvider, PropertiesCredentialsProvider)
from parameters import (
    andc_client_id, andc_client_secret, andc_username, andc_user_pwd,
    consistency, credentials_file, properties_file, entitlement_id, http_host,
    http_port, idcs_url, keystore, logger_level, pool_connections, pool_maxsize,
    protocol, proxy_host, proxy_password, proxy_port, proxy_username,
    retry_handler, sc_port, security, sec_info_timeout, tier_name,
    table_request_timeout, timeout)

sc_url_base = ('http://' + http_host + ':' + str(sc_port) + '/V0/service/')
sc_tier_base = sc_url_base + 'tier/'
sc_nd_tenant_base = sc_url_base + 'tenant/nondefault/'
logger = None


def get_simple_handle_config(tenant_id):
    # Creates a simple NoSQLHandleConfig
    get_logger()
    config = NoSQLHandleConfig(protocol, http_host, http_port).set_logger(
        logger)
    set_access_token_provider(config, tenant_id)
    return config


def get_handle_config(tenant_id):
    # Creates a NoSQLHandleConfig
    get_logger()
    config = NoSQLHandleConfig(protocol, http_host, http_port).set_timeout(
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


def get_handle(tenant_id):
    # Returns a connection to the server
    config = get_handle_config(tenant_id)
    return NoSQLHandle(config)


def set_access_token_provider(config, tenant_id):
    if idcs_url is None:
        if security:
            config.set_authorization_provider(
                KeystoreAccessTokenProvider().set_tenant(tenant_id))
        else:
            config.set_authorization_provider(
                NonSecurityAccessTokenProvider(tenant_id))
    else:
        generate_credentials_file()
        authorization_provider = DefaultAccessTokenProvider(
            entitlement_id=entitlement_id, idcs_url=idcs_url,
            use_refresh_token=True, timeout_ms=timeout)
        if credentials_file is None:
            raise IllegalArgumentException(
                'Must specify idcs.creds.')
        authorization_provider.set_credentials_provider(
            PropertiesCredentialsProvider().set_properties_file(
                credentials_file))
        config.set_authorization_provider(authorization_provider)


def add_test_tier_tenant(tenant_id):
    add_tier()
    add_tenant(tenant_id)


def delete_test_tier_tenant(tenant_id):
    delete_tenant(tenant_id)
    delete_tier()


def add_tier():
    if tier_name is not None:
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
    if tier_name is not None:
        tier_url = sc_tier_base + tier_name
        response = delete(tier_url, data=None)
        # allow 404 -- not found -- in this path
        if (response.status_code != codes.ok and
                response.status_code != codes.not_found):
            raise IllegalStateException('Delete tier failed.')


def add_tenant(tenant_id):
    if tier_name is not None:
        tenant_url = sc_nd_tenant_base + tenant_id + '/' + tier_name
        response = post(tenant_url, data=None)
        if response.status_code != codes.ok:
            raise IllegalStateException('Add tenant failed.')


def delete_tenant(tenant_id):
    if tier_name is not None:
        tenant_url = sc_nd_tenant_base + tenant_id
        response = delete(tenant_url, data=None)
        # allow 404 -- not found -- in this path
        if (response.status_code != codes.ok and
                response.status_code != codes.not_found):
            raise IllegalStateException('Delete tenant failed.')


def generate_credentials_file():
    # Generate credentials file
    if path.exists(credentials_file):
        remove(credentials_file)
    with open(credentials_file, 'w') as cred_file:
        cred_file.write('andc_client_id=' + andc_client_id + '\n')
        cred_file.write('andc_client_secret=' + andc_client_secret + '\n')
        cred_file.write('andc_username=' + andc_username + '\n')
        cred_file.write('andc_user_pwd=' + andc_user_pwd + '\n')


def generate_properties_file(test_idcs_url):
    # Generate properties file
    if path.exists(properties_file):
        remove(properties_file)
    with open(properties_file, 'w') as prop_file:
        prop_file.write('idcs_url=' + test_idcs_url + '\n')
        prop_file.write('entitlement_id=' + entitlement_id + '\n')
        prop_file.write('creds_file=' + credentials_file + '\n')


def get_logger():
    global logger
    if logger is None:
        logger = Logger('unittest')
        logger.setLevel(logger_level)
        log_dir = (path.abspath(path.dirname(argv[0])) + sep + 'logs')
        if not path.exists(log_dir):
            mkdir(log_dir)
        logger.addHandler(FileHandler(log_dir + sep + 'unittest.log'))


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


class NonSecurityAccessTokenProvider(AccessTokenProvider):
    def __init__(self, ns_tenant_id):
        super(NonSecurityAccessTokenProvider, self).__init__()
        self.__ns_tenant_id = ns_tenant_id

    def get_account_access_token(self):
        return self.__ns_tenant_id

    def get_service_access_token(self):
        return self.__ns_tenant_id
