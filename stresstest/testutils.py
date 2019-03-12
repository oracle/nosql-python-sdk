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
from os import path, remove
from re import findall
from requests import codes, delete, post
from rsa import PrivateKey, sign
from threading import Lock, Thread
from time import ctime, sleep, time

from borneo import (
    IllegalArgumentException, IllegalStateException, NoSQLHandle,
    NoSQLHandleConfig)
from borneo.idcs import (
    AccessTokenProvider, DefaultAccessTokenProvider,
    PropertiesCredentialsProvider)

from parameters import (
    andc_client_id, andc_client_secret, andc_username, andc_user_pwd,
    consistency, credentials_file, entitlement_id, http_host, http_port,
    idcs_url, interval, keystore, num_threads, pool_connections, pool_maxsize,
    protocol, proxy_host, proxy_password, proxy_port, proxy_username,
    retry_handler, sc_port, security, sec_info_timeout, table_request_timeout,
    tenant_id, tier_name, timeout)


def enum(**enums):
    return type('Enum', (object,), enums)


EXERCISE_OPS = enum(DELETE=0,
                    GET=1,
                    MULTI_DELETE=2,
                    PUT_IF_ABSENT=3,
                    PUT_IF_PRESENT=4,
                    QUERY=5,
                    WRITE_MULTIPLE=6,
                    IGNORE=7)


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

    def set_tenant(self, ks_tenant_id):
        self.__ks_tenant_id = ks_tenant_id
        return self

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


class LogUtils:
    # Utility methods to facilitate Logging.
    def __init__(self, proc_id, logger):
        self.__proc_id = proc_id
        self.__logger = logger

    def log_critical(self, msg, start=None):
        if self.__logger is not None:
            msg = self.__generate_msg(msg, start)
            self.__logger.critical('Process' + self.__proc_id + '-[' + ctime() +
                                   ']' + msg)

    def log_error(self, msg, start=None):
        if self.__logger is not None:
            msg = self.__generate_msg(msg, start)
            self.__logger.error('Process' + self.__proc_id + '-[' + ctime() +
                                ']' + msg)

    def log_warning(self, msg, start=None):
        if self.__logger is not None:
            msg = self.__generate_msg(msg, start)
            self.__logger.warning('Process' + self.__proc_id + '-[' + ctime() +
                                  ']' + msg)

    def log_info(self, msg, start=None):
        if self.__logger is not None:
            msg = self.__generate_msg(msg, start)
            self.__logger.info('Process' + self.__proc_id + '-[' + ctime() +
                               ']' + msg)

    def log_debug(self, msg, start=None):
        if self.__logger is not None:
            msg = self.__generate_msg(msg, start)
            self.__logger.debug('Process' + self.__proc_id + '-[' + ctime() +
                                ']' + msg)

    def __generate_msg(self, msg, start):
        if start is not None:
            return msg + '[' + str(int(time() - start)) + 's]'
        else:
            return msg


class NonSecurityAccessTokenProvider(AccessTokenProvider):
    def __init__(self, ns_tenant_id):
        super(NonSecurityAccessTokenProvider, self).__init__()
        self.__ns_tenant_id = ns_tenant_id

    def get_account_access_token(self):
        return self.__ns_tenant_id

    def get_service_access_token(self):
        return self.__ns_tenant_id


class ReportingThread(Thread):
    def __init__(self, utils, logutils, logfile, iteration):
        super(ReportingThread, self).__init__()
        self.__utils = utils
        self.__logutils = logutils
        self.__logfile = logfile
        self.__iteration = iteration
        self.__done = False
        self.__populate = self.__utils.get_populate_count()
        self.__exercise = self.__utils.get_exercise_count()

    def run(self):
        while True:
            sleep(interval)
            content = self.__read_logfile()
            if (len(findall('Start populate thread: ', content)) ==
                    num_threads * self.__iteration):
                break

        while True:
            sleep(interval)
            self.__logutils.log_info(
                '== load: ' + str(self.__populate[0]) + ' rows == ')
            content = self.__read_logfile()
            if (len(findall('End populate thread: ', content)) ==
                    num_threads * self.__iteration):
                break

        while True:
            sleep(interval)
            msg = '== '
            for op in range(len(self.__exercise)):
                msg += (self.__utils.get_op_name(op) + ':' +
                        str(self.__exercise[op]) + ' ')
            msg += '=='
            self.__logutils.log_info(msg)
            content = self.__read_logfile()
            if (len(findall('End exercise thread: ', content)) ==
                    num_threads * self.__iteration):
                break

    def __read_logfile(self):
        with open(self.__logfile, 'r') as f:
            content = f.read()
        return content


class TestFailedException(RuntimeError):
    def __init__(self, message):
        self.__message = message

    def __str__(self):
        return str(self.__message)


class Utils:
    def __init__(self, logutils, proc_id):
        self.__logutils = logutils
        self.__credentials_file = credentials_file + proc_id
        self.__handle = None
        self.__config = None
        self.__lock = Lock()
        self.__populate = [0]
        self.__exercise = [0] * 8
        self.__unexpected_count = 0
        sc_url_base = ('http://' + http_host + ':' + str(sc_port) +
                       '/V0/service/')
        self.__sc_tier_base = sc_url_base + 'tier/'
        self.__sc_nd_tenant_base = sc_url_base + 'tenant/nondefault/'

    def add_test_tier_tenant(self):
        if tier_name is not None:
            self.__add_tier()
            self.__add_tenant()

    def close_handle(self):
        self.__handle.close()

    def delete_test_tier_tenant(self):
        if tier_name is not None:
            self.__delete_tenant()
            self.__delete_tier()

    def get_exercise_count(self):
        return self.__exercise

    def get_handle(self):
        # Returns a connection to the server
        self.__get_handle_config()
        self.__handle = NoSQLHandle(self.__config)
        return self.__handle

    def get_lock(self):
        return self.__lock

    def get_op_name(self, op):
        if op == EXERCISE_OPS.DELETE:
            return 'DELETE'
        elif op == EXERCISE_OPS.GET:
            return 'GET'
        elif op == EXERCISE_OPS.MULTI_DELETE:
            return 'MULTI_DELETE'
        elif op == EXERCISE_OPS.PUT_IF_ABSENT:
            return 'PUT_IF_ABSENT'
        elif op == EXERCISE_OPS.PUT_IF_PRESENT:
            return 'PUT_IF_PRESENT'
        elif op == EXERCISE_OPS.QUERY:
            return 'QUERY'
        elif op == EXERCISE_OPS.WRITE_MULTIPLE:
            return 'WRITE_MULTIPLE'
        elif op == EXERCISE_OPS.IGNORE:
            return 'IGNORE'

    def get_populate_count(self):
        return self.__populate

    def get_unexpected_count(self):
        return self.__unexpected_count

    def unexpected_result(self, msg):
        with self.__lock:
            self.__unexpected_count += 1
        self.__logutils.log_error('UNEXPECTED_RESULT: ' + msg)

    def __add_tenant(self):
        tenant_url = self.__sc_nd_tenant_base + tenant_id + '/' + tier_name
        response = post(tenant_url, data=None)
        if response.status_code != codes.ok:
            raise IllegalStateException('Add tenant failed.')

    def __add_tier(self):
        tier_url = self.__sc_tier_base + tier_name
        limits = {"version": 1, "numTables": 10, "tenantSize": 5000,
                  "tenantReadUnits": 100000, "tenantWriteUnits": 40000,
                  "tableSize": 5000, "tableReadUnits": 40000,
                  "tableWriteUnits": 20000, "indexesPerTable": 5,
                  "columnsPerTable": 20, "ddlRequestsRate": 400,
                  "tableLimitReductionsRate": 4, "schemaEvolutions": 6}
        response = post(tier_url, json=limits)
        if response.status_code != codes.ok:
            raise IllegalStateException('Add tier failed.')

    def __delete_tenant(self):
        tenant_url = self.__sc_nd_tenant_base + tenant_id
        response = delete(tenant_url, data=None)
        # allow 404 -- not found -- in this path
        if (response.status_code != codes.ok and
                response.status_code != codes.not_found):
            raise IllegalStateException('Delete tenant failed.')

    def __delete_tier(self):
        tier_url = self.__sc_tier_base + tier_name
        response = delete(tier_url, data=None)
        # allow 404 -- not found -- in this path
        if (response.status_code != codes.ok and
                response.status_code != codes.not_found):
            raise IllegalStateException('Delete tier failed.')

    def __generate_credentials_file(self):
        # Generate credentials file
        if path.exists(self.__credentials_file):
            remove(self.__credentials_file)
        with open(self.__credentials_file, 'w') as cred_file:
            cred_file.write('andc_client_id=' + andc_client_id + '\n')
            cred_file.write('andc_client_secret=' + andc_client_secret + '\n')
            cred_file.write('andc_username=' + andc_username + '\n')
            cred_file.write('andc_user_pwd=' + andc_user_pwd + '\n')

    def __get_handle_config(self):
        # Creates a NoSQLHandleConfig
        self.__config = NoSQLHandleConfig(
            protocol, http_host, http_port).set_timeout(
            timeout).set_consistency(consistency).set_pool_connections(
            pool_connections).set_pool_maxsize(pool_maxsize).set_retry_handler(
            retry_handler).set_table_request_timeout(
            table_request_timeout).set_sec_info_timeout(sec_info_timeout)
        if proxy_host is not None:
            self.__config.set_proxy_host(proxy_host)
        if proxy_port != 0:
            self.__config.set_proxy_port(proxy_port)
        if proxy_username is not None:
            self.__config.set_proxy_username(proxy_username)
        if proxy_password is not None:
            self.__config.set_proxy_password(proxy_password)
        self.__set_access_token_provider()

    def __set_access_token_provider(self):
        if idcs_url is None:
            if security:
                self.__config.set_authorization_provider(
                    KeystoreAccessTokenProvider().set_tenant(tenant_id))
            else:
                self.__config.set_authorization_provider(
                    NonSecurityAccessTokenProvider(tenant_id))
        else:
            self.__generate_credentials_file()
            authorization_provider = DefaultAccessTokenProvider(
                entitlement_id=entitlement_id, idcs_url=idcs_url,
                use_refresh_token=True, timeout_ms=timeout)
            if self.__credentials_file is None:
                raise IllegalArgumentException(
                    'Must specify idcs.creds.')
            authorization_provider.set_credentials_provider(
                PropertiesCredentialsProvider().set_properties_file(
                    self.__credentials_file))
            self.__config.set_authorization_provider(authorization_provider)
