#
# Copyright (c) 2018, 2022 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#

from collections import OrderedDict
from datetime import datetime
from decimal import Decimal
from logging import FileHandler, Logger
from os import getcwd, getenv, mkdir, path, remove
from re import match
from struct import pack

from requests import codes, delete, post
from rsa import newkeys

from borneo import (
    AuthorizationProvider, DefaultRetryHandler, IllegalArgumentException,
    IllegalStateException, NoSQLHandle, NoSQLHandleConfig, Regions)
from borneo.iam import SignatureProvider
from borneo.kv import StoreAccessTokenProvider
from parameters import (
    ca_certs, consistency, endpoint, iam_principal, is_cloudsim, is_dev_pod,
    is_minicloud, is_onprem, is_prod_pod, logger_level, password,
    pool_connections, pool_maxsize, table_request_timeout, timeout, user_name,
    version)

# The sc endpoint port for setting the tier.
sc_endpoint = 'localhost:13600'
sc_url_base = ('http://' + sc_endpoint + '/V0/service/')
sc_tier_base = sc_url_base + 'tier/'
sc_nd_tenant_base = sc_url_base + 'tenant/nondefault/'
tier_name = 'test_tier'

logger = None
namespace = 'pyNamespace'
retry_handler = DefaultRetryHandler(delay_s=5)

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

# ssl cipher suites
ssl_cipher_suites = None
# ssl protocol
ssl_protocol = None

testdir = path.abspath(getcwd())
credentials_file = path.join(testdir, 'creds')
fake_credentials_file = path.join(testdir, 'testcreds')
fake_key_file = path.join(testdir, 'testkey.pem')
# Generate fake key file
if path.exists(fake_key_file):
    remove(fake_key_file)
(pubkey, prikey) = newkeys(2048)
pri = prikey.save_pkcs1()
with open(fake_key_file, 'wb+') as f:
    f.write(pri)


def compare_version(specified, internal):
    """
    If the user specified version is newer than internal check version return 1,
    older return -1, same return 0.
    """
    specified_check = match('\d+(\.\d+){0,2}', specified)
    internal_check = match('\d+(\.\d+){0,2}', internal)
    if (specified_check is None or internal_check is None or
            specified_check.group() != specified or
            internal_check.group() != internal):
        raise IllegalArgumentException('Unexpected version number.')
    specified_list = specified.split(".")
    internal_list = internal.split(".")
    specified_len = len(specified_list)
    internal_len = len(internal_list)
    if specified_len < internal_len:
        for i in range(internal_len - specified_len):
            specified_list.append("0")
    for i in range(internal_len):
        if int(specified_list[i]) > int(internal_list[i]):
            return 1
        if int(specified_list[i]) < int(internal_list[i]):
            return -1
    return 0


if version is not None:
    if (is_cloudsim() and compare_version(version, '1.2.0') == -1 or
            is_onprem() and compare_version(version, '20.1.0') == -1):
        raise IllegalArgumentException(
            'The version number for CloudSim should be newer than 1.2.0, for ' +
            'on-prem should be newer than 20.1.0.')


def get_handle_config(tenant_id):
    # Creates a NoSQLHandleConfig
    get_logger()
    provider = generate_authorization_provider(tenant_id)
    config = NoSQLHandleConfig(endpoint, provider).set_table_request_timeout(
        table_request_timeout).set_timeout(timeout).set_default_compartment(
        tenant_id).set_pool_connections(pool_connections).set_pool_maxsize(
        pool_maxsize).set_retry_handler(retry_handler).set_consistency(
        consistency).set_logger(logger)
    if proxy_host is not None:
        config.set_proxy_host(proxy_host)
    if proxy_port != 0:
        config.set_proxy_port(proxy_port)
    if proxy_username is not None:
        config.set_proxy_username(proxy_username)
    if proxy_password is not None:
        config.set_proxy_password(proxy_password)
    if ssl_cipher_suites is not None:
        config.set_ssl_cipher_suites(ssl_cipher_suites)
    if ssl_protocol is not None:
        config.set_ssl_protocol(ssl_protocol)
    if ca_certs is not None:
        config.set_ssl_ca_certs(ca_certs)
    return config


def get_simple_handle_config(tenant_id, ep=endpoint):
    # Creates a simple NoSQLHandleConfig
    get_logger()
    config = NoSQLHandleConfig(ep).set_authorization_provider(
        generate_authorization_provider(tenant_id)).set_default_compartment(
        tenant_id).set_logger(logger)
    return config


def get_handle(tenant_id):
    # Returns a connection to the server
    config = get_handle_config(tenant_id)
    handle = NoSQLHandle(config)
    config.get_logger().info("Created new NoSQLHandle")
    return handle


def get_row(with_sid=True):
    row = OrderedDict()
    if with_sid:
        row['fld_sid'] = 1
    row['fld_id'] = 1
    row['fld_long'] = 2147483648
    row['fld_float'] = 3.1414999961853027
    row['fld_double'] = 3.1415
    row['fld_bool'] = True
    row['fld_str'] = '{"name": u1, "phone": null}'
    row['fld_bin'] = bytearray(pack('>i', 4))
    row['fld_time'] = datetime.now()
    row['fld_num'] = Decimal(5)
    location = OrderedDict()
    location['type'] = 'point'
    location['coordinates'] = [23.549, 35.2908]
    fld_json = OrderedDict()
    fld_json['json_1'] = 1
    fld_json['json_2'] = None
    fld_json['location'] = location
    row['fld_json'] = fld_json
    row['fld_arr'] = ['a', 'b', 'c']
    fld_map = OrderedDict()
    fld_map['a'] = '1'
    fld_map['b'] = '2'
    fld_map['c'] = '3'
    row['fld_map'] = fld_map
    fld_rec = OrderedDict()
    fld_rec['fld_id'] = 1
    fld_rec['fld_bool'] = False
    fld_rec['fld_str'] = None
    row['fld_rec'] = fld_rec
    return row


def generate_authorization_provider(tenant_id):
    if is_cloudsim():
        authorization_provider = InsecureAuthorizationProvider(tenant_id)
    elif is_dev_pod() or is_minicloud():
        authorization_provider = TestSignatureProvider(tenant_id)
    elif is_prod_pod():
        if iam_principal() == 'user principal':
            if credentials_file is None:
                raise IllegalArgumentException(
                    'Must specify the credentials file path.')
            authorization_provider = SignatureProvider(
                config_file=credentials_file)
        elif iam_principal() == 'instance principal':
            if isinstance(endpoint, str):
                region = Regions.from_region_id(endpoint)
            else:
                region = endpoint
            authorization_provider = (
                SignatureProvider.create_with_instance_principal(region=region))
        elif iam_principal() == 'resource principal':
            authorization_provider = (
                SignatureProvider.create_with_resource_principal())
        else:
            raise IllegalArgumentException('Must specify the principal.')
    elif is_onprem():
        if user_name is None and password is None:
            authorization_provider = StoreAccessTokenProvider()
        else:
            if user_name is None or password is None:
                raise IllegalArgumentException(
                    'Please set both the user_name and password.')
            authorization_provider = StoreAccessTokenProvider(
                user_name, password)
    else:
        raise IllegalArgumentException('Please set the test server.')
    return authorization_provider


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
        response = post(tenant_url)
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


def get_logger():
    global logger
    if logger is None:
        logger = Logger('unittest')
        logger.setLevel(getenv('LOGLEVEL', logger_level))
        test_dir = getenv('TEST_OUTPUTDIR', getcwd())
        log_dir = path.join(test_dir, 'logs')
        if not path.exists(log_dir):
            mkdir(log_dir)
        logger.addHandler(FileHandler(path.join(log_dir, 'unittest.log')))


class InsecureAuthorizationProvider(AuthorizationProvider):

    def __init__(self, tenant_id):
        super(InsecureAuthorizationProvider, self).__init__()
        self._tenant_id = tenant_id

    def close(self):
        pass

    def get_authorization_string(self, request=None):
        return 'Bearer ' + self._tenant_id


class TestSignatureProvider(AuthorizationProvider):

    def __init__(self, tenant_id='TestTenant', user_id='TestUser'):
        super(TestSignatureProvider, self).__init__()
        self._tenant_id = tenant_id
        self._user_id = user_id

    def close(self):
        pass

    def get_authorization_string(self, request=None):
        return 'Signature ' + self._tenant_id + ':' + self._user_id

    def set_required_headers(self, request, auth_string, headers):
        compartment = request.get_compartment()
        if compartment is None:
            """
            If request doesn't has compartment id, set the tenant id as the
            default compartment, which is the root compartment in IAM if using
            user principal.
            """
            compartment = self._tenant_id
        headers['x-nosql-compartment-id'] = compartment
        headers['Authorization'] = auth_string
