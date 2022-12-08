#
# Copyright (c) 2018, 2022 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#

import unittest
from requests import ConnectionError
from ssl import PROTOCOL_SSLv23, PROTOCOL_TLSv1_2

from borneo import (
    GetRequest, IllegalArgumentException, OperationThrottlingException,
    DefaultRetryHandler, Regions, RetryableException, RetryHandler,
    NoSQLHandle, NoSQLHandleConfig, TableRequest)
from borneo.iam import SignatureProvider
from borneo.kv import StoreAccessTokenProvider
from parameters import (
    ca_certs, consistency, endpoint, pool_connections, pool_maxsize, security,
    table_name, tenant_id, timeout, table_request_timeout)
from testutils import (
    fake_key_file, get_handle_config, get_simple_handle_config, proxy_host,
    proxy_port, proxy_username, proxy_password, retry_handler,
    ssl_cipher_suites, ssl_protocol)


class TestNoSQLHandleConfig(unittest.TestCase):

    def setUp(self):
        self.config = get_simple_handle_config(tenant_id)
        self.table_request = TableRequest().set_statement(
            'DROP TABLE IF EXISTS ' + table_name)
        self.get_request = GetRequest()

    def testNoSQLHandleConfigIllegalInit(self):
        # illegal endpoint
        self.assertRaises(IllegalArgumentException, NoSQLHandleConfig,
                          {'IllegalEndpoint': endpoint})
        # illegal provider
        self.assertRaises(IllegalArgumentException, NoSQLHandleConfig,
                          endpoint, provider='IllegalProvider')
        # no endpoint and provider
        self.assertRaises(IllegalArgumentException, NoSQLHandleConfig)
        # only StoreAccessTokenProvider
        self.assertRaises(IllegalArgumentException, NoSQLHandleConfig, None,
                          StoreAccessTokenProvider())
        # only SignatureProvider without region
        provider = SignatureProvider(
            tenant_id='ocid1.tenancy.oc1..tenancy',
            user_id='ocid1.user.oc1..user', fingerprint='fingerprint',
            private_key=fake_key_file)
        self.assertRaises(IllegalArgumentException, NoSQLHandleConfig, None,
                          provider)
        provider.close()
        # both endpoint and provider, region not match
        provider = SignatureProvider(
            tenant_id='ocid1.tenancy.oc1..tenancy',
            user_id='ocid1.user.oc1..user', fingerprint='fingerprint',
            private_key=fake_key_file, region=Regions.US_ASHBURN_1)
        self.assertRaises(IllegalArgumentException, NoSQLHandleConfig,
                          'us-phoenix-1', provider)
        provider.close()

    def testNoSQLHandleConfigSetIllegalEndpoint(self):
        # illegal endpoint
        self.assertRaises(IllegalArgumentException, NoSQLHandleConfig, None)
        self.assertRaises(IllegalArgumentException, NoSQLHandleConfig,
                          'localhost:8080:foo')
        self.assertRaises(IllegalArgumentException, NoSQLHandleConfig,
                          'localhost:notanint')
        self.assertRaises(IllegalArgumentException, NoSQLHandleConfig,
                          'localhost:-1')
        self.assertRaises(IllegalArgumentException, NoSQLHandleConfig,
                          'http://localhost:-1:x')
        self.assertRaises(IllegalArgumentException, NoSQLHandleConfig,
                          'ttp://localhost:8080')

        # legal endpoint format but no service at the port
        if security():
            config = get_simple_handle_config(tenant_id, 'localhost:443')
        else:
            config = get_simple_handle_config(tenant_id, 'localhost:70')
        handle = NoSQLHandle(config)
        self.assertRaises(ConnectionError, handle.table_request,
                          self.table_request)
        handle.close()

    def testNoSQLHandleConfigSetIllegalTimeout(self):
        self.assertRaises(IllegalArgumentException, self.config.set_timeout,
                          'IllegalTimeout')
        self.assertRaises(IllegalArgumentException, self.config.set_timeout, 0)
        self.assertRaises(IllegalArgumentException, self.config.set_timeout,
                          -1)

    def testNoSQLHandleConfigSetIllegalTableRequestTimeout(self):
        self.assertRaises(IllegalArgumentException,
                          self.config.set_table_request_timeout,
                          'IllegalTableRequestTimeout')
        self.assertRaises(IllegalArgumentException,
                          self.config.set_table_request_timeout, 0)
        self.assertRaises(IllegalArgumentException,
                          self.config.set_table_request_timeout, -1)

    def testNoSQLHandleConfigSetIllegalConsistency(self):
        self.assertRaises(IllegalArgumentException, self.config.set_consistency,
                          'IllegalConsistency')

    def testNoSQLHandleConfigSetIllegalPoolConnections(self):
        self.assertRaises(IllegalArgumentException,
                          self.config.set_pool_connections,
                          'IllegalPoolConnections')
        self.assertRaises(IllegalArgumentException,
                          self.config.set_pool_connections, 0)
        self.assertRaises(IllegalArgumentException,
                          self.config.set_pool_connections, -1)

    def testNoSQLHandleConfigSetIllegalPoolMaxsize(self):
        self.assertRaises(IllegalArgumentException,
                          self.config.set_pool_maxsize, 'IllegalPoolMaxsize')
        self.assertRaises(IllegalArgumentException,
                          self.config.set_pool_maxsize, 0)
        self.assertRaises(IllegalArgumentException,
                          self.config.set_pool_maxsize, -1)

    def testNoSQLHandleConfigSetIllegalMaxContentLength(self):
        self.assertRaises(IllegalArgumentException,
                          self.config.set_max_content_length,
                          'IllegalMaxContentLength')
        self.assertRaises(IllegalArgumentException,
                          self.config.set_max_content_length, -1)

    def testNoSQLHandleConfigSetIllegalRetryHandler(self):
        self.assertRaises(IllegalArgumentException,
                          self.config.set_retry_handler, 'IllegalRetryHandler')
        self.assertRaises(IllegalArgumentException, DefaultRetryHandler,
                          'IllegalNumRetries', 0)
        self.assertRaises(IllegalArgumentException, DefaultRetryHandler, -1, 0)
        self.assertRaises(IllegalArgumentException, DefaultRetryHandler,
                          5, 'IllegalDelaySeconds')
        self.assertRaises(IllegalArgumentException, DefaultRetryHandler, 5, -1)

    def testNoSQLHandleConfigConfigureIllegalDefaultRetryHandler(self):
        self.assertRaises(IllegalArgumentException,
                          self.config.configure_default_retry_handler,
                          'IllegalNumRetries', 5)
        self.assertRaises(IllegalArgumentException,
                          self.config.configure_default_retry_handler, -1, 5)
        self.assertRaises(IllegalArgumentException,
                          self.config.configure_default_retry_handler,
                          0, 'IllegalDelaySeconds')
        self.assertRaises(IllegalArgumentException,
                          self.config.configure_default_retry_handler, 0, -1)

    def testNoSQLHandleConfigSetIllegalRateLimitingEnabled(self):
        self.assertRaises(IllegalArgumentException,
                          self.config.set_rate_limiting_enabled,
                          'IllegalRateLimitingEnabled')

    def testNoSQLHandleConfigSetIllegalRateLimitingPercentage(self):
        self.assertRaises(IllegalArgumentException,
                          self.config.set_default_rate_limiting_percentage,
                          'IllegalRateLimitingPercentage')
        self.assertRaises(IllegalArgumentException,
                          self.config.set_default_rate_limiting_percentage, -1)

    def testNoSQLHandleConfigSetIllegalAuthorizationProvider(self):
        self.assertRaises(IllegalArgumentException,
                          self.config.set_authorization_provider,
                          'IllegalAuthorizationProvider')
        self.assertRaises(IllegalArgumentException,
                          self.config.set_authorization_provider, None)

    def testNoSQLHandleConfigSetIllegalProxyHost(self):
        self.assertRaises(IllegalArgumentException, self.config.set_proxy_host,
                          {'IllegalProxyHost': 'IllegalProxyHost'})

    def testNoSQLHandleConfigSetIllegalProxyPort(self):
        self.assertRaises(IllegalArgumentException, self.config.set_proxy_port,
                          'IllegalProxyPort')
        self.assertRaises(IllegalArgumentException, self.config.set_proxy_port,
                          -1)

    def testNoSQLHandleConfigSetIllegalProxyUsername(self):
        self.assertRaises(IllegalArgumentException,
                          self.config.set_proxy_username, 12345)

    def testNoSQLHandleConfigSetIllegalProxyPassword(self):
        self.assertRaises(IllegalArgumentException,
                          self.config.set_proxy_password, 12345)

    def testNoSQLHandleConfigSetIllegalLogger(self):
        self.assertRaises(IllegalArgumentException,
                          self.config.set_logger, 'IllegalLogger')

    def testNoSQLHandleConfigSetIllegalSSLCACerts(self):
        self.assertRaises(IllegalArgumentException,
                          self.config.set_ssl_ca_certs,
                          {'IllegalCACerts': 'IllegalCACerts'})
        if security():
            # set illegal CA certs
            config = get_simple_handle_config(tenant_id).set_ssl_ca_certs(
                fake_key_file)
            self.assertRaises(IllegalArgumentException, NoSQLHandle, config)

    def testNoSQLHandleConfigSetIllegalSSLCipherSuites(self):
        self.assertRaises(IllegalArgumentException,
                          self.config.set_ssl_cipher_suites,
                          {'IllegalCipherSuites': 'IllegalCipherSuites'})

    def testNoSQLHandleConfigSetIllegalSSLProtocol(self):
        self.assertRaises(IllegalArgumentException,
                          self.config.set_ssl_protocol, 'IllegalProtocol')
        self.assertRaises(IllegalArgumentException,
                          self.config.set_ssl_protocol, -1)
        if security():
            # set illegal protocol
            config = get_simple_handle_config(tenant_id).set_ssl_protocol(10)
            self.assertRaises(IllegalArgumentException, NoSQLHandle, config)

    def testNoSQLHandleConfigSetLegalSSLProtocol(self):
        if security():
            # use default protocol
            config = get_simple_handle_config(tenant_id)
            handle = NoSQLHandle(config)
            self.assertEqual(
                config.get_ssl_context().protocol, PROTOCOL_SSLv23)
            handle.close()
            # set PROTOCOL_TLSv1_2 as ssl protocol
            config = get_simple_handle_config(tenant_id).set_ssl_protocol(
                PROTOCOL_TLSv1_2)
            handle = NoSQLHandle(config)
            self.assertEqual(
                config.get_ssl_context().protocol, PROTOCOL_TLSv1_2)
            handle.close()

    def testNoSQLHandleEndpointConfig(self):
        # set only the host as endpoint
        config = get_simple_handle_config(tenant_id, 'ndcs.com')
        self._check_service_url(config, 'https', 'ndcs.com', 443)
        # set protocol://host as endpoint
        config = get_simple_handle_config(tenant_id, 'Http://ndcs.com')
        self._check_service_url(config, 'http', 'ndcs.com', 8080)
        config = get_simple_handle_config(tenant_id, 'https://ndcs.com')
        self._check_service_url(config, 'https', 'ndcs.com', 443)
        # set protocol:host as endpoint
        config = get_simple_handle_config(tenant_id, 'Http:ndcs.com')
        self._check_service_url(config, 'http', 'ndcs.com', 8080)
        config = get_simple_handle_config(tenant_id, 'https:ndcs.com')
        self._check_service_url(config, 'https', 'ndcs.com', 443)
        # set host:port as endpoint
        config = get_simple_handle_config(tenant_id, 'localhost:80')
        self._check_service_url(config, 'http', 'localhost', 80)
        config = get_simple_handle_config(tenant_id, 'localhost:443')
        self._check_service_url(config, 'https', 'localhost', 443)
        # set protocol://host:port as endpoint
        config = get_simple_handle_config(tenant_id, 'HTTPS://ndcs.com:8080')
        self._check_service_url(config, 'https', 'ndcs.com', 8080)
        # set protocol:host:port as endpoint
        config = get_simple_handle_config(tenant_id, 'Http:ndcs.com:443')
        self._check_service_url(config, 'http', 'ndcs.com', 443)

        # set a Region's id 'us-AshBURN-1' as endpoint.
        config = get_simple_handle_config(tenant_id, 'us-AshBURN-1')
        self._check_service_url(
            config, 'https', 'nosql.us-ashburn-1.oci.oraclecloud.com', 443)
        # set a Region's id 'US-LangLEY-1' as endpoint.
        config = get_simple_handle_config(tenant_id, 'US-LangLEY-1')
        self._check_service_url(
            config, 'https', 'nosql.us-langley-1.oci.oraclegovcloud.com', 443)
        # set a Region's id 'UK-GOV-LONDON-1' as endpoint.
        config = get_simple_handle_config(tenant_id, 'UK-GOV-LONDON-1')
        self._check_service_url(
            config, 'https', 'nosql.uk-gov-london-1.oci.oraclegovcloud.uk', 443)
        # set a Region's id 'Ap-CHiyODA-1' as endpoint.
        config = get_simple_handle_config(tenant_id, 'Ap-CHiyODA-1')
        self._check_service_url(
            config, 'https', 'nosql.ap-chiyoda-1.oci.oraclecloud8.com', 443)

        # set a Region Regions.US_ASHBURN_1 as endpoint.
        config = get_simple_handle_config(tenant_id, Regions.US_ASHBURN_1)
        self._check_service_url(
            config, 'https', 'nosql.us-ashburn-1.oci.oraclecloud.com', 443)
        # set a Region Regions.US_LANGLEY_1 as endpoint.
        config = get_simple_handle_config(tenant_id, Regions.US_LANGLEY_1)
        self._check_service_url(
            config, 'https', 'nosql.us-langley-1.oci.oraclegovcloud.com', 443)
        # set a Region Regions.UK_GOV_LONDON_1 as endpoint.
        config = get_simple_handle_config(tenant_id, Regions.UK_GOV_LONDON_1)
        self._check_service_url(
            config, 'https', 'nosql.uk-gov-london-1.oci.oraclegovcloud.uk', 443)
        # set a Region Regions.AP_CHIYODA_1 as endpoint.
        config = get_simple_handle_config(tenant_id, Regions.AP_CHIYODA_1)
        self._check_service_url(
            config, 'https', 'nosql.ap-chiyoda-1.oci.oraclecloud8.com', 443)

        # set a provider with region
        provider = SignatureProvider(
            tenant_id='ocid1.tenancy.oc1..tenancy',
            user_id='ocid1.user.oc1..user', fingerprint='fingerprint',
            private_key=fake_key_file, region=Regions.US_ASHBURN_1)
        config = NoSQLHandleConfig(provider=provider)
        self._check_service_url(
            config, 'https', 'nosql.us-ashburn-1.oci.oraclecloud.com', 443)
        self.assertEqual(config.get_region(), Regions.US_ASHBURN_1)
        # set a endpoint and provider with region
        config = NoSQLHandleConfig('us-ashburn-1', provider)
        self._check_service_url(
            config, 'https', 'nosql.us-ashburn-1.oci.oraclecloud.com', 443)
        self.assertEqual(config.get_region(), Regions.US_ASHBURN_1)
        provider.close()

    def testNoSQLHandleConfigRegions(self):
        for r in Regions.get_oc1_regions():
            self.assertEqual(
                r.endpoint(),
                'https://nosql.' + r.get_region_id() + '.oci.oraclecloud.com')
        for r in Regions.get_oc2_regions():
            self.assertEqual(
                r.endpoint(),
                'https://nosql.' + r.get_region_id() +
                '.oci.oraclegovcloud.com')
        for r in Regions.get_oc3_regions():
            self.assertEqual(
                r.endpoint(),
                'https://nosql.' + r.get_region_id() +
                '.oci.oraclegovcloud.com')
        for r in Regions.get_oc4_regions():
            self.assertEqual(
                r.endpoint(),
                'https://nosql.' + r.get_region_id() + '.oci.oraclegovcloud.uk')

        for r in Regions.get_oc8_regions():
            self.assertEqual(
                r.endpoint(),
                'https://nosql.' + r.get_region_id() + '.oci.oraclecloud8.com')

    def testNoSQLHandleConfigClone(self):
        config = get_handle_config(tenant_id)
        clone_config = config.clone()
        self._check_config(clone_config, config.get_service_url(),
                           retry_handler.get_num_retries())

    def testNoSQLHandleConfigRetryHandler(self):
        self.assertEqual(retry_handler.get_num_retries(), 10)
        # set illegal request to RetryHandler.do_retry
        self.assertRaises(IllegalArgumentException, retry_handler.do_retry,
                          'IllegalRequest', 3,
                          OperationThrottlingException('Test'))
        # set illegal retried number to RetryHandler.do_retry
        self.assertRaises(IllegalArgumentException, retry_handler.do_retry,
                          self.table_request, 'IllegalNumRetried',
                          RetryableException('Test'))
        self.assertRaises(IllegalArgumentException, retry_handler.do_retry,
                          self.table_request, -1, RetryableException('Test'))
        # set illegal retryable exception to RetryHandler.do_retry
        self.assertRaises(IllegalArgumentException, retry_handler.do_retry,
                          self.table_request, 3, 'IllegalException')
        self.assertRaises(IllegalArgumentException, retry_handler.do_retry,
                          self.table_request, 3,
                          IllegalArgumentException('Test'))
        # set legal retried number and retryable exception to
        # RetryHandler.do_retry
        self.assertFalse(retry_handler.do_retry(
            self.table_request, 5, OperationThrottlingException('Test')))
        self.assertFalse(retry_handler.do_retry(
            self.table_request, 5, RetryableException('Test')))
        self.assertTrue(retry_handler.do_retry(
            self.get_request, 5, RetryableException('Test')))
        self.assertFalse(retry_handler.do_retry(
            self.get_request, 10, RetryableException('Test')))
        # set illegal request to RetryHandler.delay
        self.assertRaises(IllegalArgumentException, retry_handler.delay,
                          'IllegalRequest', 5000, RetryableException('Test'))
        # set illegal retried number to RetryHandler.delay
        self.assertRaises(IllegalArgumentException, retry_handler.delay,
                          self.table_request, 'IllegalNumRetried',
                          RetryableException('Test'))
        self.assertRaises(IllegalArgumentException, retry_handler.delay,
                          self.table_request, -1, RetryableException('Test'))
        # set illegal retryable exception to RetryHandler.delay
        self.assertRaises(IllegalArgumentException, retry_handler.delay,
                          self.table_request, 5000, 'IllegalException')
        self.assertRaises(IllegalArgumentException, retry_handler.delay,
                          self.table_request, 5000,
                          IllegalArgumentException('Test'))

    def testNoSQLHandleConfigGets(self):
        config = get_handle_config(tenant_id)
        self._check_config(config, None, retry_handler)

    def _check_config(self, config, service_url, handler):
        max_content_length = 0
        # check service url
        url = config.get_service_url()
        (self.assertIsNotNone(url) if service_url is None
         else self.assertEqual(url, service_url))
        # check default timeout
        self.assertEqual(config.get_default_timeout(), timeout)
        # check default table request timeout
        self.assertEqual(config.get_default_table_request_timeout(),
                         table_request_timeout)
        # check default consistency
        self.assertEqual(config.get_default_consistency(), consistency)
        # check timeout
        self.assertEqual(config.get_timeout(), timeout)
        # check table request timeout
        self.assertEqual(config.get_table_request_timeout(),
                         table_request_timeout)
        # check consistency
        self.assertEqual(config.get_consistency(), consistency)
        # check pool connections
        self.assertEqual(config.get_pool_connections(), pool_connections)
        # check pool maxsize
        self.assertEqual(config.get_pool_maxsize(), pool_maxsize)
        # check max content length
        self.assertEqual(config.get_max_content_length(), max_content_length)
        # check retryable handler
        get_handler = config.get_retry_handler()
        (self.assertEqual(get_handler, handler) if
         isinstance(handler, RetryHandler) else
         self.assertEqual(get_handler.get_num_retries(), handler))
        # check rate limiting enabled
        self.assertEqual(config.get_rate_limiting_enabled(), False)
        # check rate limiting percentage
        self.assertEqual(config.get_default_rate_limiting_percentage(), 100.0)
        # check proxy host
        self.assertEqual(config.get_proxy_host(), proxy_host)
        # check proxy port
        self.assertEqual(config.get_proxy_port(), proxy_port)
        # check proxy username
        self.assertEqual(config.get_proxy_username(), proxy_username)
        # check proxy password
        self.assertEqual(config.get_proxy_password(), proxy_password)
        # check authorization provider
        self.assertIsNotNone(config.get_authorization_provider())
        # check ssl ca certs
        self.assertEqual(config.get_ssl_ca_certs(), ca_certs)
        # check ssl cipher suites
        self.assertEqual(config.get_ssl_cipher_suites(), ssl_cipher_suites)
        # check ssl protocol
        self.assertEqual(config.get_ssl_protocol(), ssl_protocol)
        # check logger
        self.assertIsNotNone(config.get_logger())

    def _check_service_url(self, config, protocol, host, port):
        service_url = config.get_service_url()
        self.assertEqual(service_url.scheme, protocol)
        self.assertEqual(service_url.hostname, host)
        self.assertEqual(service_url.port, port)


if __name__ == '__main__':
    unittest.main()
