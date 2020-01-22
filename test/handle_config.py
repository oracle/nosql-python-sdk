#
# Copyright (C) 2018, 2020 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
#
# Please see LICENSE.txt file included in the top-level directory of the
# appropriate download for a copy of the license and additional information.
#

import unittest
from requests import ConnectionError

from borneo import (
    GetRequest, IllegalArgumentException, OperationThrottlingException,
    DefaultRetryHandler, Regions, RetryableException, RetryHandler,
    SecurityInfoNotReadyException, NoSQLHandle, NoSQLHandleConfig, TableRequest)
from parameters import (
    consistency, endpoint, pool_connections, pool_maxsize, security, table_name,
    tenant_id, timeout, table_request_timeout)
from testutils import (
    get_handle_config, get_simple_handle_config, proxy_host, proxy_port,
    proxy_username, proxy_password, retry_handler, sec_info_timeout)


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
        # illegal region
        self.assertRaises(IllegalArgumentException, NoSQLHandleConfig,
                          region='IllegalRegion')
        # illegal provider
        self.assertRaises(IllegalArgumentException, NoSQLHandleConfig,
                          endpoint, provider='IllegalProvider')
        # no endpoint and region
        self.assertRaises(IllegalArgumentException, NoSQLHandleConfig)
        # both endpoint and region
        self.assertRaises(IllegalArgumentException, NoSQLHandleConfig,
                          endpoint, Regions.UK_LONDON_1)

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

    def testNoSQLHandleConfigSetIllegalSecInfoTimeout(self):
        self.assertRaises(IllegalArgumentException,
                          self.config.set_sec_info_timeout,
                          'IllegalSecInfoTimeout')
        self.assertRaises(IllegalArgumentException,
                          self.config.set_sec_info_timeout, 0)
        self.assertRaises(IllegalArgumentException,
                          self.config.set_sec_info_timeout, -1)

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

    def testNoSQLHandleEndpointConfig(self):
        # set only the host as endpoint
        config = get_simple_handle_config(tenant_id, 'ndcs.com')
        self._check_service_url(config, 'https', 'ndcs.com', 443)
        # set proto://host as endpoint
        config = get_simple_handle_config(tenant_id, 'Http://ndcs.com')
        self._check_service_url(config, 'http', 'ndcs.com', 8080)
        config = get_simple_handle_config(tenant_id, 'https://ndcs.com')
        self._check_service_url(config, 'https', 'ndcs.com', 443)
        # set proto:host as endpoint
        config = get_simple_handle_config(tenant_id, 'Http:ndcs.com')
        self._check_service_url(config, 'http', 'ndcs.com', 8080)
        config = get_simple_handle_config(tenant_id, 'https:ndcs.com')
        self._check_service_url(config, 'https', 'ndcs.com', 443)
        # set host:port as endpoint
        config = get_simple_handle_config(tenant_id, 'localhost:80')
        self._check_service_url(config, 'http', 'localhost', 80)
        config = get_simple_handle_config(tenant_id, 'localhost:443')
        self._check_service_url(config, 'https', 'localhost', 443)
        # set proto://host:port as endpoint
        config = get_simple_handle_config(tenant_id, 'HTTPS://ndcs.com:8080')
        self._check_service_url(config, 'https', 'ndcs.com', 8080)
        # set proto:host:port as endpoint
        config = get_simple_handle_config(tenant_id, 'Http:ndcs.com:443')
        self._check_service_url(config, 'http', 'ndcs.com', 443)

    def testNoSQLHandleConfigRegions(self):
        for r in Regions.get_oc1_regions():
            self.assertEqual(
                r.endpoint(),
                'https://nosql.' + r.get_region_id() + '.oci.oraclecloud.com')
        for r in Regions.get_gov_regions():
            self.assertEqual(
                r.endpoint(),
                'https://nosql.' + r.get_region_id() +
                '.oci.oraclegovcloud.com')
        for r in Regions.get_oc4_regions():
            self.assertEqual(
                r.endpoint(),
                'https://nosql.' + r.get_region_id() + '.oci.oraclegovcloud.uk')

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
                          self.table_request, 0, RetryableException('Test'))
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
        self.assertTrue(retry_handler.do_retry(
            self.table_request, 5, SecurityInfoNotReadyException('Test')))
        self.assertFalse(retry_handler.do_retry(
            self.table_request, 5, OperationThrottlingException('Test')))
        self.assertFalse(retry_handler.do_retry(
            self.table_request, 5, RetryableException('Test')))
        self.assertTrue(retry_handler.do_retry(
            self.get_request, 5, RetryableException('Test')))
        self.assertFalse(retry_handler.do_retry(
            self.get_request, 10, RetryableException('Test')))
        # set illegal retried number to RetryHandler.delay
        self.assertRaises(IllegalArgumentException, retry_handler.delay,
                          'IllegalNumRetried', RetryableException('Test'))
        self.assertRaises(IllegalArgumentException, retry_handler.delay, 0,
                          RetryableException('Test'))
        self.assertRaises(IllegalArgumentException, retry_handler.delay, -1,
                          RetryableException('Test'))
        # set illegal retryable exception to RetryHandler.delay
        self.assertRaises(IllegalArgumentException, retry_handler.delay,
                          5000, 'IllegalException')
        self.assertRaises(IllegalArgumentException, retry_handler.delay,
                          5000, IllegalArgumentException('Test'))

    def testNoSQLHandleConfigGets(self):
        config = get_handle_config(tenant_id)
        self._check_config(config, None, retry_handler)

    def _check_config(self, config, service_url, handler):
        max_content_length = 1024 * 1024
        # check service url
        url = config.get_service_url()
        (self.assertIsNotNone(url) if service_url is None
         else self.assertEqual(url, service_url))
        # check the region
        self.assertIsNone(config.get_region())
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
        # check security info timeout
        self.assertEqual(config.get_sec_info_timeout(), sec_info_timeout)
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
        # check logger
        self.assertIsNotNone(config.get_logger())

    def _check_service_url(self, config, protocol, host, port):
        service_url = config.get_service_url()
        self.assertEqual(service_url.scheme, protocol)
        self.assertEqual(service_url.hostname, host)
        self.assertEqual(service_url.port, port)


if __name__ == '__main__':
    unittest.main()
