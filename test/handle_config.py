#
# Copyright (C) 2018, 2019 Oracle and/or its affiliates. All rights reserved.
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
    DefaultRetryHandler, RetryableException, SecurityInfoNotReadyException,
    NoSQLHandle, NoSQLHandleConfig, TableRequest)
from parameters import (
    consistency, endpoint, pool_connections, pool_maxsize, table_name,
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
        self.assertEqual(config.get_protocol(), 'https')
        self.assertEqual(config.get_host(), 'ndcs.com')
        self.assertEqual(config.get_port(), 443)
        # set proto://host as endpoint
        config = get_simple_handle_config(tenant_id, 'Http://ndcs.com')
        self.assertEqual(config.get_protocol(), 'http')
        self.assertEqual(config.get_host(), 'ndcs.com')
        self.assertEqual(config.get_port(), 8080)
        config = get_simple_handle_config(tenant_id, 'https://ndcs.com')
        self.assertEqual(config.get_protocol(), 'https')
        self.assertEqual(config.get_host(), 'ndcs.com')
        self.assertEqual(config.get_port(), 443)
        # set proto:host as endpoint
        config = get_simple_handle_config(tenant_id, 'Http:ndcs.com')
        self.assertEqual(config.get_protocol(), 'http')
        self.assertEqual(config.get_host(), 'ndcs.com')
        self.assertEqual(config.get_port(), 8080)
        config = get_simple_handle_config(tenant_id, 'https:ndcs.com')
        self.assertEqual(config.get_protocol(), 'https')
        self.assertEqual(config.get_host(), 'ndcs.com')
        self.assertEqual(config.get_port(), 443)
        # set host:port as endpoint
        config = get_simple_handle_config(tenant_id, 'localhost:8080')
        self.assertEqual(config.get_protocol(), 'http')
        self.assertEqual(config.get_host(), 'localhost')
        self.assertEqual(config.get_port(), 8080)
        config = get_simple_handle_config(tenant_id, 'localhost:443')
        self.assertEqual(config.get_protocol(), 'https')
        self.assertEqual(config.get_host(), 'localhost')
        self.assertEqual(config.get_port(), 443)
        # set proto://host:port as endpoint
        config = get_simple_handle_config(tenant_id, 'HTTPS://ndcs.com:8080')
        self.assertEqual(config.get_protocol(), 'https')
        self.assertEqual(config.get_host(), 'ndcs.com')
        self.assertEqual(config.get_port(), 8080)
        # set proto:host:port as endpoint
        config = get_simple_handle_config(tenant_id, 'Http:ndcs.com:443')
        self.assertEqual(config.get_protocol(), 'http')
        self.assertEqual(config.get_host(), 'ndcs.com')
        self.assertEqual(config.get_port(), 443)

    def testNoSQLHandleConfigClone(self):
        max_content_length = 1024 * 1024
        config = get_handle_config(tenant_id)
        clone_config = config.clone()
        self.assertEqual(clone_config.get_endpoint(), endpoint)
        self.assertEqual(clone_config.get_default_timeout(), timeout)
        self.assertEqual(clone_config.get_default_table_request_timeout(),
                         table_request_timeout)
        self.assertEqual(clone_config.get_default_consistency(), consistency)
        self.assertEqual(clone_config.get_protocol(), config.get_protocol())
        self.assertEqual(clone_config.get_host(), config.get_host())
        self.assertEqual(clone_config.get_port(), config.get_port())
        self.assertEqual(clone_config.get_timeout(), timeout)
        self.assertEqual(clone_config.get_table_request_timeout(),
                         table_request_timeout)
        self.assertEqual(clone_config.get_sec_info_timeout(), sec_info_timeout)
        self.assertEqual(clone_config.get_consistency(), consistency)
        self.assertEqual(clone_config.get_pool_connections(), pool_connections)
        self.assertEqual(clone_config.get_pool_maxsize(), pool_maxsize)
        self.assertEqual(clone_config.get_max_content_length(),
                         max_content_length)
        self.assertEqual(clone_config.get_retry_handler().get_num_retries(),
                         retry_handler.get_num_retries())
        self.assertIsNotNone(clone_config.get_authorization_provider())
        self.assertEqual(clone_config.get_proxy_host(), proxy_host)
        self.assertEqual(clone_config.get_proxy_port(), proxy_port)
        self.assertEqual(clone_config.get_proxy_username(), proxy_username)
        self.assertEqual(clone_config.get_proxy_password(), proxy_password)
        self.assertIsNotNone(clone_config.get_logger())

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
        max_content_length = 1024 * 1024
        config = get_handle_config(tenant_id)
        self.assertEqual(config.get_endpoint(), endpoint)
        self.assertEqual(config.get_default_timeout(), timeout)
        self.assertEqual(config.get_default_table_request_timeout(),
                         table_request_timeout)
        self.assertEqual(config.get_default_consistency(), consistency)
        self.assertEqual(config.get_timeout(), timeout)
        self.assertEqual(config.get_table_request_timeout(),
                         table_request_timeout)
        self.assertEqual(config.get_sec_info_timeout(), sec_info_timeout)
        self.assertEqual(config.get_consistency(), consistency)
        self.assertEqual(config.get_pool_connections(), pool_connections)
        self.assertEqual(config.get_pool_maxsize(), pool_maxsize)
        self.assertEqual(config.get_max_content_length(), max_content_length)
        self.assertEqual(config.get_retry_handler(), retry_handler)
        self.assertIsNotNone(config.get_authorization_provider())
        self.assertIsNotNone(config.get_logger())


if __name__ == '__main__':
    unittest.main()
