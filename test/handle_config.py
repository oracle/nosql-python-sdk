#
# Copyright (C) 2018, 2019 Oracle and/or its affiliates. All rights reserved.
#
# This software is licensed with the Universal Permissive License (UPL) version 1.0
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
    consistency, http_host, http_port, pool_connections, pool_maxsize, protocol,
    proxy_host, proxy_password, proxy_port, proxy_username, retry_handler,
    sec_info_timeout, table_name, tenant_id, timeout, table_request_timeout)
from testutils import get_simple_handle_config, get_handle_config


class TestNoSQLHandleConfig(unittest.TestCase):
    def setUp(self):
        self.config = get_simple_handle_config(tenant_id)
        self.table_request = TableRequest().set_statement(
            'DROP TABLE IF EXISTS ' + table_name)
        self.get_request = GetRequest()

    def testNoSQLHandleConfigIllegalInit(self):
        # illegal protocol
        self.assertRaises(IllegalArgumentException, NoSQLHandleConfig, 12345,
                          'localhost', 8080)
        self.assertRaises(IllegalArgumentException, NoSQLHandleConfig,
                          'IllegalProtocol', 'localhost', 8080)
        # illegal host
        self.assertRaises(IllegalArgumentException, NoSQLHandleConfig, 'HTTP',
                          12345, 8080)
        # illegal port
        self.assertRaises(IllegalArgumentException, NoSQLHandleConfig, 'HTTP',
                          'localhost', 'IllegalPort')
        self.assertRaises(IllegalArgumentException, NoSQLHandleConfig, 'HTTP',
                          'localhost', -1)
        config = get_simple_handle_config(tenant_id).set_port(80)
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

    def testNoSQLHandleConfigClone(self):
        max_content_length = 1024 * 1024
        config = get_handle_config(tenant_id)
        clone_config = config.clone()
        self.assertEqual(clone_config.get_default_timeout(), timeout)
        self.assertEqual(clone_config.get_default_table_request_timeout(),
                         table_request_timeout)
        self.assertEqual(clone_config.get_default_consistency(), consistency)
        self.assertEqual(clone_config.get_protocol(), protocol)
        self.assertEqual(clone_config.get_host(), http_host)
        self.assertEqual(clone_config.get_port(), http_port)
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
        self.assertEqual(config.get_default_timeout(), timeout)
        self.assertEqual(config.get_default_table_request_timeout(),
                         table_request_timeout)
        self.assertEqual(config.get_default_consistency(), consistency)
        self.assertEqual(config.get_protocol(), protocol)
        self.assertEqual(config.get_host(), http_host)
        self.assertEqual(config.get_port(), http_port)
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
        self.assertEqual(config.get_proxy_host(), proxy_host)
        self.assertEqual(config.get_proxy_port(), proxy_port)
        self.assertEqual(config.get_proxy_username(), proxy_username)
        self.assertEqual(config.get_proxy_password(), proxy_password)
        self.assertIsNotNone(config.get_logger())


if __name__ == '__main__':
    unittest.main()
