#
# Copyright (c) 2018, 2022 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#

import unittest

from borneo import (
    IllegalArgumentException, SystemRequest, SystemState, SystemStatusRequest)
from parameters import is_onprem, tenant_id, timeout, wait_timeout
from test_base import TestBase
from testutils import get_handle_config, namespace


class TestSystemStatusRequest(unittest.TestCase, TestBase):
    if is_onprem():
        @classmethod
        def setUpClass(cls):
            cls.set_up_class()

        @classmethod
        def tearDownClass(cls):
            cls.tear_down_class()

        def setUp(self):
            self.set_up()
            self.handle_config = get_handle_config(tenant_id)
            self.create = 'CREATE NAMESPACE ' + namespace
            self.drop = 'DROP NAMESPACE ' + namespace + ' CASCADE'
            self.sys_request = SystemRequest().set_timeout(timeout)
            self.sys_status = SystemStatusRequest().set_timeout(timeout)

        def tearDown(self):
            self.tear_down()

        def testSystemStatusRequestSetIllegalOperationId(self):
            self.assertRaises(IllegalArgumentException,
                              self.sys_status.set_operation_id, {})
            self.assertRaises(IllegalArgumentException,
                              self.sys_status.set_operation_id, '')

        def testSystemStatusRequestSetIllegalStatement(self):
            self.assertRaises(IllegalArgumentException,
                              self.sys_status.set_statement, {})
            self.assertRaises(IllegalArgumentException,
                              self.sys_status.set_statement, '')

        def testSystemStatusRequestSetIllegalTimeout(self):
            self.assertRaises(IllegalArgumentException,
                              self.sys_status.set_timeout, 'IllegalTimeout')
            self.assertRaises(IllegalArgumentException,
                              self.sys_status.set_timeout, 0)
            self.assertRaises(IllegalArgumentException,
                              self.sys_status.set_timeout, -1)

        def testSystemStatusRequestSetIllegalDefaults(self):
            self.assertRaises(IllegalArgumentException,
                              self.sys_status.set_defaults, 'IllegalDefaults')

        def testSystemStatusRequestSetDefaults(self):
            self.sys_status.set_defaults(self.handle_config)
            self.assertEqual(self.sys_status.get_timeout(), timeout)

        def testSystemStatusRequestNoOperationId(self):
            self.assertRaises(IllegalArgumentException,
                              self.handle.system_status, self.sys_status)

        def testSystemStatusRequestGets(self):
            operation_id = '100'
            self.sys_status.set_operation_id(operation_id).set_statement(
                self.create)
            self.assertEqual(self.sys_status.get_operation_id(), operation_id)
            self.assertEqual(self.sys_status.get_statement(), self.create)
            self.assertEqual(self.sys_status.get_timeout(), timeout)

        def testSystemStatusRequestIllegalRequest(self):
            self.assertRaises(IllegalArgumentException,
                              self.handle.system_status, 'IllegalRequest')

        def testSystemStatusRequestNormal(self):
            # execute create namespace system request.
            self.sys_request.set_statement(self.create)
            result = self.handle.system_request(self.sys_request)
            # show the status of the create namespace system request.
            self.sys_status.set_operation_id(result.get_operation_id())
            result = self.handle.system_status(self.sys_status)
            result.wait_for_completion(self.handle, wait_timeout, 1000)
            self.check_system_result(result, SystemState.COMPLETE, True)
            # execute drop namespace system request.
            self.sys_request.set_statement(self.drop)
            result = self.handle.system_request(self.sys_request)
            # show the status of the drop namespace system request.
            self.sys_status.set_operation_id(
                result.get_operation_id()).set_statement(self.create)
            result = self.handle.system_status(self.sys_status)
            result.wait_for_completion(self.handle, wait_timeout, 1000)
            self.check_system_result(result, SystemState.COMPLETE, True,
                                     statement=self.create)


if __name__ == '__main__':
    unittest.main()
