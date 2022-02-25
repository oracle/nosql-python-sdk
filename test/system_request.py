#
# Copyright (c) 2018, 2022 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#

import unittest
from sys import version_info

from borneo import (
    IllegalArgumentException, SystemRequest, SystemState, UserInfo)
from parameters import (
    is_onprem, security, table_request_timeout, tenant_id, timeout,
    wait_timeout)
from test_base import TestBase
from testutils import get_handle_config, namespace


class TestSystemRequest(unittest.TestCase, TestBase):
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
            self.show = 'SHOW AS JSON NAMESPACES'
            self.create = 'CREATE NAMESPACE ' + namespace
            self.drop = 'DROP NAMESPACE ' + namespace + ' CASCADE'
            self.sys_request = SystemRequest()

        def tearDown(self):
            self.tear_down()

        def testSystemRequestSetIllegalStatement(self):
            self.assertRaises(IllegalArgumentException,
                              self.sys_request.set_statement, {})
            self.assertRaises(IllegalArgumentException,
                              self.sys_request.set_statement, '')
            self.sys_request.set_statement('IllegalStatement')
            self.assertRaises(IllegalArgumentException,
                              self.handle.system_request, self.sys_request)

        def testSystemRequestSetIllegalTimeout(self):
            self.assertRaises(IllegalArgumentException,
                              self.sys_request.set_timeout, 'IllegalTimeout')
            self.assertRaises(IllegalArgumentException,
                              self.sys_request.set_timeout, 0)
            self.assertRaises(IllegalArgumentException,
                              self.sys_request.set_timeout, -1)

        def testSystemRequestSetIllegalDefaults(self):
            self.assertRaises(IllegalArgumentException,
                              self.sys_request.set_defaults, 'IllegalDefaults')

        def testSystemRequestSetDefaults(self):
            self.sys_request.set_defaults(self.handle_config)
            self.assertEqual(self.sys_request.get_timeout(),
                             table_request_timeout)

        def testSystemRequestNoStatement(self):
            self.assertRaises(IllegalArgumentException,
                              self.handle.system_request, self.sys_request)

        def testSystemRequestGets(self):
            self.sys_request.set_statement(self.show).set_timeout(timeout)
            self.assertEqual(self.sys_request.get_statement(), self.show)
            self.assertEqual(self.sys_request.get_timeout(), timeout)

        def testSystemRequestIllegalRequest(self):
            self.assertRaises(IllegalArgumentException,
                              self.handle.system_request, 'IllegalRequest')

        def testSystemRequestNormal(self):
            # create namespace.
            self.sys_request.set_statement(self.create)
            result = self.handle.system_request(self.sys_request)
            self.check_system_result(
                result, SystemState.WORKING, True, statement=self.create)
            result.wait_for_completion(self.handle, wait_timeout, 1000)
            self.check_system_result(
                result, SystemState.COMPLETE, True, statement=self.create)
            # show namespaces.
            self.sys_request.set_statement(self.show)
            result = self.handle.system_request(self.sys_request)
            self.check_system_result(
                result, SystemState.COMPLETE, has_result_string=True,
                statement=self.show)
            result.wait_for_completion(self.handle, wait_timeout, 1000)
            self.check_system_result(
                result, SystemState.COMPLETE, has_result_string=True,
                statement=self.show)
            # drop namespace
            self.sys_request.set_statement(self.drop)
            result = self.handle.system_request(self.sys_request)
            self.check_system_result(
                result, SystemState.WORKING, True, statement=self.drop)
            result.wait_for_completion(self.handle, wait_timeout, 1000)
            self.check_system_result(
                result, SystemState.COMPLETE, True, statement=self.drop)

        def testDoSystemRequest(self):
            # create namespace.
            result = self.handle.do_system_request(
                self.create, wait_timeout, 1000)
            self.check_system_result(
                result, SystemState.COMPLETE, True, statement=self.create)
            # show namespaces.
            result = self.handle.do_system_request(
                self.show, wait_timeout, 1000)
            self.check_system_result(
                result, SystemState.COMPLETE, has_result_string=True,
                statement=self.show)
            # drop namespace.
            result = self.handle.do_system_request(
                self.drop, wait_timeout, 1000)
            self.check_system_result(
                result, SystemState.COMPLETE, True, statement=self.drop)

        def testListNamespaces(self):
            # show namespaces.
            results = self.handle.list_namespaces()
            self.assertGreaterEqual(len(results), 1)
            for result in results:
                self.assertTrue(self._is_str(result))

        def testListRoles(self):
            # show roles.
            results = self.handle.list_roles()
            self.assertGreaterEqual(len(results), 6)
            for result in results:
                self.assertTrue(self._is_str(result))

        def testListUsers(self):
            # show users.
            results = self.handle.list_users()
            if security():
                self.assertGreaterEqual(len(results), 1)
                for result in results:
                    self.assertTrue(isinstance(result, UserInfo))
                    self.assertTrue(self._is_str(result.get_id()))
                    self.assertTrue(self._is_str(result.get_name()))
            else:
                self.assertIsNone(results)

        @staticmethod
        def _is_str(data):
            if ((version_info.major == 2 and isinstance(data, (str, unicode)) or
                    version_info.major == 3 and isinstance(data, str)) and
                    len(data) != 0):
                return True
            return False


if __name__ == '__main__':
    unittest.main()
