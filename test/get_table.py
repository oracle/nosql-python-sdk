#
# Copyright (C) 2018, 2019 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
#
# Please see LICENSE.txt file included in the top-level directory of the
# appropriate download for a copy of the license and additional information.
#

import unittest

from borneo import (
    GetTableRequest, IllegalArgumentException, State, TableLimits,
    TableNotFoundException, TableRequest)
from parameters import (
    is_onprem, not_cloudsim, table_name, timeout, wait_timeout)
from test_base import TestBase


class TestGetTable(unittest.TestCase, TestBase):
    @classmethod
    def setUpClass(cls):
        cls.set_up_class()
        create_statement = (
            'CREATE TABLE ' + table_name + '(fld_id INTEGER, fld_long LONG, \
fld_float FLOAT, fld_double DOUBLE, fld_bool BOOLEAN, fld_str STRING, \
fld_bin BINARY, fld_time TIMESTAMP(1), fld_num NUMBER, fld_json JSON, \
fld_arr ARRAY(STRING), fld_map MAP(STRING), \
fld_rec RECORD(fld_id LONG, fld_bool BOOLEAN, fld_str STRING), \
PRIMARY KEY(fld_id)) USING TTL 30 DAYS')
        global table_limits
        table_limits = TableLimits(5000, 5000, 50)
        create_request = TableRequest().set_statement(
            create_statement).set_table_limits(table_limits)
        cls.table_request(create_request)

    @classmethod
    def tearDownClass(cls):
        cls.tear_down_class()

    def setUp(self):
        self.set_up()
        self.get_table_request = GetTableRequest().set_timeout(timeout)

    def tearDown(self):
        self.tear_down()

    def testGetTableSetIllegalTableName(self):
        self.assertRaises(IllegalArgumentException,
                          self.get_table_request.set_table_name,
                          {'name': table_name})
        self.get_table_request.set_table_name('IllegalTable')
        self.assertRaises(TableNotFoundException, self.handle.get_table,
                          self.get_table_request)

    def testGetTableSetIllegalOperationId(self):
        self.assertRaises(IllegalArgumentException,
                          self.get_table_request.set_operation_id, 0)

    def testGetTableSetIllegalTimeout(self):
        self.assertRaises(IllegalArgumentException,
                          self.get_table_request.set_timeout, 'IllegalTimeout')
        self.assertRaises(IllegalArgumentException,
                          self.get_table_request.set_timeout, 0)
        self.assertRaises(IllegalArgumentException,
                          self.get_table_request.set_timeout, -1)

    def testGetTableNoTableName(self):
        self.assertRaises(IllegalArgumentException, self.handle.get_table,
                          self.get_table_request)

    def testGetTableGets(self):
        self.get_table_request.set_table_name(table_name)
        self.assertEqual(self.get_table_request.get_table_name(), table_name)
        self.assertIsNone(self.get_table_request.get_operation_id())

    def testGetTableIllegalRequest(self):
        self.assertRaises(IllegalArgumentException, self.handle.get_table,
                          'IllegalRequest')

    def testGetTableNormal(self):
        self.get_table_request.set_table_name(table_name)
        result = self.handle.get_table(self.get_table_request)
        self._check_get_table_result(result, State.ACTIVE, table_limits)

    def testGetTableWithOperationId(self):
        drop_request = TableRequest().set_statement(
            'DROP TABLE IF EXISTS ' + table_name)
        table_result = self.handle.table_request(drop_request)
        self.get_table_request.set_table_name(table_name).set_operation_id(
            table_result.get_operation_id())
        result = self.handle.get_table(self.get_table_request)
        # TODO: A difference between old cloud proxy and new cloud proxy, during
        # DROPPING phase, the table limit is not none for old proxy but none for
        # new proxy.
        self._check_get_table_result(result, State.DROPPING,
                                     has_operation_id=True, check_limit=False)
        table_result.wait_for_completion(self.handle, wait_timeout, 1000)

    def _check_get_table_result(self, result, state, limits=None,
                                has_operation_id=False, check_limit=True):
        # check table name
        self.assertEqual(result.get_table_name(), table_name)
        # check state
        self.assertEqual(result.get_state(), state)
        # check table limits
        if check_limit:
            self.check_table_limits(result, limits)
        # check table schema
        # TODO: For on-prem proxy, TableResult.get_schema() always return None,
        # This is a known bug, when it is fixed, the test should be change.
        if not_cloudsim() and not is_onprem():
            self.assertIsNotNone(result.get_schema())
        # check operation id
        operation_id = result.get_operation_id()
        (self.assertIsNotNone(operation_id) if has_operation_id
         else self.assertIsNone(operation_id))


if __name__ == '__main__':
    unittest.main()
