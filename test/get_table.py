#
# Copyright (C) 2018, 2019 Oracle and/or its affiliates. All rights reserved.
#
# This software is licensed with the Universal Permissive License (UPL) version 1.0
#
# Please see LICENSE.txt file included in the top-level directory of the
# appropriate download for a copy of the license and additional information.
#

import unittest

from borneo import (
    GetTableRequest, IllegalArgumentException, State, TableLimits,
    TableNotFoundException, TableRequest)
from parameters import table_name, timeout, wait_timeout
from test_base import TestBase


class TestGetTable(unittest.TestCase, TestBase):
    @classmethod
    def setUpClass(cls):
        TestBase.set_up_class()
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
        cls._result = TestBase.table_request(create_request, State.ACTIVE)

    @classmethod
    def tearDownClass(cls):
        TestBase.tear_down_class()

    def setUp(self):
        TestBase.set_up(self)
        self.get_table_request = GetTableRequest().set_timeout(timeout)

    def tearDown(self):
        TestBase.tear_down(self)

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
        self.assertEqual(result.get_table_name(), table_name)
        self.assertEqual(result.get_state(), State.ACTIVE)
        self.assertEqual(result.get_table_limits().get_read_units(),
                         table_limits.get_read_units())
        self.assertEqual(result.get_table_limits().get_write_units(),
                         table_limits.get_write_units())
        self.assertEqual(result.get_table_limits().get_storage_gb(),
                         table_limits.get_storage_gb())
        self.assertIsNone(result.get_operation_id())

    def testGetTableWithOperationId(self):
        drop_request = TableRequest().set_statement(
            'DROP TABLE IF EXISTS ' + table_name)
        table_result = self.handle.table_request(drop_request)
        self.get_table_request.set_table_name(table_name).set_operation_id(
            table_result.get_operation_id())
        result = self.handle.get_table(self.get_table_request)
        self.assertEqual(result.get_table_name(), table_name)
        self.assertEqual(result.get_state(), State.DROPPING)
        self.assertEqual(result.get_table_limits().get_read_units(),
                         table_limits.get_read_units())
        self.assertEqual(result.get_table_limits().get_write_units(),
                         table_limits.get_write_units())
        self.assertEqual(result.get_table_limits().get_storage_gb(),
                         table_limits.get_storage_gb())
        table_result.wait_for_state(self.handle, table_name, State.DROPPED,
                                    wait_timeout, 1000)


if __name__ == '__main__':
    unittest.main()
