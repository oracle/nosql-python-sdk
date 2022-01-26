#
# Copyright (c) 2018, 2022 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#

import unittest

from borneo import (
    GetTableRequest, IllegalArgumentException, State, TableLimits,
    TableNotFoundException, TableRequest)
from parameters import is_minicloud, is_pod, table_name, timeout, wait_timeout
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
        table_limits = TableLimits(100, 100, 1)
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

    def testGetTableSetIllegalCompartment(self):
        self.assertRaises(IllegalArgumentException,
                          self.get_table_request.set_compartment, {})
        self.assertRaises(IllegalArgumentException,
                          self.get_table_request.set_compartment, '')

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
        self.assertIsNone(self.get_table_request.get_compartment())
        self.assertIsNone(self.get_table_request.get_operation_id())

    def testGetTableIllegalRequest(self):
        self.assertRaises(IllegalArgumentException, self.handle.get_table,
                          'IllegalRequest')

    def testGetTableNormal(self):
        self.get_table_request.set_table_name(table_name)
        result = self.handle.get_table(self.get_table_request)
        if is_minicloud() or is_pod():
            self.check_table_result(result, State.ACTIVE, table_limits)
        else:
            self.check_table_result(result, State.ACTIVE, table_limits,
                                    has_operation_id=False)

    def testGetTableWithOperationId(self):
        drop_request = TableRequest().set_statement(
            'DROP TABLE IF EXISTS ' + table_name)
        table_result = self.handle.table_request(drop_request)
        self.get_table_request.set_table_name(table_name).set_operation_id(
            table_result.get_operation_id())
        result = self.handle.get_table(self.get_table_request)
        if is_minicloud() or is_pod():
            self.check_table_result(
                result, [State.DROPPING, State.DROPPED], table_limits)
        else:
            self.check_table_result(
                result, [State.DROPPING, State.DROPPED])
        table_result.wait_for_completion(self.handle, wait_timeout, 1000)


if __name__ == '__main__':
    unittest.main()
