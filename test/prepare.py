#
# Copyright (C) 2018, 2019 Oracle and/or its affiliates. All rights reserved.
#
# This software is licensed with the Universal Permissive License (UPL) version 1.0
#
# Please see LICENSE.txt file included in the top-level directory of the
# appropriate download for a copy of the license and additional information.
#

import unittest
from time import sleep

from borneo import (
    IllegalArgumentException, PrepareRequest, State, TableLimits,
    TableNotFoundException, TableRequest)
from parameters import protocol, table_name, tenant_id, timeout, wait_timeout
from testutils import add_test_tier_tenant, delete_test_tier_tenant, get_handle


class TestPrepare(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        add_test_tier_tenant(tenant_id)
        cls._handle = get_handle(tenant_id)
        if protocol == 'https':
            # sleep a while to avoid the OperationThrottlingException
            sleep(60)
        drop_statement = 'DROP TABLE IF EXISTS ' + table_name
        cls._drop_request = TableRequest().set_statement(drop_statement)
        cls._result = cls._handle.table_request(cls._drop_request)
        cls._result.wait_for_state(cls._handle, table_name, State.DROPPED,
                                   wait_timeout, 1000)
        create_statement = (
            'CREATE TABLE ' + table_name + '(fld_id INTEGER, fld_long LONG, \
fld_float FLOAT, fld_double DOUBLE, fld_bool BOOLEAN, fld_str STRING, \
fld_bin BINARY, fld_time TIMESTAMP(3), fld_num NUMBER, fld_json JSON, \
fld_arr ARRAY(STRING), fld_map MAP(STRING), \
fld_rec RECORD(fld_id LONG, fld_bool BOOLEAN, fld_str STRING), \
PRIMARY KEY(fld_id)) USING TTL 1 HOURS')
        limits = TableLimits(5000, 5000, 50)
        create_request = TableRequest().set_statement(
            create_statement).set_table_limits(limits)
        cls._result = cls._handle.table_request(create_request)
        cls._result.wait_for_state(cls._handle, table_name, State.ACTIVE,
                                   wait_timeout, 1000)

    @classmethod
    def tearDownClass(cls):
        try:
            cls._result = cls._handle.table_request(cls._drop_request)
            cls._result.wait_for_state(cls._handle, table_name, State.DROPPED,
                                       wait_timeout, 1000)
        finally:
            cls._handle.close()
            delete_test_tier_tenant(tenant_id)

    def setUp(self):
        self.handle = get_handle(tenant_id)
        self.prepare_statement = ('SELECT fld_id FROM ' + table_name)
        self.prepare_request = PrepareRequest().set_timeout(timeout)

    def tearDown(self):
        self.handle.close()

    def testPrepareSetIllegalStatement(self):
        self.prepare_request.set_statement('IllegalStatement')
        self.assertRaises(IllegalArgumentException, self.handle.prepare,
                          self.prepare_request)
        self.prepare_request.set_statement(
            'SELECT fld_id FROM IllegalTableName')
        self.assertRaises(TableNotFoundException, self.handle.prepare,
                          self.prepare_request)

    def testPrepareSetIllegalTimeout(self):
        self.assertRaises(IllegalArgumentException,
                          self.prepare_request.set_timeout, 'IllegalTimeout')
        self.assertRaises(IllegalArgumentException,
                          self.prepare_request.set_timeout, 0)
        self.assertRaises(IllegalArgumentException,
                          self.prepare_request.set_timeout, -1)

    def testPrepareNoStatement(self):
        self.assertRaises(IllegalArgumentException, self.handle.prepare,
                          self.prepare_request)

    def testPrepareGets(self):
        self.prepare_request.set_statement(self.prepare_statement)
        self.assertEqual(self.prepare_request.get_statement(),
                         self.prepare_statement)
        self.assertEqual(self.prepare_request.get_timeout(), timeout)

    def testPrepareIllegalRequest(self):
        self.assertRaises(IllegalArgumentException, self.handle.prepare,
                          'IllegalRequest')

    def testPrepareNormal(self):
        self.prepare_request.set_statement(self.prepare_statement)
        result = self.handle.prepare(self.prepare_request)
        prepared_statement = result.get_prepared_statement()
        self.assertIsNotNone(prepared_statement)
        self.assertEqual(result.get_read_kb(), 2)
        self.assertEqual(result.get_read_units(), 2)
        self.assertEqual(result.get_write_kb(), 0)
        self.assertEqual(result.get_write_units(), 0)
        # test PreparedStatement
        statement = prepared_statement.get_statement()
        self.assertIsNotNone(statement)
        # test set illegal variable to the prepared statement
        self.assertRaises(IllegalArgumentException,
                          prepared_statement.set_variable, 0, 0)
        # test set variable to the prepared statement
        set_vars = dict()
        set_vars['$fld_id'] = 0
        set_vars['$fld_long'] = 2147483648
        for var in set_vars:
            prepared_statement.set_variable(var, set_vars[var])
        # test get variables from the prepared statement
        get_vars = prepared_statement.get_variables()
        self.assertEqual(set_vars, get_vars)
        # test copy the prepared statement
        copied_statement = prepared_statement.copy_statement()
        self.assertEqual(copied_statement.get_statement(), statement)
        self.assertEqual(0, len(copied_statement.get_variables()))
        # test clear variables from the prepared statement
        prepared_statement.clear_variables()
        self.assertEqual(prepared_statement.get_statement(), statement)
        self.assertEqual(prepared_statement.get_variables(), {})


if __name__ == '__main__':
    unittest.main()
