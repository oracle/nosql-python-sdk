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
    IllegalArgumentException, State, TableLimits, TableNotFoundException,
    TableRequest, TableResult)
from parameters import (
    not_cloudsim, table_name, table_request_timeout, tenant_id, wait_timeout)
from testutils import get_handle_config
from test_base import TestBase


class TestTableRequest(unittest.TestCase, TestBase):
    @classmethod
    def setUpClass(cls):
        cls.set_up_class()

    @classmethod
    def tearDownClass(cls):
        cls.tear_down_class()

    def setUp(self):
        self.set_up()
        self.handle_config = get_handle_config(tenant_id)
        index_name = 'idx_' + table_name
        self.create_tb_statement = (
            'CREATE TABLE ' + table_name + '(fld_id INTEGER, fld_long LONG, \
fld_float FLOAT, fld_double DOUBLE, fld_bool BOOLEAN, fld_str STRING, \
fld_bin BINARY, fld_time TIMESTAMP(4), fld_num NUMBER, fld_json JSON, \
fld_arr ARRAY(STRING), fld_map MAP(STRING), \
fld_rec RECORD(fld_id LONG, fld_bool BOOLEAN, fld_str STRING), \
PRIMARY KEY(fld_id)) USING TTL 30 DAYS')
        self.create_idx_statement = (
            'CREATE INDEX ' + index_name + ' ON ' + table_name +
            '(fld_str, fld_double)')
        self.alter_fld_statement = (
            'ALTER TABLE ' + table_name + '(DROP fld_num)')
        self.alter_ttl_statement = (
            'ALTER TABLE ' + table_name + ' USING TTL 16 HOURS')
        self.drop_idx_statement = (
            'DROP INDEX ' + index_name + ' ON ' + table_name)
        self.drop_tb_statement = ('DROP TABLE IF EXISTS ' + table_name)
        self.table_request = TableRequest()
        self.table_limits = TableLimits(5000, 5000, 50)

    def tearDown(self):
        try:
            TableResult.wait_for_state(self.handle, table_name, State.ACTIVE,
                                       wait_timeout, 1000)
            drop_request = TableRequest().set_statement(self.drop_tb_statement)
            result = self.handle.table_request(drop_request)
            result.wait_for_state(self.handle, table_name, State.DROPPED,
                                  wait_timeout, 1000)
        except TableNotFoundException:
            pass
        finally:
            self.tear_down()

    def testTableRequestSetIllegalStatement(self):
        self.table_request.set_statement('IllegalStatement')
        self.assertRaises(IllegalArgumentException, self.handle.table_request,
                          self.table_request)
        self.table_request.set_statement(
            'ALTER TABLE IllegalTable (DROP fld_num)')
        self.assertRaises(TableNotFoundException, self.handle.table_request,
                          self.table_request)

    def testTableRequestSetIllegalTableLimits(self):
        self.assertRaises(IllegalArgumentException,
                          self.table_request.set_table_limits,
                          'IllegalTableLimits')
        self.assertRaises(IllegalArgumentException,
                          self.table_request.set_table_limits, None)
        self.table_request.set_statement(
            self.create_tb_statement).set_table_limits(TableLimits(5000, 0, 50))
        self.assertRaises(IllegalArgumentException, self.handle.table_request,
                          self.table_request)

    def testTableRequestSetIllegalTableName(self):
        self.assertRaises(IllegalArgumentException,
                          self.table_request.set_table_name,
                          {'name': table_name})
        self.table_request.set_table_name(
            'IllegalTable').set_table_limits(self.table_limits)
        self.assertRaises(TableNotFoundException, self.handle.table_request,
                          self.table_request)

    def testTableRequestSetIllegalTimeout(self):
        self.assertRaises(IllegalArgumentException,
                          self.table_request.set_timeout, 'IllegalTimeout')
        self.assertRaises(IllegalArgumentException,
                          self.table_request.set_timeout, 0)
        self.assertRaises(IllegalArgumentException,
                          self.table_request.set_timeout, -1)

    def testTableRequestSetIllegalDefaults(self):
        self.assertRaises(IllegalArgumentException,
                          self.table_request.set_defaults, 'IllegalDefaults')

    def testTableRequestSetDefaults(self):
        self.table_request.set_defaults(self.handle_config)
        self.assertEqual(self.table_request.get_timeout(),
                         table_request_timeout)

    def testTableRequestNoStatementAndTableName(self):
        self.assertRaises(IllegalArgumentException, self.handle.table_request,
                          self.table_request)

    def testTableRequestBothStatementAndTableName(self):
        self.table_request.set_statement(
            self.create_tb_statement).set_table_name(table_name)
        self.assertRaises(IllegalArgumentException, self.handle.table_request,
                          self.table_request)

    def testTableRequestOnlyTableName(self):
        self.table_request.set_table_name(table_name)
        self.assertRaises(IllegalArgumentException, self.handle.table_request,
                          self.table_request)

    def testTableRequestGets(self):
        self.table_request.set_table_name(table_name).set_statement(
            self.create_tb_statement).set_table_limits(self.table_limits)
        self.assertEqual(self.table_request.get_statement(),
                         self.create_tb_statement)
        self.assertEqual(self.table_request.get_table_limits(),
                         self.table_limits)
        self.assertEqual(self.table_request.get_table_name(), table_name)

    def testTableRequestIllegalRequest(self):
        self.assertRaises(IllegalArgumentException, self.handle.table_request,
                          'IllegalRequest')

    def testTableRequestCreateDropTable(self):
        # create table failed without TableLimits set
        self.table_request.set_statement(self.create_tb_statement)
        self.assertRaises(IllegalArgumentException, self.handle.table_request,
                          self.table_request)
        # create table succeed with TableLimits set
        self.table_request.set_table_limits(self.table_limits)
        result = self.handle.table_request(self.table_request)
        self.assertEqual(result.get_table_name(), table_name)
        self.assertEqual(result.get_state(), State.CREATING)
        self.assertEqual(result.get_table_limits().get_read_units(),
                         self.table_limits.get_read_units())
        self.assertEqual(result.get_table_limits().get_write_units(),
                         self.table_limits.get_write_units())
        self.assertEqual(result.get_table_limits().get_storage_gb(),
                         self.table_limits.get_storage_gb())
        if not_cloudsim():
            self.assertIsNone(result.get_schema())
        wait_result = result.wait_for_state(self.handle, table_name,
                                            State.ACTIVE, wait_timeout, 1000)
        self.assertEqual(wait_result.get_table_name(), table_name)
        self.assertEqual(wait_result.get_state(), State.ACTIVE)
        self.assertEqual(wait_result.get_table_limits().get_read_units(),
                         self.table_limits.get_read_units())
        self.assertEqual(wait_result.get_table_limits().get_write_units(),
                         self.table_limits.get_write_units())
        self.assertEqual(wait_result.get_table_limits().get_storage_gb(),
                         self.table_limits.get_storage_gb())
        if not_cloudsim():
            self.assertIsNotNone(wait_result.get_schema())
        self.assertIsNone(wait_result.get_operation_id())
        # drop table by resetting the statement
        self.table_request.set_statement(self.drop_tb_statement)
        result = self.handle.table_request(self.table_request)
        self.assertEqual(result.get_table_name(), table_name)
        self.assertEqual(result.get_state(), State.DROPPING)
        self.assertEqual(result.get_table_limits().get_read_units(),
                         self.table_limits.get_read_units())
        self.assertEqual(result.get_table_limits().get_write_units(),
                         self.table_limits.get_write_units())
        self.assertEqual(result.get_table_limits().get_storage_gb(),
                         self.table_limits.get_storage_gb())
        if not_cloudsim():
            self.assertIsNotNone(wait_result.get_schema())
        wait_result = result.wait_for_state(self.handle, table_name,
                                            State.DROPPED, wait_timeout, 1000)
        self.assertEqual(wait_result.get_table_name(), table_name)
        self.assertEqual(wait_result.get_state(), State.DROPPED)
        self.assertEqual(wait_result.get_table_limits().get_read_units(),
                         self.table_limits.get_read_units())
        self.assertEqual(wait_result.get_table_limits().get_write_units(),
                         self.table_limits.get_write_units())
        self.assertEqual(wait_result.get_table_limits().get_storage_gb(),
                         self.table_limits.get_storage_gb())
        if not_cloudsim():
            self.assertIsNone(wait_result.get_schema())
        self.assertIsNone(wait_result.get_operation_id())

    def testTableRequestCreateDropIndex(self):
        # create table before creating index
        request = TableRequest().set_statement(
            self.create_tb_statement).set_table_limits(self.table_limits)
        result = self.handle.table_request(request)
        result.wait_for_state(self.handle, table_name, State.ACTIVE,
                              wait_timeout, 1000)
        # create index by resetting the statement
        self.table_request.set_statement(self.create_idx_statement)
        result = self.handle.table_request(self.table_request)
        self.assertEqual(result.get_table_name(), table_name)
        self.assertEqual(result.get_state(), State.UPDATING)
        self.assertEqual(result.get_table_limits().get_read_units(),
                         self.table_limits.get_read_units())
        self.assertEqual(result.get_table_limits().get_write_units(),
                         self.table_limits.get_write_units())
        self.assertEqual(result.get_table_limits().get_storage_gb(),
                         self.table_limits.get_storage_gb())
        if not_cloudsim():
            self.assertIsNotNone(result.get_schema())
        wait_result = result.wait_for_state(self.handle, table_name,
                                            State.ACTIVE, wait_timeout, 1000)
        self.assertEqual(wait_result.get_table_name(), table_name)
        self.assertEqual(wait_result.get_state(), State.ACTIVE)
        self.assertEqual(wait_result.get_table_limits().get_read_units(),
                         self.table_limits.get_read_units())
        self.assertEqual(wait_result.get_table_limits().get_write_units(),
                         self.table_limits.get_write_units())
        self.assertEqual(wait_result.get_table_limits().get_storage_gb(),
                         self.table_limits.get_storage_gb())
        if not_cloudsim():
            self.assertIsNotNone(wait_result.get_schema())
        self.assertIsNone(wait_result.get_operation_id())
        # drop index by resetting the statement
        self.table_request.set_statement(self.drop_idx_statement)
        result = self.handle.table_request(self.table_request)
        self.assertEqual(result.get_table_name(), table_name)
        self.assertEqual(result.get_state(), State.UPDATING)
        self.assertEqual(result.get_table_limits().get_read_units(),
                         self.table_limits.get_read_units())
        self.assertEqual(result.get_table_limits().get_write_units(),
                         self.table_limits.get_write_units())
        self.assertEqual(result.get_table_limits().get_storage_gb(),
                         self.table_limits.get_storage_gb())
        if not_cloudsim():
            self.assertIsNotNone(wait_result.get_schema())
        wait_result = result.wait_for_state(self.handle, table_name,
                                            State.ACTIVE, wait_timeout, 1000)
        self.assertEqual(wait_result.get_table_name(), table_name)
        self.assertEqual(wait_result.get_state(), State.ACTIVE)
        self.assertEqual(wait_result.get_table_limits().get_read_units(),
                         self.table_limits.get_read_units())
        self.assertEqual(wait_result.get_table_limits().get_write_units(),
                         self.table_limits.get_write_units())
        self.assertEqual(wait_result.get_table_limits().get_storage_gb(),
                         self.table_limits.get_storage_gb())
        if not_cloudsim():
            self.assertIsNotNone(wait_result.get_schema())
        self.assertIsNone(wait_result.get_operation_id())
        # drop table after dropping index
        self.table_request.set_statement(self.drop_tb_statement)
        result = self.handle.table_request(self.table_request)
        result.wait_for_state(self.handle, table_name, State.DROPPED,
                              wait_timeout, 1000)

    def testTableRequestAlterTable(self):
        # create table before altering table
        request = TableRequest().set_statement(
            self.create_tb_statement).set_table_limits(self.table_limits)
        result = self.handle.table_request(request)
        result.wait_for_state(self.handle, table_name, State.ACTIVE,
                              wait_timeout, 1000)
        # alter table failed with TableLimits set
        request.set_statement(self.alter_fld_statement)
        self.assertRaises(IllegalArgumentException, self.handle.table_request,
                          request)
        # alter table succeed without TableLimits set
        self.table_request.set_statement(self.alter_fld_statement)
        result = self.handle.table_request(self.table_request)
        self.assertEqual(result.get_table_name(), table_name)
        self.assertEqual(result.get_state(), State.UPDATING)
        self.assertEqual(result.get_table_limits().get_read_units(),
                         self.table_limits.get_read_units())
        self.assertEqual(result.get_table_limits().get_write_units(),
                         self.table_limits.get_write_units())
        self.assertEqual(result.get_table_limits().get_storage_gb(),
                         self.table_limits.get_storage_gb())
        if not_cloudsim():
            self.assertIsNotNone(result.get_schema())
        wait_result = result.wait_for_state(self.handle, table_name,
                                            State.ACTIVE, wait_timeout, 1000)
        self.assertEqual(wait_result.get_table_name(), table_name)
        self.assertEqual(wait_result.get_state(), State.ACTIVE)
        self.assertEqual(wait_result.get_table_limits().get_read_units(),
                         self.table_limits.get_read_units())
        self.assertEqual(wait_result.get_table_limits().get_write_units(),
                         self.table_limits.get_write_units())
        self.assertEqual(wait_result.get_table_limits().get_storage_gb(),
                         self.table_limits.get_storage_gb())
        if not_cloudsim():
            self.assertIsNotNone(wait_result.get_schema())
        self.assertIsNone(wait_result.get_operation_id())
        # drop table after altering table
        request.set_statement(self.drop_tb_statement)
        result = self.handle.table_request(request)
        result.wait_for_state(self.handle, table_name, State.DROPPED,
                              wait_timeout, 1000)

    def testTableRequestAlterTableTTL(self):
        # create table before altering table
        request = TableRequest().set_statement(
            self.create_tb_statement).set_table_limits(self.table_limits)
        result = self.handle.table_request(request)
        result.wait_for_state(self.handle, table_name, State.ACTIVE,
                              wait_timeout, 1000)
        # alter table ttl
        self.table_request.set_statement(self.alter_ttl_statement)
        result = self.handle.table_request(self.table_request)
        self.assertEqual(result.get_table_name(), table_name)
        self.assertEqual(result.get_state(), State.UPDATING)
        self.assertEqual(result.get_table_limits().get_read_units(),
                         self.table_limits.get_read_units())
        self.assertEqual(result.get_table_limits().get_write_units(),
                         self.table_limits.get_write_units())
        self.assertEqual(result.get_table_limits().get_storage_gb(),
                         self.table_limits.get_storage_gb())
        if not_cloudsim():
            self.assertIsNotNone(result.get_schema())
        wait_result = result.wait_for_state(self.handle, table_name,
                                            State.ACTIVE, wait_timeout, 1000)
        self.assertEqual(wait_result.get_table_name(), table_name)
        self.assertEqual(wait_result.get_state(), State.ACTIVE)
        self.assertEqual(wait_result.get_table_limits().get_read_units(),
                         self.table_limits.get_read_units())
        self.assertEqual(wait_result.get_table_limits().get_write_units(),
                         self.table_limits.get_write_units())
        self.assertEqual(wait_result.get_table_limits().get_storage_gb(),
                         self.table_limits.get_storage_gb())
        if not_cloudsim():
            self.assertIsNotNone(wait_result.get_schema())
        self.assertIsNone(wait_result.get_operation_id())
        # drop table after altering table
        request.set_statement(self.drop_tb_statement)
        result = self.handle.table_request(request)
        result.wait_for_state(self.handle, table_name, State.DROPPED,
                              wait_timeout, 1000)

    def testTableRequestModifyTableLimits(self):
        # create table before modifying the table limits
        request = TableRequest().set_statement(
            self.create_tb_statement).set_table_limits(self.table_limits)
        result = self.handle.table_request(request)
        result.wait_for_state(self.handle, table_name, State.ACTIVE,
                              wait_timeout, 1000)
        # modify the table limits
        table_limits = TableLimits(10000, 10000, 100)
        self.table_request.set_table_name(table_name).set_table_limits(
            table_limits)
        result = self.handle.table_request(self.table_request)
        self.assertEqual(result.get_table_name(), table_name)
        self.assertEqual(result.get_state(), State.UPDATING)
        if not_cloudsim():
            self.assertIsNotNone(result.get_schema())
        wait_result = result.wait_for_state(self.handle, table_name,
                                            State.ACTIVE, wait_timeout, 1000)
        self.assertEqual(wait_result.get_table_name(), table_name)
        self.assertEqual(wait_result.get_state(), State.ACTIVE)
        self.assertEqual(wait_result.get_table_limits().get_read_units(),
                         table_limits.get_read_units())
        self.assertEqual(wait_result.get_table_limits().get_write_units(),
                         table_limits.get_write_units())
        self.assertEqual(wait_result.get_table_limits().get_storage_gb(),
                         table_limits.get_storage_gb())
        if not_cloudsim():
            self.assertIsNotNone(wait_result.get_schema())
        self.assertIsNone(wait_result.get_operation_id())
        # drop table after modifying the table limits
        request.set_statement(self.drop_tb_statement)
        result = self.handle.table_request(request)
        result.wait_for_state(self.handle, table_name, State.DROPPED,
                              wait_timeout, 1000)


if __name__ == '__main__':
    unittest.main()
