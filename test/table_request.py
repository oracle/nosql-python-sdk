#
# Copyright (c) 2018, 2022 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#

import unittest
from time import sleep

from borneo import (
    GetTableRequest, IllegalArgumentException, OperationNotSupportedException,
    State, TableLimits, TableNotFoundException, TableRequest)
from parameters import (
    index_name, is_onprem, is_pod, table_name, table_request_timeout, tenant_id,
    wait_timeout)
from test_base import TestBase
from testutils import get_handle_config


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
        self.drop_tb_statement1 = ('DROP TABLE ' + table_name)
        self.table_request = TableRequest()
        self.table_limits = TableLimits(100, 100, 1)
        # this will be ignored by servers that do not support it
        # set this to verify one-time messages about unsupported features
        self.ondemand_limits = TableLimits(0, 0, 1)
        self.ondemand_limits.set_mode(TableLimits.CAPACITY_MODE.ON_DEMAND)
        self.ondemand_return_limits = TableLimits(2147483646, 2147483646, 1)
        self.ondemand_return_limits.set_mode(TableLimits.CAPACITY_MODE.ON_DEMAND)

    def tearDown(self):
        try:
            get_table = GetTableRequest().set_table_name(table_name)
            result = self.handle.get_table(get_table)
            result.wait_for_completion(self.handle, wait_timeout, 1000)
            drop_request = TableRequest().set_statement(self.drop_tb_statement)
            self._do_table_request(drop_request)
        except TableNotFoundException:
            pass
        finally:
            self.tear_down()

    def testTableRequestSetIllegalStatement(self):
        self.assertRaises(IllegalArgumentException,
                          self.table_request.set_statement, {})
        self.assertRaises(IllegalArgumentException,
                          self.table_request.set_statement, '')
        self.table_request.set_statement('IllegalStatement')
        self.assertRaises(IllegalArgumentException, self.handle.table_request,
                          self.table_request)
        self.table_request.set_statement(
            'CREATE INDEX ' + index_name + ' ON IllegalTable(fld_num)')
        self.assertRaises(TableNotFoundException, self.handle.table_request,
                          self.table_request)

    def testTableRequestSetIllegalCompartment(self):
        self.assertRaises(IllegalArgumentException,
                          self.table_request.set_compartment, {})
        self.assertRaises(IllegalArgumentException,
                          self.table_request.set_compartment, '')

    def testTableRequestSetIllegalTableLimits(self):
        self.assertRaises(IllegalArgumentException,
                          self.table_request.set_table_limits,
                          'IllegalTableLimits')
        self.assertRaises(IllegalArgumentException,
                          self.table_request.set_table_limits, None)
        self.table_request.set_statement(
            self.create_tb_statement).set_table_limits(TableLimits(100, 0, 1))
        self.assertRaises(IllegalArgumentException, self.handle.table_request,
                          self.table_request)

    def testTableRequestSetIllegalTableName(self):
        self.assertRaises(IllegalArgumentException,
                          self.table_request.set_table_name,
                          {'name': table_name})
        if not is_onprem():
            self.table_request.set_table_name('IllegalTable').set_table_limits(
                self.table_limits)
            self.assertRaises(TableNotFoundException,
                              self.handle.table_request, self.table_request)

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
        self.assertIsNone(self.table_request.get_compartment())
        self.assertEqual(self.table_request.get_table_limits(),
                         self.table_limits)
        self.assertEqual(self.table_request.get_table_name(), table_name)

    def testTableRequestIllegalRequest(self):
        self.assertRaises(IllegalArgumentException, self.handle.table_request,
                          'IllegalRequest')

    def testTableRequestCreateDropTable(self):
        # create table failed without TableLimits set
        self.table_request.set_statement(self.create_tb_statement)
        if not is_onprem():
            self.assertRaises(IllegalArgumentException,
                              self.handle.table_request, self.table_request)
        # create table succeed with TableLimits set
        self.table_request.set_table_limits(self.table_limits)
        result = self.handle.table_request(self.table_request)
        if is_onprem():
            self.check_table_result(result, State.CREATING, has_schema=False)
        else:
            self.check_table_result(
                result, State.CREATING, self.table_limits, False)
        result.wait_for_completion(self.handle, wait_timeout, 1000)
        self.check_table_result(result, State.ACTIVE, self.table_limits)
        # drop table by resetting the statement
        self.table_request.set_statement(self.drop_tb_statement)
        result = self.handle.table_request(self.table_request)
        self.check_table_result(
            result, State.DROPPING, check_limit=False, check_schema=False)
        result.wait_for_completion(self.handle, wait_timeout, 1000)
        self.check_table_result(result, State.DROPPED, has_schema=False)

        # ensure that this succeeds if run again
        result = self.handle.table_request(self.table_request)
        result.wait_for_completion(self.handle, wait_timeout, 1000)
        self.check_table_result(result, State.DROPPED, has_schema=False, has_operation_id=False)

        # ensure that dropping without "if exists" results in not found
        self.table_request.set_statement(self.drop_tb_statement1)
        try:
            result = self.handle.table_request(self.table_request)
            result.wait_for_completion(self.handle, wait_timeout, 1000)
            self.fail('TNFE should have been raised')
        except TableNotFoundException:
            pass

    def testTableRequestCreateDropIndex(self):
        # create table before creating index
        request = TableRequest().set_statement(
            self.create_tb_statement).set_table_limits(self.table_limits)
        self._do_table_request(request)
        # create index by resetting the statement
        self.table_request.set_statement(self.create_idx_statement)
        result = self.handle.table_request(self.table_request)
        self.check_table_result(
            result, State.UPDATING, check_limit=False, check_schema=False)
        result.wait_for_completion(self.handle, wait_timeout, 1000)
        self.check_table_result(result, State.ACTIVE, self.table_limits)
        # drop index by resetting the statement
        self.table_request.set_statement(self.drop_idx_statement)
        result = self.handle.table_request(self.table_request)
        self.check_table_result(
            result, State.UPDATING, check_limit=False, check_schema=False)
        result.wait_for_completion(self.handle, wait_timeout, 1000)
        self.check_table_result(result, State.ACTIVE, self.table_limits)
        # drop table after dropping index
        self.table_request.set_statement(self.drop_tb_statement)
        self._do_table_request(self.table_request)

    def testTableRequestAlterTable(self):
        # create table before altering table
        request = TableRequest().set_statement(
            self.create_tb_statement).set_table_limits(self.table_limits)
        self._do_table_request(request)
        # alter table failed with TableLimits set
        if not is_onprem():
            request.set_statement(self.alter_fld_statement)
            self.assertRaises(IllegalArgumentException,
                              self.handle.table_request, request)
        # alter table succeed without TableLimits set
        self.table_request.set_statement(self.alter_fld_statement)
        result = self.handle.table_request(self.table_request)
        self.check_table_result(
            result, State.UPDATING, check_limit=False, check_schema=False)
        result.wait_for_completion(self.handle, wait_timeout, 1000)
        self.check_table_result(result, State.ACTIVE, self.table_limits)
        # drop table after altering table
        request.set_statement(self.drop_tb_statement)
        self._do_table_request(request)

    def testTableRequestOnDemand(self):
        ondemand_limits = TableLimits(0, 0, 1)
        ondemand_limits.set_mode(TableLimits.CAPACITY_MODE.ON_DEMAND)
        ondemand_return_limits = TableLimits(2147483646, 2147483646, 1)
        ondemand_return_limits.set_mode(TableLimits.CAPACITY_MODE.ON_DEMAND)
        request = TableRequest().set_statement(
            self.create_tb_statement).set_table_limits(ondemand_limits)
        try:
            result = self.handle.table_request(request)
        except OperationNotSupportedException as e:
            # in V2 we expect an error
            if (self.handle.get_client().serial_version < 3
                    or not self.handle.get_client().get_is_cloud()):
                return
            else:
                raise e
        result.wait_for_completion(self.handle, wait_timeout, 1000)
        self.check_table_result(result, State.ACTIVE, ondemand_return_limits)
        # alter table succeed without TableLimits set
        self.table_request.set_statement(self.alter_fld_statement)
        result = self.handle.table_request(self.table_request)
        self.check_table_result(
            result, State.UPDATING, check_limit=False, check_schema=False)
        result.wait_for_completion(self.handle, wait_timeout, 1000)
        self.check_table_result(result, State.ACTIVE, ondemand_return_limits)
        # drop table after altering table
        request.set_statement(self.drop_tb_statement)
        self._do_table_request(request)

    def testTableRequestAlterTableTTL(self):
        # create table before altering table
        request = TableRequest().set_statement(
            self.create_tb_statement).set_table_limits(self.table_limits)
        self._do_table_request(request)
        # alter table ttl
        self.table_request.set_statement(self.alter_ttl_statement)
        result = self.handle.table_request(self.table_request)
        self.check_table_result(
            result, State.UPDATING, check_limit=False, check_schema=False)
        result.wait_for_completion(self.handle, wait_timeout, 1000)
        self.check_table_result(result, State.ACTIVE, self.table_limits)
        # drop table after altering table
        request.set_statement(self.drop_tb_statement)
        self._do_table_request(request)

    def testTableRequestModifyTableLimits(self):
        # create table before modifying the table limits
        request = TableRequest().set_statement(
            self.create_tb_statement).set_table_limits(self.table_limits)
        self._do_table_request(request)
        # modify the table limits
        table_limits = TableLimits(50, 50, 1)
        self.table_request.set_table_name(table_name).set_table_limits(
            table_limits)
        if is_onprem():
            self.assertRaises(OperationNotSupportedException,
                              self.handle.table_request, self.table_request)
            return
        result = self.handle.table_request(self.table_request)
        self.check_table_result(
            result, State.UPDATING, check_limit=False, check_schema=False)
        result.wait_for_completion(self.handle, wait_timeout, 1000)
        self.check_table_result(result, State.ACTIVE, table_limits)
        # drop table after modifying the table limits
        request.set_statement(self.drop_tb_statement)
        self._do_table_request(request)

    def _do_table_request(self, request):
        #
        # Optionally delay to handle the 4 DDL ops/minute limit
        # in the real service
        #
        if is_pod():
            sleep(30)
        result = self.handle.table_request(request)
        result.wait_for_completion(self.handle, wait_timeout, 1000)


if __name__ == '__main__':
    unittest.main()
