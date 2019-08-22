#
# Copyright (C) 2018, 2019 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
#
# Please see LICENSE.txt file included in the top-level directory of the
# appropriate download for a copy of the license and additional information.
#

import unittest
from time import sleep

from borneo import (
    IllegalArgumentException, OperationNotSupportedException, State,
    TableLimits, TableNotFoundException, TableRequest, TableResult)
from parameters import (
    is_minicloud, is_onprem, is_pod, not_cloudsim, table_name,
    table_request_timeout, tenant_id, wait_timeout)
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
            TableResult.wait_for_state(self.handle, State.ACTIVE, wait_timeout,
                                       1000, table_name)
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
        if not is_onprem():
            self.table_request.set_table_name('IllegalTable').set_table_limits(
                self.table_limits)
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
        if not is_onprem():
            self.assertRaises(IllegalArgumentException,
                              self.handle.table_request, self.table_request)
        # create table succeed with TableLimits set
        self.table_request.set_table_limits(self.table_limits)
        result = self.handle.table_request(self.table_request)
        self.check_table_result(
            result, State.CREATING, self.table_limits, False)
        self._wait_for_completion(result)
        self.check_table_result(result, State.ACTIVE, self.table_limits)
        # drop table by resetting the statement
        self.table_request.set_statement(self.drop_tb_statement)
        result = self.handle.table_request(self.table_request)
        # TODO: A difference between old cloud proxy and new cloud proxy, during
        # DROPPING phase, the table limit is not none for old proxy but none for
        # new proxy.
        self._check_table_result(result, State.DROPPING, check_limit=False)
        self._wait_for_completion(result)
        # TODO: A difference between old cloud proxy and new cloud proxy, after
        # table DROPPED, the table limit is not none for old proxy but none for
        # new proxy.
        self._check_table_result(result, State.DROPPED, has_schema=False,
                                 check_limit=False)

    def testTableRequestCreateDropIndex(self):
        # create table before creating index
        request = TableRequest().set_statement(
            self.create_tb_statement).set_table_limits(self.table_limits)
        self._do_table_request(request)
        # create index by resetting the statement
        self.table_request.set_statement(self.create_idx_statement)
        result = self.handle.table_request(self.table_request)
        self._check_table_result(result, State.UPDATING, self.table_limits)
        self._wait_for_completion(result)
        self._check_table_result(result, State.ACTIVE, self.table_limits)
        # drop index by resetting the statement
        self.table_request.set_statement(self.drop_idx_statement)
        result = self.handle.table_request(self.table_request)
        self._check_table_result(result, State.UPDATING, self.table_limits)
        self._wait_for_completion(result)
        self._check_table_result(result, State.ACTIVE, self.table_limits)
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
        self.check_table_result(result, State.UPDATING, self.table_limits)
        self._wait_for_completion(result)
        self.check_table_result(result, State.ACTIVE, self.table_limits)
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
        self.check_table_result(result, State.UPDATING, self.table_limits)
        self._wait_for_completion(result)
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
        table_limits = TableLimits(10000, 10000, 100)
        self.table_request.set_table_name(table_name).set_table_limits(
            table_limits)
        if is_onprem():
            self.assertRaises(OperationNotSupportedException,
                              self.handle.table_request, self.table_request)
            return
        result = self.handle.table_request(self.table_request)
        self.assertEqual(result.get_table_name(), table_name)
        self.assertEqual(result.get_state(), State.UPDATING)
        if not_cloudsim():
            self.assertIsNotNone(result.get_schema())
        self.assertIsNotNone(result.get_operation_id())
        self._wait_for_completion(result)
        self.check_table_result(result, State.ACTIVE, table_limits)
        # drop table after modifying the table limits
        request.set_statement(self.drop_tb_statement)
        self._do_table_request(request)

    def _check_table_result(self, result, state, table_limits=None,
                            has_schema=True, check_limit=True):
        # TODO: For minicloud, the SC module doesn't return operation id for
        # now. This affects drop table as well as create/drop index. When the SC
        # is changed to return the operation id, the test need to be changed.
        if is_minicloud():
            self.check_table_result(result, state, table_limits, has_schema,
                                    False, check_limit)
        else:
            self.check_table_result(result, state, table_limits, has_schema,
                                    True, check_limit)

    def _do_table_request(self, request):
        #
        # Optionally delay to handle the 4 DDL ops/minute limit
        # in the real service
        #
        if is_pod():
            sleep(20)
        result = self.handle.table_request(request)
        self._wait_for_completion(result)

    def _wait_for_completion(self, result):
        # TODO: For minicloud, the SC module doesn't return operation id for
        # now. In TableResult.wait_for_completion, it check if the operation id
        # is none, if none, raise IllegalArgumentException, at the moment we
        # should ignore this exception in minicloud testing. This affects drop
        # table as well as create/drop index. When the SC is changed to return
        # the operation id, the test need to be changed.
        if is_minicloud():
            result.wait_for_state(self.handle, [State.ACTIVE, State.DROPPED],
                                  wait_timeout, 1000, result=result)
        else:
            result.wait_for_completion(self.handle, wait_timeout, 1000)


if __name__ == '__main__':
    unittest.main()
