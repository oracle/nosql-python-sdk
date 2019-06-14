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
    IllegalArgumentException, PrepareRequest, State, TableLimits,
    TableNotFoundException, TableRequest)
from parameters import index_name, table_name, timeout
from testutils import check_cost
from test_base import TestBase


class TestPrepare(unittest.TestCase, TestBase):
    @classmethod
    def setUpClass(cls):
        TestBase.set_up_class()
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
        cls._result = TestBase.table_request(create_request, State.ACTIVE)

        create_idx_request = TableRequest()
        create_idx_statement = (
            'CREATE INDEX ' + index_name + '1 ON ' + table_name + '(fld_str)')
        create_idx_request.set_statement(create_idx_statement)
        cls._result = TestBase.table_request(create_idx_request, State.ACTIVE)
        create_idx_statement = (
            'CREATE INDEX ' + index_name + '2 ON ' + table_name +
            '(fld_map.values())')
        create_idx_request.set_statement(create_idx_statement)
        cls._result = TestBase.table_request(create_idx_request, State.ACTIVE)

    @classmethod
    def tearDownClass(cls):
        TestBase.tear_down_class()

    def setUp(self):
        TestBase.set_up(self)
        self.prepare_statement = ('SELECT fld_id FROM ' + table_name)
        self.prepare_request = PrepareRequest().set_timeout(timeout)

    def tearDown(self):
        TestBase.tear_down(self)

    def testPrepareSetIllegalStatement(self):
        self.prepare_request.set_statement('IllegalStatement')
        self.assertRaises(IllegalArgumentException, self.handle.prepare,
                          self.prepare_request)
        self.prepare_request.set_statement(
            'SELECT fld_id FROM IllegalTableName')
        self.assertRaises(TableNotFoundException, self.handle.prepare,
                          self.prepare_request)

    def testPrepareSetIllegalGetQueryPlan(self):
        self.assertRaises(IllegalArgumentException,
                          self.prepare_request.set_get_query_plan,
                          'IllegalGetQueryPlan')

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
        self.prepare_request.set_statement(
            self.prepare_statement).set_get_query_plan(True)
        self.assertEqual(self.prepare_request.get_statement(),
                         self.prepare_statement)
        self.assertTrue(self.prepare_request.get_query_plan())
        self.assertEqual(self.prepare_request.get_timeout(), timeout)

    def testPrepareIllegalRequest(self):
        self.assertRaises(IllegalArgumentException, self.handle.prepare,
                          'IllegalRequest')

    def testPrepareNormal(self):
        self.prepare_request.set_statement(
            self.prepare_statement).set_get_query_plan(True)
        result = self.handle.prepare(self.prepare_request)
        prepared_statement = result.get_prepared_statement()
        self.assertIsNotNone(prepared_statement)
        check_cost(self, result, 2, 2, 0, 0)
        # test set illegal variable to the prepared statement
        self.assertRaises(IllegalArgumentException,
                          prepared_statement.set_variable, 0, 0)
        # test set variable to the prepared statement
        set_vars = dict()
        set_vars['$fld_id'] = 0
        set_vars['$fld_long'] = 2147483648
        for var in set_vars:
            prepared_statement.set_variable(var, set_vars[var])
        # test get query plan from the prepared statement
        self.assertIsNotNone(prepared_statement.get_query_plan())
        # test get sql text from the prepared statement
        self.assertIsNotNone(prepared_statement.get_sql_text())
        # test get variables from the prepared statement
        get_vars = prepared_statement.get_variables()
        self.assertEqual(set_vars, get_vars)
        # test copy the prepared statement
        statement = prepared_statement.get_statement()
        copied_statement = prepared_statement.copy_statement()
        self.assertEqual(copied_statement.get_statement(), statement)
        self.assertEqual(copied_statement.get_query_plan(),
                         prepared_statement.get_query_plan())
        self.assertEqual(copied_statement.get_sql_text(),
                         prepared_statement.get_sql_text())
        self.assertIsNone(copied_statement.get_variables())
        # test clear variables from the prepared statement
        prepared_statement.clear_variables()
        self.assertEqual(prepared_statement.get_statement(), statement)
        self.assertEqual(prepared_statement.get_variables(), {})

    def testPrepareOrderBy(self):
        # test order by primary index field
        statement = ('SELECT fld_time FROM ' + table_name + ' ORDER BY fld_id')
        self.prepare_request.set_statement(statement)
        result = self.handle.prepare(self.prepare_request)
        prepared_statement = result.get_prepared_statement()
        self.assertIsNotNone(prepared_statement)
        check_cost(self, result, 2, 2, 0, 0)
        # test get query plan from the prepared statement
        self.assertIsNone(prepared_statement.get_query_plan())
        # test get sql text from the prepared statement
        self.assertIsNotNone(prepared_statement.get_sql_text())
        # test get variables from the prepared statement
        self.assertIsNone(prepared_statement.get_variables())

        # test order by secondary index field
        statement = ('SELECT fld_time FROM ' +
                     table_name + ' ORDER BY fld_str')
        self.prepare_request.set_statement(statement).set_get_query_plan(True)
        result = self.handle.prepare(self.prepare_request)
        prepared_statement = result.get_prepared_statement()
        self.assertIsNotNone(prepared_statement)
        check_cost(self, result, 2, 2, 0, 0)
        # test get query plan from the prepared statement
        self.assertIsNotNone(prepared_statement.get_query_plan())
        # test get sql text from the prepared statement
        self.assertIsNotNone(prepared_statement.get_sql_text())
        # test get variables from the prepared statement
        self.assertIsNone(prepared_statement.get_variables())

    def testPrepareFuncMinMaxGroupBy(self):
        # test min function group by primary index field
        statement = ('SELECT min(fld_time) FROM ' + table_name +
                     ' GROUP BY fld_id')
        self.prepare_request.set_statement(statement)
        result = self.handle.prepare(self.prepare_request)
        prepared_statement = result.get_prepared_statement()
        self.assertIsNotNone(prepared_statement)
        check_cost(self, result, 2, 2, 0, 0)
        # test get query plan from the prepared statement
        self.assertIsNone(prepared_statement.get_query_plan())
        # test get sql text from the prepared statement
        self.assertIsNotNone(prepared_statement.get_sql_text())
        # test get variables from the prepared statement
        self.assertIsNone(prepared_statement.get_variables())

        # test max function group by primary index field
        statement = ('SELECT max(fld_time) FROM ' + table_name +
                     ' GROUP BY fld_id')
        self.prepare_request.set_statement(statement).set_get_query_plan(True)
        result = self.handle.prepare(self.prepare_request)
        prepared_statement = result.get_prepared_statement()
        self.assertIsNotNone(prepared_statement)
        check_cost(self, result, 2, 2, 0, 0)
        # test get query plan from the prepared statement
        self.assertIsNotNone(prepared_statement.get_query_plan())
        # test get sql text from the prepared statement
        self.assertIsNotNone(prepared_statement.get_sql_text())
        # test get variables from the prepared statement
        self.assertIsNone(prepared_statement.get_variables())

        # test min function group by secondary index field
        statement = ('SELECT min(fld_time) FROM ' + table_name +
                     ' GROUP BY fld_str')
        self.prepare_request.set_statement(statement).set_get_query_plan(False)
        result = self.handle.prepare(self.prepare_request)
        prepared_statement = result.get_prepared_statement()
        self.assertIsNotNone(prepared_statement)
        check_cost(self, result, 2, 2, 0, 0)
        # test get query plan from the prepared statement
        self.assertIsNone(prepared_statement.get_query_plan())
        # test get sql text from the prepared statement
        self.assertIsNotNone(prepared_statement.get_sql_text())
        # test get variables from the prepared statement
        self.assertIsNone(prepared_statement.get_variables())

        # test max function group by secondary index field
        statement = ('SELECT max(fld_time) FROM ' + table_name +
                     ' GROUP BY fld_str')
        self.prepare_request.set_statement(statement).set_get_query_plan(True)
        result = self.handle.prepare(self.prepare_request)
        prepared_statement = result.get_prepared_statement()
        self.assertIsNotNone(prepared_statement)
        check_cost(self, result, 2, 2, 0, 0)
        # test get query plan from the prepared statement
        self.assertIsNotNone(prepared_statement.get_query_plan())
        # test get sql text from the prepared statement
        self.assertIsNotNone(prepared_statement.get_sql_text())
        # test get variables from the prepared statement
        self.assertIsNone(prepared_statement.get_variables())

    def testPrepareFuncSumGroupBy(self):
        # test sum function group by primary index field
        statement = (
            'SELECT sum(fld_float) FROM ' + table_name + ' GROUP BY fld_id')
        self.prepare_request.set_statement(statement)
        result = self.handle.prepare(self.prepare_request)
        prepared_statement = result.get_prepared_statement()
        self.assertIsNotNone(prepared_statement)
        check_cost(self, result, 2, 2, 0, 0)
        # test get query plan from the prepared statement
        self.assertIsNone(prepared_statement.get_query_plan())
        # test get sql text from the prepared statement
        self.assertIsNotNone(prepared_statement.get_sql_text())
        # test get variables from the prepared statement
        self.assertIsNone(prepared_statement.get_variables())

        # test sum function group by secondary index field
        statement = (
            'SELECT sum(fld_float) FROM ' + table_name + ' GROUP BY fld_str')
        self.prepare_request.set_statement(statement).set_get_query_plan(True)
        result = self.handle.prepare(self.prepare_request)
        prepared_statement = result.get_prepared_statement()
        self.assertIsNotNone(prepared_statement)
        check_cost(self, result, 2, 2, 0, 0)
        # test get query plan from the prepared statement
        self.assertIsNotNone(prepared_statement.get_query_plan())
        # test get sql text from the prepared statement
        self.assertIsNotNone(prepared_statement.get_sql_text())
        # test get variables from the prepared statement
        self.assertIsNone(prepared_statement.get_variables())

    def testPrepareFuncAvgGroupBy(self):
        # test avg function group by primary index field
        statement = (
            'SELECT avg(fld_double) FROM ' + table_name + ' GROUP BY fld_id')
        self.prepare_request.set_statement(statement)
        result = self.handle.prepare(self.prepare_request)
        prepared_statement = result.get_prepared_statement()
        self.assertIsNotNone(prepared_statement)
        check_cost(self, result, 2, 2, 0, 0)
        # test get query plan from the prepared statement
        self.assertIsNone(prepared_statement.get_query_plan())
        # test get sql text from the prepared statement
        self.assertIsNotNone(prepared_statement.get_sql_text())
        # test get variables from the prepared statement
        self.assertIsNone(prepared_statement.get_variables())

        # test avg function group by secondary index field
        statement = (
            'SELECT avg(fld_double) FROM ' + table_name + ' GROUP BY fld_str')
        self.prepare_request.set_statement(statement).set_get_query_plan(True)
        result = self.handle.prepare(self.prepare_request)
        prepared_statement = result.get_prepared_statement()
        self.assertIsNotNone(prepared_statement)
        check_cost(self, result, 2, 2, 0, 0)
        # test get query plan from the prepared statement
        self.assertIsNotNone(prepared_statement.get_query_plan())
        # test get sql text from the prepared statement
        self.assertIsNotNone(prepared_statement.get_sql_text())
        # test get variables from the prepared statement
        self.assertIsNone(prepared_statement.get_variables())

    def testPrepareFuncCountGroupBy(self):
        # test count function group by primary index field
        statement = (
            'SELECT count(*) FROM ' + table_name + ' GROUP BY fld_id')
        self.prepare_request.set_statement(statement)
        result = self.handle.prepare(self.prepare_request)
        prepared_statement = result.get_prepared_statement()
        self.assertIsNotNone(prepared_statement)
        check_cost(self, result, 2, 2, 0, 0)
        # test get query plan from the prepared statement
        self.assertIsNone(prepared_statement.get_query_plan())
        # test get sql text from the prepared statement
        self.assertIsNotNone(prepared_statement.get_sql_text())
        # test get variables from the prepared statement
        self.assertIsNone(prepared_statement.get_variables())

        # test count function group by secondary index field
        statement = (
            'SELECT count(*) FROM ' + table_name + ' GROUP BY fld_str')
        self.prepare_request.set_statement(statement).set_get_query_plan(True)
        result = self.handle.prepare(self.prepare_request)
        prepared_statement = result.get_prepared_statement()
        self.assertIsNotNone(prepared_statement)
        check_cost(self, result, 2, 2, 0, 0)
        # test get query plan from the prepared statement
        self.assertIsNotNone(prepared_statement.get_query_plan())
        # test get sql text from the prepared statement
        self.assertIsNotNone(prepared_statement.get_sql_text())
        # test get variables from the prepared statement
        self.assertIsNone(prepared_statement.get_variables())

    def testPrepareOrderByWithLimit(self):
        statement = ('SELECT fld_str FROM ' + table_name +
                     ' ORDER BY fld_str LIMIT 10')
        self.prepare_request.set_statement(statement).set_get_query_plan(True)
        result = self.handle.prepare(self.prepare_request)
        prepared_statement = result.get_prepared_statement()
        self.assertIsNotNone(prepared_statement)
        check_cost(self, result, 2, 2, 0, 0)
        # test get query plan from the prepared statement
        self.assertIsNotNone(prepared_statement.get_query_plan())
        # test get sql text from the prepared statement
        self.assertIsNotNone(prepared_statement.get_sql_text())
        # test get variables from the prepared statement
        self.assertIsNone(prepared_statement.get_variables())

    def testPrepareOrderByWithOffset(self):
        statement = (
            'DECLARE $offset INTEGER; SELECT fld_str FROM ' + table_name +
            ' ORDER BY fld_str OFFSET $offset')
        self.prepare_request.set_statement(statement)
        result = self.handle.prepare(self.prepare_request)
        prepared_statement = result.get_prepared_statement()
        self.assertIsNotNone(prepared_statement)
        check_cost(self, result, 2, 2, 0, 0)
        # test get query plan from the prepared statement
        self.assertIsNone(prepared_statement.get_query_plan())
        # test get sql text from the prepared statement
        self.assertIsNotNone(prepared_statement.get_sql_text())
        # test get variables from the prepared statement
        self.assertIsNone(prepared_statement.get_variables())

    def testPrepareFuncGeoNear(self):
        statement = (
            'SELECT fld_id, tb.fld_json.point FROM ' + table_name +
            ' tb WHERE geo_near(tb.fld_json.point, ' +
            '{"type": "point", "coordinates": [ 24.0175, 35.5156 ]}, 5000)')
        self.prepare_request.set_statement(statement)
        result = self.handle.prepare(self.prepare_request)
        prepared_statement = result.get_prepared_statement()
        self.assertIsNotNone(prepared_statement)
        check_cost(self, result, 2, 2, 0, 0)
        # test get query plan from the prepared statement
        self.assertIsNone(prepared_statement.get_query_plan())
        # test get sql text from the prepared statement
        self.assertIsNotNone(prepared_statement.get_sql_text())
        # test get variables from the prepared statement
        self.assertIsNone(prepared_statement.get_variables())


if __name__ == '__main__':
    unittest.main()
