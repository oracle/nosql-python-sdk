#
# Copyright (c) 2018, 2022 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#

import unittest

from borneo import (
    IllegalArgumentException, PrepareRequest, TableLimits,
    TableNotFoundException, TableRequest)
from parameters import index_name, table_name, timeout
from test_base import TestBase


class TestPrepare(unittest.TestCase, TestBase):

    @classmethod
    def setUpClass(cls):
        cls.set_up_class()
        create_statement = (
            'CREATE TABLE ' + table_name + '(fld_id INTEGER, fld_long LONG, \
fld_float FLOAT, fld_double DOUBLE, fld_bool BOOLEAN, fld_str STRING, \
fld_bin BINARY, fld_time TIMESTAMP(3), fld_num NUMBER, fld_json JSON, \
fld_arr ARRAY(STRING), fld_map MAP(STRING), \
fld_rec RECORD(fld_id LONG, fld_bool BOOLEAN, fld_str STRING), \
PRIMARY KEY(fld_id)) USING TTL 1 HOURS')
        limits = TableLimits(100, 100, 1)
        create_request = TableRequest().set_statement(
            create_statement).set_table_limits(limits)
        cls.table_request(create_request)

        create_idx_request = TableRequest()
        create_idx_statement = ('CREATE INDEX ' + index_name + '1 ON ' +
                                table_name + '(fld_str)')
        create_idx_request.set_statement(create_idx_statement)
        cls.table_request(create_idx_request)
        create_idx_statement = ('CREATE INDEX ' + index_name + '2 ON ' +
                                table_name + '(fld_map.values())')
        create_idx_request.set_statement(create_idx_statement)
        cls.table_request(create_idx_request)

    @classmethod
    def tearDownClass(cls):
        cls.tear_down_class()

    def setUp(self):
        self.set_up()
        self.prepare_statement = ('SELECT fld_id FROM ' + table_name)
        self.prepare_request = PrepareRequest().set_timeout(timeout)

    def tearDown(self):
        self.tear_down()

    def testPrepareSetIllegalStatement(self):
        self.assertRaises(IllegalArgumentException,
                          self.prepare_request.set_statement, {})
        self.assertRaises(IllegalArgumentException,
                          self.prepare_request.set_statement, '')
        self.prepare_request.set_statement('IllegalStatement')
        self.assertRaises(IllegalArgumentException, self.handle.prepare,
                          self.prepare_request)
        self.prepare_request.set_statement(
            'SELECT fld_id FROM IllegalTableName')
        self.assertRaises(TableNotFoundException, self.handle.prepare,
                          self.prepare_request)

    def testPrepareSetIllegalCompartment(self):
        self.assertRaises(IllegalArgumentException,
                          self.prepare_request.set_compartment, {})
        self.assertRaises(IllegalArgumentException,
                          self.prepare_request.set_compartment, '')

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
        self.assertIsNone(self.prepare_request.get_compartment())
        self.assertTrue(self.prepare_request.get_query_plan())
        self.assertEqual(self.prepare_request.get_timeout(), timeout)

    def testPrepareIllegalRequest(self):
        self.assertRaises(IllegalArgumentException, self.handle.prepare,
                          'IllegalRequest')

    def testPrepareNameVariablesInStatement(self):
        prepare_statement = (
            'DECLARE $fld_long LONG; $fld_str STRING; SELECT fld_id FROM ' +
            table_name + ' WHERE fld_long = $fld_long AND fld_str = $fld_str ' +
            'ORDER BY fld_id')
        self.prepare_request.set_statement(
            prepare_statement).set_get_query_plan(False)
        result = self.handle.prepare(self.prepare_request)
        prepared_statement = result.get_prepared_statement()
        # test set less variables to the prepared statement by name
        set_var = {'$fld_long': 2147483648}
        prepared_statement.set_variable('$fld_long', 2147483648)
        self._check_prepared_result(result, variables=set_var)
        # test set equal variables to the prepared statement by name
        set_vars = {'$fld_long': 2147483648, '$fld_str': 'string'}
        prepared_statement.set_variable('$fld_str', 'string')
        self._check_prepared_result(result, variables=set_vars)
        # test set more variables to the prepared statement by name
        self.assertRaises(IllegalArgumentException,
                          prepared_statement.set_variable,
                          '$fld_float', 3.1414999961853027)

    def testPreparePositionVariablesInStatement(self):
        prepare_statement = (
            'SELECT fld_id FROM ' + table_name + ' WHERE fld_long = ? AND ' +
            'fld_str = ? ORDER BY fld_id')
        self.prepare_request.set_statement(
            prepare_statement).set_get_query_plan(True)
        result = self.handle.prepare(self.prepare_request)
        prepared_statement = result.get_prepared_statement()
        # test set less variables to the prepared statement by position
        set_var = {'$$0': 2147483648}
        prepared_statement.set_variable(1, 2147483648)
        self._check_prepared_result(result, True, variables=set_var)
        # test set equal variables to the prepared statement by position
        set_vars = {'$$0': 2147483648, '$$1': 'string'}
        prepared_statement.set_variable(2, 'string')
        self._check_prepared_result(result, True, variables=set_vars)
        # test set more variables to the prepared statement by position
        self.assertRaises(IllegalArgumentException,
                          prepared_statement.set_variable,
                          3, 3.1414999961853027)

    def testPrepareNoVariablesInStatement(self):
        self.prepare_request.set_statement(
            self.prepare_statement).set_get_query_plan(True)
        result = self.handle.prepare(self.prepare_request)
        prepared_statement = result.get_prepared_statement()
        # test set illegal variable to the prepared statement
        self.assertRaises(IllegalArgumentException,
                          prepared_statement.set_variable, {}, 0)
        self.assertRaises(IllegalArgumentException,
                          prepared_statement.set_variable, 0, 0)
        self.assertRaises(IllegalArgumentException,
                          prepared_statement.set_variable, -1, 0)
        # test set variable to the prepared statement by name
        set_vars = {'$fld_id': 0, '$fld_long': 2147483648}
        for var in set_vars:
            prepared_statement.set_variable(var, set_vars[var])
        self._check_prepared_result(result, True, variables=set_vars)
        prepared_statement.clear_variables()
        # test set variable to the prepared statement by position
        set_vars = {1: 0, 2: 2147483648}
        for var in set_vars:
            prepared_statement.set_variable(var, set_vars[var])
        self._check_prepared_result(
            result, True, variables={'#1': 0, '#2': 2147483648})
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
        statement = ('SELECT fld_time FROM ' + table_name +
                     ' ORDER BY fld_id')
        self.prepare_request.set_statement(statement)
        result = self.handle.prepare(self.prepare_request)
        self._check_prepared_result(result)
        # test order by secondary index field
        statement = ('SELECT fld_time FROM ' + table_name +
                     ' ORDER BY fld_str')
        self.prepare_request.set_statement(statement).set_get_query_plan(
            True)
        result = self.handle.prepare(self.prepare_request)
        self._check_prepared_result(result, True)

    def testPrepareFuncMinMaxGroupBy(self):
        # test min function group by primary index field
        statement = ('SELECT min(fld_time) FROM ' + table_name +
                     ' GROUP BY fld_id')
        self.prepare_request.set_statement(statement)
        result = self.handle.prepare(self.prepare_request)
        self._check_prepared_result(result)
        # test max function group by primary index field
        statement = ('SELECT max(fld_time) FROM ' + table_name +
                     ' GROUP BY fld_id')
        self.prepare_request.set_statement(statement).set_get_query_plan(
            True)
        result = self.handle.prepare(self.prepare_request)
        self._check_prepared_result(result, True)
        # test min function group by secondary index field
        statement = ('SELECT min(fld_time) FROM ' + table_name +
                     ' GROUP BY fld_str')
        self.prepare_request.set_statement(statement).set_get_query_plan(
            False)
        result = self.handle.prepare(self.prepare_request)
        self._check_prepared_result(result)
        # test max function group by secondary index field
        statement = ('SELECT max(fld_time) FROM ' + table_name +
                     ' GROUP BY fld_str')
        self.prepare_request.set_statement(statement).set_get_query_plan(
            True)
        result = self.handle.prepare(self.prepare_request)
        self._check_prepared_result(result, True)

    def testPrepareFuncSumGroupBy(self):
        # test sum function group by primary index field
        statement = ('SELECT sum(fld_float) FROM ' + table_name +
                     ' GROUP BY fld_id')
        self.prepare_request.set_statement(statement)
        result = self.handle.prepare(self.prepare_request)
        self._check_prepared_result(result)
        # test sum function group by secondary index field
        statement = ('SELECT sum(fld_float) FROM ' + table_name +
                     ' GROUP BY fld_str')
        self.prepare_request.set_statement(statement).set_get_query_plan(
            True)
        result = self.handle.prepare(self.prepare_request)
        self._check_prepared_result(result, True)

    def testPrepareFuncAvgGroupBy(self):
        # test avg function group by primary index field
        statement = ('SELECT avg(fld_double) FROM ' + table_name +
                     ' GROUP BY fld_id')
        self.prepare_request.set_statement(statement)
        result = self.handle.prepare(self.prepare_request)
        self._check_prepared_result(result)
        # test avg function group by secondary index field
        statement = ('SELECT avg(fld_double) FROM ' + table_name +
                     ' GROUP BY fld_str')
        self.prepare_request.set_statement(statement).set_get_query_plan(
            True)
        result = self.handle.prepare(self.prepare_request)
        self._check_prepared_result(result, True)

    def testPrepareFuncCountGroupBy(self):
        # test count function group by primary index field
        statement = ('SELECT count(*) FROM ' + table_name +
                     ' GROUP BY fld_id')
        self.prepare_request.set_statement(statement)
        result = self.handle.prepare(self.prepare_request)
        self._check_prepared_result(result)
        # test count function group by secondary index field
        statement = ('SELECT count(*) FROM ' + table_name +
                     ' GROUP BY fld_str')
        self.prepare_request.set_statement(statement).set_get_query_plan(
            True)
        result = self.handle.prepare(self.prepare_request)
        self._check_prepared_result(result, True)

    def testPrepareOrderByWithLimit(self):
        statement = ('SELECT fld_str FROM ' + table_name +
                     ' ORDER BY fld_str LIMIT 10')
        self.prepare_request.set_statement(statement).set_get_query_plan(
            True)
        result = self.handle.prepare(self.prepare_request)
        self._check_prepared_result(result, True)

    def testPrepareOrderByWithOffset(self):
        statement = ('DECLARE $offset INTEGER; SELECT fld_str FROM ' +
                     table_name + ' ORDER BY fld_str OFFSET $offset')
        self.prepare_request.set_statement(statement)
        result = self.handle.prepare(self.prepare_request)
        self._check_prepared_result(result)

    def testPrepareFuncGeoNear(self):
        statement = (
            'SELECT fld_id, tb.fld_json.point FROM ' + table_name +
            ' tb WHERE geo_near(tb.fld_json.point, ' +
            '{"type": "point", "coordinates": [ 24.0175, 35.5156 ]}, 5000)')
        self.prepare_request.set_statement(statement)
        result = self.handle.prepare(self.prepare_request)
        self._check_prepared_result(result)

    def _check_prepared_result(self, result, has_query_plan=False,
                               has_sql_text=True, variables=None):
        prepared_statement = result.get_prepared_statement()
        self.assertIsNotNone(prepared_statement)
        self.check_cost(result, 2, 2, 0, 0)
        # test get query plan from the prepared statement
        query_plan = prepared_statement.get_query_plan()
        (self.assertIsNotNone(query_plan) if has_query_plan
         else self.assertIsNone(query_plan))
        # test get sql text from the prepared statement
        sql_text = prepared_statement.get_sql_text()
        (self.assertIsNotNone(sql_text) if has_sql_text
         else self.assertIsNone(sql_text))
        # test get variables from the prepared statement
        self.assertEqual(prepared_statement.get_variables(), variables)


if __name__ == '__main__':
    unittest.main()
