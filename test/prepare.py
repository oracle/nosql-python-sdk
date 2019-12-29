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
    IllegalArgumentException, PrepareRequest, TableLimits,
    TableNotFoundException, TableRequest)
from parameters import index_name, table_name, tenant_id, timeout
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
        create_request = TableRequest().set_compartment_id(
            tenant_id).set_statement(create_statement).set_table_limits(limits)
        cls.table_request(create_request)

        create_idx_request = TableRequest().set_compartment_id(tenant_id)
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
        self.prepare_request = PrepareRequest().set_timeout(
            timeout).set_compartment_id(tenant_id)

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

    def testPrepareSetIllegalCompartmentId(self):
        self.assertRaises(IllegalArgumentException,
                          self.prepare_request.set_compartment_id, {})
        self.assertRaises(IllegalArgumentException,
                          self.prepare_request.set_compartment_id, '')

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
        self.assertEqual(self.prepare_request.get_compartment_id(), tenant_id)
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
        # test set illegal variable to the prepared statement
        self.assertRaises(IllegalArgumentException,
                          prepared_statement.set_variable, 0, 0)
        # test set variable to the prepared statement
        set_vars = dict()
        set_vars['$fld_id'] = 0
        set_vars['$fld_long'] = 2147483648
        for var in set_vars:
            prepared_statement.set_variable(var, set_vars[var])
        self._check_prepared_result(result, True, variables=set_vars)
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
