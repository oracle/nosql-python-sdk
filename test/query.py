#
# Copyright (C) 2018, 2019 Oracle and/or its affiliates. All rights reserved.
#
# This software is licensed with the Universal Permissive License (UPL) version 1.0
#
# Please see LICENSE.txt file included in the top-level directory of the
# appropriate download for a copy of the license and additional information.
#

import unittest
from datetime import datetime
from decimal import Decimal
from struct import pack
from time import time

from borneo import (
    Consistency, GetRequest, IllegalArgumentException, PrepareRequest,
    PutRequest, QueryRequest, State, TableLimits, TableNotFoundException,
    TableRequest, TimeToLive, WriteMultipleRequest)
from parameters import table_name, tenant_id, timeout
from testutils import get_handle_config
from test_base import TestBase


class TestQuery(unittest.TestCase, TestBase):
    @classmethod
    def setUpClass(cls):
        TestBase.set_up_class()
        index_name = 'idx_' + table_name
        create_statement = (
            'CREATE TABLE ' + table_name + '(fld_sid INTEGER, fld_id INTEGER, \
fld_long LONG, fld_float FLOAT, fld_double DOUBLE, fld_bool BOOLEAN, \
fld_str STRING, fld_bin BINARY, fld_time TIMESTAMP(6), fld_num NUMBER, \
fld_json JSON, fld_arr ARRAY(STRING), fld_map MAP(STRING), \
fld_rec RECORD(fld_id LONG, fld_bool BOOLEAN, fld_str STRING), \
PRIMARY KEY(SHARD(fld_sid), fld_id))')
        limits = TableLimits(5000, 5000, 50)
        create_request = TableRequest().set_statement(
            create_statement).set_table_limits(limits)
        cls._result = TestBase.table_request(create_request, State.ACTIVE)
        create_index_statement = (
            'CREATE INDEX ' + index_name + ' ON ' + table_name + '(fld_long)')
        create_index_request = TableRequest().set_statement(
            create_index_statement)
        cls._result = TestBase.table_request(
            create_index_request, State.ACTIVE)
        global prepare_cost
        prepare_cost = 2
        global query_statement
        query_statement = ('SELECT fld_sid, fld_id FROM ' + table_name +
                           ' WHERE fld_sid = 1')

    @classmethod
    def tearDownClass(cls):
        TestBase.tear_down_class()

    def setUp(self):
        TestBase.set_up(self)
        self.handle_config = get_handle_config(tenant_id)
        shardkeys = [0, 1]
        ids = [0, 1, 2, 3, 4, 5]
        write_multiple_request = WriteMultipleRequest()
        for sk in shardkeys:
            for i in ids:
                row = {'fld_sid': sk, 'fld_id': i, 'fld_long': 2147483648,
                       'fld_float': 3.1414999961853027, 'fld_double': 3.1415,
                       'fld_bool': True,
                       'fld_str': '{"name": u1, "phone": null}',
                       'fld_bin': bytearray(pack('>i', 4)),
                       'fld_time': datetime.now(), 'fld_num': Decimal(5),
                       'fld_json': {'a': '1', 'b': None, 'c': '3'},
                       'fld_arr': ['a', 'b', 'c'],
                       'fld_map': {'a': '1', 'b': '2', 'c': '3'},
                       'fld_rec': {'fld_id': 1, 'fld_bool': False,
                                   'fld_str': None}}
                write_multiple_request.add(
                    PutRequest().set_value(row).set_table_name(table_name),
                    True)
            self.handle.write_multiple(write_multiple_request)
            write_multiple_request.clear()
        prepare_statement_update = (
            'DECLARE $fld_sid INTEGER; $fld_id INTEGER; UPDATE ' + table_name +
            ' u SET u.fld_long = u.fld_long + 1 WHERE fld_sid = $fld_sid ' +
            'AND fld_id = $fld_id')
        prepare_request_update = PrepareRequest().set_statement(
            prepare_statement_update)
        self.prepare_result_update = self.handle.prepare(
            prepare_request_update)
        prepare_statement_select = (
            'DECLARE $fld_long LONG; SELECT fld_sid, fld_id, fld_long FROM ' +
            table_name + ' WHERE fld_long = $fld_long')
        prepare_request_select = PrepareRequest().set_statement(
            prepare_statement_select)
        self.prepare_result_select = self.handle.prepare(
            prepare_request_select)
        self.query_request = QueryRequest().set_timeout(timeout)
        self.get_request = GetRequest().set_table_name(table_name)

    def tearDown(self):
        TestBase.tear_down(self)

    def testQuerySetIllegalLimit(self):
        self.assertRaises(IllegalArgumentException,
                          self.query_request.set_limit, 'IllegalLimit')
        self.assertRaises(IllegalArgumentException,
                          self.query_request.set_limit, -1)

    def testQuerySetIllegalMaxReadKb(self):
        self.assertRaises(IllegalArgumentException,
                          self.query_request.set_max_read_kb, 'IllegalLimit')
        self.assertRaises(IllegalArgumentException,
                          self.query_request.set_max_read_kb, -1)
        self.assertRaises(IllegalArgumentException,
                          self.query_request.set_max_read_kb, 2049)

    def testQuerySetIllegalConsistency(self):
        self.assertRaises(IllegalArgumentException,
                          self.query_request.set_consistency,
                          'IllegalConsistency')

    def testQuerySetIllegalContinuationKey(self):
        self.assertRaises(IllegalArgumentException,
                          self.query_request.set_continuation_key,
                          'IllegalContinuationKey')

    def testQuerySetIllegalStatement(self):
        self.query_request.set_statement('IllegalStatement')
        self.assertRaises(IllegalArgumentException, self.handle.query,
                          self.query_request)
        self.query_request.set_statement('SELECT fld_id FROM IllegalTableName')
        self.assertRaises(TableNotFoundException, self.handle.query,
                          self.query_request)

    def testQuerySetIllegalPreparedStatement(self):
        self.assertRaises(IllegalArgumentException,
                          self.query_request.set_prepared_statement,
                          'IllegalPreparedStatement')

    def testQuerySetIllegalTimeout(self):
        self.assertRaises(IllegalArgumentException,
                          self.query_request.set_timeout, 'IllegalTimeout')
        self.assertRaises(IllegalArgumentException,
                          self.query_request.set_timeout, 0)
        self.assertRaises(IllegalArgumentException,
                          self.query_request.set_timeout, -1)

    def testQuerySetIllegalDefaults(self):
        self.assertRaises(IllegalArgumentException,
                          self.query_request.set_defaults, 'IllegalDefaults')

    def testQuerySetDefaults(self):
        self.query_request.set_defaults(self.handle_config)
        self.assertEqual(self.query_request.get_timeout(), timeout)
        self.assertEqual(self.query_request.get_consistency(),
                         Consistency.ABSOLUTE)

    def testQueryNoStatementAndBothStatement(self):
        self.assertRaises(IllegalArgumentException, self.handle.query,
                          self.query_request)
        self.query_request.set_statement(query_statement)
        self.query_request.set_prepared_statement(self.prepare_result_select)
        self.assertRaises(IllegalArgumentException, self.handle.query,
                          self.query_request)

    def testQueryGets(self):
        continuation_key = bytearray(5)
        self.query_request.set_consistency(Consistency.EVENTUAL).set_statement(
            query_statement).set_prepared_statement(
            self.prepare_result_select).set_limit(3).set_max_read_kb(
            2).set_continuation_key(continuation_key)
        self.assertEqual(self.query_request.get_limit(), 3)
        self.assertEqual(self.query_request.get_max_read_kb(), 2)
        self.assertEqual(self.query_request.get_consistency(),
                         Consistency.EVENTUAL)
        self.assertEqual(self.query_request.get_continuation_key(),
                         continuation_key)
        self.assertEqual(self.query_request.get_statement(), query_statement)
        self.assertEqual(self.query_request.get_prepared_statement(),
                         self.prepare_result_select.get_prepared_statement())
        self.assertEqual(self.query_request.get_timeout(), timeout)

    def testQueryIllegalRequest(self):
        self.assertRaises(IllegalArgumentException, self.handle.query,
                          'IllegalRequest')

    def testQueryStatementSelect(self):
        num_records = 6
        self.query_request.set_statement(query_statement)
        result = self.handle.query(self.query_request)
        records = result.get_results()
        self.assertEqual(len(records), num_records)
        for idx in range(num_records):
            self.assertEqual(records[idx], {'fld_sid': 1, 'fld_id': idx})
        self.assertIsNone(result.get_continuation_key())
        self.assertEqual(result.get_read_kb(), num_records + prepare_cost)
        self.assertEqual(result.get_read_units(),
                         num_records * 2 + prepare_cost)
        self.assertEqual(result.get_write_kb(), 0)
        self.assertEqual(result.get_write_units(), 0)

    def testQueryStatementSelectWithLimit(self):
        limit = 3
        self.query_request.set_statement(query_statement).set_limit(limit)
        result = self.handle.query(self.query_request)
        records = result.get_results()
        self.assertEqual(len(records), limit)
        for idx in range(limit):
            self.assertEqual(records[idx], {'fld_sid': 1, 'fld_id': idx})
        self.assertIsNotNone(result.get_continuation_key())
        self.assertEqual(result.get_read_kb(), limit + prepare_cost)
        self.assertEqual(result.get_read_units(), limit * 2 + prepare_cost)
        self.assertEqual(result.get_write_kb(), 0)
        self.assertEqual(result.get_write_units(), 0)

    def testQueryStatementSelectWithMaxReadKb(self):
        max_read_kb = 4
        self.query_request.set_statement(query_statement).set_max_read_kb(
            max_read_kb)
        result = self.handle.query(self.query_request)
        records = result.get_results()
        self.assertEqual(len(records), max_read_kb + 1)
        for idx in range(len(records)):
            self.assertEqual(records[idx], {'fld_sid': 1, 'fld_id': idx})
        self.assertIsNotNone(result.get_continuation_key())
        self.assertEqual(result.get_read_kb(), max_read_kb + prepare_cost + 1)
        self.assertEqual(result.get_read_units(),
                         max_read_kb * 2 + prepare_cost + 2)
        self.assertEqual(result.get_write_kb(), 0)
        self.assertEqual(result.get_write_units(), 0)

    def testQueryStatementSelectWithConsistency(self):
        num_records = 6
        self.query_request.set_statement(query_statement).set_consistency(
            Consistency.ABSOLUTE)
        result = self.handle.query(self.query_request)
        records = result.get_results()
        self.assertEqual(len(records), num_records)
        for idx in range(num_records):
            self.assertEqual(records[idx], {'fld_sid': 1, 'fld_id': idx})
        self.assertIsNone(result.get_continuation_key())
        self.assertEqual(result.get_read_kb(), num_records + prepare_cost)
        self.assertEqual(result.get_read_units(),
                         num_records * 2 + prepare_cost)
        self.assertEqual(result.get_write_kb(), 0)
        self.assertEqual(result.get_write_units(), 0)

    def testQueryStatementSelectWithContinuationKey(self):
        num_records = 6
        limit = 4
        self.query_request.set_statement(query_statement).set_limit(limit)
        count = 0
        while True:
            completed = count * limit
            result = self.handle.query(self.query_request)
            records = result.get_results()
            if completed + limit <= num_records:
                num_get = limit
                read_kb = num_get
                self.assertIsNotNone(result.get_continuation_key())
            else:
                num_get = num_records - completed
                read_kb = (1 if num_get == 0 else num_get)
                self.assertIsNone(result.get_continuation_key())
            self.assertEqual(len(records), num_get)
            for idx in range(num_get):
                self.assertEqual(records[idx],
                                 {'fld_sid': 1, 'fld_id': completed + idx})
            self.assertEqual(result.get_read_kb(), read_kb + prepare_cost)
            self.assertEqual(result.get_read_units(),
                             read_kb * 2 + prepare_cost)
            self.assertEqual(result.get_write_kb(), 0)
            self.assertEqual(result.get_write_units(), 0)
            count += 1
            if result.get_continuation_key() is None:
                break
            self.query_request.set_continuation_key(
                result.get_continuation_key())
        self.assertEqual(count, num_records // limit + 1)

    def testQueryStatementSelectWithDefault(self):
        num_records = 6
        self.query_request.set_statement(
            query_statement).set_defaults(self.handle_config)
        result = self.handle.query(self.query_request)
        records = result.get_results()
        self.assertEqual(len(records), num_records)
        for idx in range(num_records):
            self.assertEqual(records[idx], {'fld_sid': 1, 'fld_id': idx})
        self.assertIsNone(result.get_continuation_key())
        self.assertEqual(result.get_read_kb(), num_records + prepare_cost)
        self.assertEqual(result.get_read_units(),
                         num_records * 2 + prepare_cost)
        self.assertEqual(result.get_write_kb(), 0)
        self.assertEqual(result.get_write_units(), 0)

    def testQueryPreparedStatementUpdate(self):
        fld_sid = 0
        fld_id = 2
        fld_long = 2147483649
        prepared_statement = self.prepare_result_update.get_prepared_statement()
        # update a non-existing row
        prepared_statement.set_variable('$fld_sid', 2).set_variable(
            '$fld_id', 0)
        self.query_request.set_prepared_statement(self.prepare_result_update)
        result = self.handle.query(self.query_request)
        records = result.get_results()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0], {'NumRowsUpdated': 0})
        self.assertIsNone(result.get_continuation_key())
        self.assertEqual(result.get_read_kb(), 1)
        self.assertEqual(result.get_read_units(), 2)
        self.assertEqual(result.get_write_kb(), 0)
        self.assertEqual(result.get_write_units(), 0)
        # update an existing row
        prepared_statement.set_variable('$fld_sid', fld_sid).set_variable(
            '$fld_id', fld_id)
        self.query_request.set_prepared_statement(self.prepare_result_update)
        result = self.handle.query(self.query_request)
        records = result.get_results()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0], {'NumRowsUpdated': 1})
        self.assertIsNone(result.get_continuation_key())
        self.assertEqual(result.get_read_kb(), 2)
        self.assertEqual(result.get_read_units(), 4)
        self.assertEqual(result.get_write_kb(), 4)
        self.assertEqual(result.get_write_units(), 4)
        # check the updated row
        prepared_statement = self.prepare_result_select.get_prepared_statement()
        prepared_statement.set_variable('$fld_long', fld_long)
        self.query_request.set_prepared_statement(prepared_statement)
        result = self.handle.query(self.query_request)
        records = result.get_results()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0], {'fld_sid': fld_sid, 'fld_id': fld_id,
                                      'fld_long': fld_long})
        self.assertIsNone(result.get_continuation_key())
        self.assertEqual(result.get_read_kb(), 1)
        self.assertEqual(result.get_read_units(), 2)
        self.assertEqual(result.get_write_kb(), 0)
        self.assertEqual(result.get_write_units(), 0)

    def testQueryPreparedStatementUpdateWithLimit(self):
        fld_sid = 1
        fld_id = 5
        fld_long = 2147483649
        prepared_statement = self.prepare_result_update.get_prepared_statement()
        prepared_statement.set_variable('$fld_sid', fld_sid).set_variable(
            '$fld_id', fld_id)
        self.query_request.set_prepared_statement(
            self.prepare_result_update).set_limit(1)
        result = self.handle.query(self.query_request)
        records = result.get_results()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0], {'NumRowsUpdated': 1})
        self.assertIsNone(result.get_continuation_key())
        self.assertEqual(result.get_read_kb(), 2)
        self.assertEqual(result.get_read_units(), 4)
        self.assertEqual(result.get_write_kb(), 4)
        self.assertEqual(result.get_write_units(), 4)
        # check the updated row
        prepared_statement = self.prepare_result_select.get_prepared_statement()
        prepared_statement.set_variable('$fld_long', fld_long)
        self.query_request.set_prepared_statement(prepared_statement)
        result = self.handle.query(self.query_request)
        records = result.get_results()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0], {'fld_sid': fld_sid, 'fld_id': fld_id,
                                      'fld_long': fld_long})
        self.assertIsNotNone(result.get_continuation_key())
        self.assertEqual(result.get_read_kb(), 1)
        self.assertEqual(result.get_read_units(), 2)
        self.assertEqual(result.get_write_kb(), 0)
        self.assertEqual(result.get_write_units(), 0)

    def testQueryPreparedStatementUpdateWithMaxReadKb(self):
        fld_sid = 0
        fld_id = 1
        fld_long = 2147483649
        # set a small max_read_kb to read a row to update
        prepared_statement = self.prepare_result_update.get_prepared_statement()
        prepared_statement.set_variable('$fld_sid', fld_sid).set_variable(
            '$fld_id', fld_id)
        self.query_request.set_prepared_statement(
            self.prepare_result_update).set_max_read_kb(1)
        self.assertRaises(IllegalArgumentException, self.handle.query,
                          self.query_request)
        # set a enough max_read_kb to read a row to update
        self.query_request.set_max_read_kb(2)
        result = self.handle.query(self.query_request)
        records = result.get_results()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0], {'NumRowsUpdated': 1})
        self.assertIsNone(result.get_continuation_key())
        self.assertEqual(result.get_read_kb(), 2)
        self.assertEqual(result.get_read_units(), 4)
        self.assertEqual(result.get_write_kb(), 4)
        self.assertEqual(result.get_write_units(), 4)
        # check the updated row
        prepared_statement = self.prepare_result_select.get_prepared_statement()
        prepared_statement.set_variable('$fld_long', fld_long)
        self.query_request.set_prepared_statement(prepared_statement)
        result = self.handle.query(self.query_request)
        records = result.get_results()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0], {'fld_sid': fld_sid, 'fld_id': fld_id,
                                      'fld_long': fld_long})
        self.assertIsNone(result.get_continuation_key())
        self.assertEqual(result.get_read_kb(), 1)
        self.assertEqual(result.get_read_units(), 2)
        self.assertEqual(result.get_write_kb(), 0)
        self.assertEqual(result.get_write_units(), 0)

    def testQueryPreparedStatementUpdateWithConsistency(self):
        fld_sid = 1
        fld_id = 2
        fld_long = 2147483649
        prepared_statement = self.prepare_result_update.get_prepared_statement()
        prepared_statement.set_variable('$fld_sid', fld_sid).set_variable(
            '$fld_id', fld_id)
        self.query_request.set_prepared_statement(
            self.prepare_result_update).set_consistency(Consistency.ABSOLUTE)
        result = self.handle.query(self.query_request)
        records = result.get_results()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0], {'NumRowsUpdated': 1})
        self.assertIsNone(result.get_continuation_key())
        self.assertEqual(result.get_read_kb(), 2)
        self.assertEqual(result.get_read_units(), 4)
        self.assertEqual(result.get_write_kb(), 4)
        self.assertEqual(result.get_write_units(), 4)
        # check the updated row
        prepared_statement = self.prepare_result_select.get_prepared_statement()
        prepared_statement.set_variable('$fld_long', fld_long)
        self.query_request.set_prepared_statement(prepared_statement)
        result = self.handle.query(self.query_request)
        records = result.get_results()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0], {'fld_sid': fld_sid, 'fld_id': fld_id,
                                      'fld_long': fld_long})
        self.assertIsNone(result.get_continuation_key())
        self.assertEqual(result.get_read_kb(), 1)
        self.assertEqual(result.get_read_units(), 2)
        self.assertEqual(result.get_write_kb(), 0)
        self.assertEqual(result.get_write_units(), 0)

    def testQueryPreparedStatementUpdateWithContinuationKey(self):
        fld_sid = 1
        fld_id = 3
        fld_long = 2147483649
        num_records = 1
        limit = 3
        prepared_statement = self.prepare_result_update.get_prepared_statement()
        prepared_statement.set_variable('$fld_sid', fld_sid).set_variable(
            '$fld_id', fld_id)
        self.query_request.set_prepared_statement(
            self.prepare_result_update).set_limit(limit)
        count = 0
        while True:
            completed = count * limit
            result = self.handle.query(self.query_request)
            records = result.get_results()
            self.assertEqual(len(records), 1)
            if completed + limit <= num_records:
                self.assertEqual(records[0], {'NumRowsUpdated': limit})
                read_kb = limit * 2
                write_kb = limit * 4

            else:
                num_update = num_records - completed
                self.assertEqual(records[0], {'NumRowsUpdated': num_update})
                read_kb = (1 if num_update == 0 else num_update * 2)
                write_kb = (0 if num_update == 0 else num_update * 4)
            self.assertIsNone(result.get_continuation_key())
            self.assertEqual(result.get_read_kb(), read_kb)
            self.assertEqual(result.get_read_units(), read_kb * 2)
            self.assertEqual(result.get_write_kb(), write_kb)
            self.assertEqual(result.get_write_units(), write_kb)
            count += 1
            if result.get_continuation_key() is None:
                break
            self.query_request.set_continuation_key(
                result.get_continuation_key())
        self.assertEqual(count, 1)
        # check the updated row
        prepared_statement = self.prepare_result_select.get_prepared_statement()
        prepared_statement.set_variable('$fld_long', fld_long)
        self.query_request.set_prepared_statement(prepared_statement)
        result = self.handle.query(self.query_request)
        records = result.get_results()
        self.assertEqual(len(records), num_records)
        self.assertEqual(records[0], {'fld_sid': fld_sid, 'fld_id': fld_id,
                                      'fld_long': fld_long})
        if limit <= num_records:
            self.assertIsNotNone(result.get_continuation_key())
        else:
            self.assertIsNone(result.get_continuation_key())
        self.assertEqual(result.get_read_kb(), 1)
        self.assertEqual(result.get_read_units(), 2)
        self.assertEqual(result.get_write_kb(), 0)
        self.assertEqual(result.get_write_units(), 0)

    def testQueryPreparedStatementUpdateWithDefault(self):
        fld_sid = 0
        fld_id = 5
        fld_long = 2147483649
        prepared_statement = self.prepare_result_update.get_prepared_statement()
        prepared_statement.set_variable('$fld_sid', fld_sid).set_variable(
            '$fld_id', fld_id)
        self.query_request.set_prepared_statement(
            self.prepare_result_update).set_defaults(self.handle_config)
        result = self.handle.query(self.query_request)
        records = result.get_results()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0], {'NumRowsUpdated': 1})
        self.assertIsNone(result.get_continuation_key())
        self.assertEqual(result.get_read_kb(), 2)
        self.assertEqual(result.get_read_units(), 4)
        self.assertEqual(result.get_write_kb(), 4)
        self.assertEqual(result.get_write_units(), 4)
        # check the updated row
        prepared_statement = self.prepare_result_select.get_prepared_statement()
        prepared_statement.set_variable('$fld_long', fld_long)
        self.query_request.set_prepared_statement(prepared_statement)
        result = self.handle.query(self.query_request)
        records = result.get_results()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0], {'fld_sid': fld_sid, 'fld_id': fld_id,
                                      'fld_long': fld_long})
        self.assertIsNone(result.get_continuation_key())
        self.assertEqual(result.get_read_kb(), 1)
        self.assertEqual(result.get_read_units(), 2)
        self.assertEqual(result.get_write_kb(), 0)
        self.assertEqual(result.get_write_units(), 0)

    def testQueryStatementUpdateTTL(self):
        hour_in_milliseconds = 60 * 60 * 1000
        self.query_request.set_statement(
            'UPDATE ' + table_name + ' $u SET TTL CASE WHEN ' +
            'remaining_hours($u) < 0 THEN 3 ELSE remaining_hours($u) + 3 END ' +
            'HOURS WHERE fld_sid = 1 AND fld_id = 3')
        result = self.handle.query(self.query_request)
        ttl = TimeToLive.of_hours(3)
        expect_expiration = ttl.to_expiration_time(int(round(time() * 1000)))
        records = result.get_results()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0], {'NumRowsUpdated': 1})
        self.assertIsNone(result.get_continuation_key())
        self.assertEqual(result.get_read_kb(), 2 + prepare_cost)
        self.assertEqual(result.get_read_units(), 4 + prepare_cost)
        self.assertEqual(result.get_write_kb(), 3)
        self.assertEqual(result.get_write_units(), 3)
        # check the record after update ttl request succeed
        self.get_request.set_key({'fld_sid': 1, 'fld_id': 3})
        result = self.handle.get(self.get_request)
        actual_expiration = result.get_expiration_time()
        actual_expect_diff = actual_expiration - expect_expiration
        self.assertGreater(actual_expiration, 0)
        self.assertLess(actual_expect_diff, hour_in_milliseconds)
        self.assertEqual(result.get_read_kb(), 1)
        self.assertEqual(result.get_read_units(), 2)
        self.assertEqual(result.get_write_kb(), 0)
        self.assertEqual(result.get_write_units(), 0)


if __name__ == '__main__':
    unittest.main()
