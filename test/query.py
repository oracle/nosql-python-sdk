#
# Copyright (C) 2018, 2019 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
#
# Please see LICENSE.txt file included in the top-level directory of the
# appropriate download for a copy of the license and additional information.
#

import unittest
from datetime import datetime
from decimal import Context, Decimal, ROUND_HALF_EVEN
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

        create_idx_request = TableRequest()
        create_idx_statement = (
            'CREATE INDEX ' + index_name + '1 ON ' + table_name + '(fld_long)')
        create_idx_request.set_statement(create_idx_statement)
        cls._result = TestBase.table_request(create_idx_request, State.ACTIVE)
        create_idx_statement = (
            'CREATE INDEX ' + index_name + '2 ON ' + table_name + '(fld_str)')
        create_idx_request.set_statement(create_idx_statement)
        cls._result = TestBase.table_request(create_idx_request, State.ACTIVE)
        create_idx_statement = (
            'CREATE INDEX ' + index_name + '3 ON ' + table_name + '(fld_bool)')
        create_idx_request.set_statement(create_idx_statement)
        cls._result = TestBase.table_request(create_idx_request, State.ACTIVE)
        create_idx_statement = (
            'CREATE INDEX ' + index_name + '4 ON ' + table_name +
            '(fld_json.location as point)')
        create_idx_request.set_statement(create_idx_statement)
        cls._result = TestBase.table_request(create_idx_request, State.ACTIVE)

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
        self.min_time = list()
        self.max_time = list()
        shardkeys = 2
        ids = 6
        write_multiple_request = WriteMultipleRequest()
        for sk in range(shardkeys):
            for i in range(ids):
                dt = datetime.now()
                if i == 0:
                    self.min_time.append(dt)
                elif i == shardkeys - 1:
                    self.max_time.append(dt)
                row = {'fld_sid': sk, 'fld_id': i, 'fld_long': 2147483648,
                       'fld_float': 3.1414999961853027, 'fld_double': 3.1415,
                       'fld_bool': False if sk == 0 else True,
                       'fld_str': '{"name": u' +
                       str(shardkeys * ids - sk * ids - i - 1).zfill(2) + '}',
                       'fld_bin': bytearray(pack('>i', 4)),
                       'fld_time': dt, 'fld_num': Decimal(5),
                       'fld_json': {'json_1': '1', 'json_2': None, 'location':
                                    {'type': 'point', 'coordinates':
                                     [23.549 - sk * 0.5 - i,
                                      35.2908 + sk * 0.5 + i]}},
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
                          self.query_request.set_max_read_kb,
                          'IllegalMaxReadKb')
        self.assertRaises(IllegalArgumentException,
                          self.query_request.set_max_read_kb, -1)
        self.assertRaises(IllegalArgumentException,
                          self.query_request.set_max_read_kb, 2049)

    def testQuerySetIllegalMaxWriteKb(self):
        self.assertRaises(IllegalArgumentException,
                          self.query_request.set_max_write_kb,
                          'IllegalMaxWriteKb')
        self.assertRaises(IllegalArgumentException,
                          self.query_request.set_max_write_kb, -1)
        self.assertRaises(IllegalArgumentException,
                          self.query_request.set_max_write_kb, 2049)

    def testQuerySetIllegalMaxMemoryConsumption(self):
        self.assertRaises(IllegalArgumentException,
                          self.query_request.set_max_memory_consumption,
                          'IllegalMaxMemoryConsumption')
        self.assertRaises(IllegalArgumentException,
                          self.query_request.set_max_memory_consumption, -1)

    def testQuerySetIllegalMathContext(self):
        self.assertRaises(IllegalArgumentException,
                          self.query_request.set_math_context,
                          'IllegalMathContext')

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
        context = Context(prec=10, rounding=ROUND_HALF_EVEN)
        self.query_request.set_consistency(Consistency.EVENTUAL).set_statement(
            query_statement).set_prepared_statement(
            self.prepare_result_select).set_limit(3).set_max_read_kb(
            2).set_max_write_kb(3).set_max_memory_consumption(
            5).set_math_context(context).set_continuation_key(continuation_key)
        self.assertEqual(self.query_request.get_limit(), 3)
        self.assertEqual(self.query_request.get_max_read_kb(), 2)
        self.assertEqual(self.query_request.get_max_write_kb(), 3)
        self.assertEqual(self.query_request.get_max_memory_consumption(), 5)
        self.assertEqual(self.query_request.get_math_context(), context)
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
            self.assertEqual(result.get_read_kb(),
                             read_kb + (prepare_cost if count == 0 else 0))
            self.assertEqual(result.get_read_units(),
                             read_kb * 2 + (prepare_cost if count == 0 else 0))
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
        self.assertEqual(result.get_write_kb(), 6)
        self.assertEqual(result.get_write_units(), 6)
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

    def testQueryOrderBy(self):
        num_records = 12
        num_ids = 6
        # test order by primary index field
        statement = ('SELECT fld_sid, fld_id FROM ' + table_name +
                     ' ORDER BY fld_sid, fld_id')
        query_request = QueryRequest().set_statement(statement)
        count = 0
        while True:
            result = self.handle.query(query_request)
            records = result.get_results()
            continuation_key = result.get_continuation_key()
            if continuation_key is not None:
                self.assertEqual(records, [])
                self.assertGreater(result.get_read_kb(), 0)
                self.assertGreater(result.get_read_units(), 0)
                self.assertEqual(result.get_write_kb(), 0)
                self.assertEqual(result.get_write_units(), 0)
            else:
                self.assertEqual(len(records), num_records)
                for idx in range(num_records):
                    self.assertEqual(
                        records[idx],
                        {'fld_sid': idx // num_ids, 'fld_id': idx % num_ids})
                self.assertEqual(result.get_read_kb(), 0)
                self.assertEqual(result.get_read_units(), 0)
                self.assertEqual(result.get_write_kb(), 0)
                self.assertEqual(result.get_write_units(), 0)
                break
            query_request.set_continuation_key(continuation_key)
            count += 1

        # test order by secondary index field
        statement = ('SELECT fld_str FROM ' + table_name + ' ORDER BY fld_str')
        query_request = QueryRequest().set_statement(statement)
        count = 0
        while True:
            result = self.handle.query(query_request)
            records = result.get_results()
            continuation_key = result.get_continuation_key()
            if continuation_key is not None:
                self.assertEqual(records, [])
                self.assertGreater(result.get_read_kb(), 0)
                self.assertGreater(result.get_read_units(), 0)
                self.assertEqual(result.get_write_kb(), 0)
                self.assertEqual(result.get_write_units(), 0)
            else:
                self.assertEqual(len(records), num_records)
                for idx in range(num_records):
                    self.assertEqual(
                        records[idx],
                        {'fld_str': '{"name": u' + str(idx).zfill(2) + '}'})
                self.assertEqual(result.get_read_kb(), 0)
                self.assertEqual(result.get_read_units(), 0)
                self.assertEqual(result.get_write_kb(), 0)
                self.assertEqual(result.get_write_units(), 0)
                break
            query_request.set_continuation_key(continuation_key)
            count += 1

    def testQueryFuncMinMaxGroupBy(self):
        num_sids = 2
        # test min function
        statement = ('SELECT min(fld_time) FROM ' + table_name)
        query_request = QueryRequest().set_statement(statement)
        result = self.handle.query(query_request)
        records = result.get_results()
        self.assertEqual(len(records), 1)
        # self.assertEqual(records[0], {'Column_1': self.min_time[0]})
        self.assertIsNone(result.get_continuation_key())
        self.assertGreater(result.get_read_kb(), prepare_cost)
        self.assertGreater(result.get_read_units(), prepare_cost)
        self.assertEqual(result.get_write_kb(), 0)
        self.assertEqual(result.get_write_units(), 0)

        # test max function
        statement = ('SELECT max(fld_time) FROM ' + table_name)
        query_request = QueryRequest().set_statement(statement)
        result = self.handle.query(query_request)
        records = result.get_results()
        self.assertEqual(len(records), 1)
        # self.assertEqual(records[0], {'Column_1': self.max_time[1]})
        self.assertIsNone(result.get_continuation_key())
        self.assertGreater(result.get_read_kb(), prepare_cost)
        self.assertGreater(result.get_read_units(), prepare_cost)
        self.assertEqual(result.get_write_kb(), 0)
        self.assertEqual(result.get_write_units(), 0)

        # test min function group by primary index field
        statement = ('SELECT min(fld_time) FROM ' + table_name +
                     ' GROUP BY fld_sid')
        query_request = QueryRequest().set_statement(statement)
        count = 0
        while True:
            result = self.handle.query(query_request)
            records = result.get_results()
            continuation_key = result.get_continuation_key()
            if continuation_key is not None:
                self.assertEqual(records, [])
                self.assertGreater(result.get_read_kb(), 0)
                self.assertGreater(result.get_read_units(), 0)
                self.assertEqual(result.get_write_kb(), 0)
                self.assertEqual(result.get_write_units(), 0)
            else:
                self.assertEqual(len(records), num_sids)
                # for idx in range(num_sids):
                #     self.assertEqual(
                #         records[idx], {'Column_1': self.min_time[idx]})
                self.assertEqual(result.get_read_kb(), 0)
                self.assertEqual(result.get_read_units(), 0)
                self.assertEqual(result.get_write_kb(), 0)
                self.assertEqual(result.get_write_units(), 0)
                break
            query_request.set_continuation_key(continuation_key)
            count += 1

        # test max function group by primary index field
        statement = ('SELECT max(fld_time) FROM ' + table_name +
                     ' GROUP BY fld_sid')
        query_request = QueryRequest().set_statement(statement)
        count = 0
        while True:
            result = self.handle.query(query_request)
            records = result.get_results()
            continuation_key = result.get_continuation_key()
            if continuation_key is not None:
                self.assertEqual(records, [])
                self.assertGreater(result.get_read_kb(), 0)
                self.assertGreater(result.get_read_units(), 0)
                self.assertEqual(result.get_write_kb(), 0)
                self.assertEqual(result.get_write_units(), 0)
            else:
                self.assertEqual(len(records), num_sids)
                # for idx in range(num_sids):
                #     self.assertEqual(
                #         records[idx], {'Column_1': self.max_time[idx]})
                self.assertEqual(result.get_read_kb(), 0)
                self.assertEqual(result.get_read_units(), 0)
                self.assertEqual(result.get_write_kb(), 0)
                self.assertEqual(result.get_write_units(), 0)
                break
            query_request.set_continuation_key(continuation_key)
            count += 1

        # test min function group by secondary index field
        statement = ('SELECT min(fld_time) FROM ' + table_name +
                     ' GROUP BY fld_bool')
        query_request = QueryRequest().set_statement(statement)
        count = 0
        while True:
            result = self.handle.query(query_request)
            records = result.get_results()
            continuation_key = result.get_continuation_key()
            if continuation_key is not None:
                self.assertEqual(records, [])
                self.assertGreater(result.get_read_kb(), 0)
                self.assertGreater(result.get_read_units(), 0)
                self.assertEqual(result.get_write_kb(), 0)
                self.assertEqual(result.get_write_units(), 0)
            else:
                self.assertEqual(len(records), num_sids)
                # for idx in range(2):
                #     self.assertEqual(
                #         records[idx], {'Column_1': self.min_time[idx]})
                self.assertEqual(result.get_read_kb(), 0)
                self.assertEqual(result.get_read_units(), 0)
                self.assertEqual(result.get_write_kb(), 0)
                self.assertEqual(result.get_write_units(), 0)
                break
            query_request.set_continuation_key(continuation_key)
            count += 1

        # test max function group by secondary index field
        statement = ('SELECT max(fld_time) FROM ' + table_name +
                     ' GROUP BY fld_bool')
        query_request = QueryRequest().set_statement(statement)
        count = 0
        while True:
            result = self.handle.query(query_request)
            records = result.get_results()
            continuation_key = result.get_continuation_key()
            if continuation_key is not None:
                self.assertEqual(records, [])
                self.assertGreater(result.get_read_kb(), 0)
                self.assertGreater(result.get_read_units(), 0)
                self.assertEqual(result.get_write_kb(), 0)
                self.assertEqual(result.get_write_units(), 0)
            else:
                self.assertEqual(len(records), num_sids)
                # for idx in range(2):
                #     self.assertEqual(
                #         records[idx], {'Column_1': self.mix_time[idx]})
                self.assertEqual(result.get_read_kb(), 0)
                self.assertEqual(result.get_read_units(), 0)
                self.assertEqual(result.get_write_kb(), 0)
                self.assertEqual(result.get_write_units(), 0)
                break
            query_request.set_continuation_key(continuation_key)
            count += 1

    def testQueryFuncSumGroupBy(self):
        num_records = 12
        num_sids = 2
        # test sum function
        statement = ('SELECT sum(fld_double) FROM ' + table_name)
        query_request = QueryRequest().set_statement(statement)
        result = self.handle.query(query_request)
        records = result.get_results()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0], {'Column_1': 3.1415 * num_records})
        self.assertIsNone(result.get_continuation_key())
        self.assertGreater(result.get_read_kb(), prepare_cost)
        self.assertGreater(result.get_read_units(), prepare_cost)
        self.assertEqual(result.get_write_kb(), 0)
        self.assertEqual(result.get_write_units(), 0)

        # test sum function group by primary index field
        statement = ('SELECT sum(fld_double) FROM ' + table_name +
                     ' GROUP BY fld_sid')
        query_request = QueryRequest().set_statement(statement)
        count = 0
        while True:
            result = self.handle.query(query_request)
            records = result.get_results()
            continuation_key = result.get_continuation_key()
            if continuation_key is not None:
                self.assertEqual(records, [])
                self.assertGreater(result.get_read_kb(), 0)
                self.assertGreater(result.get_read_units(), 0)
                self.assertEqual(result.get_write_kb(), 0)
                self.assertEqual(result.get_write_units(), 0)
            else:
                self.assertEqual(len(records), num_sids)
                for idx in range(num_sids):
                    self.assertEqual(
                        records[idx],
                        {'Column_1': 3.1415 * (num_records // num_sids)})
                self.assertEqual(result.get_read_kb(), 0)
                self.assertEqual(result.get_read_units(), 0)
                self.assertEqual(result.get_write_kb(), 0)
                self.assertEqual(result.get_write_units(), 0)
                break
            query_request.set_continuation_key(continuation_key)
            count += 1

        # test sum function group by secondary index field
        statement = ('SELECT sum(fld_double) FROM ' + table_name +
                     ' GROUP BY fld_bool')
        query_request = QueryRequest().set_statement(statement)
        count = 0
        while True:
            result = self.handle.query(query_request)
            records = result.get_results()
            continuation_key = result.get_continuation_key()
            if continuation_key is not None:
                self.assertEqual(records, [])
                self.assertGreater(result.get_read_kb(), 0)
                self.assertGreater(result.get_read_units(), 0)
                self.assertEqual(result.get_write_kb(), 0)
                self.assertEqual(result.get_write_units(), 0)
            else:
                self.assertEqual(len(records), num_sids)
                for idx in range(num_sids):
                    self.assertEqual(
                        records[idx],
                        {'Column_1': 3.1415 * (num_records // num_sids)})
                self.assertEqual(result.get_read_kb(), 0)
                self.assertEqual(result.get_read_units(), 0)
                self.assertEqual(result.get_write_kb(), 0)
                self.assertEqual(result.get_write_units(), 0)
                break
            query_request.set_continuation_key(continuation_key)
            count += 1

    def testQueryFuncAvgGroupBy(self):
        num_sids = 2
        # test avg function
        statement = ('SELECT avg(fld_double) FROM ' + table_name)
        query_request = QueryRequest().set_statement(statement)
        result = self.handle.query(query_request)
        records = result.get_results()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0], {'Column_1': 3.1415})
        self.assertIsNone(result.get_continuation_key())
        self.assertGreater(result.get_read_kb(), prepare_cost)
        self.assertGreater(result.get_read_units(), prepare_cost)
        self.assertEqual(result.get_write_kb(), 0)
        self.assertEqual(result.get_write_units(), 0)

        # test avg function group by primary index field
        statement = ('SELECT avg(fld_double) FROM ' + table_name +
                     ' GROUP BY fld_sid')
        query_request = QueryRequest().set_statement(statement)
        count = 0
        while True:
            result = self.handle.query(query_request)
            records = result.get_results()
            continuation_key = result.get_continuation_key()
            if continuation_key is not None:
                self.assertEqual(records, [])
                self.assertGreater(result.get_read_kb(), 0)
                self.assertGreater(result.get_read_units(), 0)
                self.assertEqual(result.get_write_kb(), 0)
                self.assertEqual(result.get_write_units(), 0)
            else:
                self.assertEqual(len(records), num_sids)
                for idx in range(num_sids):
                    self.assertEqual(records[idx], {'Column_1': 3.1415})
                self.assertEqual(result.get_read_kb(), 0)
                self.assertEqual(result.get_read_units(), 0)
                self.assertEqual(result.get_write_kb(), 0)
                self.assertEqual(result.get_write_units(), 0)
                break
            query_request.set_continuation_key(continuation_key)
            count += 1

        # test avg function group by secondary index field
        statement = ('SELECT avg(fld_double) FROM ' + table_name +
                     ' GROUP BY fld_bool')
        query_request = QueryRequest().set_statement(statement)
        count = 0
        while True:
            result = self.handle.query(query_request)
            records = result.get_results()
            continuation_key = result.get_continuation_key()
            if continuation_key is not None:
                self.assertEqual(records, [])
                self.assertGreater(result.get_read_kb(), 0)
                self.assertGreater(result.get_read_units(), 0)
                self.assertEqual(result.get_write_kb(), 0)
                self.assertEqual(result.get_write_units(), 0)
            else:
                self.assertEqual(len(records), num_sids)
                for idx in range(num_sids):
                    self.assertEqual(records[idx], {'Column_1': 3.1415})
                self.assertEqual(result.get_read_kb(), 0)
                self.assertEqual(result.get_read_units(), 0)
                self.assertEqual(result.get_write_kb(), 0)
                self.assertEqual(result.get_write_units(), 0)
                break
            query_request.set_continuation_key(continuation_key)
            count += 1

    def testQueryFuncCountGroupBy(self):
        num_records = 12
        num_sids = 2
        # test count function
        statement = ('SELECT count(*) FROM ' + table_name)
        query_request = QueryRequest().set_statement(statement)
        result = self.handle.query(query_request)
        records = result.get_results()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0], {'Column_1': num_records})
        self.assertIsNone(result.get_continuation_key())
        self.assertGreater(result.get_read_kb(), prepare_cost)
        self.assertGreater(result.get_read_units(), prepare_cost)
        self.assertEqual(result.get_write_kb(), 0)
        self.assertEqual(result.get_write_units(), 0)

        # test count function group by primary index field
        statement = ('SELECT count(*) FROM ' + table_name +
                     ' GROUP BY fld_sid')
        query_request = QueryRequest().set_statement(statement)
        count = 0
        while True:
            result = self.handle.query(query_request)
            records = result.get_results()
            continuation_key = result.get_continuation_key()
            if continuation_key is not None:
                self.assertEqual(records, [])
                self.assertGreater(result.get_read_kb(), 0)
                self.assertGreater(result.get_read_units(), 0)
                self.assertEqual(result.get_write_kb(), 0)
                self.assertEqual(result.get_write_units(), 0)
            else:
                self.assertEqual(len(records), num_sids)
                for idx in range(num_sids):
                    self.assertEqual(
                        records[idx], {'Column_1': num_records // num_sids})
                self.assertEqual(result.get_read_kb(), 0)
                self.assertEqual(result.get_read_units(), 0)
                self.assertEqual(result.get_write_kb(), 0)
                self.assertEqual(result.get_write_units(), 0)
                break
            query_request.set_continuation_key(continuation_key)
            count += 1

        # test count function group by secondary index field
        statement = ('SELECT count(*) FROM ' + table_name +
                     ' GROUP BY fld_bool')
        query_request = QueryRequest().set_statement(statement)
        count = 0
        while True:
            result = self.handle.query(query_request)
            records = result.get_results()
            continuation_key = result.get_continuation_key()
            if continuation_key is not None:
                self.assertEqual(records, [])
                self.assertGreater(result.get_read_kb(), 0)
                self.assertGreater(result.get_read_units(), 0)
                self.assertEqual(result.get_write_kb(), 0)
                self.assertEqual(result.get_write_units(), 0)
            else:
                self.assertEqual(len(records), num_sids)
                for idx in range(num_sids):
                    self.assertEqual(
                        records[idx], {'Column_1': num_records // num_sids})
                self.assertEqual(result.get_read_kb(), 0)
                self.assertEqual(result.get_read_units(), 0)
                self.assertEqual(result.get_write_kb(), 0)
                self.assertEqual(result.get_write_units(), 0)
                break
            query_request.set_continuation_key(continuation_key)
            count += 1

    def testQueryOrderByWithLimit(self):
        num_records = 12
        limit = 10
        # test order by primary index field with limit
        statement = ('SELECT fld_str FROM ' + table_name +
                     ' ORDER BY fld_sid, fld_id LIMIT 10')
        query_request = QueryRequest().set_statement(statement)
        count = 0
        while True:
            result = self.handle.query(query_request)
            records = result.get_results()
            continuation_key = result.get_continuation_key()
            if continuation_key is not None:
                self.assertEqual(records, [])
                self.assertGreater(result.get_read_kb(), 0)
                self.assertGreater(result.get_read_units(), 0)
                self.assertEqual(result.get_write_kb(), 0)
                self.assertEqual(result.get_write_units(), 0)
            else:
                self.assertEqual(len(records), limit)
                for idx in range(limit):
                    self.assertEqual(
                        records[idx],
                        {'fld_str': '{"name": u' +
                         str(num_records - idx - 1).zfill(2) + '}'})
                self.assertEqual(result.get_read_kb(), 0)
                self.assertEqual(result.get_read_units(), 0)
                self.assertEqual(result.get_write_kb(), 0)
                self.assertEqual(result.get_write_units(), 0)
                break
            query_request.set_continuation_key(continuation_key)
            count += 1

        # test order by secondary index field with limit
        statement = ('SELECT fld_str FROM ' + table_name +
                     ' ORDER BY fld_str LIMIT 10')
        query_request = QueryRequest().set_statement(statement)
        count = 0
        while True:
            result = self.handle.query(query_request)
            records = result.get_results()
            continuation_key = result.get_continuation_key()
            if continuation_key is not None:
                self.assertEqual(records, [])
                self.assertGreater(result.get_read_kb(), 0)
                self.assertGreater(result.get_read_units(), 0)
                self.assertEqual(result.get_write_kb(), 0)
                self.assertEqual(result.get_write_units(), 0)
            else:
                self.assertEqual(len(records), limit)
                for idx in range(limit):
                    self.assertEqual(
                        records[idx],
                        {'fld_str': '{"name": u' + str(idx).zfill(2) + '}'})
                self.assertEqual(result.get_read_kb(), 0)
                self.assertEqual(result.get_read_units(), 0)
                self.assertEqual(result.get_write_kb(), 0)
                self.assertEqual(result.get_write_units(), 0)
                break
            query_request.set_continuation_key(continuation_key)
            count += 1

    def testQueryOrderByWithOffset(self):
        offset = 4
        num_get = 8
        # test order by primary index field with offset
        statement = (
            'DECLARE $offset INTEGER; SELECT fld_str FROM ' + table_name +
            ' ORDER BY fld_sid, fld_id OFFSET $offset')
        prepare_request = PrepareRequest().set_statement(statement)
        prepare_result = self.handle.prepare(prepare_request)
        prepared_statement = prepare_result.get_prepared_statement()
        prepared_statement.set_variable('$offset', offset)
        query_request = QueryRequest().set_prepared_statement(
            prepared_statement)
        count = 0
        while True:
            result = self.handle.query(query_request)
            records = result.get_results()
            continuation_key = result.get_continuation_key()
            if continuation_key is not None:
                self.assertEqual(records, [])
                self.assertGreater(result.get_read_kb(), 0)
                self.assertGreater(result.get_read_units(), 0)
                self.assertEqual(result.get_write_kb(), 0)
                self.assertEqual(result.get_write_units(), 0)
            else:
                self.assertEqual(len(records), num_get)
                for idx in range(num_get):
                    self.assertEqual(
                        records[idx],
                        {'fld_str': '{"name": u' +
                         str(num_get - idx - 1).zfill(2) + '}'})
                self.assertEqual(result.get_read_kb(), 0)
                self.assertEqual(result.get_read_units(), 0)
                self.assertEqual(result.get_write_kb(), 0)
                self.assertEqual(result.get_write_units(), 0)
                break
            query_request.set_continuation_key(continuation_key)
            count += 1

        # test order by secondary index field with offset
        statement = (
            'DECLARE $offset INTEGER; SELECT fld_str FROM ' + table_name +
            ' ORDER BY fld_str OFFSET $offset')
        prepare_request = PrepareRequest().set_statement(statement)
        prepare_result = self.handle.prepare(prepare_request)
        prepared_statement = prepare_result.get_prepared_statement()
        prepared_statement.set_variable('$offset', offset)
        query_request = QueryRequest().set_prepared_statement(
            prepared_statement)
        count = 0
        while True:
            result = self.handle.query(query_request)
            records = result.get_results()
            continuation_key = result.get_continuation_key()
            if continuation_key is not None:
                self.assertEqual(records, [])
                self.assertGreater(result.get_read_kb(), 0)
                self.assertGreater(result.get_read_units(), 0)
                self.assertEqual(result.get_write_kb(), 0)
                self.assertEqual(result.get_write_units(), 0)
            else:
                self.assertEqual(len(records), num_get)
                for idx in range(num_get):
                    self.assertEqual(
                        records[idx],
                        {'fld_str': '{"name": u' + str(offset + idx).zfill(2) +
                         '}'})
                self.assertEqual(result.get_read_kb(), 0)
                self.assertEqual(result.get_read_units(), 0)
                self.assertEqual(result.get_write_kb(), 0)
                self.assertEqual(result.get_write_units(), 0)
                break
            query_request.set_continuation_key(continuation_key)
            count += 1

    def testQueryFuncGeoNear(self):
        num_get = 6
        longitude = 21.547
        latitude = 37.291
        # test geo_near function
        statement = (
            'SELECT tb.fld_json.location FROM ' + table_name +
            ' tb WHERE geo_near(tb.fld_json.location, ' +
            '{"type": "point", "coordinates": [' + str(longitude) + ', ' +
            str(latitude) + ']}, 215000)')
        query_request = QueryRequest().set_statement(statement)
        result = self.handle.query(query_request)
        records = result.get_results()
        self.assertEqual(len(records), num_get)
        for i in range(1, num_get):
            pre = records[i - 1]['location']['coordinates']
            curr = records[i]['location']['coordinates']
            self.assertLess(abs(pre[0] - longitude), abs(curr[0] - longitude))
            self.assertLess(abs(pre[1] - latitude), abs(curr[1] - latitude))
        self.assertIsNone(result.get_continuation_key())
        self.assertGreater(result.get_read_kb(), prepare_cost)
        self.assertGreater(result.get_read_units(), prepare_cost)
        self.assertEqual(result.get_write_kb(), 0)
        self.assertEqual(result.get_write_units(), 0)

        # test geo_near function order by primary index field
        statement = (
            'SELECT fld_str FROM ' + table_name + ' tb WHERE geo_near(' +
            'tb.fld_json.location, {"type": "point", "coordinates": [' +
            str(longitude) + ', ' + str(latitude) + ']}, 215000) ' +
            'ORDER BY fld_sid, fld_id')
        query_request = QueryRequest().set_statement(statement)
        count = 0
        while True:
            result = self.handle.query(query_request)
            records = result.get_results()
            continuation_key = result.get_continuation_key()
            if continuation_key is not None:
                self.assertEqual(records, [])
                self.assertGreater(result.get_read_kb(), 0)
                self.assertGreater(result.get_read_units(), 0)
                self.assertEqual(result.get_write_kb(), 0)
                self.assertEqual(result.get_write_units(), 0)
            else:
                name = [10, 9, 8, 4, 3, 2]
                self.assertEqual(len(records), num_get)
                for i in range(num_get):
                    self.assertEqual(
                        records[i],
                        {'fld_str': '{"name": u' + str(name[i]).zfill(2) + '}'})
                self.assertEqual(result.get_read_kb(), 0)
                self.assertEqual(result.get_read_units(), 0)
                self.assertEqual(result.get_write_kb(), 0)
                self.assertEqual(result.get_write_units(), 0)
                break
            query_request.set_continuation_key(continuation_key)
            count += 1

        # test geo_near function order by secondary index field
        statement = (
            'SELECT fld_str FROM ' + table_name + ' tb WHERE geo_near(' +
            'tb.fld_json.location, {"type": "point", "coordinates": [' +
            str(longitude) + ', ' + str(latitude) + ']}, 215000) ' +
            'ORDER BY fld_str')
        query_request = QueryRequest().set_statement(statement)
        count = 0
        while True:
            result = self.handle.query(query_request)
            records = result.get_results()
            continuation_key = result.get_continuation_key()
            if continuation_key is not None:
                self.assertEqual(records, [])
                self.assertGreater(result.get_read_kb(), 0)
                self.assertGreater(result.get_read_units(), 0)
                self.assertEqual(result.get_write_kb(), 0)
                self.assertEqual(result.get_write_units(), 0)
            else:
                name = [2, 3, 4, 8, 9, 10]
                self.assertEqual(len(records), num_get)
                for i in range(num_get):
                    self.assertEqual(
                        records[i],
                        {'fld_str': '{"name": u' + str(name[i]).zfill(2) + '}'})
                self.assertEqual(result.get_read_kb(), 0)
                self.assertEqual(result.get_read_units(), 0)
                self.assertEqual(result.get_write_kb(), 0)
                self.assertEqual(result.get_write_units(), 0)
                break
            query_request.set_continuation_key(continuation_key)
            count += 1


if __name__ == '__main__':
    unittest.main()
