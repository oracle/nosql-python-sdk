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
from decimal import Decimal
from struct import pack

from borneo import (
    FieldRange, IllegalArgumentException, MultiDeleteRequest, PrepareRequest,
    PutRequest, QueryRequest, State, TableLimits, TableNotFoundException,
    TableRequest, WriteMultipleRequest)
from parameters import table_name, timeout
from test_base import TestBase


class TestMultiDelete(unittest.TestCase, TestBase):
    @classmethod
    def setUpClass(cls):
        TestBase.set_up_class()
        create_statement = (
            'CREATE TABLE ' + table_name + '(fld_sid INTEGER, fld_id INTEGER, \
fld_long LONG, fld_float FLOAT, fld_double DOUBLE, fld_bool BOOLEAN, \
fld_str STRING, fld_bin BINARY, fld_time TIMESTAMP(8), fld_num NUMBER, \
fld_json JSON, fld_arr ARRAY(STRING), fld_map MAP(STRING), \
fld_rec RECORD(fld_id LONG, fld_bool BOOLEAN, fld_str STRING), \
PRIMARY KEY(SHARD(fld_sid), fld_id)) USING TTL 1 HOURS')
        limits = TableLimits(5000, 5000, 50)
        create_request = TableRequest().set_statement(
            create_statement).set_table_limits(limits)
        cls._result = TestBase.table_request(create_request, State.ACTIVE)

    @classmethod
    def tearDownClass(cls):
        TestBase.tear_down_class()

    def setUp(self):
        TestBase.set_up(self)
        self.shardkeys = [0, 1]
        ids = [0, 1, 2, 3, 4, 5]
        write_multiple_request = WriteMultipleRequest()
        for sk in self.shardkeys:
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
        self.key = {'fld_sid': 1}
        self.multi_delete_request = MultiDeleteRequest().set_timeout(timeout)
        prep_request = PrepareRequest().set_statement(
            'SELECT fld_sid, fld_id FROM ' + table_name)
        prep_result = self.handle.prepare(prep_request)
        self.query_request = QueryRequest().set_prepared_statement(prep_result)

    def tearDown(self):
        self.multi_delete_request.set_table_name(table_name)
        for sk in self.shardkeys:
            key = {'fld_sid': sk}
            self.handle.multi_delete(self.multi_delete_request.set_key(key))
        TestBase.tear_down(self)

    def testMultiDeleteSetIllegalTableName(self):
        self.assertRaises(IllegalArgumentException,
                          self.multi_delete_request.set_table_name,
                          {'name': table_name})
        self.multi_delete_request.set_table_name('IllegalTable').set_key(
            self.key)
        self.assertRaises(TableNotFoundException, self.handle.multi_delete,
                          self.multi_delete_request)

    def testMultiDeleteSetIllegalKey(self):
        self.assertRaises(IllegalArgumentException,
                          self.multi_delete_request.set_key, 'IllegalKey')
        self.multi_delete_request.set_table_name(table_name).set_key(
            {'fld_long': 2147483648})
        self.assertRaises(IllegalArgumentException, self.handle.multi_delete,
                          self.multi_delete_request)
        self.multi_delete_request.set_key({'fld_id': 1})
        self.assertRaises(IllegalArgumentException, self.handle.multi_delete,
                          self.multi_delete_request)

    def testMultiDeleteSetIllegalContinuationKey(self):
        self.assertRaises(IllegalArgumentException,
                          self.multi_delete_request.set_continuation_key,
                          'IllegalContinuationKey')

    def testMultiDeleteSetIllegalRange(self):
        self.assertRaises(IllegalArgumentException,
                          self.multi_delete_request.set_range, 'IllegalRange')
        self.multi_delete_request.set_table_name(table_name).set_key(
            self.key).set_range(FieldRange('fld_sid'))
        self.assertRaises(IllegalArgumentException, self.handle.multi_delete,
                          self.multi_delete_request)
        self.multi_delete_request.set_range(
            FieldRange('fld_sid').set_start(2, True))
        self.assertRaises(IllegalArgumentException, self.handle.multi_delete,
                          self.multi_delete_request)
        self.multi_delete_request.set_range(
            FieldRange('fld_long').set_start(2147483648, True))
        self.assertRaises(IllegalArgumentException, self.handle.multi_delete,
                          self.multi_delete_request)
        self.multi_delete_request.set_range(None)

    def testMultiDeleteSetIllegalMaxWriteKb(self):
        self.assertRaises(IllegalArgumentException,
                          self.multi_delete_request.set_max_write_kb,
                          'IllegalMaxWriteKb')
        self.assertRaises(IllegalArgumentException,
                          self.multi_delete_request.set_max_write_kb, -1)
        self.assertRaises(IllegalArgumentException,
                          self.multi_delete_request.set_max_write_kb, 2049)

    def testMultiDeleteSetIllegalTimeout(self):
        self.assertRaises(IllegalArgumentException,
                          self.multi_delete_request.set_timeout,
                          'IllegalTimeout')
        self.assertRaises(IllegalArgumentException,
                          self.multi_delete_request.set_timeout, 0)
        self.assertRaises(IllegalArgumentException,
                          self.multi_delete_request.set_timeout, -1)

    def testMultiDeleteNoTableName(self):
        self.multi_delete_request.set_key(self.key)
        self.assertRaises(IllegalArgumentException, self.handle.multi_delete,
                          self.multi_delete_request)

    def testMultiDeleteNoKey(self):
        self.multi_delete_request.set_table_name(table_name)
        self.assertRaises(IllegalArgumentException, self.handle.multi_delete,
                          self.multi_delete_request)

    def testMultiDeleteGets(self):
        field_range = FieldRange('fld_id').set_start(2, True)
        continuation_key = bytearray(5)
        self.multi_delete_request.set_table_name(table_name).set_key(
            self.key).set_continuation_key(continuation_key).set_range(
            field_range).set_max_write_kb(1024)
        self.assertEqual(self.multi_delete_request.get_table_name(),
                         table_name)
        self.assertEqual(self.multi_delete_request.get_key(), self.key)
        self.assertEqual(self.multi_delete_request.get_continuation_key(),
                         continuation_key)
        self.assertEqual(self.multi_delete_request.get_range(), field_range)
        self.assertEqual(self.multi_delete_request.get_max_write_kb(), 1024)

    def testMultiDeleteIllegalRequest(self):
        self.assertRaises(IllegalArgumentException, self.handle.multi_delete,
                          'IllegalRequest')

    def testMultiDeleteNormal(self):
        # check the records before multi_delete request
        num_records = 12
        num_deletion = 6
        num_remaining = 6
        result = self.handle.query(self.query_request)
        self.assertEqual(len(result.get_results()), num_records)
        self.assertIsNone(result.get_continuation_key())
        self.assertEqual(result.get_read_kb(), num_records)
        self.assertEqual(result.get_read_units(), num_records * 2)
        self.assertEqual(result.get_write_kb(), 0)
        self.assertEqual(result.get_write_units(), 0)
        # do multi_delete
        self.multi_delete_request.set_table_name(table_name).set_key(self.key)
        result = self.handle.multi_delete(self.multi_delete_request)
        self.assertIsNone(result.get_continuation_key())
        self.assertEqual(result.get_num_deletions(), num_deletion)
        self.assertEqual(result.get_read_kb(), num_deletion)
        self.assertEqual(result.get_read_units(), num_deletion * 2)
        self.assertEqual(result.get_write_kb(), num_deletion)
        self.assertEqual(result.get_write_units(), num_deletion)
        # check the records after multi_delete request
        result = self.handle.query(self.query_request)
        records = result.get_results()
        self.assertEqual(len(records), num_remaining)
        for idx in range(num_remaining):
            self.assertEqual(records[idx], {'fld_sid': 0, 'fld_id': idx})
        self.assertIsNone(result.get_continuation_key())
        self.assertEqual(result.get_read_kb(), num_remaining)
        self.assertEqual(result.get_read_units(), num_remaining * 2)
        self.assertEqual(result.get_write_kb(), 0)
        self.assertEqual(result.get_write_units(), 0)

    def testMultiDeleteWithMaxWriteKb(self):
        # do multi_delete with max_write_kb=3
        max_write_kb = 2
        num_remaining = 10
        self.multi_delete_request.set_table_name(table_name).set_key(
            self.key).set_max_write_kb(max_write_kb)
        result = self.handle.multi_delete(self.multi_delete_request)
        self.assertIsNotNone(result.get_continuation_key())
        self.assertEqual(result.get_num_deletions(), max_write_kb)
        self.assertEqual(result.get_read_kb(), max_write_kb)
        self.assertEqual(result.get_read_units(), max_write_kb * 2)
        self.assertEqual(result.get_write_kb(), max_write_kb)
        self.assertEqual(result.get_write_units(), max_write_kb)
        # check the records after multi_delete request
        result = self.handle.query(self.query_request)
        records = result.get_results()
        self.assertEqual(len(records), num_remaining)
        sk0_id = 0
        sk1_id = max_write_kb
        for record in records:
            if record.get('fld_sid') == 0:
                self.assertEqual(record, {'fld_sid': 0, 'fld_id': sk0_id})
                sk0_id += 1
            else:
                self.assertEqual(record, {'fld_sid': 1, 'fld_id': sk1_id})
                sk1_id += 1
        self.assertIsNone(result.get_continuation_key())
        self.assertEqual(result.get_read_kb(), num_remaining)
        self.assertEqual(result.get_read_units(), num_remaining * 2)
        self.assertEqual(result.get_write_kb(), 0)
        self.assertEqual(result.get_write_units(), 0)

    def testMultiDeleteWithContinuationKey(self):
        num_records = 12
        num_deletion = 6
        max_write_kb = 3
        self.multi_delete_request.set_table_name(table_name).set_key(
            self.key).set_max_write_kb(max_write_kb)
        count = 0
        while True:
            completed = count * max_write_kb
            result = self.handle.multi_delete(self.multi_delete_request)
            if completed + max_write_kb <= num_deletion:
                deleted = max_write_kb
                read_kb = max_write_kb
                self.assertIsNotNone(result.get_continuation_key())
            else:
                deleted = num_deletion - completed
                read_kb = (1 if deleted == 0 else deleted)
                self.assertIsNone(result.get_continuation_key())
            self.assertEqual(result.get_num_deletions(), deleted)
            self.assertEqual(result.get_read_kb(), read_kb)
            self.assertEqual(result.get_read_units(), read_kb * 2)
            self.assertEqual(result.get_write_kb(), deleted)
            self.assertEqual(result.get_write_units(), deleted)
            # check the records after multi_delete request
            query_result = self.handle.query(self.query_request)
            records = query_result.get_results()
            num_remaining = num_records - (completed + deleted)
            self.assertEqual(len(records), num_remaining)
            sk0_id = 0
            sk1_id = completed + deleted
            for record in records:
                if record.get('fld_sid') == 0:
                    self.assertEqual(record, {'fld_sid': 0, 'fld_id': sk0_id})
                    sk0_id += 1
                else:
                    self.assertEqual(record, {'fld_sid': 1, 'fld_id': sk1_id})
                    sk1_id += 1
            self.assertIsNone(query_result.get_continuation_key())
            self.assertEqual(query_result.get_read_kb(), num_remaining)
            self.assertEqual(query_result.get_read_units(), num_remaining * 2)
            self.assertEqual(query_result.get_write_kb(), 0)
            self.assertEqual(query_result.get_write_units(), 0)
            count += 1
            if result.get_continuation_key() is None:
                break
            self.multi_delete_request.set_continuation_key(
                result.get_continuation_key())
        self.assertEqual(count, num_deletion // max_write_kb + 1)

    def testMultiDeleteWithRange(self):
        # do multi_delete with FieldRange set
        num_deletion = 4
        num_remaining = 8
        self.multi_delete_request.set_table_name(table_name).set_key(
            self.key).set_range(
            FieldRange('fld_id').set_start(0, False).set_end(4, True))
        result = self.handle.multi_delete(self.multi_delete_request)
        self.assertIsNone(result.get_continuation_key())
        self.assertEqual(result.get_num_deletions(), num_deletion)
        self.assertEqual(result.get_read_kb(), num_deletion)
        self.assertEqual(result.get_read_units(), num_deletion * 2)
        self.assertEqual(result.get_write_kb(), num_deletion)
        self.assertEqual(result.get_write_units(), num_deletion)
        # check the records after multi_delete request
        result = self.handle.query(self.query_request)
        records = result.get_results()
        self.assertEqual(len(records), num_remaining)
        sk0_id = 0
        sk1_id = 0
        for record in records:
            if record.get('fld_sid') == 0:
                self.assertEqual(record, {'fld_sid': 0, 'fld_id': sk0_id})
                sk0_id += 1
            else:
                self.assertEqual(record, {'fld_sid': 1, 'fld_id': sk1_id})
                sk1_id += 5
        self.assertIsNone(result.get_continuation_key())
        self.assertEqual(result.get_read_kb(), num_remaining)
        self.assertEqual(result.get_read_units(), num_remaining * 2)
        self.assertEqual(result.get_write_kb(), 0)
        self.assertEqual(result.get_write_units(), 0)


if __name__ == '__main__':
    unittest.main()
