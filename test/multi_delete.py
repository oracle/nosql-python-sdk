#
# Copyright (c) 2018, 2022 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#

import unittest
from collections import OrderedDict

from borneo import (
    FieldRange, IllegalArgumentException, MultiDeleteRequest, PrepareRequest,
    PutRequest, QueryRequest, TableLimits, TableNotFoundException, TableRequest,
    WriteMultipleRequest)
from parameters import table_name, timeout
from test_base import TestBase
from testutils import get_row


class TestMultiDelete(unittest.TestCase, TestBase):
    @classmethod
    def setUpClass(cls):
        cls.set_up_class()
        create_statement = (
            'CREATE TABLE ' + table_name + '(fld_sid INTEGER, fld_id INTEGER, \
fld_long LONG, fld_float FLOAT, fld_double DOUBLE, fld_bool BOOLEAN, \
fld_str STRING, fld_bin BINARY, fld_time TIMESTAMP(8), fld_num NUMBER, \
fld_json JSON, fld_arr ARRAY(STRING), fld_map MAP(STRING), \
fld_rec RECORD(fld_id LONG, fld_bool BOOLEAN, fld_str STRING), \
PRIMARY KEY(SHARD(fld_sid), fld_id)) USING TTL 1 HOURS')
        limits = TableLimits(100, 100, 1)
        create_request = TableRequest().set_statement(
            create_statement).set_table_limits(limits)
        cls.table_request(create_request)

    @classmethod
    def tearDownClass(cls):
        cls.tear_down_class()

    def setUp(self):
        self.set_up()
        self.shardkeys = [0, 1]
        ids = [0, 1, 2, 3, 4, 5]
        write_multiple_request = WriteMultipleRequest()
        for sk in self.shardkeys:
            for i in ids:
                row = get_row()
                row['fld_sid'] = sk
                row['fld_id'] = i
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
        self.tear_down()

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

    def testMultiDeleteSetIllegalCompartment(self):
        self.assertRaises(IllegalArgumentException,
                          self.multi_delete_request.set_compartment, {})
        self.assertRaises(IllegalArgumentException,
                          self.multi_delete_request.set_compartment, '')

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
        self.assertIsNone(self.multi_delete_request.get_compartment())
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
        self.check_query_result(result, num_records)
        self.check_cost(result, num_records, num_records * 2, 0, 0,
                        multi_shards=True)
        # do multi_delete
        self.multi_delete_request.set_table_name(table_name).set_key(self.key)
        result = self.handle.multi_delete(self.multi_delete_request)
        self._check_multi_delete_result(result, num_deletion)
        self.check_cost(result, num_deletion, num_deletion * 2,
                        num_deletion, num_deletion)
        # check the records after multi_delete request
        result = self.handle.query(self.query_request)
        self.check_query_result(result, num_remaining)
        records = result.get_results()
        for idx in range(num_remaining):
            self.assertEqual(records[idx], self._expected_row(0, idx))
        self.check_cost(result, num_remaining, num_remaining * 2, 0, 0,
                        multi_shards=True)

    def testMultiDeleteWithMaxWriteKb(self):
        # do multi_delete with max_write_kb=3
        max_write_kb = 2
        num_remaining = 10
        self.multi_delete_request.set_table_name(table_name).set_key(
            self.key).set_max_write_kb(max_write_kb)
        result = self.handle.multi_delete(self.multi_delete_request)
        self._check_multi_delete_result(result, max_write_kb, True)
        self.check_cost(result, max_write_kb, max_write_kb * 2,
                        max_write_kb, max_write_kb)
        # check the records after multi_delete request
        result = self.handle.query(self.query_request)
        self.check_query_result(result, num_remaining)
        records = result.get_results()
        sk0_id = 0
        sk1_id = max_write_kb
        for record in records:
            if record['fld_sid'] == 0:
                self.assertEqual(record, self._expected_row(0, sk0_id))
                sk0_id += 1
            else:
                self.assertEqual(record, self._expected_row(1, sk1_id))
                sk1_id += 1
        self.check_cost(result, num_remaining, num_remaining * 2, 0, 0,
                        multi_shards=True)

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
                self._check_multi_delete_result(result, deleted, True)
            else:
                deleted = num_deletion - completed
                read_kb = 1 if deleted == 0 else deleted
                self._check_multi_delete_result(result, deleted)
            self.check_cost(result, read_kb, read_kb * 2, deleted, deleted)
            # check the records after multi_delete request
            query_result = self.handle.query(self.query_request)
            num_remaining = num_records - (completed + deleted)
            self.check_query_result(query_result, num_remaining)
            records = query_result.get_results()
            sk0_id = 0
            sk1_id = completed + deleted
            for record in records:
                if record['fld_sid'] == 0:
                    self.assertEqual(record, self._expected_row(0, sk0_id))
                    sk0_id += 1
                else:
                    self.assertEqual(record, self._expected_row(1, sk1_id))
                    sk1_id += 1
            self.check_cost(query_result, num_remaining, num_remaining * 2, 0,
                            0, multi_shards=True)
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
        self._check_multi_delete_result(result, num_deletion)
        self.check_cost(result, num_deletion, num_deletion * 2,
                        num_deletion, num_deletion)
        # check the records after multi_delete request
        result = self.handle.query(self.query_request)
        self.check_query_result(result, num_remaining)
        records = result.get_results()
        sk0_id = 0
        sk1_id = 0
        for record in records:
            if record['fld_sid'] == 0:
                self.assertEqual(record, self._expected_row(0, sk0_id))
                sk0_id += 1
            else:
                self.assertEqual(record, self._expected_row(1, sk1_id))
                sk1_id += 5
        self.check_cost(result, num_remaining, num_remaining * 2, 0, 0,
                        multi_shards=True)

    def _check_multi_delete_result(self, result, num_deletion,
                                   continuation_key=False):
        # check deleted records number
        self.assertEqual(result.get_num_deletions(), num_deletion)
        # check continuation_key
        cont_key = result.get_continuation_key()
        (self.assertIsNotNone(cont_key) if continuation_key
         else self.assertIsNone(cont_key))

    @staticmethod
    def _expected_row(fld_sid, fld_id):
        expected_row = OrderedDict()
        expected_row['fld_sid'] = fld_sid
        expected_row['fld_id'] = fld_id
        return expected_row


if __name__ == '__main__':
    unittest.main()
