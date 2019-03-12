#
# Copyright (C) 2018, 2019 Oracle and/or its affiliates. All rights reserved.
#
# This software is licensed with the Universal Permissive License (UPL) version 1.0
#
# Please see LICENSE.txt file included in the top-level directory of the
# appropriate download for a copy of the license and additional information.
#

import unittest
from copy import deepcopy
from datetime import datetime
from decimal import Decimal
from struct import pack
from time import sleep, time

from borneo import (
    BatchOperationNumberLimitException, DeleteRequest, GetRequest,
    IllegalArgumentException, MultiDeleteRequest, PutOption, PutRequest, State,
    TableLimits, TableRequest, TimeToLive, WriteMultipleRequest)
from parameters import protocol, table_name, tenant_id, timeout, wait_timeout
from testutils import add_test_tier_tenant, delete_test_tier_tenant, get_handle


class TestWriteMultiple(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        add_test_tier_tenant(tenant_id)
        cls._handle = get_handle(tenant_id)
        if protocol == 'https':
            # sleep a while to avoid the OperationThrottlingException
            sleep(60)
        drop_statement = 'DROP TABLE IF EXISTS ' + table_name
        cls._drop_request = TableRequest().set_statement(drop_statement)
        cls._result = cls._handle.table_request(cls._drop_request)
        cls._result.wait_for_state(cls._handle, table_name, State.DROPPED,
                                   wait_timeout, 1000)
        create_statement = (
            'CREATE TABLE ' + table_name + '(fld_sid INTEGER, fld_id INTEGER, \
fld_long LONG, fld_float FLOAT, fld_double DOUBLE, fld_bool BOOLEAN, \
fld_str STRING, fld_bin BINARY, fld_time TIMESTAMP(8), fld_num NUMBER, \
fld_json JSON, fld_arr ARRAY(STRING), fld_map MAP(STRING), \
fld_rec RECORD(fld_id LONG, fld_bool BOOLEAN, fld_str STRING), \
PRIMARY KEY(SHARD(fld_sid), fld_id))')
        limits = TableLimits(5000, 5000, 50)
        create_request = TableRequest().set_statement(
            create_statement).set_table_limits(limits)
        cls._result = cls._handle.table_request(create_request)
        cls._result.wait_for_state(cls._handle, table_name, State.ACTIVE,
                                   wait_timeout, 1000)

    @classmethod
    def tearDownClass(cls):
        try:
            cls._result = cls._handle.table_request(cls._drop_request)
            cls._result.wait_for_state(cls._handle, table_name, State.DROPPED,
                                       wait_timeout, 1000)
        finally:
            cls._handle.close()
            delete_test_tier_tenant(tenant_id)

    def setUp(self):
        self.handle = get_handle(tenant_id)
        self.shardkeys = [0, 1]
        self.ids = [0, 1, 2, 3, 4, 5]
        self.rows = list()
        self.new_rows = list()
        self.versions = list()
        self.requests = list()
        self.illegal_requests = list()
        ttl = TimeToLive.of_days(16)
        for sk in self.shardkeys:
            self.rows.append(list())
            self.new_rows.append(list())
            self.versions.append(list())
            for i in self.ids:
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
                new_row = deepcopy(row)
                new_row.update({'fld_long': 2147483649})
                self.rows[sk].append(row)
                self.new_rows[sk].append(new_row)
                put_request = PutRequest().set_value(row).set_table_name(
                    table_name).set_ttl(ttl)
                self.versions[sk].append(
                    self.handle.put(put_request).get_version())
        self.old_expect_expiration = ttl.to_expiration_time(
            int(round(time() * 1000)))
        self.ttl = TimeToLive.of_hours(1)
        self.ops_sk = 0
        illegal_sk = 1
        self.requests.append(PutRequest().set_value(
            self.new_rows[self.ops_sk][0]).set_table_name(table_name).set_ttl(
            self.ttl).set_return_row(True))
        self.requests.append(PutRequest().set_value(
            self.new_rows[self.ops_sk][1]).set_table_name(
            table_name).set_option(PutOption.IF_ABSENT).set_ttl(
            self.ttl).set_return_row(True))
        self.requests.append(PutRequest().set_value(
            self.new_rows[self.ops_sk][2]).set_use_table_default_ttl(
            True).set_table_name(table_name).set_option(
            PutOption.IF_PRESENT).set_return_row(True))
        self.requests.append(PutRequest().set_value(
            self.new_rows[self.ops_sk][3]).set_table_name(
            table_name).set_option(PutOption.IF_VERSION).set_ttl(
            self.ttl).set_match_version(
            self.versions[self.ops_sk][3]).set_return_row(True))
        self.requests.append(DeleteRequest().set_key(
            {'fld_sid': self.ops_sk, 'fld_id': 4}).set_table_name(
            table_name).set_return_row(True))
        self.requests.append(DeleteRequest().set_key(
            {'fld_sid': self.ops_sk, 'fld_id': 5}).set_table_name(
                table_name).set_match_version(
                self.versions[self.ops_sk][0]).set_return_row(True))
        self.illegal_requests.append(DeleteRequest().set_key(
            {'fld_sid': self.ops_sk, 'fld_id': 0}).set_table_name(
            'IllegalUsers'))
        self.illegal_requests.append(DeleteRequest().set_key(
            {'fld_sid': illegal_sk, 'fld_id': 0}).set_table_name(table_name))
        self.write_multiple_request = WriteMultipleRequest().set_timeout(
            timeout)
        self.get_request = GetRequest().set_table_name(table_name)
        self.hour_in_milliseconds = 60 * 60 * 1000
        self.day_in_milliseconds = 24 * 60 * 60 * 1000

    def tearDown(self):
        for sk in self.shardkeys:
            key = {'fld_sid': sk}
            request = MultiDeleteRequest().set_table_name(
                table_name).set_key(key)
            self.handle.multi_delete(request)
        self.handle.close()

    def testWriteMultipleAddIllegalRequestAndAbortIfUnsuccessful(self):
        self.assertRaises(IllegalArgumentException,
                          self.write_multiple_request.add,
                          'IllegalRequest', True)
        self.assertRaises(IllegalArgumentException,
                          self.write_multiple_request.add,
                          PutRequest(), 'IllegalAbortIfUnsuccessful')
        # add two operations with different table name
        self.write_multiple_request.add(self.requests[0], True)
        self.assertRaises(IllegalArgumentException,
                          self.write_multiple_request.add,
                          self.illegal_requests[0], False)
        self.write_multiple_request.clear()
        # add two operations with different major paths
        self.write_multiple_request.add(
            self.requests[0], True).add(self.illegal_requests[1], False)
        self.assertRaises(IllegalArgumentException, self.handle.write_multiple,
                          self.write_multiple_request)
        self.write_multiple_request.clear()
        # add operations when sub requests reached the max number
        count = 0
        while count < 50:
            self.write_multiple_request.add(self.requests[0], True)
            count += 1
        self.assertRaises(BatchOperationNumberLimitException,
                          self.write_multiple_request.add,
                          self.requests[0], True)

    def testWriteMultipleGetRequestWithIllegalIndex(self):
        self.assertRaises(IllegalArgumentException,
                          self.write_multiple_request.get_request,
                          'IllegalIndex')
        self.assertRaises(IllegalArgumentException,
                          self.write_multiple_request.get_request, -1)
        self.assertRaises(IndexError, self.write_multiple_request.get_request,
                          0)

    def testWriteMultipleSetIllegalTimeout(self):
        self.assertRaises(IllegalArgumentException,
                          self.write_multiple_request.set_timeout,
                          'IllegalTimeout')
        self.assertRaises(IllegalArgumentException,
                          self.write_multiple_request.set_timeout, 0)
        self.assertRaises(IllegalArgumentException,
                          self.write_multiple_request.set_timeout, -1)

    def testWriteMultipleNoOperations(self):
        self.assertRaises(IllegalArgumentException, self.handle.write_multiple,
                          self.write_multiple_request)

    def testWriteMultipleGets(self):
        num_operations = 6
        for request in self.requests:
            self.write_multiple_request.add(request, True)
        self.assertEqual(self.write_multiple_request.get_table_name(),
                         table_name)
        self.assertEqual(self.write_multiple_request.get_request(2),
                         self.requests[2])
        requests = self.write_multiple_request.get_operations()
        for idx in range(len(requests)):
            self.assertEqual(requests[idx].get_request(),
                             self.requests[idx])
            self.assertTrue(requests[idx].is_abort_if_unsuccessful())
        self.assertEqual(self.write_multiple_request.get_num_operations(),
                         num_operations)
        self.assertEqual(self.write_multiple_request.get_timeout(), timeout)
        self.write_multiple_request.clear()
        self.assertIsNone(self.write_multiple_request.get_table_name())
        self.assertEqual(self.write_multiple_request.get_operations(), [])
        self.assertEqual(self.write_multiple_request.get_num_operations(), 0)
        self.assertEqual(self.write_multiple_request.get_timeout(), timeout)

    def testWriteMultipleNormal(self):
        num_operations = 6
        for request in self.requests:
            self.write_multiple_request.add(request, False)
        result = self.handle.write_multiple(self.write_multiple_request)
        expect_expiration = self.ttl.to_expiration_time(
            int(round(time() * 1000)))
        self.assertEqual(result.size(), num_operations)
        op_results = result.get_results()
        for idx in range(result.size()):
            if idx == 1 or idx == 5:
                # putIfAbsent and deleteIfVersion failed
                self.assertIsNone(op_results[idx].get_version())
                self.assertFalse(op_results[idx].get_success())
                self.assertEqual(
                    op_results[idx].get_existing_version().get_bytes(),
                    self.versions[self.ops_sk][idx].get_bytes())
                self.assertEqual(op_results[idx].get_existing_value(),
                                 self.rows[self.ops_sk][idx])
            elif idx == 4:
                # delete succeed
                self.assertIsNone(op_results[idx].get_version())
                self.assertTrue(op_results[idx].get_success())
                self.assertIsNone(op_results[idx].get_existing_version())
                self.assertIsNone(op_results[idx].get_existing_value())
            else:
                # put, putIfPresent and putIfVersion succeed
                self.assertIsNotNone(op_results[idx].get_version())
                self.assertNotEqual(op_results[idx].get_version(),
                                    self.versions[self.ops_sk][idx])
                self.assertTrue(op_results[idx].get_success())
                self.assertIsNone(op_results[idx].get_existing_version())
                self.assertIsNone(op_results[idx].get_existing_value())
        self.assertIsNone(result.get_failed_operation_result())
        self.assertEqual(result.get_failed_operation_index(), -1)
        self.assertTrue(result.get_success())
        self.assertEqual(result.get_read_kb(), 0 + 1 + 1 + 1 + 1 + 1)
        self.assertEqual(result.get_read_units(), 0 + 2 + 2 + 2 + 2 + 2)
        self.assertEqual(result.get_write_kb(), 2 + 0 + 2 + 2 + 1 + 0)
        self.assertEqual(result.get_write_units(), 2 + 0 + 2 + 2 + 1 + 0)
        # check the records after write_multiple request succeed
        for sk in self.shardkeys:
            for i in self.ids:
                self.get_request.set_key({'fld_sid': sk, 'fld_id': i})
                result = self.handle.get(self.get_request)
                if sk == 1 or i == 1 or i == 5:
                    self.assertEqual(result.get_value(), self.rows[sk][i])
                    self.assertEqual(result.get_version().get_bytes(),
                                     self.versions[sk][i].get_bytes())
                    actual_expiration = result.get_expiration_time()
                    actual_expect_diff = (actual_expiration -
                                          self.old_expect_expiration)
                    self.assertGreater(actual_expiration, 0)
                    self.assertLess(actual_expect_diff,
                                    self.day_in_milliseconds)
                elif i == 4:
                    self.assertIsNone(result.get_value())
                    self.assertIsNone(result.get_version())
                    self.assertEqual(result.get_expiration_time(), 0)
                else:
                    self.assertEqual(result.get_value(), self.new_rows[sk][i])
                    self.assertNotEqual(result.get_version().get_bytes(), 0)
                    self.assertNotEqual(result.get_version().get_bytes(),
                                        self.versions[sk][i].get_bytes())
                    if i == 2:
                        self.assertEqual(result.get_expiration_time(), 0)
                    else:
                        actual_expiration = result.get_expiration_time()
                        actual_expect_diff = (actual_expiration -
                                              expect_expiration)
                        self.assertGreater(actual_expiration, 0)
                        self.assertLess(actual_expect_diff,
                                        self.hour_in_milliseconds)
                self.assertEqual(result.get_read_kb(), 1)
                self.assertEqual(result.get_read_units(), 2)
                self.assertEqual(result.get_write_kb(), 0)
                self.assertEqual(result.get_write_units(), 0)

    def testWriteMultipleAbortIfUnsuccessful(self):
        failed_idx = 1
        for request in self.requests:
            self.write_multiple_request.add(request, True)
        result = self.handle.write_multiple(self.write_multiple_request)
        self.assertEqual(result.size(), 1)
        op_results = result.get_results()
        self.assertIsNone(op_results[0].get_version())
        self.assertFalse(op_results[0].get_success())
        self.assertEqual(op_results[0].get_existing_version().get_bytes(),
                         self.versions[self.ops_sk][failed_idx].get_bytes())
        self.assertEqual(op_results[0].get_existing_value(),
                         self.rows[self.ops_sk][failed_idx])
        failed_result = result.get_failed_operation_result()
        self.assertIsNone(failed_result.get_version())
        self.assertFalse(failed_result.get_success())
        self.assertEqual(failed_result.get_existing_version().get_bytes(),
                         self.versions[self.ops_sk][failed_idx].get_bytes())
        self.assertEqual(failed_result.get_existing_value(),
                         self.rows[self.ops_sk][failed_idx])
        self.assertEqual(result.get_failed_operation_index(), failed_idx)
        self.assertFalse(result.get_success())
        self.assertEqual(result.get_read_kb(), 0 + 1)
        self.assertEqual(result.get_read_units(), 0 + 2)
        self.assertEqual(result.get_write_kb(), 2 + 0)
        self.assertEqual(result.get_write_units(), 2 + 0)
        # check the records after multi_delete request failed
        for sk in self.shardkeys:
            for i in self.ids:
                self.get_request.set_key({'fld_sid': sk, 'fld_id': i})
                result = self.handle.get(self.get_request)
                self.assertEqual(result.get_value(), self.rows[sk][i])
                self.assertEqual(result.get_version().get_bytes(),
                                 self.versions[sk][i].get_bytes())
                actual_expiration = result.get_expiration_time()
                actual_expect_diff = (actual_expiration -
                                      self.old_expect_expiration)
                self.assertGreater(actual_expiration, 0)
                self.assertLess(actual_expect_diff, self.day_in_milliseconds)
                self.assertEqual(result.get_read_kb(), 1)
                self.assertEqual(result.get_read_units(), 2)
                self.assertEqual(result.get_write_kb(), 0)
                self.assertEqual(result.get_write_units(), 0)


if __name__ == '__main__':
    unittest.main()
