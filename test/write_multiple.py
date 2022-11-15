#
# Copyright (c) 2018, 2022 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#

import unittest
from collections import OrderedDict
from copy import deepcopy
from parameters import table_prefix
from time import time

from borneo import (
    BatchOperationNumberLimitException, DeleteRequest, GetRequest,
    IllegalArgumentException, MultiDeleteRequest, PutOption, PutRequest,
    RequestSizeLimitException, TableLimits, TableRequest, TimeToLive, TimeUnit,
    WriteMultipleRequest)
from parameters import is_onprem, table_name, timeout
from test_base import TestBase
from testutils import get_row


class TestWriteMultiple(unittest.TestCase, TestBase):
    @classmethod
    def setUpClass(cls):
        cls.set_up_class()
        create_statement = (
                'CREATE TABLE ' + table_name + '(fld_sid INTEGER, fld_id INTEGER, \
fld_long LONG, fld_float FLOAT, fld_double DOUBLE, fld_bool BOOLEAN, \
fld_str STRING, fld_bin BINARY, fld_time TIMESTAMP(8), fld_num NUMBER, \
fld_json JSON, fld_arr ARRAY(STRING), fld_map MAP(STRING), \
fld_rec RECORD(fld_id LONG, fld_bool BOOLEAN, fld_str STRING), \
PRIMARY KEY(SHARD(fld_sid), fld_id))')
        limits = TableLimits(50, 50, 1)
        create_request = TableRequest().set_statement(
            create_statement).set_table_limits(limits)
        cls.table_request(create_request)
        global serial_version
        serial_version = cls.handle.get_client().serial_version

        cls.child_table_name = table_name + '.child'
        create_child_statement = (
                'CREATE TABLE ' + cls.child_table_name + ' (childid INTEGER, childname STRING, \
                childdata STRING, \
                PRIMARY KEY(childid)) USING TTL 1 DAYS')
        limits = TableLimits(50, 50, 1)
        create_request = TableRequest().set_statement(
            create_child_statement)
        cls.table_request(create_request)

    @classmethod
    def tearDownClass(cls):
        cls.table_request(TableRequest().set_statement('DROP TABLE ' + cls.child_table_name))
        cls.tear_down_class()

    def setUp(self):
        self.set_up()
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
                row = get_row()
                row['fld_sid'] = sk
                row['fld_id'] = i
                new_row = deepcopy(row)
                new_row['fld_long'] = 2147483649
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
            self.new_rows[self.ops_sk][3]).set_table_name(table_name).set_ttl(
            self.ttl).set_option(PutOption.IF_VERSION).set_match_version(
            self.versions[self.ops_sk][3]).set_return_row(True))
        self.requests.append(DeleteRequest().set_key(
            {'fld_sid': self.ops_sk, 'fld_id': 4}).set_table_name(
            table_name).set_return_row(True))
        self.requests.append(DeleteRequest().set_key(
            {'fld_sid': self.ops_sk, 'fld_id': 5}).set_table_name(
            table_name).set_return_row(True).set_match_version(
            self.versions[self.ops_sk][0]))
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
            request = MultiDeleteRequest().set_table_name(table_name).set_key(
                key)
            self.handle.multi_delete(request)
        self.tear_down()

    def testWriteMultipleSetIllegalCompartment(self):
        self.assertRaises(IllegalArgumentException,
                          self.write_multiple_request.set_compartment, {})
        self.assertRaises(IllegalArgumentException,
                          self.write_multiple_request.set_compartment, '')

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
        if not is_onprem():
            # add operations when the request size exceeded the limit
            self.write_multiple_request.clear()
            for op in range(64):
                row = get_row()
                row['fld_str'] = self.get_random_str(0.4)
                self.write_multiple_request.add(PutRequest().set_value(
                    row).set_table_name(table_name), True)
            self.assertRaises(RequestSizeLimitException,
                              self.handle.write_multiple,
                              self.write_multiple_request)
            # add operations when sub requests reached the max number
            self.write_multiple_request.clear()
            for op in range(51):
                row = get_row()
                row['fld_id'] = op
                self.write_multiple_request.add(PutRequest().set_value(
                    row).set_table_name(table_name), True)
            self.assertRaises(BatchOperationNumberLimitException,
                              self.handle.write_multiple,
                              self.write_multiple_request)

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
        self.assertIsNone(self.write_multiple_request.get_compartment())
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
        op_results = self._check_write_multiple_result(result, num_operations)
        for idx in range(result.size()):
            if idx == 1 or idx == 5:
                # putIfAbsent and deleteIfVersion failed
                self._check_operation_result(
                    op_results[idx],
                    existing_version=self.versions[self.ops_sk][idx],
                    existing_value=self.rows[self.ops_sk][idx])
            elif idx == 4:
                # delete succeed
                self._check_operation_result(op_results[idx], success=True)
            else:
                # put, putIfPresent and putIfVersion succeed
                self._check_operation_result(op_results[idx], True, True)
        self.check_cost(result, 5, 10, 7, 7)
        # check the records after write_multiple request succeed
        for sk in self.shardkeys:
            for i in self.ids:
                self.get_request.set_key({'fld_sid': sk, 'fld_id': i})
                result = self.handle.get(self.get_request)
                if sk == 1 or i == 1 or i == 5:
                    self.check_get_result(
                        result, self.rows[sk][i], self.versions[sk][i],
                        self.old_expect_expiration, TimeUnit.DAYS,
                        True, (serial_version > 2))
                elif i == 4:
                    self.check_get_result(result)
                elif i == 2:
                    self.check_get_result(
                        result, self.new_rows[sk][i], self.versions[sk][i],
                        ver_eq=False, mod_time_recent=(serial_version > 2))
                else:
                    self.check_get_result(
                        result, self.new_rows[sk][i], self.versions[sk][i],
                        expect_expiration, TimeUnit.HOURS, False,
                        (serial_version > 2))
                self.check_cost(result, 1, 2, 0, 0)

    def testWriteMultipleChild(self):
        num_operations = 6
        wm_req = WriteMultipleRequest()
        for i in range(num_operations):
            parent_row = dict()
            parent_row['fld_sid'] = 1
            parent_row['fld_id'] = i
            parent_row['fld_str'] = 'str_' + str(i)
            request = \
                PutRequest() \
                    .set_value(parent_row) \
                    .set_table_name(table_name) \
                    .set_ttl(self.ttl) \
                    .set_return_row(True)
            wm_req.add(request, True)

            child_row: dict = dict()
            child_row['fld_sid'] = 1
            child_row['fld_id'] = i
            child_row['childid'] = i
            child_row['childname'] = 'name_' + str(i)
            child_row['childdata'] = 'data_' + str(i)

            request = PutRequest() \
                .set_value(child_row) \
                .set_table_name(self.child_table_name) \
                .set_ttl(self.ttl) \
                .set_return_row(True)
            wm_req.add(request, True)

        result = self.handle.write_multiple(wm_req)

        op_results = self._check_write_multiple_result(result, num_operations * 2)
        for idx in range(result.size()):
            self._check_operation_result(op_results[idx], True, True)

        for i in range(num_operations):
            parent_row = dict()
            parent_row['fld_sid'] = 1
            parent_row['fld_id'] = i
            request = \
                GetRequest() \
                    .set_key(parent_row) \
                    .set_table_name(table_name)
            result = self.handle.get(request)
            self.assertEqual('str_' + str(i), result.get_value()['fld_str'])

            child_row: dict = dict()
            child_row['fld_sid'] = 1
            child_row['fld_id'] = i
            child_row['childid'] = i
            request = \
                GetRequest() \
                    .set_key(child_row) \
                    .set_table_name(self.child_table_name)
            result = self.handle.get(request)
            self.assertEqual('name_' + str(i), result.get_value()['childname'])
            self.assertEqual('data_' + str(i), result.get_value()['childdata'])

    def testWriteMultipleAbortIfUnsuccessful(self):
        failed_idx = 1
        for request in self.requests:
            self.write_multiple_request.add(request, True)
        result = self.handle.write_multiple(self.write_multiple_request)
        op_results = self._check_write_multiple_result(
            result, 1, True, failed_idx, False)
        self._check_operation_result(
            op_results[0],
            existing_version=self.versions[self.ops_sk][failed_idx],
            existing_value=self.rows[self.ops_sk][failed_idx])
        failed_result = result.get_failed_operation_result()
        self._check_operation_result(
            failed_result,
            existing_version=self.versions[self.ops_sk][failed_idx],
            existing_value=self.rows[self.ops_sk][failed_idx])
        self.check_cost(result, 1, 2, 2, 2)
        # check the records after multi_delete request failed
        for sk in self.shardkeys:
            for i in self.ids:
                self.get_request.set_key({'fld_sid': sk, 'fld_id': i})
                result = self.handle.get(self.get_request)
                self.check_get_result(
                    result, self.rows[sk][i], self.versions[sk][i],
                    self.old_expect_expiration, TimeUnit.DAYS,
                    True, (serial_version > 2))
                self.check_cost(result, 1, 2, 0, 0)

    def testWriteMultipleWithIdentityColumn(self):
        num_operations = 10
        id_table = table_prefix + 'Identity'
        create_request = TableRequest().set_statement(
            'CREATE TABLE ' + id_table + '(sid INTEGER, id LONG GENERATED \
ALWAYS AS IDENTITY, name STRING, PRIMARY KEY(SHARD(sid), id))')
        create_request.set_table_limits(TableLimits(50, 50, 1))
        self.table_request(create_request)

        # add ten operations
        row = {'name': 'myname', 'sid': 1}
        for idx in range(num_operations):
            put_request = PutRequest().set_table_name(id_table).set_value(row)
            put_request.set_identity_cache_size(idx)
            self.write_multiple_request.add(put_request, False)
        # execute the write multiple request
        versions = list()
        result = self.handle.write_multiple(self.write_multiple_request)
        op_results = self._check_write_multiple_result(result, num_operations)
        generated = 0
        for idx in range(result.size()):
            version, generated = self._check_operation_result(
                op_results[idx], True, True, generated)
            versions.append(version)
        self.check_cost(result, 0, 0, num_operations, num_operations)
        # check the records after write_multiple request succeed
        self.get_request.set_table_name(id_table)
        for idx in range(num_operations):
            curr_id = generated - num_operations + idx + 1
            self.get_request.set_key({'sid': 1, 'id': curr_id})
            result = self.handle.get(self.get_request)
            expected = OrderedDict()
            expected['sid'] = 1
            expected['id'] = curr_id
            expected['name'] = 'myname'
            self.check_get_result(result, expected, versions[idx],
                                  mod_time_recent=(serial_version > 2))
            self.check_cost(result, 1, 2, 0, 0)

    def _check_operation_result(
            self, op_result, version=False, success=False, last_generated=None,
            existing_version=None, existing_value=None):
        # check version of operation result
        ver = op_result.get_version()
        self.assertIsNotNone(ver) if version else self.assertIsNone(ver)
        # check if the operation success
        self.assertEqual(op_result.get_success(), success)
        # check generated value of operation result
        generated = op_result.get_generated_value()
        if last_generated is None:
            self.assertIsNone(generated)
        else:
            self.assertGreater(generated, last_generated)
        # check existing version
        existing_ver = op_result.get_existing_version()
        (self.assertIsNone(existing_ver) if existing_version is None
         else self.assertEqual(existing_ver.get_bytes(),
                               existing_version.get_bytes()))
        # check existing value
        self.assertEqual(op_result.get_existing_value(), existing_value)
        return ver, generated

    def _check_write_multiple_result(
            self, result, num_operations, has_failed_operation=False,
            failed_index=-1, success=True):
        # check number of operations
        self.assertEqual(result.size(), num_operations)
        # check failed operation
        failed_result = result.get_failed_operation_result()
        (self.assertIsNotNone(failed_result) if has_failed_operation
         else self.assertIsNone(failed_result))
        # check failed operation index
        self.assertEqual(result.get_failed_operation_index(), failed_index)
        # check operation status
        self.assertEqual(result.get_success(), success)
        return result.get_results()


if __name__ == '__main__':
    unittest.main()
