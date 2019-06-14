#
# Copyright (C) 2018, 2019 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
#
# Please see LICENSE.txt file included in the top-level directory of the
# appropriate download for a copy of the license and additional information.
#

import unittest
from copy import deepcopy
from datetime import datetime
from decimal import Decimal
from parameters import table_prefix
from struct import pack
from time import time

from borneo import (
    DeleteRequest, GetRequest, IllegalArgumentException, IllegalStateException,
    PutOption, PutRequest, State, TableLimits, TableNotFoundException,
    TableRequest, TimeToLive)
from parameters import table_name, timeout
from testutils import check_cost
from test_base import TestBase


class TestPut(unittest.TestCase, TestBase):
    @classmethod
    def setUpClass(cls):
        TestBase.set_up_class()
        global table_ttl
        table_ttl = TimeToLive.of_days(30)
        create_statement = (
            'CREATE TABLE ' + table_name + '(fld_id INTEGER, fld_long LONG, \
fld_float FLOAT, fld_double DOUBLE, fld_bool BOOLEAN, fld_str STRING, \
fld_bin BINARY, fld_time TIMESTAMP(9), fld_num NUMBER, fld_json JSON, \
fld_arr ARRAY(STRING), fld_map MAP(STRING), \
fld_rec RECORD(fld_id LONG, fld_bool BOOLEAN, fld_str STRING), \
PRIMARY KEY(fld_id)) USING TTL ' + str(table_ttl))
        create_request = TableRequest().set_statement(
            create_statement).set_table_limits(TableLimits(5000, 5000, 50))
        cls._result = TestBase.table_request(create_request, State.ACTIVE)

    @classmethod
    def tearDownClass(cls):
        TestBase.tear_down_class()

    def setUp(self):
        TestBase.set_up(self)
        self.row = {'fld_id': 1, 'fld_long': 2147483648,
                    'fld_float': 3.1414999961853027, 'fld_double': 3.1415,
                    'fld_bool': True, 'fld_str': '{"name": u1, "phone": null}',
                    'fld_bin': bytearray(pack('>i', 4)),
                    'fld_time': datetime.now(), 'fld_num': Decimal(5),
                    'fld_json': {'a': '1', 'b': None, 'c': '3'},
                    'fld_arr': ['a', 'b', 'c'],
                    'fld_map': {'a': '1', 'b': '2', 'c': '3'},
                    'fld_rec': {'fld_id': 1, 'fld_bool': False,
                                'fld_str': None}}
        self.key = {'fld_id': 1}
        self.put_request = PutRequest().set_value(self.row).set_table_name(
            table_name).set_timeout(timeout)
        self.get_request = GetRequest().set_key(self.key).set_table_name(
            table_name)
        self.ttl = TimeToLive.of_hours(24)
        self.hour_in_milliseconds = 60 * 60 * 1000
        self.day_in_milliseconds = 24 * 60 * 60 * 1000

    def tearDown(self):
        request = DeleteRequest().set_key(self.key).set_table_name(table_name)
        self.handle.delete(request)
        TestBase.tear_down(self)

    def testPutSetIllegalValue(self):
        self.assertRaises(IllegalArgumentException, self.put_request.set_value,
                          'IllegalValue')

    def testPutSetIllegalValueFromJson(self):
        self.assertRaises(ValueError, self.put_request.set_value_from_json,
                          'IllegalJson')
        self.put_request.set_value_from_json('{"invalid_field": "value"}')
        self.assertRaises(IllegalArgumentException, self.handle.put,
                          self.put_request)

    def testPutSetIllegalOption(self):
        self.put_request.set_option('IllegalOption')
        self.assertRaises(IllegalStateException, self.handle.put,
                          self.put_request)

    def testPutSetIllegalMatchVersion(self):
        self.assertRaises(IllegalArgumentException,
                          self.put_request.set_match_version,
                          'IllegalMatchVersion')

    def testPutSetIllegalTtl(self):
        self.assertRaises(IllegalArgumentException,
                          self.put_request.set_ttl, 'IllegalTtl')

    def testPutSetIllegalUseTableDefaultTtl(self):
        self.assertRaises(IllegalArgumentException,
                          self.put_request.set_use_table_default_ttl,
                          'IllegalUseTableDefaultTtl')

    def testPutSetIllegalExactMatch(self):
        self.assertRaises(IllegalArgumentException,
                          self.put_request.set_exact_match, 'IllegalExactMatch')

    def testPutSetIllegalIdentityCacheSize(self):
        self.assertRaises(IllegalArgumentException,
                          self.put_request.set_identity_cache_size,
                          'IllegalIdentityCacheSize')

    def testPutSetIllegalTimeout(self):
        self.assertRaises(IllegalArgumentException,
                          self.put_request.set_timeout, 'IllegalTimeout')
        self.assertRaises(IllegalArgumentException,
                          self.put_request.set_timeout, 0)
        self.assertRaises(IllegalArgumentException,
                          self.put_request.set_timeout, -1)

    def testPutSetIllegalTableName(self):
        self.assertRaises(IllegalArgumentException,
                          self.put_request.set_table_name, {'name': table_name})
        self.put_request.set_table_name('IllegalTable')
        self.assertRaises(TableNotFoundException, self.handle.put,
                          self.put_request)

    def testPutSetIllegalReturnRow(self):
        self.assertRaises(IllegalArgumentException,
                          self.put_request.set_return_row, 'IllegalReturnRow')

    def testPutIfVersionWithoutMatchVersion(self):
        self.put_request.set_option(PutOption.IF_VERSION)
        self.assertRaises(IllegalArgumentException, self.handle.put,
                          self.put_request)

    def testPutNoVersionWithMatchVersion(self):
        version = self.handle.put(self.put_request).get_version()
        self.put_request.set_option(
            PutOption.IF_ABSENT).set_match_version(version)
        self.assertRaises(IllegalArgumentException, self.handle.put,
                          self.put_request)
        self.put_request.set_option(
            PutOption.IF_PRESENT).set_match_version(version)
        self.assertRaises(IllegalArgumentException, self.handle.put,
                          self.put_request)

    def testPutSetTtlAndUseTableDefaultTtl(self):
        self.put_request.set_ttl(self.ttl).set_use_table_default_ttl(True)
        self.assertRaises(IllegalArgumentException, self.handle.put,
                          self.put_request)

    def testPutGets(self):
        identity_cache_size = 5
        version = self.handle.put(self.put_request).get_version()
        self.put_request.set_option(PutOption.IF_ABSENT).set_match_version(
            version).set_ttl(self.ttl).set_use_table_default_ttl(
            True).set_exact_match(True).set_identity_cache_size(
            identity_cache_size).set_return_row(True)
        self.assertEqual(self.put_request.get_value(), self.row)
        self.assertEqual(self.put_request.get_option(), PutOption.IF_ABSENT)
        self.assertEqual(self.put_request.get_match_version(), version)
        self.assertEqual(self.put_request.get_ttl(), self.ttl)
        self.assertTrue(self.put_request.get_use_table_default_ttl())
        self.assertTrue(self.put_request.get_update_ttl())
        self.assertEqual(self.put_request.get_timeout(), timeout)
        self.assertEqual(self.put_request.get_table_name(), table_name)
        self.assertTrue(self.put_request.get_exact_match())
        self.assertEqual(self.put_request.get_identity_cache_size(),
                         identity_cache_size)
        self.assertTrue(self.put_request.get_return_row())

    def testPutIllegalRequest(self):
        self.assertRaises(IllegalArgumentException, self.handle.put,
                          'IllegalRequest')

    def testPutNormal(self):
        # test put with normal values
        result = self.handle.put(self.put_request)
        tb_expect_expiration = table_ttl.to_expiration_time(
            int(round(time() * 1000)))
        version = result.get_version()
        self.assertIsNotNone(version)
        self.assertIsNone(result.get_generated_value())
        self.assertIsNone(result.get_existing_version())
        self.assertIsNone(result.get_existing_value())
        check_cost(self, result, 0, 0, 1, 1)
        result = self.handle.get(self.get_request)
        self.assertEqual(result.get_value(), self.row)
        self.assertEqual(result.get_version().get_bytes(), version.get_bytes())
        actual_expiration = result.get_expiration_time()
        actual_expect_diff = actual_expiration - tb_expect_expiration
        self.assertGreater(actual_expiration, 0)
        self.assertLess(actual_expect_diff, self.day_in_milliseconds)
        check_cost(self, result, 1, 2, 0, 0)
        # put a row with the same primary key to update the row
        self.row.update({'fld_long': 2147483649})
        self.put_request.set_value(self.row).set_ttl(self.ttl)
        result = self.handle.put(self.put_request)
        expect_expiration = self.ttl.to_expiration_time(
            int(round(time() * 1000)))
        version = result.get_version()
        self.assertIsNotNone(version)
        self.assertIsNone(result.get_generated_value())
        self.assertIsNone(result.get_existing_version())
        self.assertIsNone(result.get_existing_value())
        check_cost(self, result, 0, 0, 2, 2)
        result = self.handle.get(self.get_request)
        self.assertEqual(result.get_value(), self.row)
        self.assertEqual(result.get_version().get_bytes(), version.get_bytes())
        actual_expiration = result.get_expiration_time()
        actual_expect_diff = actual_expiration - expect_expiration
        self.assertGreater(actual_expiration, 0)
        self.assertLess(actual_expect_diff, self.hour_in_milliseconds)
        check_cost(self, result, 1, 2, 0, 0)
        # update the ttl of the row to never expire
        self.put_request.set_ttl(TimeToLive.of_days(0))
        result = self.handle.put(self.put_request)
        version = result.get_version()
        self.assertIsNotNone(version)
        self.assertIsNone(result.get_generated_value())
        self.assertIsNone(result.get_existing_version())
        self.assertIsNone(result.get_existing_value())
        check_cost(self, result, 0, 0, 2, 2)
        result = self.handle.get(self.get_request)
        self.assertEqual(result.get_value(), self.row)
        self.assertEqual(result.get_version().get_bytes(), version.get_bytes())
        actual_expiration = result.get_expiration_time()
        self.assertEqual(actual_expiration, 0)
        check_cost(self, result, 1, 2, 0, 0)

    def testPutIfAbsent(self):
        # test PutIfAbsent with normal values
        self.put_request.set_option(PutOption.IF_ABSENT).set_ttl(
            self.ttl).set_return_row(True)
        result = self.handle.put(self.put_request)
        expect_expiration = self.ttl.to_expiration_time(
            int(round(time() * 1000)))
        version = result.get_version()
        self.assertIsNotNone(version)
        self.assertIsNone(result.get_generated_value())
        self.assertIsNone(result.get_existing_version())
        self.assertIsNone(result.get_existing_value())
        check_cost(self, result, 1, 2, 1, 1)
        result = self.handle.get(self.get_request)
        self.assertEqual(result.get_value(), self.row)
        self.assertEqual(result.get_version().get_bytes(), version.get_bytes())
        actual_expiration = result.get_expiration_time()
        actual_expect_diff = actual_expiration - expect_expiration
        self.assertGreater(actual_expiration, 0)
        self.assertLess(actual_expect_diff, self.hour_in_milliseconds)
        check_cost(self, result, 1, 2, 0, 0)
        # put a row with the same primary key to update the row, operation
        # should fail, and return the existing row
        result = self.handle.put(self.put_request)
        self.assertIsNone(result.get_version())
        self.assertIsNone(result.get_generated_value())
        self.assertEqual(result.get_existing_version().get_bytes(),
                         version.get_bytes())
        self.assertEqual(result.get_existing_value(), self.row)
        check_cost(self, result, 1, 2, 0, 0)

    def testPutIfPresent(self):
        # test PutIfPresent with normal values, operation should fail because
        # there is no existing row in store
        self.put_request.set_option(PutOption.IF_PRESENT)
        result = self.handle.put(self.put_request)
        self.assertIsNone(result.get_version())
        self.assertIsNone(result.get_generated_value())
        self.assertIsNone(result.get_existing_version())
        self.assertIsNone(result.get_existing_value())
        check_cost(self, result, 1, 2, 0, 0)
        # insert a row
        self.put_request.set_option(PutOption.IF_ABSENT).set_ttl(self.ttl)
        self.handle.put(self.put_request)
        expect_expiration = self.ttl.to_expiration_time(
            int(round(time() * 1000)))
        # test PutIfPresent with normal values, operation should succeed
        self.row.update({'fld_long': 2147483649})
        self.put_request.set_value(self.row).set_option(
            PutOption.IF_PRESENT).set_return_row(True)
        result = self.handle.put(self.put_request)
        version = result.get_version()
        self.assertIsNotNone(version)
        self.assertIsNone(result.get_generated_value())
        self.assertIsNone(result.get_existing_version())
        self.assertIsNone(result.get_existing_value())
        check_cost(self, result, 1, 2, 2, 2)
        result = self.handle.get(self.get_request)
        self.assertEqual(result.get_value(), self.row)
        self.assertEqual(result.get_version().get_bytes(), version.get_bytes())
        actual_expiration = result.get_expiration_time()
        actual_expect_diff = actual_expiration - expect_expiration
        self.assertGreater(actual_expiration, 0)
        self.assertLess(actual_expect_diff, self.hour_in_milliseconds)
        check_cost(self, result, 1, 2, 0, 0)
        # test PutIfPresent with normal values, update the ttl with table
        # default ttl
        self.put_request.set_ttl(None).set_use_table_default_ttl(True)
        result = self.handle.put(self.put_request)
        tb_expect_expiration = table_ttl.to_expiration_time(
            int(round(time() * 1000)))
        version = result.get_version()
        self.assertIsNotNone(version)
        self.assertIsNone(result.get_generated_value())
        self.assertIsNone(result.get_existing_version())
        self.assertIsNone(result.get_existing_value())
        check_cost(self, result, 1, 2, 2, 2)
        result = self.handle.get(self.get_request)
        self.assertEqual(result.get_value(), self.row)
        self.assertEqual(result.get_version().get_bytes(), version.get_bytes())
        actual_expiration = result.get_expiration_time()
        actual_expect_diff = actual_expiration - tb_expect_expiration
        self.assertGreater(actual_expiration, 0)
        self.assertLess(actual_expect_diff, self.day_in_milliseconds)
        check_cost(self, result, 1, 2, 0, 0)

    def testPutIfVersion(self):
        # insert a row
        result = self.handle.put(self.put_request)
        version_old = result.get_version()
        # test PutIfVersion with normal values, operation should succeed
        self.row.update({'fld_bool': False})
        self.put_request.set_value(self.row).set_ttl(
            self.ttl).set_match_version(version_old).set_return_row(True)
        result = self.handle.put(self.put_request)
        expect_expiration = self.ttl.to_expiration_time(
            int(round(time() * 1000)))
        version = result.get_version()
        self.assertIsNotNone(version)
        self.assertIsNone(result.get_generated_value())
        self.assertIsNone(result.get_existing_version())
        self.assertIsNone(result.get_existing_value())
        check_cost(self, result, 1, 2, 2, 2)
        result = self.handle.get(self.get_request)
        self.assertEqual(result.get_value(), self.row)
        self.assertEqual(result.get_version().get_bytes(), version.get_bytes())
        actual_expiration = result.get_expiration_time()
        actual_expect_diff = actual_expiration - expect_expiration
        self.assertGreater(actual_expiration, 0)
        self.assertLess(actual_expect_diff, self.hour_in_milliseconds)
        check_cost(self, result, 1, 2, 0, 0)
        # test PutIfVersion with normal values, operation should fail because
        # version not match, and return the existing row
        self.put_request.set_ttl(None).set_use_table_default_ttl(True)
        result = self.handle.put(self.put_request)
        self.assertIsNone(result.get_version())
        self.assertIsNone(result.get_generated_value())
        self.assertEqual(result.get_existing_version().get_bytes(),
                         version.get_bytes())
        self.assertEqual(result.get_existing_value(), self.row)
        check_cost(self, result, 1, 2, 0, 0)

    def testPutWithExactMatch(self):
        # test put a row with an extra field not in the table, by default this
        # will succeed
        row = deepcopy(self.row)
        row.update({'fld_id': 2, 'extra': 5})
        key = {'fld_id': 2}
        self.row.update({'fld_id': 2})
        self.put_request.set_value(row)
        result = self.handle.put(self.put_request)
        tb_expect_expiration = table_ttl.to_expiration_time(
            int(round(time() * 1000)))
        version = result.get_version()
        self.assertIsNotNone(version)
        self.assertIsNone(result.get_generated_value())
        self.assertIsNone(result.get_existing_version())
        self.assertIsNone(result.get_existing_value())
        check_cost(self, result, 0, 0, 1, 1)
        self.get_request.set_key(key)
        result = self.handle.get(self.get_request)
        self.assertEqual(result.get_value(), self.row)
        self.assertEqual(result.get_version().get_bytes(), version.get_bytes())
        actual_expiration = result.get_expiration_time()
        actual_expect_diff = actual_expiration - tb_expect_expiration
        self.assertGreater(actual_expiration, 0)
        self.assertLess(actual_expect_diff, self.day_in_milliseconds)
        check_cost(self, result, 1, 2, 0, 0)
        # test put a row with an extra field not in the table, this will fail
        # because it's not an exact match when we set exact_match=True
        self.put_request.set_exact_match(True)
        self.assertRaises(IllegalArgumentException, self.handle.put,
                          self.put_request)

    def testPutWithIdentityColumn(self):
        id_table = table_prefix + 'Identity'
        create_request = TableRequest().set_statement(
            'CREATE TABLE ' + id_table + '(sid INTEGER, id LONG GENERATED \
ALWAYS AS IDENTITY, name STRING, PRIMARY KEY(SHARD(sid), id))')
        create_request.set_table_limits(TableLimits(5000, 5000, 50))
        TestBase.table_request(create_request, State.ACTIVE)

        # test put a row with an extra field not in the table, by default this
        # will succeed
        row = {'sid': 1, 'name': 'myname', 'extra': 'extra'}
        key = {'sid': 1, 'id': 1}
        get_row = {'sid': 1, 'id': 1, 'name': 'myname'}
        self.put_request.set_table_name(id_table).set_value(row)
        result = self.handle.put(self.put_request)
        version = result.get_version()
        self.assertIsNotNone(version)
        self.assertIsNotNone(result.get_generated_value())
        self.assertIsNone(result.get_existing_version())
        self.assertIsNone(result.get_existing_value())
        check_cost(self, result, 0, 0, 1, 1)
        self.get_request.set_table_name(id_table).set_key(key)
        result = self.handle.get(self.get_request)
        self.assertEqual(result.get_value(), get_row)
        self.assertEqual(result.get_version().get_bytes(), version.get_bytes())
        self.assertEqual(result.get_expiration_time(), 0)
        check_cost(self, result, 1, 2, 0, 0)
        # test put a row with identity field, this will fail because id is
        # 'generated always' and in that path it is not legal to provide a value
        # for id
        row.update({'id': 1})
        self.assertRaises(IllegalArgumentException, self.handle.put,
                          self.put_request)


if __name__ == '__main__':
    unittest.main()
