#
# Copyright (c) 2018, 2022 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#

import unittest

from borneo import (
    DeleteRequest, GetRequest, IllegalArgumentException, PutRequest,
    TableLimits, TableNotFoundException, TableRequest, TimeToLive, TimeUnit)
from parameters import table_name, timeout
from test_base import TestBase
from testutils import get_row
from time import time


class TestDelete(unittest.TestCase, TestBase):
    @classmethod
    def setUpClass(cls):
        cls.set_up_class()
        global table_ttl
        table_ttl = TimeToLive.of_days(2)
        create_statement = (
            'CREATE TABLE ' + table_name + '(fld_id INTEGER, fld_long LONG, \
fld_float FLOAT, fld_double DOUBLE, fld_bool BOOLEAN, fld_str STRING, \
fld_bin BINARY, fld_time TIMESTAMP(6), fld_num NUMBER, fld_json JSON, \
fld_arr ARRAY(STRING), fld_map MAP(STRING), \
fld_rec RECORD(fld_id LONG, fld_bool BOOLEAN, fld_str STRING), \
PRIMARY KEY(fld_id)) USING TTL ' + str(table_ttl))
        create_request = TableRequest().set_statement(
            create_statement).set_table_limits(TableLimits(100, 100, 1))
        cls.table_request(create_request)
        global serial_version
        serial_version = cls.handle.get_client().serial_version

    @classmethod
    def tearDownClass(cls):
        cls.tear_down_class()

    def setUp(self):
        self.set_up()
        self.row = get_row(with_sid=False)
        self.key = {'fld_id': 1}
        self.put_request = PutRequest().set_value(self.row).set_table_name(
            table_name)
        self.version = self.handle.put(self.put_request).get_version()
        self.get_request = GetRequest().set_key(self.key).set_table_name(
            table_name)
        self.delete_request = DeleteRequest().set_key(self.key).set_table_name(
            table_name).set_timeout(timeout)

    def tearDown(self):
        self.tear_down()

    def testDeleteSetIllegalKey(self):
        self.assertRaises(IllegalArgumentException, self.delete_request.set_key,
                          'IllegalKey')

    def testDeleteSetIllegalKeyFromJson(self):
        self.assertRaises(ValueError, self.delete_request.set_key_from_json,
                          'IllegalJson')
        self.delete_request.set_key_from_json('{"invalid_field": "key"}')
        self.assertRaises(IllegalArgumentException, self.handle.delete,
                          self.delete_request)

    def testDeleteSetIllegalMatchVersion(self):
        self.assertRaises(IllegalArgumentException,
                          self.delete_request.set_match_version,
                          'IllegalMatchVersion')

    def testDeleteSetIllegalTimeout(self):
        self.assertRaises(IllegalArgumentException,
                          self.delete_request.set_timeout, 'IllegalTimeout')
        self.assertRaises(IllegalArgumentException,
                          self.delete_request.set_timeout, 0)
        self.assertRaises(IllegalArgumentException,
                          self.delete_request.set_timeout, -1)

    def testDeleteSetIllegalTableName(self):
        self.assertRaises(IllegalArgumentException,
                          self.delete_request.set_table_name,
                          {'name': table_name})
        self.delete_request.set_table_name('IllegalTable')
        self.assertRaises(TableNotFoundException, self.handle.delete,
                          self.delete_request)

    def testDeleteSetIllegalCompartment(self):
        self.assertRaises(IllegalArgumentException,
                          self.delete_request.set_compartment, {})
        self.assertRaises(IllegalArgumentException,
                          self.delete_request.set_compartment, '')

    def testDeleteSetIllegalReturnRow(self):
        self.assertRaises(IllegalArgumentException,
                          self.delete_request.set_return_row,
                          'IllegalReturnRow')

    def testDeleteWithoutKey(self):
        self.delete_request.set_key(None)
        self.assertRaises(IllegalArgumentException, self.handle.delete,
                          self.delete_request)

    def testDeleteGets(self):
        self.delete_request.set_match_version(
            self.version).set_return_row(True)
        self.assertEqual(self.delete_request.get_key(), self.key)
        self.assertIsNone(self.delete_request.get_compartment())
        self.assertEqual(self.delete_request.get_match_version(), self.version)
        self.assertEqual(self.delete_request.get_timeout(), timeout)
        self.assertEqual(self.delete_request.get_table_name(), table_name)
        self.assertTrue(self.delete_request.get_return_row())

    def testDeleteIllegalRequest(self):
        self.assertRaises(IllegalArgumentException, self.handle.delete,
                          'IllegalRequest')

    def testDeleteNormal(self):
        self.delete_request.set_return_row(True)
        result = self.handle.delete(self.delete_request)
        self._check_delete_result(result)
        self.check_cost(result, 1, 2, 1, 1)
        result = self.handle.get(self.get_request)
        self.check_get_result(result)
        self.check_cost(result, 1, 2, 0, 0)

    def testDeleteNonExisting(self):
        self.delete_request.set_key({'fld_id': 2})
        result = self.handle.delete(self.delete_request)
        self._check_delete_result(result, False)
        self.check_cost(result, 1, 2, 0, 0)

    def testDeleteIfVersion(self):
        self.row['fld_long'] = 2147483649
        self.put_request.set_value(self.row)
        version = self.handle.put(self.put_request).get_version()
        tb_expect_expiration = table_ttl.to_expiration_time(
            int(round(time() * 1000)))
        # delete failed because version not match
        self.delete_request.set_match_version(self.version)
        result = self.handle.delete(self.delete_request)
        self._check_delete_result(result, False)
        self.check_cost(result, 1, 2, 0, 0)
        result = self.handle.get(self.get_request)
        self.check_get_result(result, self.row, version, tb_expect_expiration,
                              TimeUnit.DAYS, True, (serial_version > 2))
        self.check_cost(result, 1, 2, 0, 0)
        # delete succeed when version match
        self.delete_request.set_match_version(version)
        result = self.handle.delete(self.delete_request)
        self._check_delete_result(result)
        self.check_cost(result, 1, 2, 1, 1)
        result = self.handle.get(self.get_request)
        self.check_get_result(result)
        self.check_cost(result, 1, 2, 0, 0)

    def testDeleteIfVersionWithReturnRow(self):
        self.row['fld_long'] = 2147483649
        self.put_request.set_value(self.row)
        version = self.handle.put(self.put_request).get_version()
        tb_expect_expiration = table_ttl.to_expiration_time(
            int(round(time() * 1000)))
        # delete failed because version not match
        self.delete_request.set_match_version(
            self.version).set_return_row(True)
        result = self.handle.delete(self.delete_request)
        self._check_delete_result(result, False, self.row, version.get_bytes())
        self.check_cost(result, 1, 2, 0, 0)
        result = self.handle.get(self.get_request)
        self.check_get_result(result, self.row, version, tb_expect_expiration,
                              TimeUnit.DAYS, True, (serial_version > 2))
        self.check_cost(result, 1, 2, 0, 0)
        # delete succeed when version match
        self.delete_request.set_match_version(version)
        result = self.handle.delete(self.delete_request)
        self._check_delete_result(result)
        self.check_cost(result, 1, 2, 1, 1)
        result = self.handle.get(self.get_request)
        self.check_get_result(result)
        self.check_cost(result, 1, 2, 0, 0)

    def _check_delete_result(self, result, success=True, value=None,
                             version=None):
        # check whether success
        self.assertEqual(result.get_success(), success)
        # check existing value
        self.assertEqual(result.get_existing_value(), value)
        # check existing version
        ver = result.get_existing_version()
        (self.assertIsNone(ver) if version is None
         else self.assertEqual(ver.get_bytes(), version))


if __name__ == '__main__':
    unittest.main()
