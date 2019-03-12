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
from time import sleep

from borneo import (
    DeleteRequest, GetRequest, IllegalArgumentException, PutRequest, State,
    TableLimits, TableNotFoundException, TableRequest)
from parameters import protocol, table_name, tenant_id, timeout, wait_timeout
from testutils import add_test_tier_tenant, delete_test_tier_tenant, get_handle


class TestDelete(unittest.TestCase):
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
            'CREATE TABLE ' + table_name + '(fld_id INTEGER, fld_long LONG, \
fld_float FLOAT, fld_double DOUBLE, fld_bool BOOLEAN, fld_str STRING, \
fld_bin BINARY, fld_time TIMESTAMP(6), fld_num NUMBER, fld_json JSON, \
fld_arr ARRAY(STRING), fld_map MAP(STRING), \
fld_rec RECORD(fld_id LONG, fld_bool BOOLEAN, fld_str STRING), \
PRIMARY KEY(fld_id)) USING TTL 2 DAYS')
        create_request = TableRequest().set_statement(
            create_statement).set_table_limits(TableLimits(5000, 5000, 50))
        cls._result = cls._handle.table_request(create_request)
        cls._result.wait_for_state(cls._handle, table_name, State.ACTIVE,
                                   wait_timeout, 1000)
        global hour_in_milliseconds
        hour_in_milliseconds = 60 * 60 * 1000

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
            table_name)
        self.version = self.handle.put(self.put_request).get_version()
        self.get_request = GetRequest().set_key(self.key).set_table_name(
            table_name)
        self.delete_request = DeleteRequest().set_key(self.key).set_table_name(
            table_name).set_timeout(timeout)

    def tearDown(self):
        self.handle.close()

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
        self.assertTrue(result.get_success())
        self.assertIsNone(result.get_existing_value())
        self.assertIsNone(result.get_existing_version())
        self.assertEqual(result.get_read_kb(), 1)
        self.assertEqual(result.get_read_units(), 2)
        self.assertEqual(result.get_write_kb(), 1)
        self.assertEqual(result.get_write_units(), 1)
        result = self.handle.get(self.get_request)
        self.assertIsNone(result.get_value())
        self.assertIsNone(result.get_version())
        self.assertEqual(result.get_expiration_time(), 0)
        self.assertEqual(result.get_read_kb(), 1)
        self.assertEqual(result.get_read_units(), 2)
        self.assertEqual(result.get_write_kb(), 0)
        self.assertEqual(result.get_write_units(), 0)

    def testDeleteNonExisting(self):
        self.delete_request.set_key({'fld_id': 2})
        result = self.handle.delete(self.delete_request)
        self.assertFalse(result.get_success())
        self.assertIsNone(result.get_existing_value())
        self.assertIsNone(result.get_existing_version())
        self.assertEqual(result.get_read_kb(), 1)
        self.assertEqual(result.get_read_units(), 2)
        self.assertEqual(result.get_write_kb(), 0)
        self.assertEqual(result.get_write_units(), 0)

    def testDeleteIfVersion(self):
        self.row.update({'fld_long': 2147483649})
        self.put_request.set_value(self.row)
        version = self.handle.put(self.put_request).get_version()
        # delete failed because version not match
        self.delete_request.set_match_version(self.version)
        result = self.handle.delete(self.delete_request)
        self.assertFalse(result.get_success())
        self.assertIsNone(result.get_existing_value())
        self.assertIsNone(result.get_existing_version())
        self.assertEqual(result.get_read_kb(), 1)
        self.assertEqual(result.get_read_units(), 2)
        self.assertEqual(result.get_write_kb(), 0)
        self.assertEqual(result.get_write_units(), 0)
        result = self.handle.get(self.get_request)
        self.assertEqual(result.get_value(), self.row)
        self.assertEqual(result.get_version().get_bytes(), version.get_bytes())
        self.assertNotEqual(result.get_expiration_time(), 0)
        self.assertEqual(result.get_read_kb(), 1)
        self.assertEqual(result.get_read_units(), 2)
        self.assertEqual(result.get_write_kb(), 0)
        self.assertEqual(result.get_write_units(), 0)
        # delete succeed when version match
        self.delete_request.set_match_version(version)
        result = self.handle.delete(self.delete_request)
        self.assertTrue(result.get_success())
        self.assertIsNone(result.get_existing_value())
        self.assertIsNone(result.get_existing_version())
        self.assertEqual(result.get_read_kb(), 1)
        self.assertEqual(result.get_read_units(), 2)
        self.assertEqual(result.get_write_kb(), 1)
        self.assertEqual(result.get_write_units(), 1)
        result = self.handle.get(self.get_request)
        self.assertIsNone(result.get_value())
        self.assertIsNone(result.get_version())
        self.assertEqual(result.get_expiration_time(), 0)
        self.assertEqual(result.get_read_kb(), 1)
        self.assertEqual(result.get_read_units(), 2)
        self.assertEqual(result.get_write_kb(), 0)
        self.assertEqual(result.get_write_units(), 0)

    def testDeleteIfVersionWithReturnRow(self):
        self.row.update({'fld_long': 2147483649})
        self.put_request.set_value(self.row)
        version = self.handle.put(self.put_request).get_version()
        # delete failed because version not match
        self.delete_request.set_match_version(
            self.version).set_return_row(True)
        result = self.handle.delete(self.delete_request)
        self.assertFalse(result.get_success())
        self.assertEqual(result.get_existing_value(), self.row)
        self.assertEqual(result.get_existing_version().get_bytes(),
                         version.get_bytes())
        self.assertEqual(result.get_read_kb(), 1)
        self.assertEqual(result.get_read_units(), 2)
        self.assertEqual(result.get_write_kb(), 0)
        self.assertEqual(result.get_write_units(), 0)
        result = self.handle.get(self.get_request)
        self.assertEqual(result.get_value(), self.row)
        self.assertEqual(result.get_version().get_bytes(), version.get_bytes())
        self.assertNotEqual(result.get_expiration_time(), 0)
        self.assertEqual(result.get_read_kb(), 1)
        self.assertEqual(result.get_read_units(), 2)
        self.assertEqual(result.get_write_kb(), 0)
        self.assertEqual(result.get_write_units(), 0)
        # delete succeed when version match
        self.delete_request.set_match_version(version)
        result = self.handle.delete(self.delete_request)
        self.assertTrue(result.get_success())
        self.assertIsNone(result.get_existing_value())
        self.assertIsNone(result.get_existing_version())
        self.assertEqual(result.get_read_kb(), 1)
        self.assertEqual(result.get_read_units(), 2)
        self.assertEqual(result.get_write_kb(), 1)
        self.assertEqual(result.get_write_units(), 1)
        result = self.handle.get(self.get_request)
        self.assertIsNone(result.get_value())
        self.assertIsNone(result.get_version())
        self.assertEqual(result.get_expiration_time(), 0)
        self.assertEqual(result.get_read_kb(), 1)
        self.assertEqual(result.get_read_units(), 2)
        self.assertEqual(result.get_write_kb(), 0)
        self.assertEqual(result.get_write_units(), 0)


if __name__ == '__main__':
    unittest.main()
