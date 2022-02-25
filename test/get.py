#
# Copyright (c) 2018, 2022 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#

import unittest
from time import time

from borneo import (
    Consistency, GetRequest, IllegalArgumentException, PutRequest, TableLimits,
    TableNotFoundException, TableRequest, TimeToLive, TimeUnit)
from parameters import table_name, timeout
from test_base import TestBase
from testutils import get_row


class TestGet(unittest.TestCase, TestBase):
    @classmethod
    def setUpClass(cls):
        cls.set_up_class()
        table_ttl = TimeToLive.of_hours(16)
        create_statement = (
            'CREATE TABLE ' + table_name + '(fld_sid INTEGER, fld_id INTEGER, \
fld_long LONG, fld_float FLOAT, fld_double DOUBLE, fld_bool BOOLEAN, \
fld_str STRING, fld_bin BINARY, fld_time TIMESTAMP(7), fld_num NUMBER, \
fld_json JSON, fld_arr ARRAY(STRING), fld_map MAP(STRING), \
fld_rec RECORD(fld_id LONG, fld_bool BOOLEAN, fld_str STRING), \
PRIMARY KEY(SHARD(fld_sid), fld_id)) USING TTL ' + str(table_ttl))
        create_request = TableRequest().set_statement(
            create_statement).set_table_limits(TableLimits(100, 100, 1))
        cls.table_request(create_request)
        global row, tb_expect_expiration, version
        row = get_row()
        put_request = PutRequest().set_value(row).set_table_name(table_name)
        version = cls.handle.put(put_request).get_version()
        tb_expect_expiration = table_ttl.to_expiration_time(
            int(round(time() * 1000)))
        global serial_version
        serial_version = cls.handle.get_client().serial_version

    @classmethod
    def tearDownClass(cls):
        cls.tear_down_class()

    def setUp(self):
        self.set_up()
        self.key = {'fld_sid': 1, 'fld_id': 1}
        self.get_request = GetRequest().set_key(self.key).set_table_name(
            table_name).set_timeout(timeout)

    def tearDown(self):
        self.tear_down()

    def testGetSetIllegalKey(self):
        self.assertRaises(IllegalArgumentException, self.get_request.set_key,
                          'IllegalKey')
        self.get_request.set_key({'fld_sid': 1})
        self.assertRaises(IllegalArgumentException, self.handle.get,
                          self.get_request)
        self.get_request.set_key({'fld_id': 1})
        self.assertRaises(IllegalArgumentException, self.handle.get,
                          self.get_request)

    def testGetSetIllegalKeyFromJson(self):
        self.assertRaises(ValueError, self.get_request.set_key_from_json,
                          'IllegalJson')
        self.get_request.set_key_from_json('{"invalid_field": "key"}')
        self.assertRaises(IllegalArgumentException, self.handle.get,
                          self.get_request)

    def testGetSetIllegalTableName(self):
        self.assertRaises(IllegalArgumentException,
                          self.get_request.set_table_name, {'name': table_name})
        self.get_request.set_table_name('IllegalTable')
        self.assertRaises(TableNotFoundException, self.handle.get,
                          self.get_request)

    def testGetSetIllegalCompartment(self):
        self.assertRaises(IllegalArgumentException,
                          self.get_request.set_compartment, {})
        self.assertRaises(IllegalArgumentException,
                          self.get_request.set_compartment, '')

    def testGetSetIllegalConsistency(self):
        self.assertRaises(IllegalArgumentException,
                          self.get_request.set_consistency,
                          'IllegalConsistency')

    def testGetSetIllegalTimeout(self):
        self.assertRaises(IllegalArgumentException,
                          self.get_request.set_timeout, 'IllegalTimeout')
        self.assertRaises(IllegalArgumentException,
                          self.get_request.set_timeout, 0)
        self.assertRaises(IllegalArgumentException,
                          self.get_request.set_timeout, -1)

    def testGetWithoutKey(self):
        self.get_request.set_key(None)
        self.assertRaises(IllegalArgumentException, self.handle.get,
                          self.get_request)

    def testGetGets(self):
        self.assertEqual(self.get_request.get_key(), self.key)
        self.assertIsNone(self.get_request.get_compartment())

    def testGetIllegalRequest(self):
        self.assertRaises(IllegalArgumentException, self.handle.get,
                          'IllegalRequest')

    def testGetNormal(self):
        result = self.handle.get(self.get_request)
        self.check_get_result(result, row, version, tb_expect_expiration,
                              TimeUnit.HOURS, True, (serial_version > 2))
        self.check_cost(result, 1, 2, 0, 0)

    def testGetEventual(self):
        self.get_request.set_consistency(Consistency.EVENTUAL)
        result = self.handle.get(self.get_request)
        self.check_get_result(
            result, row, expect_expiration=tb_expect_expiration,
            timeunit=TimeUnit.HOURS, ver_eq=False,
            mod_time_recent=(serial_version > 2))
        self.check_cost(result, 1, 1, 0, 0)

    def testGetNonExisting(self):
        self.get_request.set_key({'fld_sid': 2, 'fld_id': 2})
        result = self.handle.get(self.get_request)
        self.check_get_result(result)
        self.check_cost(result, 1, 2, 0, 0)


if __name__ == '__main__':
    unittest.main()
