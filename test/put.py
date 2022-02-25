#
# Copyright (c) 2018, 2022 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#

import unittest
from collections import OrderedDict
from copy import deepcopy
from dateutil import tz
from parameters import table_prefix
from time import time

from borneo import (
    DeleteRequest, Durability, GetRequest, IllegalArgumentException, IllegalStateException,
    PutOption, PutRequest, RequestSizeLimitException, TableLimits,
    TableNotFoundException, TableRequest, TimeToLive, TimeUnit)
from parameters import is_onprem, table_name, tenant_id, timeout
from test_base import TestBase
from testutils import get_row


class TestPut(unittest.TestCase, TestBase):

    @classmethod
    def setUpClass(cls):
        cls.set_up_class()
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
            create_statement).set_table_limits(TableLimits(50, 50, 1))
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
        dur = Durability(Durability.SYNC_POLICY.SYNC,
                         Durability.SYNC_POLICY.SYNC,
                         Durability.REPLICA_ACK_POLICY.SIMPLE_MAJORITY)
        self.put_request = PutRequest().set_value(self.row).set_table_name(
            table_name).set_timeout(timeout).set_durability(dur)
        self.get_request = GetRequest().set_key(self.key).set_table_name(
            table_name)
        self.ttl = TimeToLive.of_hours(24)
        self.hour_in_milliseconds = 60 * 60 * 1000
        self.day_in_milliseconds = 24 * 60 * 60 * 1000

    def tearDown(self):
        request = DeleteRequest().set_key(self.key).set_table_name(table_name)
        self.handle.delete(request)
        self.tear_down()

    def testPutSetIllegalValue(self):
        self.assertRaises(IllegalArgumentException, self.put_request.set_value,
                          'IllegalValue')

    def testPutSetIllegalValueFromJson(self):
        self.assertRaises(ValueError, self.put_request.set_value_from_json,
                          'IllegalJson')
        self.put_request.set_value_from_json('{"invalid_field": "value"}')
        self.assertRaises(IllegalArgumentException, self.handle.put,
                          self.put_request)

    def testPutSetIllegalCompartment(self):
        self.assertRaises(IllegalArgumentException,
                          self.put_request.set_compartment, {})
        self.assertRaises(IllegalArgumentException,
                          self.put_request.set_compartment, '')

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

    def testPutSetLargeSizeValue(self):
        self.row['fld_str'] = self.get_random_str(2)
        self.put_request.set_value(self.row)
        if is_onprem():
            version = self.handle.put(self.put_request).get_version()
            self.assertIsNotNone(version)
        else:
            self.assertRaises(RequestSizeLimitException, self.handle.put,
                              self.put_request)

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
        self.assertEqual(self.put_request.get_compartment(), tenant_id)
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
        self._check_put_result(result)
        self.check_cost(result, 0, 0, 1, 1)
        result = self.handle.get(self.get_request)
        self.check_get_result(result, self.row, version, tb_expect_expiration,
                              TimeUnit.DAYS, True, (serial_version > 2))
        self.check_cost(result, 1, 2, 0, 0)
        # put a row with the same primary key to update the row
        self.row['fld_time'] = (
            self.row['fld_time'].replace(tzinfo=tz.gettz('EST')))
        self.put_request.set_value(self.row).set_ttl(self.ttl)
        # the replace(tzinfo=None) at the end makes the object a "naive"
        # datetime. Without that there is a datetime comparison problem in
        # check_get_result()
        self.row['fld_time'] = (
            self.row['fld_time'].astimezone(tz.UTC).replace(tzinfo=None))
        result = self.handle.put(self.put_request)
        expect_expiration = self.ttl.to_expiration_time(
            int(round(time() * 1000)))
        version = result.get_version()
        self._check_put_result(result)
        self.check_cost(result, 0, 0, 2, 2)
        result = self.handle.get(self.get_request)
        self.check_get_result(result, self.row, version, expect_expiration,
                              TimeUnit.HOURS, True, (serial_version > 2))
        self.check_cost(result, 1, 2, 0, 0)
        # update the ttl of the row to never expire
        self.put_request.set_ttl(TimeToLive.of_days(0))
        result = self.handle.put(self.put_request)
        version = result.get_version()
        self._check_put_result(result)
        self.check_cost(result, 0, 0, 2, 2)
        result = self.handle.get(self.get_request)
        self.check_get_result(result, self.row, version,
                              0, None, True, (serial_version > 2))
        self.check_cost(result, 1, 2, 0, 0)

    def testPutIfAbsent(self):
        # test PutIfAbsent with normal values
        self.put_request.set_option(PutOption.IF_ABSENT).set_ttl(
            self.ttl).set_return_row(True)
        result = self.handle.put(self.put_request)
        expect_expiration = self.ttl.to_expiration_time(
            int(round(time() * 1000)))
        version = result.get_version()
        self._check_put_result(result)
        self.check_cost(result, 1, 2, 1, 1)
        result = self.handle.get(self.get_request)
        self.check_get_result(result, self.row, version, expect_expiration,
                              TimeUnit.HOURS, True, (serial_version > 2))
        self.check_cost(result, 1, 2, 0, 0)
        # put a row with the same primary key to update the row, operation
        # should fail, and return the existing row
        result = self.handle.put(self.put_request)
        self._check_put_result(result, False, existing_version=version,
                               existing_value=self.row)
        self.check_cost(result, 1, 2, 0, 0)

    def testPutIfPresent(self):
        # test PutIfPresent with normal values, operation should fail because
        # there is no existing row in store
        self.put_request.set_option(PutOption.IF_PRESENT)
        result = self.handle.put(self.put_request)
        self._check_put_result(result, False)
        self.check_cost(result, 1, 2, 0, 0)
        # insert a row
        self.put_request.set_option(PutOption.IF_ABSENT).set_ttl(self.ttl)
        self.handle.put(self.put_request)
        expect_expiration = self.ttl.to_expiration_time(
            int(round(time() * 1000)))
        # test PutIfPresent with normal values, operation should succeed
        self.row['fld_long'] = 2147483649
        self.put_request.set_value(self.row).set_option(
            PutOption.IF_PRESENT).set_return_row(True)
        result = self.handle.put(self.put_request)
        version = result.get_version()
        self._check_put_result(result)
        self.check_cost(result, 1, 2, 2, 2)
        result = self.handle.get(self.get_request)
        self.check_get_result(result, self.row, version, expect_expiration,
                              TimeUnit.HOURS, True, (serial_version > 2))
        self.check_cost(result, 1, 2, 0, 0)
        # test PutIfPresent with normal values, update the ttl with table
        # default ttl
        self.put_request.set_ttl(None).set_use_table_default_ttl(True)
        result = self.handle.put(self.put_request)
        tb_expect_expiration = table_ttl.to_expiration_time(
            int(round(time() * 1000)))
        version = result.get_version()
        self._check_put_result(result)
        self.check_cost(result, 1, 2, 2, 2)
        result = self.handle.get(self.get_request)
        self.check_get_result(result, self.row, version, tb_expect_expiration,
                              TimeUnit.DAYS, True, (serial_version > 2))
        self.check_cost(result, 1, 2, 0, 0)

    def testPutIfVersion(self):
        # insert a row
        result = self.handle.put(self.put_request)
        version_old = result.get_version()
        # test PutIfVersion with normal values, operation should succeed
        self.row['fld_bool'] = False
        self.put_request.set_value(self.row).set_ttl(
            self.ttl).set_match_version(version_old).set_return_row(True)
        result = self.handle.put(self.put_request)
        expect_expiration = self.ttl.to_expiration_time(
            int(round(time() * 1000)))
        version = result.get_version()
        self._check_put_result(result)
        self.check_cost(result, 1, 2, 2, 2)
        result = self.handle.get(self.get_request)
        self.check_get_result(result, self.row, version, expect_expiration,
                              TimeUnit.HOURS, True, (serial_version > 2))
        self.check_cost(result, 1, 2, 0, 0)
        # test PutIfVersion with normal values, operation should fail because
        # version not match, and return the existing row
        self.put_request.set_ttl(None).set_use_table_default_ttl(True)
        result = self.handle.put(self.put_request)
        self._check_put_result(result, False, existing_version=version,
                               existing_value=self.row)
        self.check_cost(result, 1, 2, 0, 0)

    def testPutWithExactMatch(self):
        # test put a row with an extra field not in the table, by default this
        # will succeed
        row = deepcopy(self.row)
        row.update({'fld_id': 2, 'extra': 5})
        key = {'fld_id': 2}
        self.row['fld_id'] = 2
        self.put_request.set_value(row)
        result = self.handle.put(self.put_request)
        tb_expect_expiration = table_ttl.to_expiration_time(
            int(round(time() * 1000)))
        version = result.get_version()
        self._check_put_result(result)
        self.check_cost(result, 0, 0, 1, 1)
        self.get_request.set_key(key)
        result = self.handle.get(self.get_request)
        self.check_get_result(result, self.row, version, tb_expect_expiration,
                              TimeUnit.DAYS, True, (serial_version > 2))
        self.check_cost(result, 1, 2, 0, 0)
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
        create_request.set_table_limits(TableLimits(50, 50, 1))
        self.table_request(create_request)

        # test put a row with an extra field not in the table, by default this
        # will succeed
        row = {'name': 'myname', 'extra': 'extra', 'sid': 1}
        key = {'sid': 1, 'id': 1}
        expected = OrderedDict()
        expected['sid'] = 1
        expected['id'] = 1
        expected['name'] = 'myname'
        self.put_request.set_table_name(id_table).set_value(row)
        result = self.handle.put(self.put_request)
        version = result.get_version()
        self._check_put_result(result, has_generated_value=True)
        self.check_cost(result, 0, 0, 1, 1)
        self.get_request.set_table_name(id_table).set_key(key)
        result = self.handle.get(self.get_request)
        self.check_get_result(result, expected, version,
                              0, None, True, (serial_version > 2))
        self.check_cost(result, 1, 2, 0, 0)
        # test put a row with identity field, this will fail because id is
        # 'generated always' and in that path it is not legal to provide a value
        # for id
        row['id'] = 1
        self.assertRaises(IllegalArgumentException, self.handle.put,
                          self.put_request)

    def _check_put_result(self, result, has_version=True,
                          has_generated_value=False, existing_version=None,
                          existing_value=None):
        # check version
        version = result.get_version()
        (self.assertIsNotNone(version) if has_version
         else self.assertIsNone(version))
        # check generated_value
        generated_value = result.get_generated_value()
        (self.assertIsNotNone(generated_value) if has_generated_value
         else self.assertIsNone(generated_value))
        # check existing version
        ver = result.get_existing_version()
        (self.assertIsNone(ver) if existing_version is None
         else self.assertEqual(ver.get_bytes(), existing_version.get_bytes()))
        # check existing value
        self.assertEqual(result.get_existing_value(), existing_value)


if __name__ == '__main__':
    unittest.main()
