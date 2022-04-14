#
# Copyright (c) 2018, 2022 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#

from random import choice
from string import ascii_letters, digits
from sys import version_info
from time import sleep, time
from unittest import TestCase

from borneo import ListTablesRequest, QueryResult, TableRequest, TimeUnit
from parameters import (
    is_cloudsim, is_onprem, is_pod, not_cloudsim, table_prefix, table_name,
    tenant_id, version, wait_timeout)
from testutils import (
    add_test_tier_tenant, compare_version, delete_test_tier_tenant, get_handle)


# noinspection PyPep8
class TestBase(object):
    handle = None
    HOUR_IN_MILLIS = 60 * 60 * 1000
    DAY_IN_MILLIS = 24 * 60 * 60 * 1000

    def __init__(self):
        self.handle = None

    def check_cost(self, result, read_kb, read_units, write_kb, write_units,
                   advance=False, multi_shards=False):
        assert isinstance(self, TestCase)
        if is_onprem():
            self.assertEqual(result.get_read_kb(), 0)
            self.assertEqual(result.get_read_units(), 0)
            self.assertEqual(result.get_write_kb(), 0)
            self.assertEqual(result.get_write_units(), 0)
        elif isinstance(result, QueryResult) and advance:
            self.assertGreater(result.get_read_kb(), read_kb)
            self.assertGreater(result.get_read_units(), read_units)
            self.assertEqual(result.get_write_kb(), write_kb)
            self.assertEqual(result.get_write_units(), write_units)
        elif isinstance(result, QueryResult) and multi_shards:
            self.assertGreaterEqual(result.get_read_kb(), read_kb)
            self.assertLessEqual(result.get_read_kb(), read_kb + 1)
            self.assertGreaterEqual(result.get_read_units(), read_units)
            self.assertLessEqual(result.get_read_units(), read_units + 2)
            self.assertEqual(result.get_write_kb(), write_kb)
            self.assertEqual(result.get_write_units(), write_units)
        else:
            self.assertEqual(result.get_read_kb(), read_kb)
            self.assertEqual(result.get_read_units(), read_units)
            self.assertEqual(result.get_write_kb(), write_kb)
            self.assertEqual(result.get_write_units(), write_units)

    def check_get_result(self, result, value=None, exp_version=None,
                         expect_expiration=0, timeunit=None, ver_eq=True,
                         mod_time_recent=False):
        assert isinstance(self, TestCase)
        # check value
        self.assertEqual(result.get_value(), value)
        # check version
        ver = result.get_version()
        if exp_version is None:
            self.assertIsNone(ver) if ver_eq else self.assertIsNotNone(ver)
        elif ver_eq:
            self.assertEqual(ver.get_bytes(), exp_version.get_bytes())
        else:
            self.assertNotEqual(ver.get_bytes(), exp_version.get_bytes())
        # check expiration time
        if expect_expiration == 0:
            self.assertEqual(result.get_expiration_time(), 0)
        else:
            actual_expiration = result.get_expiration_time()
            actual_expect_diff = actual_expiration - expect_expiration
            self.assertGreater(actual_expiration, 0)
            if timeunit == TimeUnit.HOURS:
                self.assertLess(actual_expect_diff, TestBase.HOUR_IN_MILLIS)
            else:
                self.assertLess(actual_expect_diff, TestBase.DAY_IN_MILLIS)
        modtime = result.get_modification_time()
        if mod_time_recent:
            now = round(time() * 1000)
            self.assertGreater(modtime, now - 10000)
        else:
            self.assertLess(modtime, 1)

    def check_query_result(self, result, num_records,
                           has_continuation_key=False, rec=None):
        assert isinstance(self, TestCase)
        records = result.get_results() if rec is None else rec
        # check number of the records
        self.assertEqual(len(records), num_records)
        # check continuation_key
        continuation_key = result.get_continuation_key()
        (self.assertIsNotNone(continuation_key) if has_continuation_key
         else self.assertIsNone(continuation_key))
        return records

    def check_system_result(self, result, state, has_operation_id=False,
                            has_result_string=False, statement=None):
        assert isinstance(self, TestCase)
        # check state
        self.assertEqual(result.get_operation_state(), state)
        # check operation id
        operation_id = result.get_operation_id()
        (self.assertIsNotNone(operation_id) if has_operation_id
         else self.assertIsNone(operation_id))
        # check result string
        result_string = result.get_result_string()
        if has_result_string:
            if version_info.major == 2:
                self.assertRegexpMatches(
                    result_string, '^{"namespaces" : \[("\w*"[, ]*)+\]}$')
            else:
                self.assertRegex(
                    result_string, '^{"namespaces" : \[("\w*"[, ]*)+\]}$')
        else:
            self.assertIsNone(result_string)
        # check statement
        self.assertEqual(result.get_statement(), statement)

    def check_table_result(self, result, state, table_limits=None,
                           has_schema=True, has_operation_id=True,
                           check_limit=True, check_schema=True,
                           check_operation_id=True):
        assert isinstance(self, TestCase)
        # check compartment id
        if (version is None or is_onprem() or
                version is not None and is_cloudsim() and
                compare_version(version, '1.4.0') >= 0):
            self.assertIsNone(result.get_compartment_id())
        else:
            self.assertEqual(result.get_compartment_id(), tenant_id)
        # check table name
        self.assertEqual(result.get_table_name(), table_name)
        # check state
        if isinstance(state, str):
            self.assertEqual(result.get_state(), state)
        else:
            self.assertTrue(result.get_state() in state)
        # check table limits
        if check_limit:
            if table_limits is None or is_onprem():
                self.assertIsNone(result.get_table_limits())
            else:
                self.assertEqual(result.get_table_limits().get_read_units(),
                                 table_limits.get_read_units())
                self.assertEqual(result.get_table_limits().get_write_units(),
                                 table_limits.get_write_units())
                self.assertEqual(result.get_table_limits().get_storage_gb(),
                                 table_limits.get_storage_gb())
                self.assertEqual(result.get_table_limits().get_mode(),
                                 table_limits.get_mode())
        # check table schema
        # TODO: For on-prem proxy, TableResult.get_schema() always return None,
        # This is a known bug, when it is fixed, the test should be change.
        if check_schema and not_cloudsim() and not is_onprem():
            (self.assertIsNotNone(result.get_schema()) if has_schema
             else self.assertIsNone(result.get_schema()))
        # check operation id
        if check_operation_id:
            operation_id = result.get_operation_id()
            (self.assertIsNotNone(operation_id) if has_operation_id
             else self.assertIsNone(operation_id))

    def set_up(self):
        self.handle = get_handle(tenant_id)

    def tear_down(self):
        self.handle.close()

    @staticmethod
    def get_random_str(mb):
        length = int(mb * 1024 * 1024)
        str_list = [choice(ascii_letters + digits) for _ in range(length)]
        return ''.join(str_list)

    @classmethod
    def set_up_class(cls):
        add_test_tier_tenant(tenant_id)
        cls.handle = get_handle(tenant_id)
        cls.drop_all_tables()

    @classmethod
    def tear_down_class(cls):
        try:
            cls.drop_all_tables()
        finally:
            cls.handle.close()
            delete_test_tier_tenant(tenant_id)

    @classmethod
    def drop_all_tables(cls):
        ltr = ListTablesRequest()
        result = cls.handle.list_tables(ltr)
        for table in result.get_tables():
            if table.startswith(table_prefix):
                cls.drop_table(table)

    @classmethod
    def drop_table(cls, table):
        dtr = TableRequest().set_statement('DROP TABLE IF EXISTS ' + table)
        cls.table_request(dtr)

    @classmethod
    def table_request(cls, request, test_handle=None):
        test_handle = cls.handle if test_handle is None else test_handle
        #
        # Optionally delay to handle the 4 DDL ops/minute limit
        # in the real service
        #
        if is_pod():
            sleep(30)
        test_handle.do_table_request(request, wait_timeout, 1000)
