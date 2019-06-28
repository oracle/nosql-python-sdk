#
# Copyright (C) 2018, 2019 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
#
# Please see LICENSE.txt file included in the top-level directory of the
# appropriate download for a copy of the license and additional information.
#

from time import sleep
from unittest import TestCase

from borneo import ListTablesRequest, QueryResult, State, TableRequest
from parameters import is_onprem, is_pod, table_prefix, tenant_id, wait_timeout
from testutils import add_test_tier_tenant, delete_test_tier_tenant, get_handle


class TestBase(object):
    handle = None

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

    def set_up(self):
        self.handle = get_handle(tenant_id)

    def tear_down(self):
        self.handle.close()

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
        return cls.table_request(dtr, State.DROPPED)

    @classmethod
    def table_request(cls, request, state):
        #
        # Optionally delay to handle the 4 DDL ops/minute limit
        # in the real service
        #
        if is_pod():
            sleep(20)
        result = cls.handle.table_request(request)
        result.wait_for_state_with_res(cls.handle, state, wait_timeout, 1000)
