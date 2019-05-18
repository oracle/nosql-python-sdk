#
# Copyright (C) 2018, 2019 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
#
# Please see LICENSE.txt file included in the top-level directory of the
# appropriate download for a copy of the license and additional information.
#

from time import sleep

from borneo import State, TableRequest, ListTablesRequest
from parameters import is_pod, table_prefix, tenant_id, wait_timeout
from testutils import (
    add_test_tier_tenant, delete_test_tier_tenant, get_handle)


class TestBase:
    def __init__(self):
        self.handle = None

    @classmethod
    def set_up_class(cls):
        add_test_tier_tenant(tenant_id)
        cls._handle = get_handle(tenant_id)
        cls.drop_all_tables()

    @classmethod
    def tear_down_class(cls):
        try:
            cls.drop_all_tables()
        finally:
            cls._handle.close()
            delete_test_tier_tenant(tenant_id)

    def set_up(self):
        self.handle = get_handle(tenant_id)

    def tear_down(self):
        self.handle.close()

    @classmethod
    def drop_all_tables(cls):
        ltr = ListTablesRequest()
        result = cls._handle.list_tables(ltr)
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
        result = cls._handle.table_request(request)
        result.wait_for_state_with_res(cls._handle, state, wait_timeout, 1000)
