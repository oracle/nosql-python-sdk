#
# Copyright (C) 2018, 2019 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
#
# Please see LICENSE.txt file included in the top-level directory of the
# appropriate download for a copy of the license and additional information.
#

import unittest
from time import sleep

from borneo import (
    IllegalArgumentException, ListTablesRequest, State, TableLimits,
    TableRequest)
from parameters import (
    is_pod, not_cloudsim, table_name, table_prefix, tenant_id, timeout,
    wait_timeout)
from testutils import (
    add_tenant, add_tier, delete_tenant, delete_tier, get_handle,
    make_table_name)


class TestListTables(unittest.TestCase):
    handles = None

    @classmethod
    def setUpClass(cls):
        add_tier()
        cls.handles = list()
        global table_names
        table_names = list()
        num_tables = 3
        #
        # In pod env create 1 handle, otherwise create 2 handles for additional
        # testing
        #
        num_handles = 1 if is_pod() else 2
        for handle in range(num_handles):
            add_tenant(tenant_id + str(handle))
            table_names.append(list())
            cls.handles.append(get_handle(tenant_id + str(handle)))
            for table in range(handle + num_tables):
                tb_name = table_name + str(table)
                table_names[handle].append(tb_name)
                #
                # Add a sleep for a pod to let things happen
                #
                if is_pod():
                    sleep(60)
                drop_request = TableRequest().set_statement(
                    'DROP TABLE IF EXISTS ' + tb_name)
                result = cls.handles[handle].table_request(drop_request)
                result.wait_for_state(cls.handles[handle], tb_name,
                                      State.DROPPED, wait_timeout, 1000)
                create_statement = (
                    'CREATE TABLE ' + tb_name + '(fld_id INTEGER, \
fld_long LONG, fld_float FLOAT, fld_double DOUBLE, fld_bool BOOLEAN, \
fld_str STRING, fld_bin BINARY, fld_time TIMESTAMP(2), fld_num NUMBER, \
fld_json JSON, fld_arr ARRAY(STRING), fld_map MAP(STRING), \
fld_rec RECORD(fld_id LONG, fld_bool BOOLEAN, fld_str STRING), \
PRIMARY KEY(fld_id)) USING TTL 16 HOURS')
                limits = TableLimits(5000, 5000, 50)
                create_request = TableRequest().set_statement(
                    create_statement).set_table_limits(limits)
                result = cls.handles[handle].table_request(create_request)
                result.wait_for_state(cls.handles[handle], tb_name,
                                      State.ACTIVE, wait_timeout, 1000)

    @classmethod
    def tearDownClass(cls):
        for handle in cls.handles:
            try:
                ltr = ListTablesRequest()
                result = handle.list_tables(ltr)
                for table in result.get_tables():
                    if table.startswith(table_prefix):
                        drop_request = TableRequest().set_statement(
                            'DROP TABLE IF EXISTS ' + table)
                        result = handle.table_request(drop_request)
                        result.wait_for_state(handle, table, State.DROPPED,
                                              wait_timeout, 1000)
            finally:
                handle.close()
                delete_tenant(tenant_id + str(handle))
        delete_tier()

    def setUp(self):
        self.handles = list()
        self.num_handles = 1 if is_pod() else 2
        for handle in range(self.num_handles):
            self.handles.append(get_handle(tenant_id + str(handle)))
        self.list_tables_request = ListTablesRequest().set_timeout(timeout)

    def tearDown(self):
        for handle in self.handles:
            handle.close()

    def testListTablesSetIllegalStartIndex(self):
        self.assertRaises(IllegalArgumentException,
                          self.list_tables_request.set_start_index,
                          'IllegalStartIndex')
        self.assertRaises(IllegalArgumentException,
                          self.list_tables_request.set_start_index, -1)

    def testListTablesSetIllegalTimeout(self):
        self.assertRaises(IllegalArgumentException,
                          self.list_tables_request.set_timeout,
                          'IllegalTimeout')
        self.assertRaises(IllegalArgumentException,
                          self.list_tables_request.set_timeout, 0)
        self.assertRaises(IllegalArgumentException,
                          self.list_tables_request.set_timeout, -1)

    def testListTablesSetIllegalLimit(self):
        self.assertRaises(IllegalArgumentException,
                          self.list_tables_request.set_limit, 'IllegalLimit')
        self.assertRaises(IllegalArgumentException,
                          self.list_tables_request.set_start_index, -1)

    def testListTablesGets(self):
        self.list_tables_request.set_limit(5)
        self.assertEqual(self.list_tables_request.get_start_index(), 0)
        self.assertEqual(self.list_tables_request.get_limit(), 5)

    def testListTablesIllegalRequest(self):
        for handle in range(self.num_handles):
            self.assertRaises(IllegalArgumentException,
                              self.handles[handle].list_tables,
                              'IllegalRequest')

    def testListTablesNormal(self):
        last_returned_index = [3, 4]
        for handle in range(self.num_handles):
            result = self.handles[handle].list_tables(self.list_tables_request)
            # TODO: add and use startIndex,numTables.
            if not not_cloudsim():
                self.assertEqual(result.get_tables(), table_names[handle])
                self.assertEqual(result.get_last_returned_index(),
                                 last_returned_index[handle])

    def testListTablesWithStartIndex(self):
        last_returned_index = [3, 4]
        # set a start index larger than the number of tables
        self.list_tables_request.set_start_index(5)
        for handle in range(self.num_handles):
            result = self.handles[handle].list_tables(self.list_tables_request)
            # TODO: add and use startIndex,numTables.
            if not not_cloudsim():
                self.assertEqual(result.get_tables(), [])
                self.assertEqual(result.get_last_returned_index(),
                                 last_returned_index[handle])
        # set start_index = 1
        part_table_names = [[make_table_name('Users1'),
                             make_table_name('Users2')],
                            [make_table_name('Users1'),
                             make_table_name('Users2'),
                             make_table_name('Users3')]]
        self.list_tables_request.set_start_index(1)
        for handle in range(self.num_handles):
            result = self.handles[handle].list_tables(self.list_tables_request)
            # TODO: add and use startIndex,numTables.
            if not not_cloudsim():
                self.assertEqual(result.get_tables(), part_table_names[handle])
                self.assertEqual(result.get_last_returned_index(),
                                 last_returned_index[handle])

    def testListTablesWithLimit(self):
        # set limit = 2
        last_returned_index = 2
        part_table_names = [[make_table_name('Users0'),
                             make_table_name('Users1')],
                            [make_table_name('Users0'),
                             make_table_name('Users1')]]
        self.list_tables_request.set_limit(2)
        for handle in range(self.num_handles):
            result = self.handles[handle].list_tables(self.list_tables_request)
            # TODO: add and use startIndex,numTables.
            if not not_cloudsim():
                self.assertEqual(result.get_tables(), part_table_names[handle])
                self.assertEqual(result.get_last_returned_index(),
                                 last_returned_index)


if __name__ == '__main__':
    unittest.main()
