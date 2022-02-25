#
# Copyright (c) 2018, 2022 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#

import unittest
from time import sleep

from borneo import (
    IllegalArgumentException, ListTablesRequest, TableLimits, TableRequest)
from parameters import (
    is_onprem, is_pod, is_prod_pod, table_name, table_prefix, tenant_id,
    timeout)
from test_base import TestBase
from testutils import (
    add_tenant, add_tier, delete_tenant, delete_tier, get_handle, namespace)


class TestListTables(unittest.TestCase, TestBase):
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
        num_handles = 1 if is_prod_pod() or is_onprem() else 2
        for handle in range(num_handles):
            tenant = tenant_id + ('' if handle == 0 else str(handle))
            add_tenant(tenant)
            table_names.append(list())
            cls.handles.append(get_handle(tenant))
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
                cls.table_request(drop_request, cls.handles[handle])
                create_statement = (
                    'CREATE TABLE ' + tb_name + '(fld_id INTEGER, \
fld_long LONG, fld_float FLOAT, fld_double DOUBLE, fld_bool BOOLEAN, \
fld_str STRING, fld_bin BINARY, fld_time TIMESTAMP(2), fld_num NUMBER, \
fld_json JSON, fld_arr ARRAY(STRING), fld_map MAP(STRING), \
fld_rec RECORD(fld_id LONG, fld_bool BOOLEAN, fld_str STRING), \
PRIMARY KEY(fld_id)) USING TTL 16 HOURS')
                limits = TableLimits(10, 10, 1)
                create_request = TableRequest().set_statement(
                    create_statement).set_table_limits(limits)
                cls.table_request(create_request, cls.handles[handle])

    @classmethod
    def tearDownClass(cls):
        for handle in range(len(cls.handles)):
            tenant = tenant_id + ('' if handle == 0 else str(handle))
            try:
                ltr = ListTablesRequest()
                result = cls.handles[handle].list_tables(ltr)
                for table in result.get_tables():
                    if table.startswith(table_prefix):
                        drop_request = TableRequest().set_statement(
                            'DROP TABLE IF EXISTS ' + table)
                        cls.table_request(drop_request, cls.handles[handle])
            finally:
                cls.handles[handle].close()
                delete_tenant(tenant)
        delete_tier()

    def setUp(self):
        self.handles = list()
        self.list_tables_requests = list()
        self.num_handles = 1 if is_prod_pod() or is_onprem() else 2
        for handle in range(self.num_handles):
            tenant = tenant_id + ('' if handle == 0 else str(handle))
            self.handles.append(get_handle(tenant))
            self.list_tables_requests.append(ListTablesRequest().set_timeout(
                timeout))
        self.list_tables_request = ListTablesRequest().set_timeout(timeout)

    def tearDown(self):
        for handle in self.handles:
            handle.close()

    def testListTablesSetIllegalCompartment(self):
        self.assertRaises(IllegalArgumentException,
                          self.list_tables_request.set_compartment, {})
        self.assertRaises(IllegalArgumentException,
                          self.list_tables_request.set_compartment, '')

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

    def testListTablesSetIllegalNamespace(self):
        self.assertRaises(IllegalArgumentException,
                          self.list_tables_request.set_namespace, {})

    def testListTablesGets(self):
        self.list_tables_request.set_limit(5).set_namespace(namespace)
        self.assertIsNone(self.list_tables_request.get_compartment())
        self.assertEqual(self.list_tables_request.get_start_index(), 0)
        self.assertEqual(self.list_tables_request.get_limit(), 5)
        self.assertEqual(self.list_tables_request.get_namespace(), namespace)

    def testListTablesIllegalRequest(self):
        for handle in range(self.num_handles):
            self.assertRaises(IllegalArgumentException,
                              self.handles[handle].list_tables,
                              'IllegalRequest')

    def testListTablesNormal(self):
        last_returned_index = [3, 4]
        self._check_list_tables_result(table_names, last_returned_index)

    def testListTablesWithStartIndex(self):
        last_returned_index = [3, 4]
        # set start_index = 1
        part_table_names = [[self._make_table_name('Users1'),
                             self._make_table_name('Users2')],
                            [self._make_table_name('Users1'),
                             self._make_table_name('Users2'),
                             self._make_table_name('Users3')]]
        for handle in range(self.num_handles):
            self.list_tables_requests[handle].set_start_index(1)
        self._check_list_tables_result(part_table_names, last_returned_index)

    def testListTablesWithLimit(self):
        # set limit = 2
        tables = [[], []]
        start_index = [0, 0]
        while True:
            more = False
            for handle in range(self.num_handles):
                self.list_tables_requests[handle].set_start_index(
                    start_index[handle]).set_limit(2)
                result = self.handles[handle].list_tables(
                    self.list_tables_requests[handle])
                tbs = result.get_tables()
                start_index[handle] = result.get_last_returned_index()
                self.assertLessEqual(len(tbs), 2)
                tables[handle].extend(tbs)
                more = more or len(tbs) != 0
            if not more:
                break
        for handle in range(self.num_handles):
            self.assertTrue(
                set(tables[handle]).issuperset(set(table_names[handle])))

    def testListTablesWithNamespace(self):
        if is_onprem():
            # set a namespace that not exist
            for handle in range(self.num_handles):
                self.list_tables_requests[handle].set_namespace(namespace)
                result = self.handles[handle].list_tables(
                    self.list_tables_requests[handle])
                self.assertEqual(result.get_tables(), [])
                self.assertEqual(result.get_last_returned_index(), 0)

    def _check_list_tables_result(self, names, last_returned_index):
        for handle in range(self.num_handles):
            result = self.handles[handle].list_tables(
                self.list_tables_requests[handle])
            self.assertTrue(
                set(result.get_tables()).issuperset(set(names[handle])))
            self.assertGreaterEqual(result.get_last_returned_index(),
                                    last_returned_index[handle])

    @staticmethod
    def _make_table_name(name):
        return table_prefix + name


if __name__ == '__main__':
    unittest.main()
