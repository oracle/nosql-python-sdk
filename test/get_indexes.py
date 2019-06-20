#
# Copyright (C) 2018, 2019 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
#
# Please see LICENSE.txt file included in the top-level directory of the
# appropriate download for a copy of the license and additional information.
#

import unittest

from borneo import (
    GetIndexesRequest, IllegalArgumentException, IndexNotFoundException, State,
    TableLimits, TableNotFoundException, TableRequest)
from parameters import index_name, table_name, timeout
from test_base import TestBase


class TestGetIndexes(unittest.TestCase, TestBase):
    @classmethod
    def setUpClass(cls):
        cls.set_up_class()
        global table_names, index_names, num_indexes, index_fields
        table_names = list()
        num_tables = 2
        index_names = list()
        num_indexes = 1
        index_fields = list()
        for index in range(2):
            index_fields.append(list())
        index_fields[0].append('fld_double')
        index_fields[1].append('fld_str')
        for table in range(num_tables):
            tb_name = table_name + str(table)
            table_names.append(tb_name)
            create_statement = ('CREATE TABLE ' + tb_name + '(fld_id INTEGER, \
fld_long LONG, fld_float FLOAT, fld_double DOUBLE, fld_bool BOOLEAN, \
fld_str STRING, fld_bin BINARY, fld_time TIMESTAMP(0), fld_num NUMBER, \
fld_json JSON, fld_arr ARRAY(STRING), fld_map MAP(STRING), \
fld_rec RECORD(fld_id LONG, fld_bool BOOLEAN, fld_str STRING), \
PRIMARY KEY(fld_id)) USING TTL 2 DAYS')
            limits = TableLimits(5000, 5000, 50)
            create_request = TableRequest().set_statement(
                create_statement).set_table_limits(limits)
            cls.table_request(create_request, State.ACTIVE)

            index_names.append(list())
            for index in range(table + num_indexes):
                idx_name = index_name + str(index)
                index_names[table].append(idx_name)
                create_index_statement = (
                    'CREATE INDEX ' + idx_name + ' ON ' + tb_name +
                    '(' + ','.join(index_fields[index]) + ')')
                create_index_request = TableRequest().set_statement(
                    create_index_statement)
                cls.table_request(create_index_request, State.ACTIVE)

    @classmethod
    def tearDownClass(cls):
        cls.tear_down_class()

    def setUp(self):
        self.set_up()
        self.get_indexes_request = GetIndexesRequest().set_timeout(timeout)

    def tearDown(self):
        self.tear_down()

    def testGetIndexesSetIllegalTableName(self):
        self.assertRaises(IllegalArgumentException,
                          self.get_indexes_request.set_table_name,
                          {'name': table_name})
        self.get_indexes_request.set_table_name('IllegalTable')
        self.assertRaises(TableNotFoundException, self.handle.get_indexes,
                          self.get_indexes_request)

    def testGetIndexesSetIllegalIndexName(self):
        self.get_indexes_request.set_table_name(table_names[0])
        self.assertRaises(IllegalArgumentException,
                          self.get_indexes_request.set_index_name,
                          {'name': index_name})
        self.get_indexes_request.set_index_name('IllegalIndex')
        self.assertRaises(IndexNotFoundException, self.handle.get_indexes,
                          self.get_indexes_request)

    def testGetIndexesSetIllegalTimeout(self):
        self.assertRaises(IllegalArgumentException,
                          self.get_indexes_request.set_timeout,
                          'IllegalTimeout')
        self.assertRaises(IllegalArgumentException,
                          self.get_indexes_request.set_timeout, 0)
        self.assertRaises(IllegalArgumentException,
                          self.get_indexes_request.set_timeout, -1)

    def testGetIndexesNoTableName(self):
        self.assertRaises(IllegalArgumentException, self.handle.get_indexes,
                          self.get_indexes_request)

    def testGetIndexesGets(self):
        self.get_indexes_request.set_table_name(table_name).set_index_name(
            index_name)
        self.assertEqual(self.get_indexes_request.get_table_name(), table_name)
        self.assertEqual(self.get_indexes_request.get_index_name(), index_name)

    def testGetIndexesIllegalRequest(self):
        self.assertRaises(IllegalArgumentException, self.handle.get_indexes,
                          'IllegalRequest')

    def testGetIndexesNormal(self):
        for table in range(len(table_names)):
            self.get_indexes_request.set_table_name(table_names[table])
            result = self.handle.get_indexes(self.get_indexes_request)
            indexes = result.get_indexes()
            self.assertEqual(len(indexes), table + num_indexes)
            for index in range(len(indexes)):
                self.assertEqual(indexes[index].get_index_name(),
                                 index_names[table][index])
                self.assertEqual(indexes[index].get_field_names(),
                                 index_fields[index])

    def testGetIndexesWithIndexName(self):
        for table in range(len(table_names)):
            self.get_indexes_request.set_table_name(
                table_names[table]).set_index_name(index_names[table][0])
            result = self.handle.get_indexes(self.get_indexes_request)
            indexes = result.get_indexes()
            self.assertEqual(len(indexes), 1)
            self.assertEqual(indexes[0].get_index_name(),
                             index_names[table][0])
            self.assertEqual(indexes[0].get_field_names(), index_fields[0])


if __name__ == '__main__':
    unittest.main()
