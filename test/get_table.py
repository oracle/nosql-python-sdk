#
# Copyright (C) 2018, 2019 Oracle and/or its affiliates. All rights reserved.
#
# This software is licensed with the Universal Permissive License (UPL) version 1.0
#
# Please see LICENSE.txt file included in the top-level directory of the
# appropriate download for a copy of the license and additional information.
#

import unittest
from time import sleep

from borneo import (
    GetTableRequest, IllegalArgumentException, State, TableLimits,
    TableNotFoundException, TableRequest)
from parameters import (
    idcs_url, not_cloudsim, protocol, table_name, tenant_id, timeout,
    wait_timeout)
from testutils import add_test_tier_tenant, delete_test_tier_tenant, get_handle


class TestGetTable(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if idcs_url is not None:
            global tenant_id
            tenant_id = idcs_url[idcs_url.find('i'):idcs_url.find('.')]
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
fld_bin BINARY, fld_time TIMESTAMP(1), fld_num NUMBER, fld_json JSON, \
fld_arr ARRAY(STRING), fld_map MAP(STRING), \
fld_rec RECORD(fld_id LONG, fld_bool BOOLEAN, fld_str STRING), \
PRIMARY KEY(fld_id)) USING TTL 30 DAYS')
        global table_limits, schema
        table_limits = TableLimits(5000, 5000, 50)
        create_request = TableRequest().set_statement(
            create_statement).set_table_limits(table_limits)
        cls._result = cls._handle.table_request(create_request)
        cls._result.wait_for_state(cls._handle, table_name, State.ACTIVE,
                                   wait_timeout, 1000)
        schema = '{"json_version":1,"type":"table","name":"' + table_name + '\
","namespace":"' + tenant_id + '","ttl":"30 DAYS","owner":"nosqladmin(id:u1)",\
"shardKey":["fld_id"],"primaryKey":["fld_id"],\
"limits":[{"readLimit":5000,"writeLimit":5000,"sizeLimit":50,\
"indexLimit":5,"childTableLimit":0,"indexKeySizeLimit":64}],"fields":[\
{"name":"fld_id","type":"INTEGER","nullable":false,"default":null},\
{"name":"fld_long","type":"LONG","nullable":true,"default":null},\
{"name":"fld_float","type":"FLOAT","nullable":true,"default":null},\
{"name":"fld_double","type":"DOUBLE","nullable":true,"default":null},\
{"name":"fld_bool","type":"BOOLEAN","nullable":true,"default":null},\
{"name":"fld_str","type":"STRING","nullable":true,"default":null},\
{"name":"fld_bin","type":"BINARY","nullable":true,"default":null},\
{"name":"fld_time","type":"TIMESTAMP","precision":1,"nullable":true,\
"default":null},\
{"name":"fld_num","type":"NUMBER","nullable":true,"default":null},\
{"name":"fld_json","type":"JSON","nullable":true,"default":null},\
{"name":"fld_arr","type":"ARRAY","collection":{"type":"STRING"},\
"nullable":true,"default":null},\
{"name":"fld_map","type":"MAP","collection":{"type":"STRING"},"nullable":true,\
"default":null},\
{"name":"fld_rec","type":"RECORD","fields":[\
{"name":"fld_id","type":"LONG","nullable":true,"default":null},\
{"name":"fld_bool","type":"BOOLEAN","nullable":true,"default":null},\
{"name":"fld_str","type":"STRING","nullable":true,"default":null}],\
"nullable":true,"default":null}]}'

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
        self.get_table_request = GetTableRequest().set_timeout(timeout)

    def tearDown(self):
        self.handle.close()

    def testGetTableSetIllegalTableName(self):
        self.assertRaises(IllegalArgumentException,
                          self.get_table_request.set_table_name,
                          {'name': table_name})
        self.get_table_request.set_table_name('IllegalTable')
        self.assertRaises(TableNotFoundException, self.handle.get_table,
                          self.get_table_request)

    def testGetTableSetIllegalOperationId(self):
        self.assertRaises(IllegalArgumentException,
                          self.get_table_request.set_operation_id, 0)

    def testGetTableSetIllegalTimeout(self):
        self.assertRaises(IllegalArgumentException,
                          self.get_table_request.set_timeout, 'IllegalTimeout')
        self.assertRaises(IllegalArgumentException,
                          self.get_table_request.set_timeout, 0)
        self.assertRaises(IllegalArgumentException,
                          self.get_table_request.set_timeout, -1)

    def testGetTableNoTableName(self):
        self.assertRaises(IllegalArgumentException, self.handle.get_table,
                          self.get_table_request)

    def testGetTableGets(self):
        self.get_table_request.set_table_name(table_name)
        self.assertEqual(self.get_table_request.get_table_name(), table_name)
        self.assertIsNone(self.get_table_request.get_operation_id())

    def testGetTableIllegalRequest(self):
        self.assertRaises(IllegalArgumentException, self.handle.get_table,
                          'IllegalRequest')

    def testGetTableNormal(self):
        self.get_table_request.set_table_name(table_name)
        result = self.handle.get_table(self.get_table_request)
        self.assertEqual(result.get_tenant_id(), tenant_id)
        self.assertEqual(result.get_table_name(), table_name)
        self.assertEqual(result.get_state(), State.ACTIVE)
        self.assertEqual(result.get_table_limits().get_read_units(),
                         table_limits.get_read_units())
        self.assertEqual(result.get_table_limits().get_write_units(),
                         table_limits.get_write_units())
        self.assertEqual(result.get_table_limits().get_storage_gb(),
                         table_limits.get_storage_gb())
        if not_cloudsim():
            self.assertEqual(result.get_schema(), schema)
        self.assertIsNone(result.get_operation_id())

    def testGetTableWithOperationId(self):
        drop_request = TableRequest().set_statement(
            'DROP TABLE IF EXISTS ' + table_name)
        table_result = self.handle.table_request(drop_request)
        self.get_table_request.set_table_name(table_name).set_operation_id(
            table_result.get_operation_id())
        result = self.handle.get_table(self.get_table_request)
        self.assertEqual(result.get_tenant_id(), tenant_id)
        self.assertEqual(result.get_table_name(), table_name)
        self.assertEqual(result.get_state(), State.DROPPING)
        self.assertEqual(result.get_table_limits().get_read_units(),
                         table_limits.get_read_units())
        self.assertEqual(result.get_table_limits().get_write_units(),
                         table_limits.get_write_units())
        self.assertEqual(result.get_table_limits().get_storage_gb(),
                         table_limits.get_storage_gb())
        if not_cloudsim():
            self.assertEqual(result.get_schema(), schema)
        table_result.wait_for_state(self.handle, table_name, State.DROPPED,
                                    wait_timeout, 1000)


if __name__ == '__main__':
    unittest.main()
