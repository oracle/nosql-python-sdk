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
    IllegalArgumentException, State, TableLimits, TableNotFoundException,
    TableRequest, TableResult)
from parameters import (
    idcs_url, not_cloudsim, protocol, table_name, table_request_timeout,
    tenant_id, wait_timeout)
from testutils import (
    add_test_tier_tenant, delete_test_tier_tenant, get_handle,
    get_handle_config)


class TestTableRequest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if idcs_url is not None:
            global tenant_id
            tenant_id = idcs_url[idcs_url.find('i'):idcs_url.find('.')]
        add_test_tier_tenant(tenant_id)

    @classmethod
    def tearDownClass(cls):
        delete_test_tier_tenant(tenant_id)

    def setUp(self):
        self.handle_config = get_handle_config(tenant_id)
        self.handle = get_handle(tenant_id)
        index_name = 'idx_' + table_name
        self.create_tb_statement = (
            'CREATE TABLE ' + table_name + '(fld_id INTEGER, fld_long LONG, \
fld_float FLOAT, fld_double DOUBLE, fld_bool BOOLEAN, fld_str STRING, \
fld_bin BINARY, fld_time TIMESTAMP(4), fld_num NUMBER, fld_json JSON, \
fld_arr ARRAY(STRING), fld_map MAP(STRING), \
fld_rec RECORD(fld_id LONG, fld_bool BOOLEAN, fld_str STRING), \
PRIMARY KEY(fld_id)) USING TTL 30 DAYS')
        self.create_idx_statement = (
            'CREATE INDEX ' + index_name + ' ON ' + table_name +
            '(fld_str, fld_double)')
        self.alter_fld_statement = (
            'ALTER TABLE ' + table_name + '(DROP fld_num)')
        self.alter_ttl_statement = (
            'ALTER TABLE ' + table_name + ' USING TTL 16 HOURS')
        self.drop_idx_statement = (
            'DROP INDEX ' + index_name + ' ON ' + table_name)
        self.drop_tb_statement = ('DROP TABLE IF EXISTS ' + table_name)
        self.table_request = TableRequest()
        self.table_limits = TableLimits(5000, 5000, 50)
        self.index_schema = '{"json_version":1,"type":"table",\
"name":"' + table_name + '","namespace":"' + tenant_id + '","ttl":"30 DAYS",\
"owner":"nosqladmin(id:u1)","shardKey":["fld_id"],"primaryKey":["fld_id"],\
"limits":[{"readLimit":5000,"writeLimit":5000,"sizeLimit":50,"indexLimit":5,\
"childTableLimit":0,"indexKeySizeLimit":64}],"fields":[\
{"name":"fld_id","type":"INTEGER","nullable":false,"default":null},\
{"name":"fld_long","type":"LONG","nullable":true,"default":null},\
{"name":"fld_float","type":"FLOAT","nullable":true,"default":null},\
{"name":"fld_double","type":"DOUBLE","nullable":true,"default":null},\
{"name":"fld_bool","type":"BOOLEAN","nullable":true,"default":null},\
{"name":"fld_str","type":"STRING","nullable":true,"default":null},\
{"name":"fld_bin","type":"BINARY","nullable":true,"default":null},\
{"name":"fld_time","type":"TIMESTAMP","precision":4,"nullable":true,\
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
"nullable":true,"default":null}],"indexes":[{"name":"idx_users",\
"namespace":"' + tenant_id + '","table":"users","type":"secondary",\
"fields":["fld_str","fld_double"]}]}'
        self.table_schema = self.index_schema.replace(',"indexes":\
[{"name":"idx_users","namespace":"' + tenant_id + '","table":"users",\
"type":"secondary","fields":["fld_str","fld_double"]}]', '')
        self.alter_fld_schema = self.table_schema.replace('{"name":"fld_num",\
"type":"NUMBER","nullable":true,"default":null},', '')
        self.alter_ttl_schema = self.table_schema.replace('"ttl":"30 DAYS"',
                                                          '"ttl":"16 HOURS"')
        self.modify_limits_schema = self.table_schema.replace(
            '"readLimit":5000,"writeLimit":5000,"sizeLimit":50',
            '"readLimit":10000,"writeLimit":10000,"sizeLimit":100')

    def tearDown(self):
        try:
            TableResult.wait_for_state(self.handle, table_name, State.ACTIVE,
                                       wait_timeout, 1000)
            drop_request = TableRequest().set_statement(self.drop_tb_statement)
            result = self.handle.table_request(drop_request)
            result.wait_for_state(self.handle, table_name, State.DROPPED,
                                  wait_timeout, 1000)
        except TableNotFoundException:
            pass
        finally:
            self.handle.close()

    def testTableRequestSetIllegalStatement(self):
        self.table_request.set_statement('IllegalStatement')
        self.assertRaises(IllegalArgumentException, self.handle.table_request,
                          self.table_request)
        self.table_request.set_statement(
            'ALTER TABLE IllegalTable (DROP fld_num)')
        self.assertRaises(TableNotFoundException, self.handle.table_request,
                          self.table_request)

    def testTableRequestSetIllegalTableLimits(self):
        self.assertRaises(IllegalArgumentException,
                          self.table_request.set_table_limits,
                          'IllegalTableLimits')
        self.assertRaises(IllegalArgumentException,
                          self.table_request.set_table_limits, None)
        self.table_request.set_statement(
            self.create_tb_statement).set_table_limits(TableLimits(5000, 0, 50))
        self.assertRaises(IllegalArgumentException, self.handle.table_request,
                          self.table_request)

    def testTableRequestSetIllegalTableName(self):
        self.assertRaises(IllegalArgumentException,
                          self.table_request.set_table_name,
                          {'name': table_name})
        self.table_request.set_table_name(
            'IllegalTable').set_table_limits(self.table_limits)
        self.assertRaises(TableNotFoundException, self.handle.table_request,
                          self.table_request)

    def testTableRequestSetIllegalTimeout(self):
        self.assertRaises(IllegalArgumentException,
                          self.table_request.set_timeout, 'IllegalTimeout')
        self.assertRaises(IllegalArgumentException,
                          self.table_request.set_timeout, 0)
        self.assertRaises(IllegalArgumentException,
                          self.table_request.set_timeout, -1)

    def testTableRequestSetIllegalDefaults(self):
        self.assertRaises(IllegalArgumentException,
                          self.table_request.set_defaults, 'IllegalDefaults')

    def testTableRequestSetDefaults(self):
        self.table_request.set_defaults(self.handle_config)
        self.assertEqual(self.table_request.get_timeout(),
                         table_request_timeout)

    def testTableRequestNoStatementAndTableName(self):
        self.assertRaises(IllegalArgumentException, self.handle.table_request,
                          self.table_request)

    def testTableRequestBothStatementAndTableName(self):
        self.table_request.set_statement(
            self.create_tb_statement).set_table_name(table_name)
        self.assertRaises(IllegalArgumentException, self.handle.table_request,
                          self.table_request)

    def testTableRequestOnlyTableName(self):
        self.table_request.set_table_name(table_name)
        self.assertRaises(IllegalArgumentException, self.handle.table_request,
                          self.table_request)

    def testTableRequestGets(self):
        self.table_request.set_table_name(table_name).set_statement(
            self.create_tb_statement).set_table_limits(self.table_limits)
        self.assertEqual(self.table_request.get_statement(),
                         self.create_tb_statement)
        self.assertEqual(self.table_request.get_table_limits(),
                         self.table_limits)
        self.assertEqual(self.table_request.get_table_name(), table_name)

    def testTableRequestIllegalRequest(self):
        self.assertRaises(IllegalArgumentException, self.handle.table_request,
                          'IllegalRequest')

    def testTableRequestCreateDropTable(self):
        if protocol == 'https':
            sleep(60)
        # create table failed without TableLimits set
        self.table_request.set_statement(self.create_tb_statement)
        self.assertRaises(IllegalArgumentException, self.handle.table_request,
                          self.table_request)
        # create table succeed with TableLimits set
        self.table_request.set_table_limits(self.table_limits)
        result = self.handle.table_request(self.table_request)
        self.assertEqual(result.get_tenant_id(), tenant_id)
        self.assertEqual(result.get_table_name(), table_name)
        self.assertEqual(result.get_state(), State.CREATING)
        self.assertEqual(result.get_table_limits().get_read_units(),
                         self.table_limits.get_read_units())
        self.assertEqual(result.get_table_limits().get_write_units(),
                         self.table_limits.get_write_units())
        self.assertEqual(result.get_table_limits().get_storage_gb(),
                         self.table_limits.get_storage_gb())
        if not_cloudsim():
            self.assertIsNone(result.get_schema())
        wait_result = result.wait_for_state(self.handle, table_name,
                                            State.ACTIVE, wait_timeout, 1000)
        self.assertEqual(wait_result.get_tenant_id(), tenant_id)
        self.assertEqual(wait_result.get_table_name(), table_name)
        self.assertEqual(wait_result.get_state(), State.ACTIVE)
        self.assertEqual(wait_result.get_table_limits().get_read_units(),
                         self.table_limits.get_read_units())
        self.assertEqual(wait_result.get_table_limits().get_write_units(),
                         self.table_limits.get_write_units())
        self.assertEqual(wait_result.get_table_limits().get_storage_gb(),
                         self.table_limits.get_storage_gb())
        if not_cloudsim():
            self.assertEqual(wait_result.get_schema(), self.table_schema)
        self.assertIsNone(wait_result.get_operation_id())
        # drop table by resetting the statement
        self.table_request.set_statement(self.drop_tb_statement)
        result = self.handle.table_request(self.table_request)
        self.assertEqual(result.get_tenant_id(), tenant_id)
        self.assertEqual(result.get_table_name(), table_name)
        self.assertEqual(result.get_state(), State.DROPPING)
        self.assertEqual(result.get_table_limits().get_read_units(),
                         self.table_limits.get_read_units())
        self.assertEqual(result.get_table_limits().get_write_units(),
                         self.table_limits.get_write_units())
        self.assertEqual(result.get_table_limits().get_storage_gb(),
                         self.table_limits.get_storage_gb())
        if not_cloudsim():
            self.assertEqual(result.get_schema(), self.table_schema)
        wait_result = result.wait_for_state(self.handle, table_name,
                                            State.DROPPED, wait_timeout, 1000)
        self.assertEqual(wait_result.get_tenant_id(), tenant_id)
        self.assertEqual(wait_result.get_table_name(), table_name)
        self.assertEqual(wait_result.get_state(), State.DROPPED)
        self.assertEqual(wait_result.get_table_limits().get_read_units(),
                         self.table_limits.get_read_units())
        self.assertEqual(wait_result.get_table_limits().get_write_units(),
                         self.table_limits.get_write_units())
        self.assertEqual(wait_result.get_table_limits().get_storage_gb(),
                         self.table_limits.get_storage_gb())
        if not_cloudsim():
            self.assertIsNone(wait_result.get_schema())
        self.assertIsNone(wait_result.get_operation_id())

    def testTableRequestCreateDropIndex(self):
        if protocol == 'https':
            sleep(60)
        # create table before creating index
        request = TableRequest().set_statement(
            self.create_tb_statement).set_table_limits(self.table_limits)
        result = self.handle.table_request(request)
        result.wait_for_state(self.handle, table_name, State.ACTIVE,
                              wait_timeout, 1000)
        # create index by resetting the statement
        self.table_request.set_statement(self.create_idx_statement)
        result = self.handle.table_request(self.table_request)
        self.assertEqual(result.get_tenant_id(), tenant_id)
        self.assertEqual(result.get_table_name(), table_name)
        self.assertEqual(result.get_state(), State.UPDATING)
        self.assertEqual(result.get_table_limits().get_read_units(),
                         self.table_limits.get_read_units())
        self.assertEqual(result.get_table_limits().get_write_units(),
                         self.table_limits.get_write_units())
        self.assertEqual(result.get_table_limits().get_storage_gb(),
                         self.table_limits.get_storage_gb())
        if not_cloudsim():
            self.assertEqual(result.get_schema(), self.table_schema)
        wait_result = result.wait_for_state(self.handle, table_name,
                                            State.ACTIVE, wait_timeout, 1000)
        self.assertEqual(wait_result.get_tenant_id(), tenant_id)
        self.assertEqual(wait_result.get_table_name(), table_name)
        self.assertEqual(wait_result.get_state(), State.ACTIVE)
        self.assertEqual(wait_result.get_table_limits().get_read_units(),
                         self.table_limits.get_read_units())
        self.assertEqual(wait_result.get_table_limits().get_write_units(),
                         self.table_limits.get_write_units())
        self.assertEqual(wait_result.get_table_limits().get_storage_gb(),
                         self.table_limits.get_storage_gb())
        if not_cloudsim():
            self.assertEqual(wait_result.get_schema(), self.index_schema)
        self.assertIsNone(wait_result.get_operation_id())
        # drop index by resetting the statement
        self.table_request.set_statement(self.drop_idx_statement)
        result = self.handle.table_request(self.table_request)
        self.assertEqual(result.get_tenant_id(), tenant_id)
        self.assertEqual(result.get_table_name(), table_name)
        self.assertEqual(result.get_state(), State.UPDATING)
        self.assertEqual(result.get_table_limits().get_read_units(),
                         self.table_limits.get_read_units())
        self.assertEqual(result.get_table_limits().get_write_units(),
                         self.table_limits.get_write_units())
        self.assertEqual(result.get_table_limits().get_storage_gb(),
                         self.table_limits.get_storage_gb())
        if not_cloudsim():
            self.assertEqual(result.get_schema(), self.index_schema)
        wait_result = result.wait_for_state(self.handle, table_name,
                                            State.ACTIVE, wait_timeout, 1000)
        self.assertEqual(wait_result.get_tenant_id(), tenant_id)
        self.assertEqual(wait_result.get_table_name(), table_name)
        self.assertEqual(wait_result.get_state(), State.ACTIVE)
        self.assertEqual(wait_result.get_table_limits().get_read_units(),
                         self.table_limits.get_read_units())
        self.assertEqual(wait_result.get_table_limits().get_write_units(),
                         self.table_limits.get_write_units())
        self.assertEqual(wait_result.get_table_limits().get_storage_gb(),
                         self.table_limits.get_storage_gb())
        if not_cloudsim():
            self.assertEqual(wait_result.get_schema(), self.table_schema)
        self.assertIsNone(wait_result.get_operation_id())
        # drop table after dropping index
        self.table_request.set_statement(self.drop_tb_statement)
        result = self.handle.table_request(self.table_request)
        result.wait_for_state(self.handle, table_name, State.DROPPED,
                              wait_timeout, 1000)

    def testTableRequestAlterTable(self):
        if protocol == 'https':
            sleep(60)
        # create table before altering table
        request = TableRequest().set_statement(
            self.create_tb_statement).set_table_limits(self.table_limits)
        result = self.handle.table_request(request)
        result.wait_for_state(self.handle, table_name, State.ACTIVE,
                              wait_timeout, 1000)
        # alter table failed with TableLimits set
        request.set_statement(self.alter_fld_statement)
        self.assertRaises(IllegalArgumentException, self.handle.table_request,
                          request)
        # alter table succeed without TableLimits set
        self.table_request.set_statement(self.alter_fld_statement)
        result = self.handle.table_request(self.table_request)
        self.assertEqual(result.get_tenant_id(), tenant_id)
        self.assertEqual(result.get_table_name(), table_name)
        self.assertEqual(result.get_state(), State.UPDATING)
        self.assertEqual(result.get_table_limits().get_read_units(),
                         self.table_limits.get_read_units())
        self.assertEqual(result.get_table_limits().get_write_units(),
                         self.table_limits.get_write_units())
        self.assertEqual(result.get_table_limits().get_storage_gb(),
                         self.table_limits.get_storage_gb())
        if not_cloudsim():
            self.assertEqual(result.get_schema(), self.table_schema)
        wait_result = result.wait_for_state(self.handle, table_name,
                                            State.ACTIVE, wait_timeout, 1000)
        self.assertEqual(wait_result.get_tenant_id(), tenant_id)
        self.assertEqual(wait_result.get_table_name(), table_name)
        self.assertEqual(wait_result.get_state(), State.ACTIVE)
        self.assertEqual(wait_result.get_table_limits().get_read_units(),
                         self.table_limits.get_read_units())
        self.assertEqual(wait_result.get_table_limits().get_write_units(),
                         self.table_limits.get_write_units())
        self.assertEqual(wait_result.get_table_limits().get_storage_gb(),
                         self.table_limits.get_storage_gb())
        if not_cloudsim():
            self.assertEqual(wait_result.get_schema(), self.alter_fld_schema)
        self.assertIsNone(wait_result.get_operation_id())
        # drop table after altering table
        request.set_statement(self.drop_tb_statement)
        result = self.handle.table_request(request)
        result.wait_for_state(self.handle, table_name, State.DROPPED,
                              wait_timeout, 1000)

    def testTableRequestAlterTableTTL(self):
        if protocol == 'https':
            sleep(60)
        # create table before altering table
        request = TableRequest().set_statement(
            self.create_tb_statement).set_table_limits(self.table_limits)
        result = self.handle.table_request(request)
        result.wait_for_state(self.handle, table_name, State.ACTIVE,
                              wait_timeout, 1000)
        # alter table ttl
        self.table_request.set_statement(self.alter_ttl_statement)
        result = self.handle.table_request(self.table_request)
        self.assertEqual(result.get_tenant_id(), tenant_id)
        self.assertEqual(result.get_table_name(), table_name)
        self.assertEqual(result.get_state(), State.UPDATING)
        self.assertEqual(result.get_table_limits().get_read_units(),
                         self.table_limits.get_read_units())
        self.assertEqual(result.get_table_limits().get_write_units(),
                         self.table_limits.get_write_units())
        self.assertEqual(result.get_table_limits().get_storage_gb(),
                         self.table_limits.get_storage_gb())
        if not_cloudsim():
            self.assertEqual(result.get_schema(), self.table_schema)
        wait_result = result.wait_for_state(self.handle, table_name,
                                            State.ACTIVE, wait_timeout, 1000)
        self.assertEqual(wait_result.get_tenant_id(), tenant_id)
        self.assertEqual(wait_result.get_table_name(), table_name)
        self.assertEqual(wait_result.get_state(), State.ACTIVE)
        self.assertEqual(wait_result.get_table_limits().get_read_units(),
                         self.table_limits.get_read_units())
        self.assertEqual(wait_result.get_table_limits().get_write_units(),
                         self.table_limits.get_write_units())
        self.assertEqual(wait_result.get_table_limits().get_storage_gb(),
                         self.table_limits.get_storage_gb())
        if not_cloudsim():
            self.assertEqual(wait_result.get_schema(), self.alter_ttl_schema)
        self.assertIsNone(wait_result.get_operation_id())
        # drop table after altering table
        request.set_statement(self.drop_tb_statement)
        result = self.handle.table_request(request)
        result.wait_for_state(self.handle, table_name, State.DROPPED,
                              wait_timeout, 1000)

    def testTableRequestModifyTableLimits(self):
        if protocol == 'https':
            sleep(60)
        # create table before modifying the table limits
        request = TableRequest().set_statement(
            self.create_tb_statement).set_table_limits(self.table_limits)
        result = self.handle.table_request(request)
        result.wait_for_state(self.handle, table_name, State.ACTIVE,
                              wait_timeout, 1000)
        # modify the table limits
        table_limits = TableLimits(10000, 10000, 100)
        self.table_request.set_table_name(table_name).set_table_limits(
            table_limits)
        result = self.handle.table_request(self.table_request)
        self.assertEqual(result.get_tenant_id(), tenant_id)
        self.assertEqual(result.get_table_name(), table_name)
        self.assertEqual(result.get_state(), State.UPDATING)
        if not_cloudsim():
            self.assertEqual(result.get_schema(), self.table_schema)
        wait_result = result.wait_for_state(self.handle, table_name,
                                            State.ACTIVE, wait_timeout, 1000)
        self.assertEqual(wait_result.get_tenant_id(), tenant_id)
        self.assertEqual(wait_result.get_table_name(), table_name)
        self.assertEqual(wait_result.get_state(), State.ACTIVE)
        self.assertEqual(wait_result.get_table_limits().get_read_units(),
                         table_limits.get_read_units())
        self.assertEqual(wait_result.get_table_limits().get_write_units(),
                         table_limits.get_write_units())
        self.assertEqual(wait_result.get_table_limits().get_storage_gb(),
                         table_limits.get_storage_gb())
        if not_cloudsim():
            self.assertEqual(wait_result.get_schema(),
                             self.modify_limits_schema)
        self.assertIsNone(wait_result.get_operation_id())
        # drop table after modifying the table limits
        request.set_statement(self.drop_tb_statement)
        result = self.handle.table_request(request)
        result.wait_for_state(self.handle, table_name, State.DROPPED,
                              wait_timeout, 1000)


if __name__ == '__main__':
    unittest.main()
