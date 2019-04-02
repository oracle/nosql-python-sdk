#
# Copyright (C) 2018, 2019 Oracle and/or its affiliates. All rights reserved.
#
# This software is licensed with the Universal Permissive License (UPL) version 1.0
#
# Please see LICENSE.txt file included in the top-level directory of the
# appropriate download for a copy of the license and additional information.
#

import unittest
from datetime import datetime, timedelta
from decimal import Decimal
from struct import pack
from time import mktime, sleep, time

from borneo import (
    GetRequest, IllegalArgumentException, PutRequest, State, TableLimits,
    TableNotFoundException, TableRequest, TableUsageRequest)
from parameters import not_cloudsim, table_name, timeout
from test_base import TestBase


class TestTableUsage(unittest.TestCase, TestBase):
    @classmethod
    def setUpClass(cls):
        TestBase.set_up_class()
        create_statement = (
            'CREATE TABLE ' + table_name + '(fld_id INTEGER, fld_long LONG, \
fld_float FLOAT, fld_double DOUBLE, fld_bool BOOLEAN, fld_str STRING, \
fld_bin BINARY, fld_time TIMESTAMP(7), fld_num NUMBER, fld_json JSON, \
fld_arr ARRAY(STRING), fld_map MAP(STRING), \
fld_rec RECORD(fld_id LONG, fld_bool BOOLEAN, fld_str STRING), \
PRIMARY KEY(fld_id)) USING TTL 1 HOURS')
        limits = TableLimits(5000, 5000, 50)
        create_request = TableRequest().set_statement(
            create_statement).set_table_limits(limits)
        cls._result = TestBase.table_request(create_request, State.ACTIVE)
        # put and get some data, read_units = 100, write_units = 199
        row = {'fld_id': 1, 'fld_long': 2147483648,
               'fld_float': 3.1414999961853027, 'fld_double': 3.1415,
               'fld_bool': True, 'fld_str': '{"name": u1, "phone": null}',
               'fld_bin': bytearray(pack('>i', 4)), 'fld_time': datetime.now(),
               'fld_num': Decimal(5),
               'fld_json': {'a': '1', 'b': None, 'c': '3'},
               'fld_arr': ['a', 'b', 'c'],
               'fld_map': {'a': '1', 'b': '2', 'c': '3'},
               'fld_rec': {'fld_id': 1, 'fld_bool': False, 'fld_str': None}}
        key = {'fld_id': 1}
        put_request = PutRequest().set_value(row).set_table_name(table_name)
        get_request = GetRequest().set_key(key).set_table_name(table_name)
        count = 0
        while count < 100:
            cls._handle.put(put_request)
            cls._handle.get(get_request)
            count += 1
            # sleep to allow records to accumulate over time, but not if
            # using Cloudsim.
            if not_cloudsim():
                sleep(2)
        # need to sleep to allow usage records to accumulate but not if
        # using CloudSim, which doesn't generate usage records.
        if not_cloudsim():
            sleep(40)

    @classmethod
    def tearDownClass(cls):
        TestBase.tear_down_class()

    def setUp(self):
        TestBase.set_up(self)
        self.table_usage_request = TableUsageRequest().set_timeout(timeout)

    def tearDown(self):
        TestBase.tear_down(self)

    def testTableUsageSetIllegalTableName(self):
        self.assertRaises(IllegalArgumentException,
                          self.table_usage_request.set_table_name,
                          {'name': table_name})
        self.table_usage_request.set_table_name('IllegalTable')
        self.assertRaises(TableNotFoundException, self.handle.get_table_usage,
                          self.table_usage_request)

    def testTableUsageSetIllegalStartTime(self):
        self.assertRaises(IllegalArgumentException,
                          self.table_usage_request.set_start_time,
                          {'IllegalStartTime': 'IllegalStartTime'})
        self.assertRaises(IllegalArgumentException,
                          self.table_usage_request.set_start_time, -1)

    def testTableUsageSetIllegalEndTime(self):
        self.assertRaises(IllegalArgumentException,
                          self.table_usage_request.set_end_time,
                          {'IllegalEndTime': 'IllegalEndTime'})
        self.assertRaises(IllegalArgumentException,
                          self.table_usage_request.set_end_time, -1)

    def testTableUsageSetIllegalLimit(self):
        self.assertRaises(IllegalArgumentException,
                          self.table_usage_request.set_limit, 'IllegalLimit')
        self.assertRaises(IllegalArgumentException,
                          self.table_usage_request.set_limit, -1)

    def testTableUsageSetIllegalTimeout(self):
        self.assertRaises(IllegalArgumentException,
                          self.table_usage_request.set_timeout,
                          'IllegalTimeout')
        self.assertRaises(IllegalArgumentException,
                          self.table_usage_request.set_timeout, 0)
        self.assertRaises(IllegalArgumentException,
                          self.table_usage_request.set_timeout, -1)

    def testTableUsageNoTableName(self):
        self.assertRaises(IllegalArgumentException, self.handle.get_table_usage,
                          self.table_usage_request)

    def testTableUsageGets(self):
        start = int(round(time() * 1000))
        start_str = datetime.fromtimestamp(float(start) / 1000).isoformat()
        end_str = datetime.fromtimestamp(
            round(time() * 1000) / 1000).isoformat()
        end = int(mktime(datetime.strptime(
            end_str, '%Y-%m-%dT%H:%M:%S.%f').timetuple()) * 1000)
        self.table_usage_request.set_table_name(table_name).set_start_time(
            start).set_end_time(end_str).set_limit(5)
        self.assertEqual(self.table_usage_request.get_table_name(), table_name)
        self.assertEqual(self.table_usage_request.get_start_time(), start)
        self.assertEqual(self.table_usage_request.get_start_time_string(),
                         start_str)
        self.assertEqual(self.table_usage_request.get_end_time(), end)
        self.assertEqual(self.table_usage_request.get_end_time_string(),
                         end_str[0:end_str.index('.')])
        self.assertEqual(self.table_usage_request.get_limit(), 5)

    def testTableUsageIllegalRequest(self):
        self.assertRaises(IllegalArgumentException, self.handle.get_table_usage,
                          'IllegalRequest')

    def testTableUsageNormal(self):
        self.table_usage_request.set_table_name(table_name)
        result = self.handle.get_table_usage(self.table_usage_request)
        self.assertEqual(result.get_table_name(), table_name)
        self.assertEqual(len(result.get_usage_records()), 1)
        current = int(round(time() * 1000))
        for usage_record in result.get_usage_records():
            start_time_res = usage_record.get_start_time()
            self.assertIsNotNone(start_time_res)
            self.assertEqual(
                usage_record.get_start_time_string(),
                datetime.fromtimestamp(
                    float(start_time_res) / 1000).isoformat())
            self.assertGreaterEqual(usage_record.get_read_units(), 0)
            self.assertGreaterEqual(usage_record.get_write_units(), 0)
            if not_cloudsim():
                # the record is generated in 1 min
                # self.assertGreater(start_time_res, current - 60000)
                self.assertGreater(start_time_res, current - 60000 * 2)
                self.assertLess(start_time_res, current)
                self.assertLessEqual(usage_record.get_seconds_in_period(), 60)
                self.assertLessEqual(usage_record.get_storage_gb(), 0)
            self.assertEqual(usage_record.get_read_throttle_count(), 0)
            self.assertEqual(usage_record.get_write_throttle_count(), 0)
            self.assertEqual(usage_record.get_storage_throttle_count(), 0)

    def testTableUsageWithStartTime(self):
        # set the start time
        start_time = int(round(time() * 1000)) - 120000
        self.table_usage_request.set_table_name(table_name).set_start_time(
            start_time)
        result = self.handle.get_table_usage(self.table_usage_request)
        self.assertEqual(result.get_table_name(), table_name)
        self.assertEqual(len(result.get_usage_records()), 1)
        for usage_record in result.get_usage_records():
            start_time_res = usage_record.get_start_time()
            self.assertEqual(
                usage_record.get_start_time_string(),
                datetime.fromtimestamp(
                    float(start_time_res) / 1000).isoformat())
            self.assertGreater(usage_record.get_read_units(), 0)
            self.assertGreater(usage_record.get_write_units(), 0)
            if not_cloudsim():
                self.assertGreater(start_time_res, start_time)
                self.assertLess(start_time_res, start_time + 60000)
                self.assertEqual(usage_record.get_seconds_in_period(), 60)
                self.assertLessEqual(usage_record.get_storage_gb(), 0)
            self.assertEqual(usage_record.get_read_throttle_count(), 0)
            self.assertEqual(usage_record.get_write_throttle_count(), 0)
            self.assertEqual(usage_record.get_storage_throttle_count(), 0)
        # set the start time in ISO 8601 formatted string
        start_str = (datetime.now() + timedelta(seconds=-120)).isoformat()
        start_time = int(mktime(datetime.strptime(
            start_str, '%Y-%m-%dT%H:%M:%S.%f').timetuple()) * 1000)
        self.table_usage_request.set_start_time(start_str)
        result = self.handle.get_table_usage(self.table_usage_request)
        self.assertEqual(result.get_table_name(), table_name)
        self.assertEqual(len(result.get_usage_records()), 1)
        for usage_record in result.get_usage_records():
            start_time_res = usage_record.get_start_time()
            self.assertEqual(
                usage_record.get_start_time_string(),
                datetime.fromtimestamp(
                    float(start_time_res) / 1000).isoformat())
            self.assertGreater(usage_record.get_read_units(), 0)
            self.assertGreater(usage_record.get_write_units(), 0)
            if not_cloudsim():
                self.assertGreater(start_time_res, start_time)
                self.assertLess(start_time_res, start_time + 60000)
                self.assertEqual(usage_record.get_seconds_in_period(), 60)
                self.assertLessEqual(usage_record.get_storage_gb(), 0)
            self.assertEqual(usage_record.get_read_throttle_count(), 0)
            self.assertEqual(usage_record.get_write_throttle_count(), 0)
            self.assertEqual(usage_record.get_storage_throttle_count(), 0)

    def testTableUsageWithEndTime(self):
        # set a start time to avoid unexpected table usage information, and set
        # the end time
        current = int(round(time() * 1000))
        start_time = current - 120000
        end_time = current - 120000
        self.table_usage_request.set_table_name(table_name).set_start_time(
            start_time).set_end_time(end_time)
        result = self.handle.get_table_usage(self.table_usage_request)
        self.assertEqual(result.get_table_name(), table_name)
        self.assertEqual(len(result.get_usage_records()),
                         0 if not_cloudsim() else 1)
        # set current time as end time
        self.table_usage_request.set_end_time(current)
        result = self.handle.get_table_usage(self.table_usage_request)
        self.assertEqual(result.get_table_name(), table_name)
        self.assertEqual(len(result.get_usage_records()), 1)
        for usage_record in result.get_usage_records():
            start_time_res = usage_record.get_start_time()
            self.assertEqual(
                usage_record.get_start_time_string(),
                datetime.fromtimestamp(
                    float(start_time_res) / 1000).isoformat())
            self.assertGreater(usage_record.get_read_units(), 0)
            self.assertGreater(usage_record.get_write_units(), 0)
            if not_cloudsim():
                self.assertGreater(start_time_res, start_time)
                self.assertLess(start_time_res, start_time + 60000)
                self.assertLess(start_time_res, current)
                self.assertEqual(usage_record.get_seconds_in_period(), 60)
                self.assertLessEqual(usage_record.get_storage_gb(), 0)
            self.assertEqual(usage_record.get_read_throttle_count(), 0)
            self.assertEqual(usage_record.get_write_throttle_count(), 0)
            self.assertEqual(usage_record.get_storage_throttle_count(), 0)
        # set current time in ISO 8601 formatted string as end time
        end_str = datetime.now().isoformat()
        end_time = int(mktime(datetime.strptime(
            end_str, '%Y-%m-%dT%H:%M:%S.%f').timetuple()) * 1000)
        self.table_usage_request.set_end_time(end_str)
        result = self.handle.get_table_usage(self.table_usage_request)
        self.assertEqual(result.get_table_name(), table_name)
        self.assertEqual(len(result.get_usage_records()), 1)
        for usage_record in result.get_usage_records():
            start_time_res = usage_record.get_start_time()
            self.assertEqual(
                usage_record.get_start_time_string(),
                datetime.fromtimestamp(
                    float(start_time_res) / 1000).isoformat())
            self.assertGreater(usage_record.get_read_units(), 0)
            self.assertGreater(usage_record.get_write_units(), 0)
            if not_cloudsim():
                self.assertGreater(start_time_res, start_time)
                self.assertLess(start_time_res, start_time + 60000)
                self.assertLess(start_time_res, end_time)
                self.assertEqual(usage_record.get_seconds_in_period(), 60)
                self.assertLessEqual(usage_record.get_storage_gb(), 0)
            self.assertEqual(usage_record.get_read_throttle_count(), 0)
            self.assertEqual(usage_record.get_write_throttle_count(), 0)
            self.assertEqual(usage_record.get_storage_throttle_count(), 0)

    def testTableUsageWithLimit(self):
        # set the limit
        self.table_usage_request.set_table_name(table_name).set_limit(3)
        result = self.handle.get_table_usage(self.table_usage_request)
        self.assertEqual(result.get_table_name(), table_name)
        self.assertEqual(len(result.get_usage_records()), 1)
        current = int(round(time() * 1000))
        for usage_record in result.get_usage_records():
            start_time_res = usage_record.get_start_time()
            self.assertIsNotNone(start_time_res)
            self.assertEqual(
                usage_record.get_start_time_string(),
                datetime.fromtimestamp(
                    float(start_time_res) / 1000).isoformat())
            self.assertGreater(usage_record.get_read_units(), 0)
            self.assertGreater(usage_record.get_write_units(), 0)
            if not_cloudsim():
                # the record is generated in 1 min
                # self.assertGreater(start_time_res, current - 60000)
                self.assertGreater(start_time_res, current - 60000 * 2)
                self.assertLess(start_time_res, current)
                self.assertLessEqual(usage_record.get_seconds_in_period(), 60)
                self.assertLessEqual(usage_record.get_storage_gb(), 0)
            self.assertEqual(usage_record.get_read_throttle_count(), 0)
            self.assertEqual(usage_record.get_write_throttle_count(), 0)
            self.assertEqual(usage_record.get_storage_throttle_count(), 0)

    def testTableUsageWithStartTimeAndLimit(self):
        # set the start time and limit
        start_time = int(round(time() * 1000)) - 240000
        limit = 2
        self.table_usage_request.set_table_name(table_name).set_start_time(
            start_time).set_limit(limit)
        result = self.handle.get_table_usage(self.table_usage_request)
        self.assertEqual(result.get_table_name(), table_name)
        self.assertEqual(len(result.get_usage_records()),
                         limit if not_cloudsim() else 1)
        records = result.get_usage_records()
        for count in range(len(records)):
            start_time_res = records[count].get_start_time()
            self.assertEqual(
                records[count].get_start_time_string(),
                datetime.fromtimestamp(
                    float(start_time_res) / 1000).isoformat())
            self.assertGreater(records[count].get_read_units(), 0)
            self.assertGreater(records[count].get_write_units(), 0)
            if not_cloudsim():
                self.assertGreater(start_time_res, start_time + count * 60000)
                self.assertLess(start_time_res,
                                start_time + (count + 1) * 60000)
                self.assertEqual(records[count].get_seconds_in_period(), 60)
                self.assertLessEqual(records[count].get_storage_gb(), 0)
            self.assertEqual(records[count].get_read_throttle_count(), 0)
            self.assertEqual(records[count].get_write_throttle_count(), 0)
            self.assertEqual(records[count].get_storage_throttle_count(), 0)
        # set the start time in ISO 8601 formatted string and limit
        start_str = (datetime.now() + timedelta(seconds=-240)).isoformat()
        start_time = int(mktime(datetime.strptime(
            start_str, '%Y-%m-%dT%H:%M:%S.%f').timetuple()) * 1000)
        self.table_usage_request.set_start_time(start_str)
        result = self.handle.get_table_usage(self.table_usage_request)
        self.assertEqual(result.get_table_name(), table_name)
        self.assertEqual(len(result.get_usage_records()),
                         limit if not_cloudsim() else 1)
        records = result.get_usage_records()
        for count in range(len(records)):
            start_time_res = records[count].get_start_time()
            self.assertEqual(
                records[count].get_start_time_string(),
                datetime.fromtimestamp(
                    float(start_time_res) / 1000).isoformat())
            self.assertGreater(records[count].get_read_units(), 0)
            self.assertGreater(records[count].get_write_units(), 0)
            if not_cloudsim():
                self.assertGreater(start_time_res, start_time + count * 60000)
                self.assertLess(start_time_res,
                                start_time + (count + 1) * 60000)
                self.assertEqual(records[count].get_seconds_in_period(), 60)
                self.assertLessEqual(records[count].get_storage_gb(), 0)
            self.assertEqual(records[count].get_read_throttle_count(), 0)
            self.assertEqual(records[count].get_write_throttle_count(), 0)
            self.assertEqual(records[count].get_storage_throttle_count(), 0)

    def testTableUsageWithStartEndTimeAndLimit(self):
        # start time, end time and limit
        current = int(round(time() * 1000))
        start_time = current - 180000
        end_time = current - 60000
        self.table_usage_request.set_table_name(table_name).set_start_time(
            start_time).set_end_time(end_time).set_limit(5)
        result = self.handle.get_table_usage(self.table_usage_request)
        self.assertEqual(result.get_table_name(), table_name)
        self.assertEqual(len(result.get_usage_records()),
                         2 if not_cloudsim() else 1)
        records = result.get_usage_records()
        for count in range(len(records)):
            start_time_res = records[count].get_start_time()
            self.assertEqual(
                records[count].get_start_time_string(),
                datetime.fromtimestamp(
                    float(start_time_res) / 1000).isoformat())
            self.assertGreater(records[count].get_read_units(), 0)
            self.assertGreater(records[count].get_write_units(), 0)
            if not_cloudsim():
                self.assertGreater(start_time_res, start_time + count * 60000)
                self.assertLess(start_time_res,
                                start_time + (count + 1) * 60000)
                self.assertLess(start_time_res, end_time)
                self.assertEqual(records[count].get_seconds_in_period(), 60)
                self.assertLessEqual(records[count].get_storage_gb(), 0)
            self.assertEqual(records[count].get_read_throttle_count(), 0)
            self.assertEqual(records[count].get_write_throttle_count(), 0)
            self.assertEqual(records[count].get_storage_throttle_count(), 0)
        # start time, end time in ISO 8601 formatted string and limit
        current = datetime.now()
        start_str = (current + timedelta(seconds=-180)).isoformat()
        end_str = (current + timedelta(seconds=-60)).isoformat()
        start_time = int(mktime(datetime.strptime(
            start_str, '%Y-%m-%dT%H:%M:%S.%f').timetuple()) * 1000)
        end_time = int(mktime(datetime.strptime(
            end_str, '%Y-%m-%dT%H:%M:%S.%f').timetuple()) * 1000)
        self.table_usage_request.set_start_time(
            start_str).set_end_time(end_str)
        result = self.handle.get_table_usage(self.table_usage_request)
        self.assertEqual(result.get_table_name(), table_name)
        self.assertEqual(len(result.get_usage_records()),
                         2 if not_cloudsim() else 1)
        records = result.get_usage_records()
        for count in range(len(records)):
            start_time_res = records[count].get_start_time()
            self.assertEqual(
                records[count].get_start_time_string(),
                datetime.fromtimestamp(
                    float(start_time_res) / 1000).isoformat())
            self.assertGreater(records[count].get_read_units(), 0)
            self.assertGreater(records[count].get_write_units(), 0)
            if not_cloudsim():
                self.assertGreater(start_time_res, start_time + count * 60000)
                self.assertLess(start_time_res,
                                start_time + (count + 1) * 60000)
                self.assertLess(start_time_res, end_time)
                self.assertEqual(records[count].get_seconds_in_period(), 60)
                self.assertLessEqual(records[count].get_storage_gb(), 0)
            self.assertEqual(records[count].get_read_throttle_count(), 0)
            self.assertEqual(records[count].get_write_throttle_count(), 0)
            self.assertEqual(records[count].get_storage_throttle_count(), 0)


if __name__ == '__main__':
    unittest.main()
