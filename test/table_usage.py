#
# Copyright (c) 2018, 2022 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#

import unittest
from datetime import datetime, timedelta
from dateutil import parser, tz
from time import mktime, sleep, time

from borneo import (
    GetRequest, IllegalArgumentException, OperationNotSupportedException,
    PutRequest, TableLimits, TableNotFoundException, TableRequest,
    TableUsageRequest)
from parameters import is_onprem, not_cloudsim, table_name, timeout
from test_base import TestBase
from testutils import get_row


class TestTableUsage(unittest.TestCase, TestBase):

    @classmethod
    def setUpClass(cls):
        cls.handle = None
        cls.set_up_class()
        create_statement = (
            'CREATE TABLE ' + table_name + '(fld_id INTEGER, fld_long LONG, \
fld_float FLOAT, fld_double DOUBLE, fld_bool BOOLEAN, fld_str STRING, \
fld_bin BINARY, fld_time TIMESTAMP(7), fld_num NUMBER, fld_json JSON, \
fld_arr ARRAY(STRING), fld_map MAP(STRING), \
fld_rec RECORD(fld_id LONG, fld_bool BOOLEAN, fld_str STRING), \
PRIMARY KEY(fld_id)) USING TTL 1 HOURS')
        limits = TableLimits(100, 100, 1)
        create_request = TableRequest().set_statement(
            create_statement).set_table_limits(limits)
        cls.table_request(create_request)
        # put and get some data, read_units = 100, write_units = 199
        row = get_row()
        key = {'fld_id': 1}
        put_request = PutRequest().set_value(row).set_table_name(table_name)
        get_request = GetRequest().set_key(key).set_table_name(table_name)
        count = 0
        while count < 100:
            cls.handle.put(put_request)
            cls.handle.get(get_request)
            count += 1
            # sleep to allow records to accumulate over time, but not if
            # using Cloudsim.
            if not_cloudsim() and not is_onprem():
                sleep(2)
        # need to sleep to allow usage records to accumulate but not if
        # using CloudSim, which doesn't generate usage records.
        if not_cloudsim() and not is_onprem():
            sleep(40)

    @classmethod
    def tearDownClass(cls):
        cls.tear_down_class()

    def setUp(self):
        self.set_up()
        self.table_usage_request = TableUsageRequest().set_timeout(timeout)

    def tearDown(self):
        self.tear_down()

    def testTableUsageSetIllegalTableName(self):
        self.assertRaises(IllegalArgumentException,
                          self.table_usage_request.set_table_name,
                          {'name': table_name})
        if not is_onprem and not_cloudsim():
            self.table_usage_request.set_table_name('IllegalTable')
            self.assertRaises(TableNotFoundException,
                              self.handle.get_table_usage,
                              self.table_usage_request)

    def testTableUsageSetIllegalCompartment(self):
        self.assertRaises(IllegalArgumentException,
                          self.table_usage_request.set_compartment, {})
        self.assertRaises(IllegalArgumentException,
                          self.table_usage_request.set_compartment, '')

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

    def testTableUsageIllegalRequest(self):
        self.assertRaises(IllegalArgumentException, self.handle.get_table_usage,
                          'IllegalRequest')

    def testTableUsageSetStartEndTime(self):
        strs = [
            '2017-12-05',
            '2017-12-05T01Z',
            '2017-12-05T1:2:0Z',
            '2017-12-05T01:02:03Z',
            '2017-12-05T01:02:03.123456789Z',
            '2017-12-05-02:01',
            '2017-12-05T12:39+00:00',
            '2017-12-05T01:02:03+00:00',
            '2017-12-05T01:02:03.123+00:00',
            '2017-12-01T01:02:03+02:01',
            '2017-12-01T01:02:03.987654321+02:01',
            '2017-12-01T01:02:03.0-03:00'
        ]
        exp_strs = [
            '2017-12-05T00:00:00+00:00',
            '2017-12-05T01:00:00+00:00',
            '2017-12-05T01:02:00+00:00',
            '2017-12-05T01:02:03+00:00',
            '2017-12-05T01:02:03.123000+00:00',
            '2017-12-05T02:01:00+00:00',
            '2017-12-05T12:39:00+00:00',
            '2017-12-05T01:02:03+00:00',
            '2017-12-05T01:02:03.123000+00:00',
            '2017-11-30T23:01:03+00:00',
            '2017-11-30T23:01:03.987000+00:00',
            '2017-12-01T04:02:03+00:00'
        ]

        for i in range(len(strs)):
            self.table_usage_request.set_start_time(strs[i])
            self.assertEqual(
                self.table_usage_request.get_start_time_string(), exp_strs[i])
            self.table_usage_request.set_end_time(strs[i])
            self.assertEqual(
                self.table_usage_request.get_end_time_string(), exp_strs[i])

    def testTableUsageGets(self):
        start = int(round(time() * 1000))
        start_dt = datetime.fromtimestamp(float(start) / 1000)
        end_dt = datetime.fromtimestamp(round(time() * 1000) / 1000)
        end_str = end_dt.isoformat()
        end = (int(mktime(end_dt.timetuple()) * 1000) +
               end_dt.microsecond // 1000)
        self.table_usage_request.set_table_name(table_name).set_start_time(
            start).set_end_time(end_str).set_limit(5)
        self.assertEqual(self.table_usage_request.get_table_name(), table_name)
        self.assertIsNone(self.table_usage_request.get_compartment())
        self.assertEqual(self.table_usage_request.get_start_time(), start)
        self.assertEqual(self.table_usage_request.get_start_time_string(),
                         start_dt.replace(tzinfo=tz.UTC).isoformat())
        self.assertEqual(self.table_usage_request.get_end_time(), end)
        self.assertEqual(self.table_usage_request.get_end_time_string(),
                         end_dt.replace(tzinfo=tz.UTC).isoformat())
        self.assertEqual(self.table_usage_request.get_limit(), 5)

    def testTableUsageNormal(self):
        self.table_usage_request.set_table_name(table_name)
        if is_onprem():
            self.assertRaises(OperationNotSupportedException,
                              self.handle.get_table_usage,
                              self.table_usage_request)
            return
        result = self.handle.get_table_usage(self.table_usage_request)
        current = int(round(time() * 1000))
        # TODO: The start time of the table usage record get from the proxy
        # should be in 1 min, that is from current - 60000 to current, but
        # currently for minicloud it is in 2 mins from current - 120000 to
        # current. Seconds in period for the table usage record is also not
        # stable, sometimes it is 0 and sometimes it is 60. So we need to check
        # it separately.
        self._check_table_usage_result(result, 1, current - 60000,
                                       check_separately=True)

    def testTableUsageWithStartTime(self):
        # set the start time
        start_time = int(round(time() * 1000)) - 120000
        self.table_usage_request.set_table_name(table_name).set_start_time(
            start_time)
        if is_onprem():
            self.assertRaises(OperationNotSupportedException,
                              self.handle.get_table_usage,
                              self.table_usage_request)
            return
        result = self.handle.get_table_usage(self.table_usage_request)
        self._check_table_usage_result(result, 1, start_time)
        # set the start time in ISO 8601 formatted string
        start_str = (datetime.now() + timedelta(seconds=-120)).isoformat()
        start_time = int(mktime(parser.parse(start_str).timetuple()) * 1000)
        self.table_usage_request.set_start_time(start_str)
        result = self.handle.get_table_usage(self.table_usage_request)
        self._check_table_usage_result(result, 1, start_time)

    def testTableUsageWithEndTime(self):
        # set a start time to avoid unexpected table usage information, and set
        # the end time (end time is smaller than start time)
        current = int(round(time() * 1000))
        start_time = current - 120000
        end_time = current - 180000
        self.table_usage_request.set_table_name(table_name).set_start_time(
            start_time).set_end_time(end_time)
        self.assertRaises(IllegalArgumentException,
                          self.handle.get_table_usage,
                          self.table_usage_request)
        if is_onprem():
            return
        # set current time as end time
        self.table_usage_request.set_end_time(current)
        result = self.handle.get_table_usage(self.table_usage_request)
        self._check_table_usage_result(result, 1, start_time, current)
        # set current time in ISO 8601 formatted string as end time
        end_str = datetime.now().isoformat()
        end_time = int(mktime(parser.parse(end_str).timetuple()) * 1000)
        self.table_usage_request.set_end_time(end_str)
        result = self.handle.get_table_usage(self.table_usage_request)
        self._check_table_usage_result(result, 1, start_time, end_time)

    def testTableUsageWithLimit(self):
        # set the limit
        self.table_usage_request.set_table_name(table_name).set_limit(3)
        if is_onprem():
            self.assertRaises(OperationNotSupportedException,
                              self.handle.get_table_usage,
                              self.table_usage_request)
            return
        result = self.handle.get_table_usage(self.table_usage_request)
        current = int(round(time() * 1000))
        # TODO: The start time of the table usage record get from the proxy
        # should be in 1 min, that is from current - 60000 to current, but
        # currently for minicloud it is in 2 mins from current - 120000 to
        # current. Seconds in period for the table usage record is also not
        # stable, sometimes it is 0 and sometimes it is 60. So we need to check
        # it separately.
        self._check_table_usage_result(result, 1, current - 60000,
                                       check_separately=True)

    def testTableUsageWithStartTimeAndLimit(self):
        # set the start time and limit
        start_time = int(round(time() * 1000)) - 240000
        limit = 2
        self.table_usage_request.set_table_name(table_name).set_start_time(
            start_time).set_limit(limit)
        if is_onprem():
            self.assertRaises(OperationNotSupportedException,
                              self.handle.get_table_usage,
                              self.table_usage_request)
            return
        result = self.handle.get_table_usage(self.table_usage_request)
        self._check_table_usage_result(
            result, limit if not_cloudsim() else 1, start_time)
        # set the start time in ISO 8601 formatted string and limit
        start_str = (datetime.now() + timedelta(seconds=-240)).isoformat()
        start_time = int(mktime(parser.parse(start_str).timetuple()) * 1000)
        self.table_usage_request.set_start_time(start_str)
        result = self.handle.get_table_usage(self.table_usage_request)
        self._check_table_usage_result(
            result, limit if not_cloudsim() else 1, start_time)

    def testTableUsageWithStartEndTimeAndLimit(self):
        # start time, end time and limit
        current = int(round(time() * 1000))
        start_time = current - 180000
        self.table_usage_request.set_table_name(table_name).set_start_time(
            start_time).set_end_time(current).set_limit(2)
        if is_onprem():
            self.assertRaises(OperationNotSupportedException,
                              self.handle.get_table_usage,
                              self.table_usage_request)
            return
        result = self.handle.get_table_usage(self.table_usage_request)
        self._check_table_usage_result(
            result, 2 if not_cloudsim() else 1, start_time, current)
        # start time, end time in ISO 8601 formatted string and limit
        current = datetime.now()
        start_str = (current + timedelta(seconds=-180)).isoformat()
        end_str = (current + timedelta(seconds=-60)).isoformat()
        start_time = int(mktime(parser.parse(start_str).timetuple()) * 1000)
        end_time = int(mktime(parser.parse(end_str).timetuple()) * 1000)
        self.table_usage_request.set_start_time(
            start_str).set_end_time(end_str)
        result = self.handle.get_table_usage(self.table_usage_request)
        self._check_table_usage_result(
            result, 2 if not_cloudsim() else 1, start_time, end_time)

    def _check_table_usage_result(self, result, num_records, start=0, end=0,
                                  check_separately=False):
        self.assertEqual(result.get_table_name(), table_name)
        self.assertEqual(len(result.get_usage_records()), num_records)
        records = result.get_usage_records()
        for count in range(len(records)):
            start_time_res = records[count].get_start_time()
            self.assertIsNotNone(start_time_res)
            self.assertEqual(records[count].get_start_time_string(),
                             datetime.fromtimestamp(
                                 float(start_time_res) / 1000).replace(
                                     tzinfo=tz.UTC).isoformat())
            self.assertGreaterEqual(records[count].get_read_units(), 0)
            self.assertGreaterEqual(records[count].get_write_units(), 0)
            self.assertGreaterEqual(records[count].get_storage_gb(), 0)
            if not_cloudsim():
                # the record is generated during the time from start to end.
                if check_separately:
                    self.assertGreater(start_time_res,
                                       start + (count - 1) * 60000)
                    self.assertTrue(
                        records[count].get_seconds_in_period() in [0, 60])
                else:
                    self.assertGreaterEqual(start_time_res,
                                            start + count * 60000)
                    self.assertEqual(
                        records[count].get_seconds_in_period(), 60)
                end_time = start + (count + 1) * 60000
                self.assertLessEqual(
                    start_time_res,
                    end_time if end == 0 else min(end_time, end))
            self.assertEqual(records[count].get_read_throttle_count(), 0)
            self.assertEqual(records[count].get_write_throttle_count(), 0)
            self.assertEqual(records[count].get_storage_throttle_count(), 0)


if __name__ == '__main__':
    unittest.main()
