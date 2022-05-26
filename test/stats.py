#
# Copyright (c) 2018, 2022 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#
import numbers
import unittest

import time

from borneo import (
    Consistency, DefaultRetryHandler, NoSQLHandle, PutRequest, QueryRequest,
    TableRequest, StatsProfile, NoSQLHandleConfig, TableLimits)

from parameters import table_name, tenant_id
from test_base import TestBase
from testutils import get_handle_config


class TestStats(unittest.TestCase, TestBase):
    """
    Tests for checking client side statistics.
    """
    # List used by stats handler to save the stats
    stats_list = []
    # Logging of stats in seconds (minimum implemented by Python is 10s)
    INTERVAL_SEC = 10
    # Wait for stats to be logged, must be more than INTERVAL_SEC
    SLEEP_SEC = INTERVAL_SEC + 2

    @classmethod
    def setUpClass(cls):
        cls.set_up_class()
        create_statement = ('CREATE TABLE ' + table_name + '(id integer, \
        name string, primary key(id))')
        create_request = TableRequest().set_statement(create_statement)\
            .set_table_limits(TableLimits(50, 50, 1))
        cls.table_request(create_request)

    @classmethod
    def tearDownClass(cls):
        cls.tear_down_class()

    def setUp(self):
        handle_config = get_handle_config(tenant_id).set_retry_handler(
            DefaultRetryHandler()).set_consistency(Consistency.EVENTUAL)

        handle_config.set_stats_interval(self.INTERVAL_SEC)
        self.assertEqual(self.INTERVAL_SEC, handle_config.get_stats_interval())

        handle_config.set_stats_profile(StatsProfile.REGULAR)
        self.assertEqual(StatsProfile.REGULAR,
                         handle_config.get_stats_profile())

        handle_config.set_stats_pretty_print(True)
        self.assertTrue(handle_config.get_stats_pretty_print())

        handle_config.set_stats_handler(lambda x:
                                        self.stats_list.append(x)
                                        #and print(x)
                                        )
        self.assertIsNotNone(handle_config.get_stats_handler())

        self.handle = NoSQLHandle(handle_config)

    def tearDown(self):
        self.handle.close()

    def testDefaultConfig(self):
        config = get_handle_config(tenant_id)
        self.assertIsNone(config.get_stats_handler())
        self.assertEqual(NoSQLHandleConfig._DEFAULT_STATS_INTERVAL,
                         config.get_stats_interval())
        self.assertEqual(NoSQLHandleConfig._DEFAULT_STATS_PROFILE,
                         config.get_stats_profile())
        self.assertEqual(NoSQLHandleConfig._DEFAULT_STATS_PRETTY_PRINT,
                         config.get_stats_pretty_print())

    def testStatsControl(self):
        stats_control = self.handle.get_stats_control()

        self.assertEqual(self.INTERVAL_SEC, stats_control.get_interval())
        self.assertEqual(StatsProfile.REGULAR, stats_control.get_profile())
        self.assertTrue(stats_control.get_pretty_print())
        self.assertIsNotNone(stats_control.get_stats_handler())

    def loadRows(self, num_rows):
        put_request = PutRequest().set_table_name(table_name)
        for i in range(num_rows):
            row = {"id": i, "name": "Name number " + str(i)}
            put_request.set_value(row)
            self.handle.put(put_request)

    def doQuery(self, query):
        results = []
        query_req = QueryRequest().set_statement(query)
        query_res = self.handle.query(query_req)
        while not query_req.is_done():
            query_res = self.handle.query(query_req)
            results.append(query_res.get_results())
        return results

    def testStatsHandle(self):
        # Start fresh
        self.stats_list = []
        self.assertTrue(len(self.stats_list) == 0)

        self.loadRows(10)

        # To get per query stats switch to ALL stats profile
        self.handle.get_stats_control().set_profile(StatsProfile.ALL)
        self.assertEqual(StatsProfile.ALL,
                         self.handle.get_stats_control().get_profile())

        query = "select * from " + table_name
        self.doQuery(query)

        self.handle.get_stats_control().set_profile(StatsProfile.REGULAR)
        self.assertEqual(StatsProfile.REGULAR,
                         self.handle.get_stats_control().get_profile())

        # wait so the stats handle is triggered
        time.sleep(self.SLEEP_SEC)

        self.assertTrue(len(self.stats_list) > 0)

        # Check basics, all entries should contain the following:
        for stats in self.stats_list:
            self.assertIsNotNone(stats.get("clientId"))
            self.assertIsNotNone(len(stats.get("clientId")) > 0)
            self.assertIsNotNone(stats.get("startTime"))
            self.assertTrue(isinstance(stats.get("startTime"), str))
            self.assertIsNotNone(stats.get("endTime"))
            self.assertTrue(isinstance(stats.get("endTime"),str))
            self.assertIsNotNone(stats.get("requests"))
            self.assertTrue(isinstance(stats.get("requests"), list))

        # Even if there are more entries at least 1 should contain at least 1
        # request
        filtered = filter(lambda s: s.get("requests") is not None and
                          isinstance(s.get("requests"), list) and
                          len(s.get("requests")) > 0 and
                          s.get("requests")[0] is not None and
                          isinstance(s.get("requests")[0], dict),
                          self.stats_list)
        self.assertTrue(len(list(filtered)) >= 1)

        # At least one entry should have a request with everything inside
        filtered = filter(lambda s:
            s.get("requests") is not None and
            isinstance(s.get("requests"), list) and
            len(s.get("requests")) > 0 and
            s.get("requests")[0] is not None and
            isinstance(s.get("requests")[0], dict) and

            s.get("requests")[0]["httpRequestCount"] is not None and
            isinstance(s.get("requests")[0]["httpRequestCount"], numbers.Number)
                and

            s.get("requests")[0]["name"] is not None and
            isinstance(s.get("requests")[0]["name"], str) and

            s.get("requests")[0]["rateLimitDelayMs"] is not None and
            isinstance(s.get("requests")[0]["rateLimitDelayMs"], numbers.Number)
                and

            s.get("requests")[0]["errors"] is not None and
            isinstance(s.get("requests")[0]["errors"], numbers.Number) and

            s.get("requests")[0]["requestSize"] is not None and
            isinstance(s.get("requests")[0]["requestSize"], dict) and
            s.get("requests")[0]["requestSize"]["min"] is not None and
            isinstance(s.get("requests")[0]["requestSize"]["min"],
                       numbers.Number) and
            s.get("requests")[0]["requestSize"]["avg"] is not None and
            isinstance(s.get("requests")[0]["requestSize"]["avg"],
                       numbers.Number) and
            s.get("requests")[0]["requestSize"]["max"] is not None and
            isinstance(s.get("requests")[0]["requestSize"]["max"],
                       numbers.Number) and

            s.get("requests")[0]["resultSize"] is not None and
            isinstance(s.get("requests")[0]["resultSize"], dict) and
            s.get("requests")[0]["resultSize"]["min"] is not None and
            isinstance(s.get("requests")[0]["resultSize"]["min"],
                       numbers.Number) and
            s.get("requests")[0]["resultSize"]["avg"] is not None and
            isinstance(s.get("requests")[0]["resultSize"]["avg"],
                       numbers.Number) and
            s.get("requests")[0]["resultSize"]["max"] is not None and
            isinstance(s.get("requests")[0]["resultSize"]["max"],
                       numbers.Number) and

            s.get("requests")[0]["httpRequestLatencyMs"] is not None and
            isinstance(s.get("requests")[0]["httpRequestLatencyMs"], dict) and
            s.get("requests")[0]["httpRequestLatencyMs"]["min"] is not None and
            isinstance(s.get("requests")[0]["httpRequestLatencyMs"]["min"],
                       numbers.Number) and
            s.get("requests")[0]["httpRequestLatencyMs"]["avg"] is not None and
            isinstance(s.get("requests")[0]["httpRequestLatencyMs"]["avg"],
                       numbers.Number) and
            s.get("requests")[0]["httpRequestLatencyMs"]["max"] is not None and
            isinstance(s.get("requests")[0]["httpRequestLatencyMs"]["max"],
                       numbers.Number) and

            s.get("requests")[0]["retry"] is not None and
            isinstance(s.get("requests")[0]["retry"], dict) and
            s.get("requests")[0]["retry"]["delayMs"] is not None and
            isinstance(s.get("requests")[0]["retry"]["delayMs"],
                       numbers.Number) and
            s.get("requests")[0]["retry"]["authCount"] is not None and
            isinstance(s.get("requests")[0]["retry"]["authCount"],
                       numbers.Number) and
            s.get("requests")[0]["retry"]["throttleCount"] is not None and
            isinstance(s.get("requests")[0]["retry"]["throttleCount"],
                       numbers.Number) and
            s.get("requests")[0]["retry"]["count"] is not None and
            isinstance(s.get("requests")[0]["retry"]["count"],
                       numbers.Number),
            self.stats_list)
        self.assertTrue(len(list(filtered)) >= 1)

        filtered = filter(lambda s: s.get("queries") is not None and
                          isinstance(s.get("queries"), list) and
                          len(s.get("queries")) == 1 and
                          s.get("queries")[0] is not None and
                          isinstance(s.get("queries")[0], dict) and
                          s.get("queries")[0]["query"] is not None and
                          s.get("queries")[0]["query"] == query,
                          self.stats_list)
        self.assertTrue(len(list(filtered)) >= 1)

        filtered = filter(lambda s:
            s.get("queries") is not None and
            isinstance(s.get("queries"), list) and
            len(s.get("queries")) == 1 and
            s.get("queries")[0] is not None and
            isinstance(s.get("queries")[0], dict) and

            s.get("queries")[0]["query"] is not None and
            isinstance(s.get("queries")[0]["query"], str) and

            s.get("queries")[0]["doesWrites"] is not None and
            isinstance(s.get("queries")[0]["doesWrites"], bool) and

            s.get("queries")[0]["unprepared"] is not None and
            isinstance(s.get("queries")[0]["unprepared"], numbers.Number) and

            s.get("queries")[0]["httpRequestCount"] is not None and
            isinstance(s.get("queries")[0]["httpRequestCount"], numbers.Number)
            and

            s.get("queries")[0]["count"] is not None and
            isinstance(s.get("queries")[0]["count"], numbers.Number) and

            s.get("queries")[0]["simple"] is not None and
            isinstance(s.get("queries")[0]["simple"], bool) and

            s.get("queries")[0]["rateLimitDelayMs"] is not None and
            isinstance(s.get("queries")[0]["rateLimitDelayMs"], numbers.Number)
            and

            s.get("queries")[0]["errors"] is not None and
            isinstance(s.get("queries")[0]["errors"], numbers.Number) and

            s.get("queries")[0]["requestSize"] is not None and
            isinstance(s.get("queries")[0]["requestSize"], dict) and
            s.get("queries")[0]["requestSize"]["min"] is not None and
            isinstance(s.get("queries")[0]["requestSize"]["min"],
                       numbers.Number) and
            s.get("queries")[0]["requestSize"]["avg"] is not None and
            isinstance(s.get("queries")[0]["requestSize"]["avg"],
                       numbers.Number) and
            s.get("queries")[0]["requestSize"]["max"] is not None and
            isinstance(s.get("queries")[0]["requestSize"]["max"],
                       numbers.Number) and

            s.get("queries")[0]["resultSize"] is not None and
            isinstance(s.get("queries")[0]["resultSize"], dict) and
            s.get("queries")[0]["resultSize"]["min"] is not None and
            isinstance(s.get("queries")[0]["resultSize"]["min"],
                       numbers.Number) and
            s.get("queries")[0]["resultSize"]["avg"] is not None and
            isinstance(s.get("queries")[0]["resultSize"]["avg"],
                       numbers.Number) and
            s.get("queries")[0]["resultSize"]["max"] is not None and
            isinstance(s.get("queries")[0]["resultSize"]["max"],
                       numbers.Number) and

            s.get("queries")[0]["httpRequestLatencyMs"] is not None and
            isinstance(s.get("queries")[0]["httpRequestLatencyMs"], dict) and
            s.get("queries")[0]["httpRequestLatencyMs"]["min"] is not None and
            isinstance(s.get("queries")[0]["httpRequestLatencyMs"]["min"],
                       numbers.Number) and
            s.get("queries")[0]["httpRequestLatencyMs"]["avg"] is not None and
            isinstance(s.get("queries")[0]["httpRequestLatencyMs"]["avg"],
                       numbers.Number) and
            s.get("queries")[0]["httpRequestLatencyMs"]["max"] is not None and
            isinstance(s.get("queries")[0]["httpRequestLatencyMs"]["max"],
                       numbers.Number) and
            s.get("queries")[0]["httpRequestLatencyMs"]["95th"] is not None and
            isinstance(s.get("queries")[0]["httpRequestLatencyMs"]["95th"],
                       numbers.Number) and
            s.get("queries")[0]["httpRequestLatencyMs"]["99th"] is not None and
            isinstance(s.get("queries")[0]["httpRequestLatencyMs"]["99th"],
                       numbers.Number) and

            s.get("queries")[0]["retry"] is not None and
            isinstance(s.get("queries")[0]["retry"], dict) and
            s.get("queries")[0]["retry"]["delayMs"] is not None and
            isinstance(s.get("queries")[0]["retry"]["delayMs"],
                       numbers.Number) and
            s.get("queries")[0]["retry"]["authCount"] is not None and
            isinstance(s.get("queries")[0]["retry"]["authCount"],
                       numbers.Number) and
            s.get("queries")[0]["retry"]["throttleCount"] is not None and
            isinstance(s.get("queries")[0]["retry"]["throttleCount"],
                       numbers.Number) and
            s.get("queries")[0]["retry"]["count"] is not None and
            isinstance(s.get("queries")[0]["retry"]["count"],
                       numbers.Number),
            self.stats_list)
        self.assertTrue(len(list(filtered)) >= 1)

    def testStopStart(self):
        # stop observations and wait out current observation
        self.handle.get_stats_control().stop()
        self.assertFalse(self.handle.get_stats_control().is_started())

        time.sleep(self.SLEEP_SEC)

        # Start fresh
        self.stats_list = []
        self.assertTrue(len(self.stats_list) == 0)

        self.loadRows(10)

        # To get per query stats switch to ALL stats profile
        self.handle.get_stats_control().set_profile(StatsProfile.ALL)
        self.assertEqual(StatsProfile.ALL,
                         self.handle.get_stats_control().get_profile())

        query = "select * from " + table_name
        self.doQuery(query)

        self.handle.get_stats_control().set_profile(StatsProfile.REGULAR)
        self.assertEqual(StatsProfile.REGULAR,
                         self.handle.get_stats_control().get_profile())

        # wait so the stats handle is triggered
        time.sleep(self.SLEEP_SEC)
        self.assertTrue(len(self.stats_list) > 0)

        # all entries should not have any requests
        filtered = filter(lambda s: s.get("requests") is not None and
                          isinstance(s.get("requests"), list) and
                          len(s.get("requests")) == 0,
                          self.stats_list)
        self.assertEqual(len(list(filtered)), len(self.stats_list))

        # and all entries should not contain any queries
        filtered = filter(lambda s: s.get("queries") is not None,
                          self.stats_list)
        self.assertEqual(0, len(list(filtered)))

        # Start observations and check if stats list contain some
        self.handle.get_stats_control().start()
        self.assertTrue(self.handle.get_stats_control().is_started())

        # start fresh
        self.stats_list = []

        self.loadRows(10)

        self.handle.get_stats_control().set_profile(StatsProfile.ALL)
        self.assertEqual(StatsProfile.ALL,
                         self.handle.get_stats_control().get_profile())

        self.doQuery(query)

        self.handle.get_stats_control().set_profile(StatsProfile.REGULAR)
        self.assertEqual(StatsProfile.REGULAR,
                         self.handle.get_stats_control().get_profile())

        # the code above should have triggered the stats collection.
        # wait for the stats handle to be called at the end of the interval
        time.sleep(self.SLEEP_SEC)

        self.assertTrue(len(self.stats_list) > 0)

        # All entries should have many requests:
        filtered = filter(lambda s: s.get("requests") is not None and
                          isinstance(s.get("requests"), list) and
                          len(s.get("requests")) > 0,
                          self.stats_list)
        self.assertTrue(len(list(filtered)) > 0)
        #  - and have 1 query
        filtered = filter(lambda s: s.get("queries") is not None and
                          isinstance(s.get("queries"), list) and
                          len(s.get("queries")) == 1,
                          self.stats_list)
        self.assertTrue(len(list(filtered)) > 0)


if __name__ == '__main__':
    unittest.main()
