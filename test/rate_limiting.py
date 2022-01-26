#
# Copyright (c) 2018, 2022 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#

import unittest

from random import random
from time import time

from borneo import (
    Consistency, DefaultRetryHandler, GetRequest, NoSQLHandle, PrepareRequest,
    PutRequest, QueryRequest, ReadThrottlingException, RequestTimeoutException,
    TableLimits, TableRequest, WriteThrottlingException)
from borneo.http import SimpleRateLimiter

from parameters import is_onprem, rate_limiter_extended, table_name, tenant_id
from test_base import TestBase
from testutils import get_handle_config


class TestRateLimiting(unittest.TestCase, TestBase):
    """
    Tests for driver-side rate limiting. These tests require a Cloud Simulator
    instance as rate limiting is not available or need for on-premise.
    """
    if not is_onprem():

        @classmethod
        def setUpClass(cls):
            cls.set_up_class()
            create_statement = ('CREATE TABLE ' + table_name + '(id integer, \
name string, primary key(id))')
            create_request = TableRequest().set_statement(
                create_statement).set_table_limits(TableLimits(100, 100, 1))
            cls.table_request(create_request)

        @classmethod
        def tearDownClass(cls):
            cls.tear_down_class()

        def setUp(self):
            handle_config = get_handle_config(tenant_id).set_retry_handler(
                DefaultRetryHandler()).set_consistency(Consistency.EVENTUAL)
            self.handle = NoSQLHandle(handle_config)

        def tearDown(self):
            self.handle.close()

        def testRateLimitingBasicInternal(self):
            self._test_limiters(False, 500, 200, 200, 10, 100.0)

        def testRateLimitingBasicExternal(self):
            self._test_limiters(True, 500, 200, 200, 10, 100.0)

        def testRateLimitingBasicInternalPercent(self):
            self._test_limiters(False, 500, 200, 200, 10, 20.0)

        def testRateLimitingBasicExternalPercent(self):
            self._test_limiters(True, 500, 200, 200, 10, 20.0)

        def testRateLimitingExtendedInternalFull(self):
            # Skip unless extended tests are enabled
            if rate_limiter_extended():
                allunits = [1, 50, 300]
                for units in allunits:
                    if units == 500:
                        self._test_limiters(False, 500, units, units, 10, 100.0)

        def testRateLimitingExtendedInternalPercent(self):
            # Skip unless extended tests are enabled
            if rate_limiter_extended():
                allunits = [10, 100, 2000]
                for units in allunits:
                    self._test_limiters(False, 500, units, units, 10, 10.0)

        def testRateLimitingExtendedExternalFull(self):
            # Skip unless extended tests are enabled
            if rate_limiter_extended():
                allunits = [1, 50, 300]
                for units in allunits:
                    self._test_limiters(True, 500, units, units, 10, 100.0)

        def testRateLimitingExtendedExternalPercent(self):
            # Skip unless extended tests are enabled
            if rate_limiter_extended():
                allunits = [10, 100, 2000]
                for units in allunits:
                    self._test_limiters(True, 500, units, units, 10, 10.0)

        def _alter_table_limits(self, limits):
            table_request = TableRequest().set_table_name(
                table_name).set_table_limits(limits).set_timeout(15000)
            return self.handle.do_table_request(table_request, 15000, 1000)

        def _do_rate_limited_ops(
                self, num_seconds, read_limit, write_limit, max_rows,
                check_units, use_percent, use_external_limiters):
            """
            Runs puts and gets continuously for N seconds.

            Verify that the resultant RUs/WUs used match the given rate limits.
            """
            if read_limit == 0 and write_limit == 0:
                return
            put_request = PutRequest().set_table_name(table_name)
            get_request = GetRequest().set_table_name(table_name)
            key = dict()
            # TODO: random sizes 0-nKB.
            value = dict()
            value['name'] = 'jane'

            start_time = int(round(time() * 1000))
            end_time = start_time + num_seconds * 1000
            read_units_used = 0
            write_units_used = 0
            total_delayed_ms = 0
            throttle_exceptions = 0
            rlim = None
            wlim = None

            max_val = float(read_limit + write_limit)
            if not use_external_limiters:
                # Reset internal limiters so they don't have unused units.
                self.handle.get_client().reset_rate_limiters(table_name)
            else:
                rlim = SimpleRateLimiter(read_limit * use_percent / 100.0, 1)
                wlim = SimpleRateLimiter(write_limit * use_percent / 100.0, 1)

            while True:
                fld_id = int(random() * max_rows)
                if read_limit == 0:
                    do_put = True
                elif write_limit == 0:
                    do_put = False
                else:
                    v = int(random() * max_val)
                    do_put = v >= read_limit
                try:
                    if do_put:
                        value['id'] = fld_id
                        put_request.set_value(value).set_read_rate_limiter(
                            None).set_write_rate_limiter(wlim)
                        pres = self.handle.put(put_request)
                        write_units_used += pres.get_write_units()
                        total_delayed_ms += pres.get_rate_limit_delayed_ms()
                        rs = pres.get_retry_stats()
                        if rs is not None:
                            throttle_exceptions += rs.get_num_exceptions(
                                WriteThrottlingException.__class__.__name__)
                    else:
                        key['id'] = fld_id
                        get_request.set_key(key).set_read_rate_limiter(
                            rlim).set_write_rate_limiter(None)
                        gres = self.handle.get(get_request)
                        read_units_used += gres.get_read_units()
                        total_delayed_ms += gres.get_rate_limit_delayed_ms()
                        rs = gres.get_retry_stats()
                        if rs is not None:
                            throttle_exceptions += rs.get_num_exceptions(
                                ReadThrottlingException.__class__.__name__)
                except ReadThrottlingException:
                    self.fail(
                        'Expected no read throttling exceptions, got one.')
                except WriteThrottlingException:
                    self.fail(
                        'Expected no write throttling exceptions, got one.')

                if int(round(time() * 1000)) >= end_time:
                    break
            num_seconds = (int(round(time() * 1000)) - start_time) / 1000
            rus = read_units_used / num_seconds
            wus = write_units_used / num_seconds
            if not check_units:
                return
            use_percent /= 100.0
            if (rus < read_limit * use_percent * 0.8 or
                    rus > read_limit * use_percent * 1.2):
                self.fail(
                    'Gets: Expected around ' + str(read_limit * use_percent) +
                    ' RUs, got ' + str(rus))
            if (wus < write_limit * use_percent * 0.8 or
                    wus > write_limit * use_percent * 1.2):
                self.fail(
                    'Puts: Expected around ' + str(write_limit * use_percent) +
                    ' WUs, got ' + str(wus))

        def _do_rate_limited_queries(
                self, num_seconds, read_limit, max_kb, single_partition,
                use_percent, use_external_limiters):
            """
            Runs queries continuously for N seconds.

            Verify that the resultant RUs used match the given rate limit.
            """
            start_time = int(round(time() * 1000))
            end_time = start_time + num_seconds * 1000
            read_units_used = 0
            rlim = None
            wlim = None
            if not use_external_limiters:
                # Reset internal limiters so they don't have unused units.
                self.handle.get_client().reset_rate_limiters(table_name)
            else:
                rlim = SimpleRateLimiter(read_limit * use_percent / 100.0, 1)
                wlim = SimpleRateLimiter(read_limit * use_percent / 100.0, 1)
            prep_req = PrepareRequest()
            if single_partition:
                # Query based on single partition scanning.
                fld_id = int(random() * 500)
                prep_req.set_statement(
                    'SELECT * FROM ' + table_name + ' WHERE id = ' +
                    str(fld_id))
            else:
                # Query based on all partitions scanning.
                prep_req.set_statement(
                    'SELECT * FROM ' + table_name + ' WHERE name = "jane"')
            prep_res = self.handle.prepare(prep_req)
            self.assertTrue(prep_res.get_prepared_statement() is not None,
                            'Prepare statement failed.')
            read_units_used += prep_res.get_read_units()

            while True:
                """
                We need a 20 second timeout because in some cases this is called
                on a table with 500 rows and 50RUs (uses 1000RUs = 20 seconds).
                """
                query_req = QueryRequest().set_prepared_statement(
                    prep_res).set_timeout(20000).set_read_rate_limiter(
                    rlim).set_write_rate_limiter(wlim)
                if max_kb > 0:
                    # Query with size limit.
                    query_req.set_max_read_kb(max_kb)
                try:
                    while True:
                        res = self.handle.query(query_req)
                        res.get_results()
                        read_units_used += res.get_read_units()
                        if query_req.is_done():
                            break
                except ReadThrottlingException:
                    self.fail('Expected no throttling exceptions, got one.')
                except RequestTimeoutException:
                    # This may happen for very small limit tests.
                    pass

                if int(round(time() * 1000)) >= end_time:
                    break

            num_seconds = (int(round(time() * 1000)) - start_time) / 1000
            use_percent /= 100.0
            rus = read_units_used / num_seconds
            expected_rus = read_limit * use_percent
            # For very small expected amounts, just verify within 1 RU.
            if (expected_rus < 4 and
                    expected_rus - 1 <= rus <= expected_rus + 1):
                return
            if rus < expected_rus * 0.6 or rus > expected_rus * 1.5:
                self.fail('Queries: Expected around ' + str(expected_rus) +
                          ' RUs, got ' + str(rus))

        def _run_limited_ops_on_table(
                self, read_limit, write_limit, max_seconds, max_rows,
                use_percent, use_external_limiters):
            """
            Runs get/puts then queries on a table. Verify RUs/WUs are within
            given limits.
            """
            self._alter_table_limits(TableLimits(read_limit, write_limit, 50))
            """
            We have to do the read/write ops separately since we're running
            single-threaded, and the result is hard to tell if it's correct
            (example: we'd get 37RUs and 15WUs).
            """
            self._do_rate_limited_ops(
                max_seconds, 0, write_limit, max_rows, True, use_percent,
                use_external_limiters)
            self._do_rate_limited_ops(
                max_seconds, read_limit, 0, max_rows, True, use_percent,
                use_external_limiters)
            # Query based on single partition scanning.
            self._do_rate_limited_queries(
                max_seconds, read_limit, 20, True, use_percent,
                use_external_limiters)
            # Query based on all partitions scanning.
            self._do_rate_limited_queries(
                max_seconds, read_limit, 20, False, use_percent,
                use_external_limiters)

        def _test_limiters(
            self, use_external_limiters, max_rows, read_limit, write_limit,
                test_seconds, use_percent):
            # Clear any previous rate limiters.
            client = self.handle.get_client()
            client.enable_rate_limiting(False, 100.0)
            # Configure our handle for rate limiting.
            if not use_external_limiters:
                client.enable_rate_limiting(True, use_percent)
            # Limit bursts in tests.
            client.set_ratelimiter_duration_seconds(1)
            # Then do the actual testing.
            self._run_limited_ops_on_table(
                read_limit, write_limit, test_seconds, max_rows, use_percent,
                use_external_limiters)


if __name__ == '__main__':
    unittest.main()
