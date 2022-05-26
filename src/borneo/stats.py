#
# Copyright (c) 2018, 2022 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#

import pprint
import sys
import uuid
from collections import Callable
from datetime import datetime
from logging import INFO
from threading import Timer, Lock
from time import localtime
from typing import Any, Dict

from . import StatsProfile
from .exception import SecurityInfoNotReadyException, ThrottlingException, \
    IllegalArgumentException
from .common import LogUtils, synchronized, CheckValue
from .kv.exception import AuthenticationException
from .version import __version__


class StatsControl:
    """
    StatsControl allows user to control the collection of driver statistics at
    runtime.

    The statistics data is collected for an interval of time. At the end of the
    interval, the stats data is logged in a specified JSON format that can be
    filtered and parsed. After the logging, the counters are cleared and
    collection of data resumes.

    Collection intervals are aligned to the top of the hour. This means first
    interval logs may contain stats for a shorter interval.

    Collection of stats are controlled by the following environment variables:

      NOSQL_STATS_PROFILE=[none|regular|more|all]

         Specifies the stats profile:

          * none - disabled,
          * regular - per request: counters, errors, latencies, delays, retries.
            This incurs minimum overhead.
          * more - stats above with 95th and 99th percentile latencies.
            This may add 0.5% overhead compared to none stats profile.
          * all - stats above with per query information.
            This may add 1% overhead compared to none stats profile.

      NOSQL_STATS_INTERVAL=600 Interval in seconds to log the stats, by
      default is 10 minutes.

      NOSQL_STATS_PRETTY_PRINT=true Option to enable pretty printing of the JSON
      data, default value is false.

    Collection of stats can also be used by using the API:

    :py:meth:`NoSQLHandleConfig.set_stats_profile` or
    :py:meth:`StatsControl.set_profile`. At runtime stats collection can be
    enabled selectively by using :py:meth:`StatsControl.start` ond
    :py:meth:`StatsControl.stop`. The following example shows how to use a stats
    handler and how to control the stas at runtime::

        def stats_handler(stats):
            # type: (Dict) -> None
            print("Stats : " + str(stats))
        ...
        config = NoSQLHandleConfig( endpoint )
        config.set_stats_profile(StatsProfile.REGULAR)
        config.set_stats_interval(600)
        config.set_stats_pretty_print(False)
        config.set_stats_handler(stats_handler)

        handle = NoSQLHandle(config)

        handle = get_handle(tenant_id)

        stats_control = handle.get_stats_control()

        #... application code without stats

        # enable observations
        stats_control.start();

        #... application code with REGULAR stats

        # For particular parts of code profile can be changed to collect more stats.
        stats_control.set_stats_profile(StatsProfile.ALL)
        #... more sensitive code with ALL stats

        stats_control.set_stats_profile(StatsProfile.REGULAR)
        #... application code with REGULAR stats

        # disable observations
        stats_control.stop()

        #... application code without stats
        handle.close()

    The following is an example of stats log entry using the ALL
    profile:

    - A one time entry containing stats id and options::

        INFO: Client stats|{    // INFO log entry
        "sdkName" : "Oracle NoSQL SDK for Python",  // SDK name
        "sdkVersion" : "5.2.4",                 // SDK version
        "clientId" : "f595b333",                  // NoSQLHandle id
        "profile" : "ALL",                        // stats profile
        "intervalSec" : 600,                      // interval length in seconds
        "prettyPrint" : true,                     // JSON pretty print
        "rateLimitingEnabled" : false}            // if rate limiting is enabled

    - An entry at the end of each interval containing the stats values::

         ``INFO: Client stats|{
         "clientId" : "b7bc7734",              // id of NoSQLHandle object
         "startTime" : "2021-09-20T20:11:42Z", // UTC start interval time
         "endTime" : "2021-09-20T20:11:47Z",   // UTC end interval time
         "requests" : [{                       // array of types of requests
           "name" : "Get",                       // stats for GET request type
           "httpRequestCount" : 2,               // count of http requests
           "errors" : 0,                         // number of errors in interval
           "httpRequestLatencyMs" : {            // response time of http requests
             "min" : 4,                            // minimum value in interval
             "avg" : 4.5,                          // average value in interval
             "max" : 5,                            // maximum value in interval
             "95th" : 5,                           // 95th percentile value
             "99th" : 5                            // 99th percentile value
           },
           "requestSize" : {                     // http request size in bytes
             "min" : 42,                           // minimum value in interval
             "avg" : 42.5,                         // average value in interval
             "max" : 43                            // maximum value in interval
           },
           "resultSize" : {                      // http result size in bytes
             "min" : 193,                          // minimum value in interval
             "avg" : 206.5,                        // average value in interval
             "max" : 220                           // maximum value in interval
           },
           "rateLimitDelayMs" : 0,               // delay in milliseconds introduced by the rate limiter
           "retry" : {                           // retries
             "delayMs" : 0,                        // delay in milliseconds introduced by retries
             "authCount" : 0,                      // no of auth retries
             "throttleCount" : 0,                  // no of throttle retries
             "count" : 0                           // total number of retries
           }
         }, {
           "name" : "Query",                   // stats for all QUERY type requests
           "httpRequestCount" : 14,
           "errors" : 0,
           "httpRequestLatencyMs" : {
             "min" : 3,
             "avg" : 13.0,
             "max" : 32,
             "95th" : 32,
             "99th" : 32
           },
           "resultSize" : {
             "min" : 146,
             "avg" : 7379.71,
             "max" : 10989
           },
           "requestSize" : {
             "min" : 65,
             "avg" : 709.85,
             "max" : 799
           },
           "rateLimitDelayMs" : 0,
           "retry" : {
             "delayMs" : 0,
             "authCount" : 0,
             "throttleCount" : 0,
             "count" : 0
           }
         }, {
           "name" : "Put",                    // stats for PUT type requests
           "httpRequestCount" : 1002,
           "errors" : 0,
           "httpRequestLatencyMs" : {
             "min" : 1,
             "avg" : 4.41,
             "max" : 80,
             "95th" : 8,
             "99th" : 20
           },
           "requestSize" : {
             "min" : 90,
             "avg" : 90.16,
             "max" : 187
           },
           "resultSize" : {
             "min" : 58,
             "avg" : 58.0,
             "max" : 58
           },
           "rateLimitDelayMs" : 0,
           "retry" : {
             "delayMs" : 0,
             "authCount" : 0,
             "throttleCount" : 0,
             "count" : 0
           }
         }],
         "queries" : [{            // query stats aggregated by query statement
                                     // query statement
           "query" : "SELECT * FROM audienceData ORDER BY cookie_id",
                                     // query plan description
           "plan" : "SFW([6])
              [
                FROM:
                  RECV([3])
                    [
                      DistributionKind : ALL_PARTITIONS,
                      Sort Fields : sort_gen,
                    ] as $from-0
                SELECT:
                 FIELD_STEP([6])
                   [
                    VAR_REF($from-0)([3]),
                    audienceData
                   ]
              ]",
           "doesWrites" : false,
           "httpRequestCount" : 12,  // number of http calls to the server
           "unprepared" : 1,         // number of query requests without prepare
           "simple" : false,         // type of query
           "count" : 20,             // number of handle.query() API calls
           "errors" : 0,             // number of calls trowing exception
           "httpRequestLatencyMs" : {// response time of http requests in milliseconds
             "min" : 8,                // minimum value in interval
             "avg" : 14.58,            // average value in interval
             "max" : 32,               // maximum value in interval
             "95th" : 32,              // 95th percentile value in interval
             "99th" : 32               // 99th percentile value in interval
           },
           "requestSize" : {         // http request size in bytes
             "min" : 65,               // minimum value in interval
             "avg" : 732.5,            // average value in interval
             "max" : 799               // maximum value in interval
           },
           "resultSize" : {          // http result size in bytes
             "min" : 914,              // minimum value in interval
             "avg" : 8585.33,          // average value in interval
             "max" : 10989             // maximum value in interval
           },
           "rateLimitDelayMs" : 0,   // total delay introduced by rate limiter in milliseconds
           "retry" : {               // automatic retries
             "delayMs" : 0,            // delay introduced by retries
             "authCount" : 0,          // count of auth related retries
             "throttleCount" : 0,      // count of throttle related retries
             "count" : 0               // total count of retries
           }
         }]
        }``

   The log entries go to the logger configured in NoSQLHandlerConfig. By
   default, if no logger is configured the statistics entries, if enabled,
   will be logged to file **logs/driver.log** in the local directory.

   Stats collection is not dependent of logging configuration, even if
   logging is disabled, collection of stats will still happen if stats
   profile other than *none* is used. In this case, the stats are available by
   using the stats handler.

   Depending on the type of query, if client processing is required, for
   example in the case of ordered or aggregate queries, indicated by the
   false **simple** field of the **query** entry, the **count** and
   **httpRequestsCount** numbers will differ. **count** represents
   the number of ``handle.query()`` API calls and **httpRequestCount**
   represents the number of internal http requests from server. For these
   type of queries, the driver executes several simpler queries, per
   shard or partition, and than combines the results locally.

    Note: connection statistics are not available for NoSQL Python driver.
    """
    LOG_PREFIX = "Client stats|"

    def __init__(self, config, logger, is_rate_limiting_enabled):
        self._logutils = LogUtils(logger)
        self._is_rate_limiting_enabled = is_rate_limiting_enabled

        self._profile = config.get_stats_profile()
        self._interval = config.get_stats_interval()
        self._pretty_print = config.get_stats_pretty_print()
        self._stats_handler = config.get_stats_handler()

        self._enable_collection = False

        self._stats = None   # type: Stats
        self._id = str(uuid.uuid4())[:8]

        if self._profile is not StatsProfile.NONE:
            self._logutils.set_level(INFO)
            self._logutils.log_info(StatsControl.LOG_PREFIX +
                                    "{\"sdkName\": \"Oracle NoSQL SDK for "
                                    "Python" +
                                    "\", \"sdkVersion\": \"" + __version__ +
                                    "\", \"clientId\": \"" + self._id +
                                    "\", \"profile\": \"" +
                                    str(self._profile.name) +
                                    "\", \"intervalSec\": " +
                                    str(self._interval) +
                                    ", \"prettyPrint\": " +
                                    str(self._pretty_print) +
                                    ", \"rateLimitingEnabled\": " +
                                    str(self._is_rate_limiting_enabled) +
                                    "}")

        self.start()

    def set_stats_handler(self, stats_handler):
        # type: (Callable[Dict]) -> StatsControl
        """
        Registers a user defined stats handler. The handler is called at the end
        of the interval with a structure containing the logged stat values.
        """
        if not isinstance(stats_handler, Callable):
            raise IllegalArgumentException(
                'stats_handler must be of Callable[Dict] type')
        self._stats_handler = stats_handler
        return self

    def get_stats_handler(self):
        # type: (...) -> Callable[Dict]
        """
        Returns the registered handler.
        """
        return self._stats_handler

    def start(self):
        """
        Collection of stats is enabled only between start and stop or from the
        beginning if environment property NOSQL_STATS_PROFILE is not "none".
        """
        if self._profile is StatsProfile.NONE:
            if self._stats is not None:
                self._stats.shutdown()
            self._stats = None
        else:
            if self._stats is None:
                self._stats = Stats(self)
            self._enable_collection = True

    def stop(self):
        """
        Stops collection of stats.
        """
        self._enable_collection = False

    def is_started(self):
        """
        Returns true if collection of stats is enabled, otherwise returns false.
        """
        return self._enable_collection

    def shutdown(self):
        """
        Logs the stats collected and stops the timer.
        """
        if self._stats is not None:
            self._stats.shutdown()

    def get_profile(self):
        # type: () -> StatsProfile
        """
        Returns the stats collection profile. Default stats profile is NONE.
        """
        return self._profile

    def set_profile(self, profile):
        # type: (StatsProfile) -> StatsControl
        """
        Set the stats collection stats_profile. Default stats stats_profile is
        NONE.
        """
        if profile is not None and not isinstance(profile, StatsProfile):
            raise IllegalArgumentException(
                'stats_profile must be a StatsProfile.')
        self._profile = profile
        return self

    def get_interval(self):
        # type: () -> int
        """
        Returns the current collection interval.
        Default interval is 600 seconds, i.e. 10 min.
        """
        return self._interval

    def get_id(self):
        # type: () -> str
        """
        Returns a pseudo unique string to identify the NoSQLHandle object.
        """
        return self._id

    def get_pretty_print(self):
        """
        Returns the current JSON pretty print flag.
        Default is disabled.
        """
        return self._pretty_print

    def set_pretty_print(self, pretty_print):
        # type: (bool) -> StatsControl
        """
        Enable JSON pretty print for easier human reading.
        Default is disabled.
        """
        CheckValue.check_boolean(pretty_print, "pretty_print")
        self._pretty_print = pretty_print
        return self

    def get_logger(self):
        """
        Returns the current logger.
        """
        if self._logutils is not None:
            return self._logutils.get_logger()
        else:
            return None

    def observe(self, request, req_size, res_size, network_latency):
        """
        Internal method only.
        """
        if self._enable_collection and self._stats is not None:
            self._stats.observe(request, False, req_size, res_size,
                                network_latency)

    def observe_error(self, request):
        """
        Internal method only.
        """
        if self._enable_collection and self._stats is not None:
            self._stats.observe_error(request)

    def observe_query(self, query_request):
        """
        Internal method only.
        """
        if self._enable_collection and self._stats is not None:
            self._stats.observe_query(query_request)


class RepeatedTimer(object):
    """
    Class that implements the timer to log stats at certain interval. The first
    event is called after the delay and the rest are called after each interval.
    """

    def __init__(self, delay, interval, function, *args, **kwargs):
        # type: (float, int, Callable, [Any], [[Any]]) -> None
        self._timer = None
        self._delay = delay
        self._interval = interval
        self._function = function
        self._args = args
        self._kwargs = kwargs
        self._firstTime = True
        self._is_running = False
        self.start()

    def _run(self):
        self._is_running = False
        self.start()
        self._function(*self._args, **self._kwargs)

    def start(self):
        if not self._is_running:
            interval = self._interval
            if self._firstTime:
                interval = self._delay
                self._firstTime = False
            self._timer = Timer(interval, self._run)
            self._timer.start()
            self._is_running = True

    def stop(self):
        self._timer.cancel()
        self._is_running = False


class QueryEntryStat:
    """
    Statistics for a certain query.
    """

    def __init__(self, profile, query_request):
        self._plan = None
        self._does_writes = False
        self._count = 0
        self._unprepared = 0
        self._simple = False
        self._req_stats = ReqStats(profile)

    def observe_query(self, query_request):
        # type: (QueryRequest) -> None
        self._count += 1
        if not query_request.is_prepared():
            self._unprepared += 1
        if query_request.is_prepared() and query_request.is_simple_query():
            self._simple = True

    def observe(self, error, retries, retry_delay,
                rate_limit_delay, auth_count, throttle_count,
                req_size, res_size, network_latency):
        # type: (bool, int, int, int, int, int, int, int, int) -> None
        self._req_stats.observe(error, retries, retry_delay, rate_limit_delay,
                                auth_count, throttle_count, req_size, res_size,
                                network_latency)

    def to_json(self, queries, query):
        # type: ([Any], str) -> None
        q = {"query": query, "count": self._count,
             "unprepared": self._unprepared, "simple": self._simple,
             "doesWrites": self._does_writes}
        if self._plan is not None:
            q["plan"] = self._plan

        self._req_stats.to_map_value(q)
        queries.append(q)

    def get_plan(self):
        return self._plan


class ExtraQueryStats:
    """
    Statistics for all queries.
    """

    def __init__(self, profile):
        self._queries = {}  # type: Dict[str, QueryEntryStat]
        self._profile = profile

    def observe_query(self, query_request):
        q_stat = self.get_extra_query_stat_entry(query_request)
        q_stat.observe_query(query_request)

    def observe_q_rec(self, query_request, error, retries,
                      retry_delay, rate_limit_delay, auth_count,
                      throttle_count, req_size, res_size,
                      network_latency):
        """
        type: (borneo.Request, bool, int, int, int, int, int, int, int, int) ->
          None
        """
        q_stat = self.get_extra_query_stat_entry(query_request)
        q_stat.observe(error, retries, retry_delay, rate_limit_delay,
                       auth_count, throttle_count, req_size, res_size,
                       network_latency)

    def get_extra_query_stat_entry(self, query_request):
        # type: (borneo.QueryRequest) -> QueryEntryStat
        sql = query_request.get_statement()
        if sql is None and self._prepared_statement is not None:
            sql = self._prepared_statement.get_sql_text()
        q_stat = self._queries.get(sql)
        if q_stat is None:
            q_stat = QueryEntryStat(self._profile, query_request)
            self._queries[sql] = q_stat
        if q_stat.get_plan() is None:
            if query_request.get_prepared_statement() is not None:
                prep_stmt = query_request.get_prepared_statement()
                q_stat._plan = prep_stmt.print_driver_plan()
                q_stat._does_writes = prep_stmt.does_writes()
        return q_stat

    def to_json(self, root):
        if len(self._queries) > 0:
            queries = []
            root["queries"] = queries
            for k in self._queries:
                self._queries[k].to_json(queries, k)

    def clear(self):
        self._queries = {}  # type: Dict[str, QueryEntryStat]


class Percentile:
    """
    Implements storing and computation of percentiles for a given set of values.
    """

    def __init__(self):
        self._values = []    # type: [int]

    def add_value(self, network_latency):
        # type: (int) -> None
        self._values.append(network_latency)

    def get_percentile(self, percentiles):
        # type: ([float]) -> [int]
        size = len(self._values)
        self._values.sort()
        # if no values available return -1
        # if requested percentile is 0 or less return first value
        # if 1 or more return last value
        # else return corresponding value
        return [-1 if size == 0 else
                self._values[0] if round(v * size) <= 0 else
                self._values[size - 1] if round(v * size) >= size
                else self._values[round(v * size) - 1]
                for v in percentiles]

    def get_95th99th_percentile(self):
        # type: () -> (int, int)
        parr = self.get_percentile([0.95, 0.99])
        return parr[0], parr[1]

    def clear(self):
        self._values = []


class ReqStats:
    """
    Statistics per type of request.
    """

    def __init__(self, profile):
        # type: (StatsProfile) -> None
        self._httpRequestCount = 0
        self._errors = 0
        self._reqSizeMin = sys.maxsize
        self._reqSizeMax = 0
        self._reqSizeSum = 0
        self._resSizeMin = sys.maxsize
        self._resSizeMax = 0
        self._resSizeSum = 0
        self._retryAuthCount = 0
        self._retryThrottleCount = 0
        self._retryCount = 0
        self._retryDelayMs = 0
        self._rateLimitDelayMs = 0
        self._requestLatencyMin = sys.maxsize
        self._requestLatencyMax = 0
        self._requestLatencySum = 0
        if profile.value >= StatsProfile.MORE.value:
            self._requestLatencyPercentile = Percentile()
        else:
            self._requestLatencyPercentile = None

    def observe(self, error, retries, retry_delay,
                rate_limit_delay, auth_count, throttle_count,
                req_size, res_size, network_latency):
        # type: (bool, int, int, int, int, int, int, int, int) -> None
        self._httpRequestCount += 1
        self._retryCount += retries
        self._retryDelayMs += retry_delay
        self._retryAuthCount += auth_count
        self._retryThrottleCount += throttle_count
        self._rateLimitDelayMs += rate_limit_delay

        if error:
            self._errors += 1
        else:
            self._reqSizeMin = min(self._reqSizeMin, req_size)
            self._reqSizeMax = max(self._reqSizeMax, req_size)
            self._reqSizeSum += req_size
            self._resSizeMin = min(self._resSizeMin, res_size)
            self._resSizeMax = max(self._resSizeMax, res_size)
            self._resSizeSum += res_size
            self._requestLatencyMin = \
                min(self._requestLatencyMin, network_latency)
            self._requestLatencyMax = \
                max(self._requestLatencyMax, network_latency)
            self._requestLatencySum += network_latency
            if self._requestLatencyPercentile is not None:
                self._requestLatencyPercentile.add_value(network_latency)

    def to_json(self, request_name, req_array):
        # type: (str, []) -> None
        if self._httpRequestCount > 0:
            req = {"name": request_name}
            self.to_map_value(req)
            req_array.append(req)

    def to_map_value(self, map_value):
        map_value["httpRequestCount"] = self._httpRequestCount
        map_value["errors"] = self._errors

        retry = {
            "count": self._retryCount,
            "delayMs": self._retryDelayMs,
            "authCount": self._retryAuthCount,
            "throttleCount": self._retryThrottleCount
        }
        map_value["retry"] = retry
        map_value["rateLimitDelayMs"] = self._rateLimitDelayMs

        if self._requestLatencyMax > 0:
            latency = {
                "min": self._requestLatencyMin,
                "max": self._requestLatencyMax,
                "avg": self._requestLatencySum / (self._httpRequestCount - self._errors)
            }
            if self._requestLatencyPercentile is not None:
                (p95th, p99th) = \
                    self._requestLatencyPercentile.get_95th99th_percentile()
                latency["95th"] = p95th
                latency["99th"] = p99th
            map_value["httpRequestLatencyMs"] = latency

        if self._reqSizeMax > 0:
            reqSize = {
                "min": self._reqSizeMin,
                "max": self._reqSizeMax,
                "avg": self._reqSizeSum / (self._httpRequestCount - self._errors)
            }
            map_value["requestSize"] = reqSize

        if self._resSizeMax > 0:
            resSize = {
                "min": self._resSizeMin,
                "max": self._resSizeMax,
                "avg": self._resSizeSum / (self._httpRequestCount - self._errors)
            }
            map_value["resultSize"] = resSize

    def clear(self):
        self._httpRequestCount = 0
        self._errors = 0
        self._reqSizeMin = sys.maxsize
        self._reqSizeMax = 0
        self._reqSizeSum = 0
        self._resSizeMin = sys.maxsize
        self._resSizeMax = 0
        self._resSizeSum = 0
        self._retryAuthCount = 0
        self._retryThrottleCount = 0
        self._retryCount = 0
        self._retryDelayMs = 0
        self._rateLimitDelayMs = 0
        self._requestLatencyMin = sys.maxsize
        self._requestLatencyMax = 0
        self._requestLatencySum = 0
        if self._requestLatencyPercentile is not None:
            self._requestLatencyPercentile.clear()


class Stats:
    """
    Implements all the statistics.
    """
    _stats_control = None  # type: StatsControl
    _timer = None         # type: RepeatedTimer
    _requests = None      # type: Dict[str, ReqStats]

    _request_names = ["Delete", "Get", "GetIndexes", "GetTable",
                      "ListTables", "MultiDelete", "Prepare", "Put", "Query",
                      "System", "SystemStatus", "Table", "TableUsage",
                      "WriteMultiple", "Write"]

    def __init__(self, stats_control):
        # type: (StatsControl) -> None
        self._stats_control = stats_control

        interval = 10

        local_time = localtime()
        delay = (1000 * interval -
                 ((1000 * 60 * local_time.tm_min +
                   1000 * local_time.tm_sec) % (1000 * interval))) / 1000

        self._timer = RepeatedTimer(delay, interval, self.log_client_stats)

        self._start_time = datetime.utcnow()
        self._end_time = self._start_time
        self._extra_query_stats = None
        profile = self._stats_control.get_profile()
        if profile is not None and profile.value >= StatsProfile.ALL.value:
            self._extra_query_stats = ExtraQueryStats(profile)

        self._requests = {}
        for i in self._request_names:
            self._requests[i] = ReqStats(stats_control.get_profile())
        self.lock = Lock()

    @synchronized
    def log_client_stats(self):
        self.__log_client_stats()

    def __log_client_stats(self):
        handler = self._stats_control.get_stats_handler()
        log = self._stats_control.get_logger() is not None and \
                 self._stats_control.get_logger().isEnabledFor(INFO)

        if handler is not None or log:
            stats = self.__generate_stats()
            self.clear()

            if handler is not None:
                handler(stats)

            if log:
                if self._stats_control.get_pretty_print():
                    stats_str = pprint.pformat(stats)
                else:
                    stats_str = str(stats)
                self._stats_control.get_logger().info(StatsControl.LOG_PREFIX +
                                                      stats_str)

    def __generate_stats(self):
        self._end_time = datetime.utcnow()
        root = {
            "startTime": self._start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "endTime": self._end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "clientId": self._stats_control.get_id()
        }

        if self._extra_query_stats is not None:
            self._extra_query_stats.to_json(root)

        req_array = []
        for k in self._requests:
            self._requests[k].to_json(k, req_array)

        root["requests"] = req_array
        return root

    def clear(self):
        self._start_time = datetime.utcnow()
        self._end_time = self._start_time
        if self._extra_query_stats is not None:
            self._extra_query_stats.clear()

        for k in self._requests:
            self._requests[k].clear()

    @synchronized
    def shutdown(self):
        self.__log_client_stats()
        self._timer.stop()

    @synchronized
    def observe_error(self, request):
        self.__observe(request, True, -1, -1, -1)

    @synchronized
    def observe(self, request, error, req_size, res_size, network_latency):
        self.__observe(request, error, req_size, res_size,
                       network_latency)

    def __observe(self, request, error, req_size, res_size,
                  network_latency):
        reqStr = request.get_request_name()
        req_stat = self._requests.get(reqStr)
        if req_stat is None:
            req_stat = ReqStats(self._stats_control.get_profile())
            self._requests[reqStr] = req_stat

        auth_count = 0
        throttle_count = 0
        if request.get_retry_stats() is not None:
            auth_count = request.get_retry_stats().get_num_exceptions(
                AuthenticationException)
            auth_count += request.get_retry_stats().get_num_exceptions(
                SecurityInfoNotReadyException)
            throttle_count = request.get_retry_stats().get_num_exceptions(
                ThrottlingException)

        req_stat.observe(error, request.get_num_retries(),
                         request.get_retry_delay_ms(),
                         request.get_rate_limit_delayed_ms(), auth_count,
                         throttle_count, req_size, res_size, network_latency)

        from . import QueryRequest

        if self._extra_query_stats is not None and \
                isinstance(request, QueryRequest) and \
                self._stats_control.get_profile().value >= \
                StatsProfile.ALL.value:
            self._extra_query_stats.observe_q_rec(request, error,
                                                  request.get_num_retries(),
                                                  request.get_retry_delay_ms(),
                                                  request.
                                                  get_rate_limit_delayed_ms(),
                                                  auth_count, throttle_count,
                                                  req_size, res_size,
                                                  network_latency)

    @synchronized
    def observe_query(self, query_request):
        if self._extra_query_stats is None and \
                self._stats_control.get_profile().value >= \
                StatsProfile.ALL.value:
            self._extra_query_stats = \
                ExtraQueryStats(self._stats_control.get_profile())
        if self._extra_query_stats is not None and \
                self._stats_control.get_profile().value >= \
                StatsProfile.ALL.value:
            self._extra_query_stats.observe_query(query_request)
