.. _stats:

How to find client statistics
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

StatsControl allows user to control the collection of driver statistics at
runtime.

The statistics data is collected for an interval of time. At the end of the
interval, the stats data is logged in a specified JSON format that can be
filtered and parsed. After the logging, the counters are cleared and
collection of data resumes.

Collection intervals are aligned to the top of the hour. This means first
interval logs may contain stats for a shorter interval.

How to enable and configure from command line
---------------------------------------------

Collection of stats are controlled by the following environment variables:

- ``NOSQL_STATS_PROFILE=[none|regular|more|all]``
   Specifies the stats profile:

   - ``none`` - disabled.

   - ``regular`` - per request: counters, errors, latencies, delays, retries.
        This incurs minimum overhead.
   - ``more`` - stats above with 95th and 99th percentile latencies.
        This may add 0.5% overhead compared to none stats profile.
   - ``all`` - stats above with per query information.
        This may add 1% overhead compared to none stats profile.

- ``NOSQL_STATS_INTERVAL=600``
    Interval in seconds to log the stats, by default is 10 minutes.

- ``NOSQL_STATS_PRETTY_PRINT=true``
    Option to enable pretty printing of the JSON data, default value is false.

How to enable and configure using the API
-----------------------------------------

Collection of stats can also be used by using the API:
``NoSQLHandleConfig.set_stats_profile()`` or
``StatsControl.set_profile()``. At runtime stats collection can be
enabled selectively by using ``StatsControl.start()`` ond
``StatsControl.stop()``. The following example shows how to use a stats
handler and how to control the stats at runtime:

.. code-block:: pycon

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


Example log entry
-----------------

The following is an example of stats log entry using the ALL
profile:

- A one time entry containing stats id and options:

.. code-block:: pycon

  INFO: Client stats|{    // INFO log entry
  "sdkName" : "Oracle NoSQL SDK for Python",  // SDK name
  "sdkVersion" : "5.2.4",                 // SDK version
  "clientId" : "f595b333",                  // NoSQLHandle id
  "profile" : "ALL",                        // stats profile
  "intervalSec" : 600,                      // interval length in seconds
  "prettyPrint" : true,                     // JSON pretty print
  "rateLimitingEnabled" : false}            // if rate limiting is enabled

- An entry at the end of each interval containing the stats values:

.. code-block:: pycon

 INFO: Client stats|{
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
