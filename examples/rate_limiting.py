#
# Copyright (c) 2018, 2022 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#

#
# This is a simple example to demonstrate use of the Python driver for the
# Oracle NoSQL Database. It can be used to run against the Oracle NoSQL Database
# cloud service, against the Cloud Simulator, or against an on-premise Oracle
# NoSQL database.
#
# See the comments in config*.py about running in different environments. By
# default the example is ready to run against the Cloud Simulator.
#
# The example demonstrates:
# o configuring and creating a handle to access the service
# o create a table
# o put, multiple write and multiple delete of simple data
# o prepare statement and query data
# o drop the table
#
# This example is not intended to be an exhaustive overview of the API, which
# has a number of additional operations.
#
# Requirements:
#  1. Python 2.7 or 3.5+
#  2. Python dependencies (install using pip or other mechanism):
#   o requests
#  3. If running against the Cloud Simulator, it can be downloaded from here:
#   http://www.oracle.com/technetwork/topics/cloud/downloads/index.html#nosqlsdk
#  It requires Java
#  4. If running against the Oracle NoSQL Database Cloud Service an account must
#  be used along with additional authentication information. See instructions in
#  the comments in config_cloud.py
#
# To run:
#  1. set PYTHONPATH to include the parent directory of ../src/borneo
#  2. modify variables in config*.py for the runtime environment after reading
#  instructions in the comments.
#  3. run
#    $ python rate_limiting.py

import traceback

from random import random
from time import time

from borneo import (
    GetRequest, NoSQLHandle, PutRequest, ReadThrottlingException, TableLimits,
    TableRequest, WriteThrottlingException)

from parameters import table_name, tenant_id
from utils import get_handle_config


def main():
    """
    A simple program to demonstrate how to enable and use rate limiting. This
    example should only be run against CloudSim, as the on-premise Oracle NoSQL
    database currently does not report read/write throughput used by rate
    limiting logic.

    This example could be used with the cloud service, but it generates a
    significant amount of data, which may use up your resources.
    """

    handle = None
    try:
        #
        # Create a handle config
        #
        config = get_handle_config(tenant_id)
        #
        # Enable rate limiting
        #
        config.set_rate_limiting_enabled(True)
        #
        # Note: the amount of table limits used by this client can be configured
        # using Config.set_default_rate_limiting_percentage().
        #

        #
        # Create a handle
        #
        handle = NoSQLHandle(config)
        #
        # Create a table, set the table limits to 50 RUs/WUs per second
        #
        statement = 'Create table if not exists ' + table_name + '(id integer, \
sid integer, name string, primary key(shard(sid), id))'
        print('Creating table: ' + statement)
        request = TableRequest().set_statement(statement).set_table_limits(
            TableLimits(30, 10, 1))
        handle.do_table_request(request, 40000, 3000)
        print('After create table')
        #
        # Create records of random sizes
        #
        min_size = 100
        max_size = 10000
        #
        # Do a bunch of write ops, verify our usage matches limits
        #
        do_rate_limited_ops(
            handle, 10, True, 10, 2000, min_size, max_size)
        #
        # Do a bunch of read ops, verify our usage matches limits
        #
        do_rate_limited_ops(
            handle, 10, False, 30, 2000, min_size, max_size)
        #
        # DROP the table
        #
        request = TableRequest().set_statement(
            'drop table if exists ' + table_name)
        handle.do_table_request(request, 30000, 2000)
        print('After drop table')
        print('Example is complete')
    except Exception as e:
        print(e)
        traceback.print_exc()
    finally:
        # If the handle isn't closed Python will not exit properly
        if handle is not None:
            handle.close()


def do_rate_limited_ops(
        handle, num_seconds, do_writes, limit, max_rows, min_size, max_size):
    """
    Runs puts and gets continuously for N seconds.

    Verify that the resultant RUs/WUs used match the given rate limits.
    """
    put_request = PutRequest().set_table_name(table_name)
    get_request = GetRequest().set_table_name(table_name)
    #
    # Generate a string of max_size with all "x"s in it
    #
    user_data = ''
    if do_writes:
        for x in range(max_size):
            user_data += 'x'

    start_time = int(round(time() * 1000))
    end_time = start_time + num_seconds * 1000

    print('Running continuous ' + ('writes' if do_writes else 'reads') +
          ' for ' + str(num_seconds) + ' seconds.')
    #
    # Keep track of how many units we used
    #
    units_used = 0
    #
    # With rate limiting enabled, we can find the amount of time our operation
    # was delayed due to rate limiting by getting the value from the result
    # using Result.get_rate_limit_delayed_ms().
    #
    delay_ms = 0

    key = dict()
    value = dict()
    while True:
        fld_id = int(random() * max_rows)
        try:
            if do_writes:
                value['id'] = fld_id
                value['sid'] = fld_id
                rec_size = int(random() * (max_size - min_size))
                rec_size += min_size
                value['name'] = user_data[:rec_size]
                put_request.set_value(value)
                put_result = handle.put(put_request)
                units_used += put_result.get_write_units()
                delay_ms += put_result.get_rate_limit_delayed_ms()
            else:
                key['id'] = fld_id
                key['sid'] = fld_id
                get_request.set_key(key)
                get_result = handle.get(get_request)
                units_used += get_result.get_read_units()
                delay_ms += get_result.get_rate_limit_delayed_ms()
        except WriteThrottlingException as wte:
            # We should not get WriteThrottlingException exception
            print('Got unexpected write throttling exception')
            raise wte
        except ReadThrottlingException as rte:
            # We should not get ReadThrottlingException exception
            print('Got unexpected read throttling exception')
            raise rte
        if int(round(time() * 1000)) >= end_time:
            break
    num_seconds = (int(round(time() * 1000)) - start_time) // 1000
    units_used /= num_seconds

    if units_used < int(limit * 0.8) or units_used > int(limit * 1.2):
        if do_writes:
            msg = ('Writes: expected around ' + str(limit) + ' WUs, got ' +
                   str(units_used))
        else:
            msg = ('Reads: expected around ' + str(limit) + ' RUs, got ' +
                   str(units_used))
        raise RuntimeError(msg)

    print(('Writes' if do_writes else 'Reads') + ': average usage = ' +
          str(units_used) + ('WUs' if do_writes else 'RUs') +
          ' (expected around ' + str(limit))

    print('Total rate limiter delay time = ' + str(delay_ms) + 'ms')


if __name__ == '__main__':
    main()
