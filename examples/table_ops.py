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
#    $ python table_ops.py
#

import traceback

from borneo import (
    GetIndexesRequest, GetTableRequest, ListTablesRequest, TableLimits,
    TableRequest, TableUsageRequest)

from parameters import (
    drop_table, index_name, table_name, tenant_id, using_on_prem)
from utils import get_handle


def main():

    handle = None
    try:
        #
        # Create a handle
        #
        handle = get_handle(tenant_id)

        #
        # List any existing tables for this tenant
        #
        print('Listing tables')
        ltr = ListTablesRequest()
        lr_result = handle.list_tables(ltr)
        print('Existing tables: ' + str(lr_result))

        #
        # Create a table
        #
        statement = 'Create table if not exists ' + table_name + '(id integer, \
sid integer, name string, primary key(shard(sid), id))'
        print('Creating table: ' + statement)
        request = TableRequest().set_statement(statement).set_table_limits(
            TableLimits(30, 10, 1))
        handle.do_table_request(request, 40000, 3000)
        print('After create table')

        #
        # Create an index
        #
        statement = ('Create index if not exists ' + index_name + ' on ' +
                     table_name + '(name)')
        print('Creating index: ' + statement)
        request = TableRequest().set_statement(statement)
        handle.do_table_request(request, 40000, 3000)
        print('After create index')

        #
        # Get the table
        #
        request = GetTableRequest().set_table_name(table_name)
        result = handle.get_table(request)
        print('After get table: ' + str(result))

        #
        # Get the indexes
        #
        request = GetIndexesRequest().set_table_name(table_name)
        result = handle.get_indexes(request)
        print('The indexes for: ' + table_name)
        for idx in result.get_indexes():
            print('\t' + str(idx))

        #
        # Get the table usage information, on-prem mode not supported
        #
        if not using_on_prem:
            request = TableUsageRequest().set_table_name(table_name)
            result = handle.get_table_usage(request)
            print('The table usage information for: ' + table_name)
            for record in result.get_usage_records():
                print('\t' + str(record))

        #
        # Drop the index
        #
        request = TableRequest().set_statement(
            'drop index ' + index_name + ' on ' + table_name)
        handle.do_table_request(request, 30000, 2000)
        print('After drop index')

        #
        # Drop the table
        #
        if drop_table:
            request = TableRequest().set_statement(
                'drop table if exists ' + table_name)
            handle.do_table_request(request, 30000, 2000)
            print('After drop table')
        else:
            print('Not dropping table')

        print('Example is complete')
    except Exception as e:
        print(e)
        traceback.print_exc()
    finally:
        # If the handle isn't closed Python will not exit properly
        if handle is not None:
            handle.close()


if __name__ == '__main__':
    main()
