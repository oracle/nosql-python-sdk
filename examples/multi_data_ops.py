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
#  the comments in config_cloud.py.
#
# To run:
#  1. set PYTHONPATH to include the parent directory of ../src/borneo
#  2. modify variables in config*.py for the runtime environment after reading
#  instructions in the comments.
#  3. run
#    $ python multi_data.py
#

import traceback

from borneo import (
    MultiDeleteRequest, PrepareRequest, PutRequest, QueryRequest, TableLimits,
    TableRequest, WriteMultipleRequest)

from parameters import drop_table, table_name, tenant_id
from utils import get_handle


def main():

    handle = None
    try:
        #
        # Create a handle
        #
        handle = get_handle(tenant_id)

        #
        # Create a table
        #
        statement = 'Create table if not exists ' + table_name + '(id integer, \
sid integer, name string, primary key(shard(sid), id))'
        print('Creating table: ' + statement)
        request = TableRequest().set_statement(statement).set_table_limits(
            TableLimits(30, 10, 1))
        handle.do_table_request(request, 50000, 3000)
        print('After create table')

        #
        # Put a few rows
        #
        request = PutRequest().set_table_name(table_name)
        for i in range(10):
            value = {'id': i, 'sid': 0, 'name': 'myname' + str(i)}
            request.set_value(value)
            handle.put(request)
        print('After put of 10 rows')

        #
        # Multiple write a few rows
        #
        request = WriteMultipleRequest()
        for i in range(10):
            value = {'id': i, 'sid': 0, 'name': 'newname' + str(i)}
            request.add(PutRequest().set_value(value).set_table_name(
                table_name), True)
        result = handle.write_multiple(request)
        print('After multiple write: ' + str(result))

        #
        # Prepare a statement
        #
        statement = 'select * from ' + table_name + ' where id > 2 and id < 8'
        request = PrepareRequest().set_statement(statement)
        prepared_result = handle.prepare(request)
        print('After prepare the statement: ' + statement)

        #
        # Query, using the prepared statement
        #
        request = QueryRequest().set_prepared_statement(prepared_result)
        print('Query results for the prepared statement: ')
        while True:
            result = handle.query(request)
            for r in result.get_results():
                print('\t' + str(r))
            if request.is_done():
                break

        #
        # Multiple delete the rows
        #
        request = MultiDeleteRequest().set_table_name(table_name).set_key(
            {'sid': 0})
        result = handle.multi_delete(request)
        print('After multiple delete: ' + str(result))

        #
        # Query again to show deletions, using the prepared statement
        #
        request = QueryRequest().set_prepared_statement(
            prepared_result)
        print('Query results for the prepared statement (should be no rows): ')
        while True:
            result = handle.query(request)
            for r in result.get_results():
                print('\t' + str(r))
            if request.is_done():
                break

        #
        # Drop the table
        #
        if drop_table:
            request = TableRequest().set_statement(
                'drop table if exists ' + table_name)
            handle.do_table_request(request, 40000, 2000)
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
