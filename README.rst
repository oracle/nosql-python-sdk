Oracle NoSQL Database Cloud Service Python SDK
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

=====
About
=====

This is the Python SDK for Oracle NoSQL Database Cloud Service. Python 2.7+ and 3.5+ are supported.

========
Examples
========

Some simple, standalone examples, example*.py, are in the examples directory.
They have comments about how to run. It is a good place to start.

A brief overview of the programming model and classes are shown below.

The driver comprises a Python interface that operates on the protocol exposed by
the service. The protocol itself is binary at this time and is not subject to
change in the short term. The protocol is stateless.

A program will have the following general structure:
 1. create a handle to the system (requires authentication information)
 2. use the handle to perform requests
 3. close the handle

The handle must be closed for the Python program to exit properly as it releases
resources related to the requests package.

The usage model for a request is
 1. create the request and configure it as desired
 2. call the handle to execute the request

Requests all return Result objects which are used to process results.
Result objects are specific to the request although they have common
state in terms of returning read and write units consumed by the operation, for example:

.. code-block::

  request = GetRequest().set_key({'id':1}).set_table_name('mytable')
  result = handle.get(request)
  print result

The example*.py examples have additional request examples.

All Request and Result objects are in the operations.py module. Requests include
Data requests (these operate on table data):

 * DeleteRequest
 * GetRequest
 * MultiDeleteRequest
 * PrepareRequest
 * PutRequest
 * QueryRequest
 * WriteMultipleRequest

Metadata requests (these get and/or affect system metadata):

 * TableRequest
 * GetTableRequest
 * GetIndexesRequest
 * ListTablesRequest
 * TableUsageRequest

Successful requests return a corresponding Result object. Unsuccessful requests
raise exceptions, which are found in the exceptions.py module. See __init__.py
for the objects that may need to be imported. Result objects are not in that
list as they are never directly created by applications. Exception objects are
not created either and they may be removed. Documentation will make this much
more clear when available.

A note on TableRequest. This request encompasses operations to create, modify,
and drop tables. It is implicitly asynchronous in that the initial return
indicates that the operation has begun. In order to know when it's completed the
GetTableRequest must be used to determine state. There is a convenience method,
TableResult.wait_for_state, that should be used. It is used in the examples,
example*.py.
