.. _tables:

Working With Tables
~~~~~~~~~~~~~~~~~~~

Applications using the Oracle NoSQL Database Cloud Service work with tables.
Tables are created and data is added, modified and removed. Indexes can be added
on tables. These topics are covered. Not all options and functions are described
here. Detailed descriptions of interfaces can be found in :ref:`api`.

---------------------
Obtain a NoSQL Handle
---------------------

:class:`borneo.NoSQLHandle` represents a connection to the service. Once created
it must be closed using the method :func:`borneo.NoSQLHandle.close` in order to
clean up resources. Handles are thread-safe and intended to be shared. A handle
is created by first creating a :class:`borneo.NoSQLHandleConfig` instance to
configure the communication endpoint, authorization information, as well as
default values for handle configuration.

Configuration requires an :class:`borneo.AuthorizationProvider` to provide
identity and authorization information to the handle.

See the section *Supplying Credentials to an Application* in :ref:`install` for
options related to using your own :class:`CredentialsProvider` class for better
credential security.

.. code-block:: pycon

    from borneo import (AuthorizationProvider, NoSQLHandleConfig, NoSQLHandle)
    from borneo.idcs import (DefaultAccessTokenProvider,
        PropertiesCredentialsProvider)

    # create AuthorizationProvider
    provider = DefaultAccessTokenProvider(<your idcs_url>, <your entitlement_id>)
    provider.set_credentials_provider( PropertiesCredentialsProvider()
        .set_properties_file(<path_to_your_credentials_file)

    # create handle config using the correct endpoint for the desired region
    config = NoSQLHandleConfig('ndcs.uscom-east-1.oracle.cloud.com')
        .set_authorization_provider(provider)

    # create the handle
    handle = NoSQLHandle(config)

To reduce resource usage and overhead of handle creation it is best to avoid
excessive creation and closing of :class:`borneo.NoSQLHandle` instances.

-------------------------
Create Tables and Indexes
-------------------------
Learn how to create tables and indexes in Oracle NoSQL Database Cloud.

Creating a table is the first step of developing your application. You use
the :class:`borneo.TableRequest` class and its methods to execute Data Definition
Language (DDL) statements, such as, creating, modifying, and dropping tables.
You also set table limits using :func:`borneo.TableRequest.set_table_limits` method.

Before creating a table, learn about:

The supported data types for Oracle NoSQL Database Cloud. See `Supported Data Types <https://docs.oracle.com/pls/topic/lookup?ctx=en/cloud/paas/nosql-cloud&id=CSNSD-GUID-833B2B2A-1A32-48AB-A19E-413EAFB964B8>`_. Also see :ref:`datatypes` for a description of how database types map to
Python.

Cloud limits. See `Oracle NoSQL Database Cloud Limits <https://docs.oracle.com/pls/topic/lookup?ctx=en/cloud/paas/nosql-cloud&id=CSNSD-GUID-30129AB3-906B-4E71-8EFB-8E0BBCD67144>`_.

Examples of DDL statements are:
::

   /* Create a new table called users */
   CREATE IF NOT EXISTS users (id INTEGER, name STRING, PRIMARY KEY (id));

   /* Create a new table called users and set the TTl value to 4 days */
   CREATE IF NOT EXISTS users (id INTEGER, name STRING, PRIMARY KEY (id)) USING TTL 4 days;

   /* Create a new index called nameIdx on the name field in the users table */
   CREATE INDEX IF NOT EXISTS nameIdx ON users(name);

DDL statements are executing using the :class:`borneo.TableRequest` class. All
calls to :func:`borneo.NoSQLHandle.table_request` are asynchronous so it is
necessary to check the result and call :func:`borneo.TableResult.wait_for_state`
to wait for the expected state.

.. code-block:: pycon

    from borneo import (TableLimits, TableRequest, State)

    statement = 'create table if not exists users(id integer, name string, primary key(id)'

    # TableLimits is a required object for table creation. It specifies the
    # throughput and capacity for the table in ReadUnits, WriteUnits, GB
    request = TableRequest().set_statement(statement).set_tableLimits(
        TableLimits(20, 10, 5))

    # assume that a handle has been created, as handle, make the request
    result = handle.table_request(request)

    # table_request is asynchronous, so wait for the ACTIVE state
    # wait for 40 seconds, polling every 3 seconds
    result.wait_for_state(handle, 'users', State.ACTIVE, 40000, 3000)

--------
Add Data
--------
Add rows to your table.

When you store data in table rows, your application can easily retrieve, add to,
or delete information from the table.

The :class:`borneo.PutRequest` class represents input to the
:func:`borneo.NoSQLHandle.put` method used to insert single rows. This method
can be used for unconditional and conditional puts to:

 * Overwrite any existing row. This is the default.
 * Succeed only if the row does not exist. Use
   :class:`borneo.PutOption.IF_ABSENT` for this case.
 * Succeed only if the row exists. Use :class:`borneo.PutOption.IF_PRESENT`
    for this case.
 * Succeed only if the row exists and its :class:`borneo.Version` matches a
   specific :class:`borneo.Version`. Use :class:`borneo.PutOption.IF_VERSION`
   for this case and :func:`borneo.PutRequest.set_match_version` to specify
   the version to match.

Options can be set using :func:`borneo.PutRequest.set_option`.

To add rows to your table:

.. code-block:: pycon

    from borneo import PutRequest

    # PutRequest requires a table name
     request = PutRequest().set_table_name('users')

    # set the value
    request.set_value( {'id': i, 'name': 'myname'})
    result = handle.put(request);

    # a successful put returns a non-empty version
    if result.get_version() is not NONE:
       # success

When adding data the values supplied must accurately correspond to  the schema
for the table. If they do not, IllegalArgumentException is raised. Columns with
default or nullable values can be left out without error, but it is recommended
that values be provided for all columns to avoid unexpected defaults. By
default, unexpected columns are ignored silently, and the value is put using the
expected columns.

If you have multiple rows that share the same shard key they can be put in a
single request using :class:`borneo.WriteMultipleRequest` which can be created
using a number of PutRequest or DeleteRequest objects.

You can also add JSON data to your table. In the case of a fixed-schema table
the JSON is converted to the target schema. JSON data can be directly inserted
into a column of type *JSON*. The use of the JSON data type allows you to
create table data without a fixed schema, allowing more flexible use of the
data.

=============
Add JSON Data
=============

The data value provided for a row or key is a Python *dict*. It can be supplied
to the relevant requests (GetRequest, PutRequest, DeleteRequest) in multiple
ways:

 * as a Python dict directly
   ::

      request.set_value({'id':1})
      request.set_key({'id':1})
 * as a JSON string
   ::

      request.set_value_from_json("""{"id":1, "name":"myname"}""")
      request.set_key_from_json("""{"id":1}""")

In both cases the keys and values provided must accurately correspond to the
schema of the table. If not an :class:`borneo.IllegalArgumentException`
exception is raised. If the data is provided as JSON and the JSON cannot be
parsed a :class:`ValueError` is raised.

---------
Read Data
---------
Learn how to read data from your table.

You can read single rows using the :func:`borneo.NoSQLHandle.get` method.
This method allows you to retrieve a record based on its primary key value. In
order to read multiple rows in a single request see *Use Queries*, below.

The :class:`borneo.GetRequest` class is used for simple get operations. It
contains the primary key value for the target row and returns an instance of
:class:`borneo.GetResult`.

.. code-block:: pycon

    from borneo import GetRequest

    # GetRequest requires a table name
    request = GetRequest().set_table_name('users')

    # set the primary key to use
    request.set_key({'id': 1})
    result = handle.get(request)

    # on success the value is not empty
    if result.get_value() is not None:
       # success

By default all read operations are eventually consistent, using
:class:`borneo.Consistency.EVENTUAL`. This type of read is
less costly than those using absolute consistency,
:class:`borneo.Consistency.ABSOLUTE`. This default can be
changed in :class:`borneo.NoSQLHandle` using
:func:`borneo.NoSQLHandleConfig.set_consistency` before creating the handle.
It can be changed for a single request using
:func:`borneo.GetRequest.set_consistency`.

-----------
Use Queries
-----------
Learn about  using queries in your application.

Oracle NoSQL Database Cloud Service provides a rich query language to read and
update data. See the `SQL For NoSQL Specification <http://www.oracle.com/pls/topic/lookup?ctx=en/cloud/paas/nosql-cloud&id=sql_nosql>`_
for a full description of the query language.

To execute a query use the :func:`borneo.NoSQLHandle.query` method. For
example, to execute a *SELECT* query to read data from your table:

.. code-block:: pycon

    from borneo import QueryRequest

    # Query at table named 'users" using the field 'name' where name may
    # match 0 or more rows in the table. The table name is inferred from the
    # query statement
    statement = 'select * from users where name = "Taylor"'
    request = QueryRequest().set_statement(statement)
    result = handle.query(request)

    # look at results for this single request
    for res in result.get_results():
       print(str(res))

A :class:`borneo.QueryResult` contains a list of results as well as an optional
*continuation key*. If the continuation key is not empty there may be
more results, so queries should generally be run in a loop. It is possible for
single request to return no results but still have a continuation key,
indicating that the query loop should continue. For example:

.. code-block:: pycon

    from borneo import QueryRequest
    statement = 'select * from users where name = "Taylor"'
    request = QueryRequest().set_statement(statement)
    result = handle.query(request)

    # handle results so far
    handle_results(result) # do something with results

    # loop until continuation key is None, handling results as they arrive
    while result.get_continuation_key() is not None:
        request.set_continuation_key(result.get_continuation_key())
        result = handle.query(request)
        handle_results(result) # do something with results

When using queries it is important to be aware of the following considerations:

 * Oracle NoSQL Database Cloud Service provides the ability to prepare queries
    for execution and reuse. It is recommended that you use prepared queries
    when you run the same query for multiple times. When you use prepared
    queries, the execution is much more efficient than starting with a query
    string every time. The query language and API support query variables to
    assist with query reuse. See :func:`borneo.NoSQLHandle.prepare` and
    :class:`borneo.PrepareRequest` for more information.
 * The :class:`borneo.QueryRequest` allows you to set the read consistency for
    a query as well as modifying the maximum amount of resource (read and write)
    to be used by a single request. This can be important to prevent a query from
    getting throttled because it uses too much resource too quickly.

Here is an example of using a prepared query with a single variable:

.. code-block:: pycon

    from borneo import(PrepareRequest, QueryRequest)

    # Use a similar query to above but make the name a variable
    statement = 'declare $name string; select * from users where name = $name'
    prequest = PrepareRequest().set_statement(statement)
    presult = handle.prepare(prequest)

    # use the prepared statement, set the variable
    pstatement = presult.get_prepared_statement()
    pstatement.set_variable('$name', 'Taylor')
    qrequest = QueryRequest().set_prepared_statement(pstatement)

    # use the prepared query in the query request
    qresult = handle.query(qrequest)

    # use a different variable value with the same prepared query
    pstatement.set_variable('$name', 'another_name')
    qresult = handle.query(qrequest)

-----------
Delete Data
-----------

Learn how to delete rows from your table.

Single rows are deleted using :class:`borneo.DeleteRequest` using a primary
key value:

.. code-block:: pycon

    from borneo import DeleteRequest

    # DeleteRequest requires table name and primary key
    request = DeleteRequest().set_table_name('users')
    request.set_key({'id':1})

    # perform the operation
    result = handle.delete(request)
    if result.get_success():
       # success -- the row was deleted

    # if the row didn't exist or was not deleted for any other reason,
    # False is returned

Delete operations can be conditional based on a :class:`borneo.Version`
returned from a get operation.  See :class:`borneo.DeleteRequest`.

You can perform multiple deletes in a single operation using a value range
using :class:`borneo.MultiDeleteRequest` and
:func:`borneo.NoSQLHandle.multi_delete`.

-------------
Modify Tables
-------------

Learn how to modify tables. You modify a table to:

 * Add or remove fields to an existing table
 * Change the default TimeToLive (TTL) value for the table
 * Modify table limits

Examples of DDL statements to modify a table are:
::

   /* Add a new field to the table */
   ALTER TABLE users (ADD age INTEGER);

   /* Drop an existing field from the table */
   ALTER TABLE users (DROP age);

   /* Modify the default TTl value*/
   ALTER TABLE users USING TTL 4 days;

Table limits can be modified using :func:`borneo.TableRequest.set_table_limits`,
for example:

.. code-block:: pycon

    from borneo import (TableLimits, TableRequest)

    # in this path the table name is required, as there is no DDL statement
    request = TableRequest().set_table_name('users')
    request.set_tableLimits( TableLimits(40, 10, 5))
    result = handle.table_request(request)

    # table_request is asynchronous, so wait for the ACTIVE state
    # wait for 40 seconds, polling every 3 seconds
    result.wait_for_state(handle, 'users', State.ACTIVE, 40000, 3000)


-------------------------
Delete Tables and Indexes
-------------------------

Learn how to delete a table or index.

To drop a table or index, use the *drop table* or *drop index* DDL statement,
for example:
::

   /* drop the table named users (implicitly drops any indexes on that table) */
   DROP TABLE users;

   /*
     * drop the index called nameIndex on the table users. Don't fail if the index
     * doesn't exist
     */
   DROP INDEX IF EXISTS nameIndex ON users;

.. code-block:: pycon

    from borneo import TableRequest

    # the drop statement
    statement = 'drop table users'
    request = TableRequest().set_statement(statement)

    # perform the operation
    result = handle.table_request(request);

    # table_request is asynchronous, so wait for the ACTIVE state
    # wait for 40 seconds, polling every 3 seconds
    result.wait_for_state(handle, 'users', State.ACTIVE, 40000, 3000)

-------------
Handle Errors
-------------

Python errors are raised as exceptions defined as part of the API. They are
all instances of Python's :class:`RuntimeError`. Most exceptions are instances of
:class:`borneo.NoSQLException` which is a base class for exceptions raised by
the Python driver.

Exceptions are split into 2 broad categories:
 * Exceptions that may be retried with the expectation that they may succeed
   on retry. These are all instances of :class:`borneo.RetryableException`.
   Examples of these are the instances of :class:`borneo.ThrottlingException`
   which is raised when resource consumption limits are exceeded.

 * Exceptions that should not be retried, as they will fail again. Examples of
   these include :class:`borneo.IllegalArgumentException`,
   :class:`borneo.TableNotFoundException`,  etc.

----------------------
Handle Resource Limits
----------------------

Programming in a resource-limited environment can be unfamiliar and can lead
to unexpected errors. Tables have user-specified throughput limits and if an
application exceeds those limits it may be throttled, which means requests
will raise instances of :class:`borneo.ThrottlingException`.

There is some support for built-in retries and users can create their own
:class:`borneo.RetryHandler` instances to be set using
:func:`borneo.NoSQLHandleConfig.set_retry_handler` allowing more direct
control over retries as well as tracing of throttling events. An application
should not rely on retries to handle throttling exceptions as that will result
in poor performance and an inability to use all of the throughput available for
the table. This happens because the default retry handler will do exponential
backoff, starting with a one-second delay.

While handling :class:`borneo.ThrottlingException` is necessary it is best to
avoid throttling entirely by rate-limiting your application. In this context
*rate-limiting* means keeping request rates under the limits for the table.
This is most common using queries, which can read a lot of data, using up
capacity very quickly. It can also happen for get and put operations that run
in a tight loop. Some tools to control your request rate include:

 * use the methods available in all Result objects that indicate how much
   read and write throughput was used by that request. For example, see
   :func:`borneo.GetResult.get_read_units` or
   :func:`borneo.PutResult.get_write_units`.
 * reduce the default amount of data read for a single query request by using
   :func:`borneo.QueryRequest.set_max_read_kb`. Remember to perform query
   operations in a loop, looking at the continuation key. Be aware that a single
   query request can return 0 results but still have a continuation key that
   means you need to keep looping.
 * add rate-limiting code in your request loop. This may be as simple as a
   delay between requests or intelligent code that considers how much data
   has been read (see :func:`borneo.QueryResult.get_read_units`) as well as
   the capacity of the table to either delay a request or reduce the amount of
   data to be read.
