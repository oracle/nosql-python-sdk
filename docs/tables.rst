.. _tables:

Working With Tables
~~~~~~~~~~~~~~~~~~~

Applications using the Oracle NoSQL Database Cloud Service work with tables. Tables are created and data is added, modified and removed. Indexes can be added on tables. These topics are covered.

---------------------
Obtain a NoSQL Handle
---------------------

.. code-block:: pycon

    >>> import borneo
    >>> config = borneo.NoSQLHandleConfig(...).set_authorization_provider(...)
    >>> handle = borneo.NoSQLHandle(config)

-------------------------
Create Tables and Indexes
-------------------------
Learn how to create tables and indexes in Oracle NoSQL Database Cloud.

Creating a table is the first step of developing your application. You use the TableRequest class and its methods to execute Data Definition Language (DDL) statements, such as, creating, modifying, and dropping tables. You also set table limits using TableRequest.set_table_limits method.

Before creating a table, learn about:

The supported data types for Oracle NoSQL Database Cloud. See `Supported Data Types <https://docs.oracle.com/en/cloud/paas/nosql-cloud/csnsd/supported-data-types.html>`_.

Cloud limits. See `Oracle NoSQL Database Cloud Limits <https://docs.oracle.com/en/cloud/paas/nosql-cloud/csnsd/cloud-limits.html>`_.

.. code-block:: pycon

    >>> from borneo import (TableLimits, TableRequest, State)
    >>> statement = 'create table if not exists users(id integer, name string, primary key(id)'
    >>> request = TableRequest().set_statement(statement).set_tableLimits(
            TableLimits(20, 10, 5))
    >>> result = handle.table_request(request)
    >>> # table_request is asynchronous, so wait for the ACTIVE state
    >>> # wait for 40 seconds, polling every 3 seconds
    >>> result.wait_for_state(handle, 'users', State.ACTIVE, 40000, 3000)

--------
Add Data
--------
=============
Add JSON Data
=============

---------
Read Data
---------

-----------
Use Queries
-----------

-----------
Delete Data
-----------

-------------
Modify Tables
-------------

-------------------------
Delete Tables and Indexes
-------------------------

-------------
Handle Errors
-------------
