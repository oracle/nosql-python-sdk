.. _datatypes:


Data Types
~~~~~~~~~~

This topic describes the mapping between types in the Oracle NoSQL Database and
Python data types. The database types are referred to as *database types* while
the Python equivalents are *Python types*.

===========================
Oracle NoSQL Database Types
===========================

See `Supported Data Types <https://docs.oracle.com/pls/topic/lookup?ctx=en/cloud
/paas/nosql-cloud&id=CSNSD-GUID-833B2B2A-1A32-48AB-A19E-413EAFB964B8>`_ for a
description of the data types supported by the service. An application uses
these types to create tables and indexes. For example, a table may be created
using this Data Definition Language (DDL) statement, which defines types in
terms of the database types::

   create table mytable(id integer, name string, created timestamp,
       address record(street string, city string, zip integer), primary key(id))

In order to insert rows into such a table your application must create a Python
dict that corresponds to that schema, for example::

   {'id': 1, 'name': 'myname', 'created': datetime.now(),
    'address' : {'street' : '14 Elm Street', 'city' : "hometown',
    'zip' : 00000}}

Similarly, when operating on rows retrieved from the database it is important to
understand the mappings to Python types.

=========================================
Mapping Between Database and Python types
=========================================

These mappings apply on both input (get/query) and output (put). In general the
system is permissive in terms of valid conversions among types and that any
lossless conversion is allowed. For example an integer will be accepted for a
float or double database type. The *Timestamp* type is also flexible and will
accept any valid IS0 8601 formatted string. Timestamps are always stored and
managed in UTC.

=============    ==============================
Database Type             Python Type
=============    ==============================
Integer                      int
Long             int (Python 3), long (Python2)
Float            float
Double           float
Number           decimal.Decimal
Boolean          bool
String           str
Timestamp        datetime.datetime
Enum             str
Binary           bytearray
FixedBinary      bytearray
Array            list
Map              dict
Record           dict
JSON             any valid JSON
=============    ==============================

===================
Timestamp in Borneo
===================

As mentioned above *Timestamp* fields are managed internally as UTC time. If a
timezone is supplied when setting a *Timestamp*, either as a string or as a
Python datetime object, it will be honored. The value will be converted to UTC
internally and will be in UTC when returned in a row. Although they are represented
in UTC returned datetime objects will be "naive" as described by Python documentation.
On input, if no timezone is supplied, python datetime instances and time strings are
treated as UTC.
