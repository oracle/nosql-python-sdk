.. _changestream:

Change Streams
~~~~~~~~~~~~~~

Change Streams is a cloud service feature for consuming table changes in
commit order. Applications create a consumer group, poll messages, process the
records in those messages, and commit progress. Detailed interface descriptions
are in :ref:`api`.

Change Streams is available through the :mod:`borneo.changestream` submodule.
The classes are not exported from the top-level :mod:`borneo` namespace.

--------------------------------------
Enable or Disable Change Streams
--------------------------------------

Change Streams must be enabled for a table before a consumer can read changes
from that table. The convenience method
:func:`borneo.NoSQLHandle.enable_change_streaming` performs the table operation
and waits for it to complete.

.. code-block:: pycon

    result = handle.enable_change_streaming(
        'users',
        compartment='mycompartment',
        enabled=True,
        timeout_ms=60000,
        poll_interval_ms=1000)

To disable Change Streams for the same table:

.. code-block:: pycon

    handle.enable_change_streaming(
        'users',
        compartment='mycompartment',
        enabled=False,
        timeout_ms=60000,
        poll_interval_ms=1000)

Advanced callers can use :class:`borneo.TableRequest` directly:

.. code-block:: pycon

    from borneo import TableRequest

    request = TableRequest().set_table_name(
        'users').set_change_streaming_enabled(True)
    request.set_compartment('mycompartment')
    result = handle.table_request(request)
    result.wait_for_completion(handle, 60000, 1000)

-----------------
Create a Consumer
-----------------

Use :class:`borneo.changestream.ConsumerBuilder` to create a consumer. The
builder resolves table names to table OCIDs using the configured handle. A table
OCID can also be supplied directly.

.. code-block:: pycon

    from borneo.changestream import ConsumerBuilder, StartLocation

    consumer = ConsumerBuilder().set_handle(handle).set_group_id(
        'users-group').set_compartment('groupcompartment').add_table(
        'users',
        compartment='tablecompartment',
        start_location=StartLocation.first_uncommitted()).build()

If no start location is supplied for a table, the default is
:func:`borneo.changestream.StartLocation.first_uncommitted`.

The builder compartment is the compartment used for the consumer group. Table
names are resolved using the compartment supplied to
:func:`borneo.changestream.ConsumerBuilder.add_table` or
:func:`borneo.changestream.ConsumerBuilder.remove_table`. If no table
compartment is supplied, the configured default compartment is used for that
table lookup. Different tables in the same consumer group can use different
table compartments.

---------------
Start Locations
---------------

Start locations control where the consumer starts reading when a table is added
to a group.

* :func:`borneo.changestream.StartLocation.first_uncommitted` starts at the
  first uncommitted message for the group.
* :func:`borneo.changestream.StartLocation.earliest` starts at the earliest
  available message in the stream.
* :func:`borneo.changestream.StartLocation.latest` starts with messages
  published after the consumer starts.
* :func:`borneo.changestream.StartLocation.at_time` starts at a time in
  milliseconds since the Epoch.

.. code-block:: pycon

    consumer = ConsumerBuilder().set_handle(handle).set_group_id(
        'users-group').add_table(
        'users',
        compartment='mycompartment',
        start_location=StartLocation.at_time(start_time_ms)).build()

-----------------------
Poll and Process Events
-----------------------

Call :func:`borneo.changestream.Consumer.poll` to fetch a
:class:`borneo.changestream.MessageBundle`. A bundle contains messages, each
message contains events, and each event contains records.

.. code-block:: pycon

    bundle = consumer.poll(limit=100, wait_ms=5000)

    for message in bundle.get_messages() or []:
        table_name = message.get_table_name()
        for event in message.get_events() or []:
            for record in event.get_records():
                key = record.get_record_key()
                current = record.get_current_image()
                before = record.get_before_image()
                value = None if current is None else current.get_value()
                previous = None if before is None else before.get_value()
                # Process the table name, key, current value, and previous value.

The value, key, and metadata objects use the normal Python SDK value
representation, such as ``dict``, ``list``, scalar values, ``bytes`` or
``bytearray``, and ``Decimal``.

------------------------
Commit Processed Records
------------------------

Consumer groups can use automatic or manual commit mode. Automatic commit mode
is the default. Manual commit mode requires the application to commit after it
has successfully processed messages.

.. code-block:: pycon

    consumer = ConsumerBuilder().set_handle(handle).set_group_id(
        'users-group').set_commit_manual().add_table('users').build()

    bundle = consumer.poll(limit=100, wait_ms=5000)
    if not bundle.is_empty():
        # Process all records first.
        bundle.commit(timeout_ms=30000)

The method :func:`borneo.changestream.MessageBundle.commit` commits the cursor
associated with that bundle. :func:`borneo.changestream.Consumer.commit`
commits the consumer's latest cursor.

----------------------
Multiple Consumers
----------------------

Multiple consumers can use the same group ID to share work. Multiple groups can
read the same table independently. A consumer group can also include multiple
tables.

.. code-block:: pycon

    consumer = ConsumerBuilder().set_handle(handle).set_group_id(
        'orders-group').add_table('orders').add_table('order_items').build()

Tables can be added to or removed from an active group:

.. code-block:: pycon

    consumer.add_table('order_events', compartment='orderscompartment')
    consumer.remove_table('order_items', compartment='orderscompartment')

------------------------
Close, Reset, and Delete
------------------------

Close a consumer when it is no longer needed:

.. code-block:: pycon

    consumer.close()

Resetting a consumer can cause messages to be delivered again, depending on the
group state and start locations:

.. code-block:: pycon

    consumer.reset()

Deleting a group removes the group state. Use ``force_stop=True`` only when the
group must be deleted while consumers may still be active.

.. code-block:: pycon

    from borneo.changestream import Consumer

    Consumer.delete_group(
        handle,
        'users-group',
        compartment='mycompartment',
        force_stop=True)
