#
# Copyright (c) 2018, 2026 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#

from time import sleep, time

from ..common import CheckValue
from ..exception import (
    IllegalArgumentException, IllegalStateException, NoSQLException,
    OperationNotSupportedException)
from ..operations import (
    ChangeStreamConsumerRequest, ChangeStreamPollRequest, GetTableRequest)
from .models import MessageBundle, StartLocation


_CHANGE_STREAM_NOT_SUPPORTED = 'Change Streams not supported by server'
_TABLE_OCID_PREFIX = 'ocid1.nosqltable.'


def _is_table_ocid(table_name):
    return (CheckValue.is_str(table_name) and
            table_name.lower().startswith(_TABLE_OCID_PREFIX))


def _raise_if_change_stream_not_supported(exc):
    msg = str(exc)
    lower_msg = msg.lower() if msg is not None else ''
    if ('unknown opcode' in lower_msg or
            'unknown operation' in lower_msg):
        raise OperationNotSupportedException(_CHANGE_STREAM_NOT_SUPPORTED)
    if (isinstance(exc, OperationNotSupportedException) and
            'change stream' in lower_msg and
            'serial version' not in lower_msg):
        raise OperationNotSupportedException(_CHANGE_STREAM_NOT_SUPPORTED)


class ConsumerBuilder(object):
    """
    Builder used to create a Change Streams :py:class:`Consumer`.
    """

    class TableConfig(object):
        """
        Internal table configuration used by Change Streams requests.
        """

        def __init__(self, table_name, compartment=None,
                     start_location=None, is_remove=False):
            CheckValue.check_str(table_name, 'table_name')
            CheckValue.check_str(compartment, 'compartment', True)
            if (start_location is not None and
                    not isinstance(start_location, StartLocation)):
                raise IllegalArgumentException(
                    'start_location must be an instance of StartLocation.')
            CheckValue.check_boolean(is_remove, 'is_remove')

            self._table_name = None
            self._table_ocid = None
            if _is_table_ocid(table_name):
                self._table_ocid = table_name
            else:
                self._table_name = table_name

            self._compartment = compartment
            if start_location is None:
                self._start_location = StartLocation.first_uncommitted()
            else:
                self._start_location = start_location
            self._is_remove = is_remove

        def get_table_name(self):
            return self._table_name

        def get_table_ocid(self):
            return self._table_ocid

        def get_compartment(self):
            return self._compartment

        def get_start_location(self):
            return self._start_location

        def is_remove(self):
            return self._is_remove

        def _set_table_ocid(self, table_ocid):
            CheckValue.check_str(table_ocid, 'table_ocid')
            self._table_ocid = table_ocid
            return self

        def __str__(self):
            return ('TableConfig [table_name=' + str(self._table_name) +
                    ', table_ocid=' + str(self._table_ocid) +
                    ', compartment=' + str(self._compartment) +
                    ', start_location=' + str(self._start_location) +
                    ', is_remove=' + str(self._is_remove) + ']')

    def __init__(self):
        self._compartment = None
        self._force_reset = False
        self._group_id = None
        self._handle = None
        self._manual_commit = False
        self._max_poll_interval_ms = None
        self._tables = None

    def set_handle(self, handle):
        """
        Sets the NoSQL handle used for all Change Streams operations.
        """
        if handle is None:
            raise IllegalArgumentException('handle must be not-none.')
        self._handle = handle
        return self

    def get_handle(self):
        return self._handle

    def add_table(self, table_name, compartment=None, start_location=None):
        """
        Adds a table to the Change Streams consumer configuration.

        The table name may be a table OCID. The compartment is used to resolve
        the table name to a table OCID. If compartment is not set, the
        configured default compartment is used for that table lookup.
        """
        if self._table_index(table_name, compartment) >= 0:
            return self
        table_config = self.TableConfig(
            table_name, compartment, start_location)
        if self._tables is None:
            self._tables = list()
        self._tables.append(table_config)
        return self

    def remove_table(self, table_name, compartment=None):
        """
        Adds a table-removal entry to the Change Streams consumer config.

        The table name may be a table OCID. The compartment is used to resolve
        the table name to a table OCID. If compartment is not set, the
        configured default compartment is used for that table lookup.
        """
        table_config = self.TableConfig(
            table_name, compartment, None, True)
        if self._tables is None:
            self._tables = list()
        self._tables.append(table_config)
        return self

    def set_group_id(self, group_id):
        """
        Sets the Change Streams consumer group ID.
        """
        CheckValue.check_str(group_id, 'group_id')
        self._group_id = group_id
        return self

    def get_group_id(self):
        return self._group_id

    def set_compartment(self, compartment):
        """
        Sets the compartment used for the Change Streams consumer group.

        Tables in the group may be in different compartments, as specified by
        add_table() and remove_table().
        """
        CheckValue.check_str(compartment, 'compartment', True)
        self._compartment = compartment
        return self

    def get_compartment(self):
        return self._compartment

    def set_commit_automatic(self):
        """
        Sets automatic commit mode for the consumer.
        """
        self._manual_commit = False
        return self

    def set_commit_manual(self):
        """
        Sets manual commit mode for the consumer.
        """
        self._manual_commit = True
        return self

    def is_manual_commit(self):
        return self._manual_commit

    def set_max_poll_interval(self, max_poll_interval_ms):
        """
        Sets the maximum interval between consumer poll calls, in milliseconds.
        """
        CheckValue.check_int_gt_zero(
            max_poll_interval_ms, 'max_poll_interval_ms')
        self._max_poll_interval_ms = max_poll_interval_ms
        return self

    def get_max_poll_interval(self):
        return self._max_poll_interval_ms

    def set_force_reset_start_location(self):
        """
        Forces existing consumer-group start locations to be reset.
        """
        self._force_reset = True
        return self

    def get_force_reset(self):
        return self._force_reset

    def get_tables(self):
        return self._tables

    def get_num_tables(self):
        if self._tables is None:
            return 0
        return len(self._tables)

    def validate(self):
        """
        Validates this builder and resolves table names to table OCIDs.
        """
        if self._handle is None:
            raise IllegalArgumentException(
                'Consumer builder missing NoSQLHandle.')
        if self._tables is None or len(self._tables) == 0:
            raise IllegalArgumentException(
                'Consumer builder missing tables information.')
        for table_config in self._tables:
            self.validate_table_config(table_config, self._handle)
        return self

    @staticmethod
    def validate_table_config(table_config, handle):
        """
        Validates one table config and resolves its table OCID when needed.
        """
        if not isinstance(table_config, ConsumerBuilder.TableConfig):
            raise IllegalArgumentException(
                'table_config must be an instance of TableConfig.')
        if table_config.get_table_ocid() is not None:
            return
        table_name = table_config.get_table_name()
        if table_name is None or len(table_name) == 0:
            raise IllegalArgumentException(
                'missing table name in consumer configuration.')
        if handle is None or not callable(getattr(handle, 'get_table', None)):
            raise IllegalArgumentException(
                'NoSQLHandle is required to resolve table OCIDs.')

        req = GetTableRequest().set_table_name(table_name)
        if table_config.get_compartment() is not None:
            req.set_compartment(table_config.get_compartment())
        try:
            res = handle.get_table(req)
            table_ocid = res.get_table_id()
            if table_ocid is None:
                raise IllegalArgumentException(
                    'table OCID is not available.')
            table_config._set_table_ocid(table_ocid)
        except Exception as exc:
            if isinstance(exc, IllegalArgumentException):
                detail = str(exc)
            else:
                detail = exc.__class__.__name__ + ': ' + str(exc)
            raise IllegalArgumentException(
                "Can't get table '" + table_name + "' information: " +
                detail)

    def build(self):
        """
        Creates a Change Streams consumer using this builder configuration.
        """
        return Consumer(self)

    def _table_index(self, table_name, compartment):
        if self._tables is None:
            return -1
        CheckValue.check_str(table_name, 'table_name')
        CheckValue.check_str(compartment, 'compartment', True)
        candidate = table_name.lower()
        for index, table_config in enumerate(self._tables):
            if self._matches_table(table_config, candidate):
                if self._same_compartment(
                        table_config.get_compartment(), compartment):
                    return index
        return -1

    @staticmethod
    def _matches_table(table_config, candidate):
        table_name = table_config.get_table_name()
        table_ocid = table_config.get_table_ocid()
        if table_name is not None and table_name.lower() == candidate:
            return True
        return table_ocid is not None and table_ocid.lower() == candidate

    @staticmethod
    def _same_compartment(left, right):
        if left is None:
            return right is None
        return right is not None and left.lower() == right.lower()


class Consumer(object):
    """
    Main object used to consume Change Streams messages.

    Use :py:class:`ConsumerBuilder` to create instances of this class. The
    :py:meth:`poll` method is not thread-safe.
    """

    _POLL_INTERVAL_MS = 100

    def __init__(self, builder):
        if not isinstance(builder, ConsumerBuilder):
            raise IllegalArgumentException(
                'builder must be an instance of ConsumerBuilder.')
        builder.validate()
        self._builder = builder
        self._closed = False
        self._cursor = None
        self._handle = builder.get_handle()
        self._metadata = None

        req = ChangeStreamConsumerRequest(
            ChangeStreamConsumerRequest.RequestMode.CREATE).set_builder(
                builder)
        res = self._execute_request(req)
        cursor = res.get_cursor()
        if cursor is None:
            raise NoSQLException(
                'Server returned invalid consumer cursor.')
        self._cursor = cursor
        self._metadata = res.get_metadata()

    def poll(self, limit, wait_ms):
        """
        Gets Change Streams messages for this consumer.
        """
        self._check_open()
        CheckValue.check_int_ge_zero(limit, 'limit')
        CheckValue.check_int_ge_zero(wait_ms, 'wait_ms')
        start_time_ms = int(round(time() * 1000))

        while True:
            bundle = self._poll_once(limit)
            if not bundle.is_empty():
                return bundle
            now_ms = int(round(time() * 1000))
            elapsed_ms = now_ms - start_time_ms
            if elapsed_ms + self._POLL_INTERVAL_MS > wait_ms:
                return bundle
            sleep(float(self._POLL_INTERVAL_MS) / 1000.0)

    def commit(self, timeout_ms=None):
        """
        Marks the messages from this consumer's latest poll as committed.
        """
        self._check_open()
        self._commit_internal(self._cursor, timeout_ms)

    def commit_bundle(self, bundle, timeout_ms=None):
        """
        Marks the messages in the specified bundle as committed.
        """
        self._check_open()
        if not isinstance(bundle, MessageBundle):
            raise IllegalArgumentException(
                'bundle must be an instance of MessageBundle.')
        cursor = bundle._get_cursor()
        if cursor is None:
            raise IllegalArgumentException(
                'MessageBundle does not contain a cursor.')
        self._commit_internal(cursor, timeout_ms)

    def close(self):
        """
        Closes this consumer and releases server-side resources for it.
        """
        if self._closed:
            return
        req = ChangeStreamConsumerRequest(
            ChangeStreamConsumerRequest.RequestMode.CLOSE).set_cursor(
                self._cursor)
        res = self._execute_request(req)
        if res.get_cursor() is not None:
            raise NoSQLException('Consumer not closed on server side.')
        self._cursor = None
        self._closed = True

    def reset(self):
        """
        Resets this consumer without committing poll results.
        """
        self._check_open()
        req = ChangeStreamConsumerRequest(
            ChangeStreamConsumerRequest.RequestMode.RESET).set_cursor(
                self._cursor)
        res = self._execute_request(req)
        cursor = res.get_cursor()
        if cursor is None:
            raise NoSQLException('Consumer not reset on server side.')
        self._cursor = cursor

    def add_table(self, table_name, compartment=None, start_location=None):
        """
        Adds a table to the current consumer group.

        The table name may be a table OCID. The compartment is used to resolve
        the table name to a table OCID. If compartment is not set, the
        configured default compartment is used for that table lookup.
        """
        self._check_open()
        builder = ConsumerBuilder().set_handle(self._handle).add_table(
            table_name, compartment, start_location)
        builder.validate()
        self._update_tables(builder)

    def remove_table(self, table_name, compartment=None):
        """
        Removes a table from the current consumer group.

        The table name may be a table OCID. The compartment is used to resolve
        the table name to a table OCID. If compartment is not set, the
        configured default compartment is used for that table lookup.
        """
        self._check_open()
        builder = ConsumerBuilder().set_handle(self._handle).remove_table(
            table_name, compartment)
        builder.validate()
        self._update_tables(builder)

    @staticmethod
    def delete_group(handle, group_id, compartment=None, force_stop=False):
        """
        Deletes a Change Streams consumer group.
        """
        Consumer._check_handle(handle)
        CheckValue.check_str(group_id, 'group_id')
        CheckValue.check_str(compartment, 'compartment', True)
        CheckValue.check_boolean(force_stop, 'force_stop')

        builder = ConsumerBuilder().set_group_id(group_id).set_compartment(
            compartment)
        if force_stop:
            builder.set_force_reset_start_location()
        req = ChangeStreamConsumerRequest(
            ChangeStreamConsumerRequest.RequestMode.DELETE).set_builder(
                builder)
        Consumer._execute_request_with_handle(handle, req)

    def _poll_once(self, limit):
        req = ChangeStreamPollRequest(self._cursor, limit)
        res = self._execute_request(req)
        bundle = res.get_bundle()
        cursor = res.get_cursor()

        if cursor is None:
            if bundle is not None:
                raise NoSQLException('Poll returned invalid cursor.')
            bundle = MessageBundle(None)
        else:
            self._cursor = cursor

        bundle._set_cursor(self._cursor)
        bundle._set_consumer(self)
        bundle._set_events_remaining(res.get_events_remaining())
        return bundle

    def _commit_internal(self, cursor, timeout_ms):
        req = ChangeStreamConsumerRequest(
            ChangeStreamConsumerRequest.RequestMode.COMMIT).set_cursor(
                cursor)
        self._set_request_timeout(req, timeout_ms)
        res = self._execute_request(req)
        cursor = res.get_cursor()
        if cursor is None:
            raise NoSQLException('Consumer not committed on server side.')
        self._cursor = cursor

    def _update_tables(self, builder):
        req = ChangeStreamConsumerRequest(
            ChangeStreamConsumerRequest.RequestMode.UPDATE).set_builder(
                builder).set_cursor(self._cursor)
        res = self._execute_request(req)
        cursor = res.get_cursor()
        if cursor is None:
            raise NoSQLException(
                'Server returned invalid consumer cursor.')
        self._cursor = cursor
        if res.get_metadata() is not None:
            self._metadata = res.get_metadata()

    def _execute_request(self, request):
        return self._execute_request_with_handle(self._handle, request)

    @staticmethod
    def _execute_request_with_handle(handle, request):
        Consumer._check_handle(handle)
        try:
            if callable(getattr(handle, '_execute', None)):
                return handle._execute(request)
            return handle.get_client().execute(request)
        except Exception as exc:
            _raise_if_change_stream_not_supported(exc)
            raise

    @staticmethod
    def _set_request_timeout(request, timeout_ms):
        if timeout_ms is not None:
            request.set_timeout(timeout_ms)

    @staticmethod
    def _check_handle(handle):
        if handle is None:
            raise IllegalArgumentException('handle must be not-none.')
        if callable(getattr(handle, '_execute', None)):
            return
        get_client = getattr(handle, 'get_client', None)
        if callable(get_client):
            client = get_client()
            if client is not None and callable(getattr(client, 'execute',
                                                       None)):
                return
        raise IllegalArgumentException(
            'handle must be an instance of NoSQLHandle.')

    def _check_open(self):
        if self._closed:
            raise IllegalStateException('Consumer has been closed.')
