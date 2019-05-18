#
# Copyright (C) 2018, 2019 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
#
# Please see LICENSE.txt file included in the top-level directory of the
# appropriate download for a copy of the license and additional information.
#

from datetime import datetime
from json import loads
from time import mktime, sleep, time

from .common import (
    CheckValue, Consistency, FieldRange, PreparedStatement, PutOption, State,
    TableLimits, TimeToLive, Version)
from .config import NoSQLHandleConfig
from .exception import (
    BatchOperationNumberLimitException, IllegalArgumentException,
    RequestTimeoutException, TableNotFoundException)
try:
    import serde
except ImportError:
    from . import serde


class Request(object):
    """
    A request is a class used as a base for all requests types. Public state and
    methods are implemented by extending classes.
    """

    def __init__(self):
        self.__timeout_ms = 0

    def _set_timeout_internal(self, timeout_ms):
        CheckValue.check_int_gt_zero(timeout_ms, 'timeout_ms')
        self.__timeout_ms = timeout_ms

    def _get_timeout_internal(self):
        return self.__timeout_ms

    def set_defaults(self, cfg):
        """
        Internal use only.

        Sets default values in a request based on the specified config object.
        This will typically be overridden by subclasses.

        :param cfg: the configuration object to use to get default values.
        :return: self.
        :raises IllegalArgumentException: raises the exception if cfg is not an
            instance of NoSQLHandleConfig.
        """
        if not isinstance(cfg, NoSQLHandleConfig):
            raise IllegalArgumentException(
                'set_defaults requires an instance of NoSQLHandleConfig as ' +
                'parameter.')
        if self.__timeout_ms == 0:
            self.__timeout_ms = cfg.get_default_timeout()
        return self

    def should_retry(self):
        # Returns True if this request should be retried.
        return True


class WriteRequest(Request):
    """
    Represents a base class for the single row modifying operations
    :py:meth:`NoSQLHandle.put` and :py:meth:`NoSQLHandle.delete`.

    This class encapsulates the common parameters of table name and the return
    row boolean, which allows applications to get information about the existing
    value of the target row on failure. By default no previous information is
    returned.
    """

    def __init__(self):
        super(WriteRequest, self).__init__()
        self.__table_name = None
        self.__return_row = False

    def set_table_name_internal(self, table_name):
        CheckValue.check_str(table_name, 'table_name')
        self.__table_name = table_name

    def get_table_name_internal(self):
        return self.__table_name

    def set_return_row_internal(self, return_row):
        CheckValue.check_boolean(return_row, 'return_row')
        self.__return_row = return_row

    def get_return_row_internal(self):
        return self.__return_row

    def validate_write_request(self, request_name):
        if request_name is None:
            raise IllegalArgumentException(
                request_name + ' requires table name')


class ReadRequest(Request):
    """
    Represents a base class for read operations such as
    :py:meth:`NoSQLHandle.get`.

    This class encapsulates the common parameters of table name and consistency.
    By default read operations use Consistency.EVENTUAL. Use of
    Consistency.ABSOLUTE should be used only when required as it incurs
    additional cost.
    """

    def __init__(self):
        super(ReadRequest, self).__init__()
        self.__table_name = None
        self.__consistency = None

    def set_table_name_internal(self, table_name):
        CheckValue.check_str(table_name, 'table_name')
        self.__table_name = table_name

    def get_table_name_internal(self):
        return self.__table_name

    def set_consistency_internal(self, consistency):
        self.__consistency = consistency

    def get_consistency_internal(self):
        return self.__consistency

    def set_defaults(self, cfg):
        super(ReadRequest, self).set_defaults(cfg)
        if self.__consistency is None:
            self.__consistency = cfg.get_default_consistency()
        return self

    def validate_read_request(self, request_name):
        if self.__table_name is None:
            raise IllegalArgumentException(
                request_name + ' requires table name.')


class DeleteRequest(WriteRequest):
    """
    Represents the input to a :py:meth:`NoSQLHandle.delete` operation.

    This request can be used to perform unconditional and conditional deletes:
        * Delete any existing row. This is the default.
        * Succeed only if the row exists and and its :py:class:`Version`
          matches a specific :py:class:`Version`. Use
          :py:meth:`set_match_version` for this case. Using this option in
          conjunction with using :py:meth:`set_return_row` allows information
          about the existing row to be returned if the operation fails because
          of a version mismatch. On success no information is returned.

    Using :py:meth:`set_return_row` may incur additional cost and affect
    operation latency.

    The table name and key are required parameters. On a successful operation
    :py:meth:`DeleteResult.get_success` returns True. Additional information,
    such as previous row information, may be available in
    :py:class:`DeleteResult`.
    """

    def __init__(self):
        super(DeleteRequest, self).__init__()
        self.__key = None
        self.__match_version = None

    def __str__(self):
        return 'DeleteRequest'

    def set_key(self, key):
        """
        Sets the key to use for the delete operation. This is a required field.

        :param key: the key value.
        :type key: dict

        :return: self.
        :raises IllegalArgumentException: raises the exception if key is not a
            dictionary.
        """
        CheckValue.check_dict(key, 'key')
        self.__key = key
        return self

    def set_key_from_json(self, json_key):
        """
        Sets the key to use for the delete operation based on a JSON string.
        The string is parsed for validity and stored internally as a dict.

        :param json_key: the key as a JSON string.
        :type json_key: str
        :return: self.
        :raises IllegalArgumentException: raises the exception if json_key is
            not a string.
        """
        CheckValue.check_str(json_key, 'json_key')
        self.__key = loads(json_key)
        return self

    def get_key(self):
        """
        Returns the key of the row to be deleted.

        :return: the key value, or None if not set.
        :rtype: dict
        """
        return self.__key

    def set_match_version(self, version):
        """
        Sets the :py:class:`Version` to use for a conditional delete operation.
        The Version is usually obtained from :py:meth:`GetResult.get_version` or
        other method that returns a Version. When set, the delete operation will
        succeed only if the row exists and its Version matches the one
        specified. Using this option will incur additional cost.

        :param version: the :py:class:`Version` to match.
        :type version: Version
        :return: self.
        :raises IllegalArgumentException: raises the exception if version is not
            an instance of Version.
        """
        if not isinstance(version, Version):
            raise IllegalArgumentException('set_match_version requires an ' +
                                           'instance of Version as parameter.')
        self.__match_version = version
        return self

    def get_match_version(self):
        """
        Returns the :py:class:`Version` used for a match on a conditional
        delete.

        :return: the Version or None if not set.
        :rtype: Version
        """
        return self.__match_version

    def set_timeout(self, timeout_ms):
        """
        Sets the optional request timeout value, in milliseconds. This overrides
        any default value set in :py:class:`NoSQLHandleConfig`. The value must
        be positive.

        :param timeout_ms: the timeout value, in milliseconds.
        :type timeout_ms: int
        :return: self.
        :raises IllegalArgumentException: raises the exception if the timeout
            value is less than or equal to 0.
        """
        super(DeleteRequest, self)._set_timeout_internal(timeout_ms)
        return self

    def get_timeout(self):
        """
        Returns the timeout to use for the operation, in milliseconds. A value
        of 0 indicates that the timeout has not been set.

        :return: the timeout value.
        :rtype: int
        """
        return super(DeleteRequest, self)._get_timeout_internal()

    def set_table_name(self, table_name):
        """
        Sets the table name to use for the operation. This is a required
        parameter.

        :param table_name: the table name.
        :type table_name: str
        :return: self.
        :raises IllegalArgumentException: raises the exception if table_name is
            not a string.
        """
        super(DeleteRequest, self).set_table_name_internal(table_name)
        return self

    def get_table_name(self):
        """
        Returns the table name for the operation.

        :return: the table name, or None if not set.
        :rtype: str
        """
        return super(DeleteRequest, self).get_table_name_internal()

    def set_return_row(self, return_row):
        """
        Sets whether information about the existing row should be returned on
        failure because of a version mismatch. If a match version has not been
        set via :py:meth:`set_match_version` this parameter is ignored and there
        will be no return information. This parameter is optional and defaults
        to False. It's use may incur additional cost.

        :param return_row: set to True if information should be returned.
        :type return_row: bool
        :return: self.
        :raises IllegalArgumentException: raises the exception if return_row is
            not True or False.
        """
        super(DeleteRequest, self).set_return_row_internal(return_row)
        return self

    def get_return_row(self):
        """
        Returns whether information about the existing row should be returned on
        failure because of a version mismatch.

        :return: True if information should be returned.
        :rtype: bool
        """
        return super(DeleteRequest, self).get_return_row_internal()

    def validate(self):
        # Validates the state of the object when complete.
        super(DeleteRequest, self).validate_write_request('DeleteRequest')
        if self.__key is None:
            raise IllegalArgumentException('DeleteRequest requires a key.')

    def create_serializer(self):
        return serde.DeleteRequestSerializer()

    def create_deserializer(self):
        return serde.DeleteRequestSerializer(cls_result=DeleteResult)


class GetIndexesRequest(Request):
    """
    Represents the argument of a :py:meth:`NoSQLHandle.get_indexes` operation
    which returns the information of a specific index or all indexes of the
    specified table, as returned in :py:class:`GetIndexesResult`.

    The table name is a required parameter.
    """

    def __init__(self):
        super(GetIndexesRequest, self).__init__()
        self.__table_name = None
        self.__index_name = None

    def set_table_name(self, table_name):
        """
        Sets the table name to use for the request.

        :param table_name: the table name. This is a required parameter.
        :type table_name: str
        :return: self.
        :raises IllegalArgumentException: raises the exception if table_name is
            not a string.
        """
        CheckValue.check_str(table_name, 'table_name')
        self.__table_name = table_name
        return self

    def get_table_name(self):
        """
        Gets the table name to use for the request.

        :return: the table name.
        :rtype: str
        """
        return self.__table_name

    def set_index_name(self, index_name):
        """
        Sets the index name to use for the request. If not set, this request
        will return all indexes of the table.

        :param index_name: the index name.
        :type index_name: str
        :return: self.
        :raises IllegalArgumentException: raises the exception if index_name is
            not a string.
        """
        CheckValue.check_str(index_name, 'index_name')
        self.__index_name = index_name
        return self

    def get_index_name(self):
        """
        Gets the index name to use for the request.

        :return: the index name.
        :rtype: str
        """
        return self.__index_name

    def set_timeout(self, timeout_ms):
        """
        Sets the request timeout value, in milliseconds. This overrides any
        default value set in :py:class:`NoSQLHandleConfig`. The value must be
        positive.

        :param timeout_ms: the timeout value, in milliseconds.
        :type timeout_ms: int
        :return: self.
        :raises IllegalArgumentException: raises the exception if the timeout
            value is less than or equal to 0.
        """
        super(GetIndexesRequest, self)._set_timeout_internal(timeout_ms)
        return self

    def get_timeout(self):
        """
        Returns the timeout to use for the operation, in milliseconds. A value
        of 0 indicates that the timeout has not been set.

        :return: the timeout value.
        :rtype: int
        """
        return super(GetIndexesRequest, self)._get_timeout_internal()

    def should_retry(self):
        # Returns True if this request should be retried.
        return False

    def validate(self):
        if self.__table_name is None:
            raise IllegalArgumentException(
                'GetIndexesRequest requires a table name.')

    def create_serializer(self):
        return serde.GetIndexesRequestSerializer()

    def create_deserializer(self):
        return serde.GetIndexesRequestSerializer(GetIndexesResult)


class GetRequest(ReadRequest):
    """
    Represents the input to a :py:meth:`NoSQLHandle.get` operation which returns
    a single row based on the specified key.

    The table name and key are required parameters.
    """

    def __init__(self):
        super(GetRequest, self).__init__()
        self.__key = None

    def set_key(self, key):
        """
        Sets the primary key used for the get operation. This is a required
        parameter.

        :param key: the primary key.
        :type key: dict
        :return: self.
        :raises IllegalArgumentException: raises the exception if key is not a
            dictionary.
        """
        CheckValue.check_dict(key, 'key')
        self.__key = key
        return self

    def set_key_from_json(self, json_key):
        """
        Sets the key to use for the get operation based on a JSON string.

        :param json_key: the key as a JSON string.
        :type json_key: str
        :return: self.
        :raises IllegalArgumentException: raises the exception if json_key is
            not a string.
        """
        CheckValue.check_str(json_key, 'json_key')
        self.__key = loads(json_key)
        return self

    def get_key(self):
        """
        Returns the primary key used for the operation. This is a required
        parameter.

        :return: the key.
        :rtype: dict
        """
        return self.__key

    def set_table_name(self, table_name):
        """
        Sets the table name to use for the operation. This is a required
        parameter.

        :param table_name: the table name.
        :type table_name: str
        :return: self.
        :raises IllegalArgumentException: raises the exception if table_name is
            not a string.
        """
        super(GetRequest, self).set_table_name_internal(table_name)
        return self

    def set_consistency(self, consistency):
        """
        Sets the consistency to use for the operation. This parameter is
        optional and if not set the default consistency configured for the
        :py:class:`NoSQLHandle` is used.

        :param consistency: the consistency.
        :type consistency: Consistency
        :return: self.
        :raises IllegalArgumentException: raises the exception if consistency
            is not Consistency.ABSOLUTE or Consistency.EVENTUAL.
        """
        if (consistency != Consistency.ABSOLUTE and
                consistency != Consistency.EVENTUAL):
            raise IllegalArgumentException(
                'Consistency must be Consistency.ABSOLUTE or ' +
                'Consistency.EVENTUAL')
        super(GetRequest, self).set_consistency_internal(consistency)
        return self

    def get_consistency(self):
        """
        Returns the consistency set for this request, or None if not set.

        :return: the consistency
        :rtype: Consistency
        """
        super(GetRequest, self).get_consistency_internal()

    def set_timeout(self, timeout_ms):
        """
        Sets the request timeout value, in milliseconds. This overrides any
        default value set in :py:class:`NoSQLHandleConfig`. The value must be
        positive.

        :param timeout_ms: the timeout value, in milliseconds.
        :type timeout_ms: int
        :return: self.
        :raises IllegalArgumentException: raises the exception if the timeout
            value is less than or equal to 0.
        """
        super(GetRequest, self)._set_timeout_internal(timeout_ms)
        return self

    def get_timeout(self):
        """
        Returns the timeout to use for the operation, in milliseconds. A value
        of 0 indicates that the timeout has not been set.

        :return: the timeout value.
        :rtype: int
        """
        return super(GetRequest, self)._get_timeout_internal()

    def validate(self):
        # Validates the state of the members of this class for use.
        super(GetRequest, self).validate_read_request('GetRequest')
        if self.__key is None:
            raise IllegalArgumentException('GetRequest requires a key.')

    def create_serializer(self):
        return serde.GetRequestSerializer()

    def create_deserializer(self):
        return serde.GetRequestSerializer(GetResult)


class GetTableRequest(Request):
    """
    Represents the argument of a :py:meth:`NoSQLHandle.get_table` operation
    which returns static information associated with a table, as returned in
    :py:class:`TableResult`. This information only changes in response to a
    change in table schema or a change in provisioned throughput or capacity for
    the table.

    The table name is a required parameter.
    """

    def __init__(self):
        super(GetTableRequest, self).__init__()
        self.__table_name = None
        self.__operation_id = None

    def set_table_name(self, table_name):
        """
        Sets the table name to use for the request.

        :param table_name: the table name. This is a required parameter.
        :type table_name: str
        :return: self.
        :raises IllegalArgumentException: raises the exception if table_name is
            not a string.
        """
        CheckValue.check_str(table_name, 'table_name')
        self.__table_name = table_name
        return self

    def get_table_name(self):
        """
        Gets the table name to use for the request.

        :return: the table name.
        :rtype: str
        """
        return self.__table_name

    def set_operation_id(self, operation_id):
        """
        Sets the operation id to use for the request. The operation id can be
        obtained via :py:meth:`TableResult.get_operation_id`. This parameter is
        optional. If non-none, it represents an asynchronous table operation
        that may be in progress. It is used to examine the result of the
        operation and if the operation has failed an exception will be thrown in
        response to a :py:meth:`NoSQLHandle.get_table` operation. If the
        operation is in progress or has completed successfully, the state of the
        table is returned.

        :param operation_id: the operation id. This is optional.
        :type operation_id: int
        :return: self.
        :raises IllegalArgumentException: raises the exception if operation_id
            is a negative number.
        """
        if operation_id is not None and not CheckValue.is_str(operation_id):
            raise IllegalArgumentException(
                'operation_id must be a string type.')
        self.__operation_id = operation_id
        return self

    def get_operation_id(self):
        """
        Returns the operation id to use for the request, None if not set.

        :return: the operation id.
        :rtype: int
        """
        return self.__operation_id

    def set_timeout(self, timeout_ms):
        """
        Sets the request timeout value, in milliseconds. This overrides any
        default value set in :py:class:`NoSQLHandleConfig`. The value must be
        positive.

        :param timeout_ms: the timeout value, in milliseconds.
        :type timeout_ms: int
        :return: self.
        :raises IllegalArgumentException: raises the exception if the timeout
            value is less than or equal to 0.
        """
        super(GetTableRequest, self)._set_timeout_internal(timeout_ms)
        return self

    def get_timeout(self):
        """
        Returns the timeout to use for the operation, in milliseconds. A value
        of 0 indicates that the timeout has not been set.

        :return: the timeout value.
        :rtype: int
        """
        return super(GetTableRequest, self)._get_timeout_internal()

    def should_retry(self):
        # Returns True if this request should be retried.
        return False

    def validate(self):
        if self.__table_name is None:
            raise IllegalArgumentException(
                'GetTableRequest requires a table name.')

    def create_serializer(self):
        return serde.GetTableRequestSerializer()

    def create_deserializer(self):
        return serde.GetTableRequestSerializer(TableResult)


class ListTablesRequest(Request):
    """
    Represents the argument of a :py:meth:`NoSQLHandle.list_tables` operation
    which lists all available tables associated with the identity associated
    with the handle used for the operation. If the list is large it can be paged
    by using the start_index and limit parameters. The list is returned in a
    simple array in :py:class:`ListTablesResult`. Names are returned sorted in
    alphabetical order in order to facilitate paging.
    """

    def __init__(self):
        super(ListTablesRequest, self).__init__()
        self.__start_index = 0
        self.__limit = 0

    def set_start_index(self, start_index):
        """
        Sets the index to use to start returning table names. This is related to
        the :py:meth:`ListTablesResult.get_last_returned_index` from a previous
        request and can be used to page table names. If not set, the list starts
        at index 0.

        :param start_index: the start index.
        :type start_index: int
        :return: self.
        :raises IllegalArgumentException: raises the exception if start_index is
            a negative number.
        """
        CheckValue.check_int_ge_zero(start_index, 'start_index')
        self.__start_index = start_index
        return self

    def get_start_index(self):
        """
        Returns the index to use to start returning table names. This is related
        to the :py:meth:`ListTablesResult.get_last_returned_index` from a
        previous request and can be used to page table names. If not set, the
        list starts at index 0.

        :return: the start index.
        :rtype: int
        """
        return self.__start_index

    def set_limit(self, limit):
        """
        Sets the maximum number of table names to return in the operation. If
        not set (0) there is no limit.

        :param limit: the maximum number of tables.
        :type limit: int
        :return: self.
        :raises IllegalArgumentException: raises the exception if limit is a
            negative number.
        """
        CheckValue.check_int_ge_zero(limit, 'limit')
        self.__limit = limit
        return self

    def get_limit(self):
        """
        Returns the maximum number of table names to return in the operation. If
        not set (0) there is no application-imposed limit.

        :return: the maximum number of tables to return in a single request.
        :rtype: int
        """
        return self.__limit

    def set_timeout(self, timeout_ms):
        """
        Sets the request timeout value, in milliseconds. This overrides any
        default value set in :py:class:`NoSQLHandleConfig`. The value must be
        positive.

        :param timeout_ms: the timeout value, in milliseconds.
        :type timeout_ms: int
        :return: self.
        :raises IllegalArgumentException: raises the exception if the timeout
            value is less than or equal to 0.
        """
        super(ListTablesRequest, self)._set_timeout_internal(timeout_ms)
        return self

    def get_timeout(self):
        """
        Returns the timeout to use for the operation, in milliseconds. A value
        of 0 indicates that the timeout has not been set.

        :return: the timeout value.
        :rtype: int
        """
        return super(ListTablesRequest, self)._get_timeout_internal()

    def should_retry(self):
        # Returns True if this request should be retried.
        return False

    def validate(self):
        if self.__start_index < 0 or self.__limit < 0:
            raise IllegalArgumentException(
                'ListTables: start index and number of tables must be ' +
                'non-negative.')

    def create_serializer(self):
        return serde.ListTablesRequestSerializer()

    def create_deserializer(self):
        return serde.ListTablesRequestSerializer(ListTablesResult)


class MultiDeleteRequest(Request):
    """
    Represents the input to a :py:meth:`NoSQLHandle.multi_delete` operation
    which can be used to delete a range of values that match the primary key and
    range provided.

    A range is specified using a partial key plus a range based on the portion
    of the key that is not provided. For example if a table's primary key is
    <id, timestamp>; and the its shard key is the id, it is possible to delete
    a range of timestamp values for a specific id by providing an id but no
    timestamp in the value used for :py:meth:`set_key` and providing a range of
    timestamp values in the :py:class:`FieldRange` used in :py:meth:`set_range`.

    Because this operation can exceed the maximum amount of data modified in a
    single operation a continuation key can be used to continue the operation.
    The continuation key is obtained from
    :py:meth:`MultiDeleteResult.get_continuation_key` and set in a new request
    using :py:meth:`set_continuation_key`. Operations with a continuation key
    still require the primary key.

    The table name and key are required parameters.
    """

    def __init__(self):
        super(MultiDeleteRequest, self).__init__()
        self.__table_name = None
        self.__key = None
        self.__continuation_key = None
        self.__range = None
        self.__max_write_kb = 0

    def set_table_name(self, table_name):
        """
        Sets the table name to use for the operation. This is a required
        parameter.

        :param table_name: the table name.
        :type table_name: str
        :return: self.
        :raises IllegalArgumentException: raises the exception if table_name is
            not a string.
        """
        CheckValue.check_str(table_name, 'table_name')
        self.__table_name = table_name
        return self

    def get_table_name(self):
        """
        Returns the tableName used for the operation.

        :return: the table name.
        :rtype: str
        """
        return self.__table_name

    def set_key(self, key):
        """
        Sets the key to be used for the operation. This is a required parameter
        and must completely specify the target table's shard key.

        :param key: the key.
        :type key: dict
        :return: self.
        :raises IllegalArgumentException: raises the exception if key is not a
            dictionary.
        """
        CheckValue.check_dict(key, 'key')
        self.__key = key
        return self

    def get_key(self):
        """
        Returns the key to be used for the operation.

        :return: the key.
        :rtype: dict
        """
        return self.__key

    def set_continuation_key(self, continuation_key):
        """
        Sets the continuation key.

        :param continuation_key: the key which should have been obtained from
            :py:meth:`MultiDeleteResult.get_continuation_key`.
        :type continuation_key: bytearray
        :return: self.
        :raises IllegalArgumentException: raises the exception if
            continuation_key is not a bytearray.
        """
        if not isinstance(continuation_key, bytearray):
            raise IllegalArgumentException(
                'set_continuation_key requires bytearray as parameter.')
        self.__continuation_key = continuation_key
        return self

    def get_continuation_key(self):
        """
        Returns the continuation key if set.

        :return: the continuation key.
        :rtype: bytearray
        """
        return self.__continuation_key

    def set_max_write_kb(self, max_write_kb):
        """
        Sets the limit on the total KB write during this operation, 0 means no
        application-defined limit. This value can only reduce the system defined
        limit. An attempt to increase the limit beyond the system defined limit
        will cause IllegalArgumentException.

        :param max_write_kb: the limit in terms of number of KB write during
            this operation.
        :type max_write_kb: int
        :return: self.
        :raises IllegalArgumentException: raises the exception if the
            max_write_kb value is less than 0 or beyond the system defined
            limit.
        """
        if not isinstance(max_write_kb, int):
            raise IllegalArgumentException(
                'set_max_write_kb requires integer as parameter.')
        elif max_write_kb < 0:
            raise IllegalArgumentException('max_write_kb must be >= 0')
        elif max_write_kb > serde.BinaryProtocol.WRITE_KB_LIMIT:
            raise IllegalArgumentException(
                'max_write_kb can not exceed ' +
                str(serde.BinaryProtocol.WRITE_KB_LIMIT))
        self.__max_write_kb = max_write_kb
        return self

    def get_max_write_kb(self):
        """
        Returns the limit on the total KB write during this operation. If not
        set by the application this value will be 0 which means the default
        system limit is used.

        :return: the limit, or 0 if not set.
        :rtype: int
        """
        return self.__max_write_kb

    def set_range(self, field_range):
        """
        Sets the :py:class:`FieldRange` to be used for the operation. This
        parameter is optional, but required to delete a specific range of rows.

        :param field_range: the field range.
        :type field_range: FieldRange
        :return: self.
        :raises IllegalArgumentException: raises the exception if field_range is
            not an instance of FieldRange.
        """
        if field_range is not None and not isinstance(field_range, FieldRange):
            raise IllegalArgumentException(
                'set_range requires an instance of FieldRange or None as ' +
                'parameter.')
        self.__range = field_range
        return self

    def get_range(self):
        """
        Returns the :py:class:`FieldRange` to be used for the operation if set.

        :return: the range, None if no range is to be used.
        :rtype: FieldRange
        """
        return self.__range

    def set_timeout(self, timeout_ms):
        """
        Sets the request timeout value, in milliseconds. This overrides any
        default value set in :py:class:`NoSQLHandleConfig`. The value must be
        positive.

        :param timeout_ms: the timeout value, in milliseconds.
        :type timeout_ms: int
        :return: self.
        :raises IllegalArgumentException: raises the exception if the timeout
            value is less than or equal to 0.
        """
        super(MultiDeleteRequest, self)._set_timeout_internal(timeout_ms)
        return self

    def get_timeout(self):
        """
        Returns the timeout to use for the operation, in milliseconds. A value
        of 0 indicates that the timeout has not been set.

        :return: the timeout value.
        :rtype: int
        """
        return super(MultiDeleteRequest, self)._get_timeout_internal()

    def validate(self):
        if self.__table_name is None:
            raise IllegalArgumentException(
                'MultiDeleteRequest requires table name.')
        if self.__key is None:
            raise IllegalArgumentException(
                'MultiDeleteRequest requires a key.')
        if self.__range is not None:
            self.__range.validate()

    def create_serializer(self):
        return serde.MultiDeleteRequestSerializer()

    def create_deserializer(self):
        return serde.MultiDeleteRequestSerializer(MultiDeleteResult)


class PrepareRequest(Request):
    """
    A request that encapsulates a query prepare call. Query preparation allows
    queries to be compiled (prepared) and reused, saving time and resources. Use
    of prepared queries vs direct execution of query strings is highly
    recommended.

    Prepared queries are implemented as :py:class:`PreparedStatement` which
    supports bind variables in queries which can be used to more easily reuse a
    query by parameterization.

    The statement is required parameter.
    """

    def __init__(self):
        super(PrepareRequest, self).__init__()
        self.__statement = None

    def set_statement(self, statement):
        """
        Sets the query statement.

        :param statement: the query statement.
        :type statement: str
        :return: self.
        :raises IllegalArgumentException: raises the exception if statement is
            not a string.
        """
        CheckValue.check_str(statement, 'statement')
        self.__statement = statement
        return self

    def get_statement(self):
        """
        Returns the query statement.

        :return: the statement, or None if it has not been set.
        :rtype: str
        """
        return self.__statement

    def set_timeout(self, timeout_ms):
        """
        Sets the request timeout value, in milliseconds. This overrides any
        default value set in :py:class:`NoSQLHandleConfig`. The value must be
        positive.

        :param timeout_ms: the timeout value, in milliseconds.
        :type timeout_ms: int
        :return: self.
        :raises IllegalArgumentException: raises the exception if the timeout
            value is less than or equal to 0.
        """
        super(PrepareRequest, self)._set_timeout_internal(timeout_ms)
        return self

    def get_timeout(self):
        """
        Returns the timeout to use for the operation, in milliseconds. A value
        of 0 indicates that the timeout has not been set.

        :return: the value.
        """
        return super(PrepareRequest, self)._get_timeout_internal()

    def validate(self):
        if self.__statement is None:
            raise IllegalArgumentException(
                'PrepareRequest requires a statement.')

    def create_serializer(self):
        return serde.PrepareRequestSerializer()

    def create_deserializer(self):
        return serde.PrepareRequestSerializer(PrepareResult)


class PutRequest(WriteRequest):
    """
    Represents the input to a :py:meth:`NoSQLHandle.put` operation.

    This request can be used to perform unconditional and conditional puts:

        Overwrite any existing row. This is the default.\n
        Succeed only if the row does not exist. Use PutOption.IF_ABSENT for this
        case.\n
        Succeed only if the row exists. Use PutOption.IF_PRESENT for this case.
        \n
        Succeed only if the row exists and its :py:class:`Version` matches a
        specific :py:class:`Version`. Use PutOption.IF_VERSION for this case and
        :py:meth:`set_match_version` to specify the version to match.

    Information about the existing row can be returned on failure of a put
    operation using PutOption.IF_VERSION or PutOption.IF_ABSENT by using
    :py:meth:`set_return_row`. Requesting this information incurs additional
    cost and may affect operation latency.

    On a successful operation the :py:class:`Version` returned by
    :py:meth:`PutResult.get_version` is non-none. Additional information, such
    as previous row information, may be available in :py:class:`PutResult`.

    The table name and value are required parameters.
    """

    def __init__(self):
        super(PutRequest, self).__init__()
        self.__value = None
        self.__option = None
        self.__match_version = None
        self.__ttl = None
        self.__update_ttl = False

    def __str__(self):
        return 'PutRequest'

    def set_value(self, value):
        """
        Sets the value to use for the put operation. This is a required
        parameter and must be set using this method or
        :py:meth:`set_value_from_json`

        :param value: the row value.
        :type value: dict
        :return: self.
        :raises IllegalArgumentException: raises the exception if value is not
            a dictionary.
        """
        CheckValue.check_dict(value, 'value')
        self.__value = value
        return self

    def set_value_from_json(self, json_value):
        """
        Sets the value to use for the put operation based on a JSON string. The
        string is parsed for validity and stored internally as a dict. This is
        a required parameter and must be set using this method or
        :py:meth:`set_value`

        :param json_value: the row value as a JSON string.
        :type json_value: str
        :return: self.
        :raises IllegalArgumentException: raises the exception if json_value is
            not a string.
        """
        CheckValue.check_str(json_value, 'json_value')
        self.__value = loads(json_value)
        return self

    def get_value(self):
        """
        Returns the value of the row to be used.

        :return: the value, or None if not set.
        :rtype: dict
        """
        return self.__value

    def set_option(self, option):
        """
        Sets the option for the put.

        :param option: the option to set.
        :type option: PutOption
        :return: self.
        """
        self.__option = option
        return self

    def get_option(self):
        """
        Returns the option specified for the put.

        :return: the option specified.
        :rtype: PutOption
        """
        return self.__option

    def set_match_version(self, version):
        """
        Sets the :py:class:`Version` to use for a conditional put operation.
        The Version is usually obtained from :py:meth:`GetResult.get_version` or
        other method that returns a Version. When set, the put operation will
        succeed only if the row exists and its Version matches the one
        specified. This condition exists to allow an application to ensure that
        it is updating a row in an atomic read-modify-write cycle. Using this
        mechanism incurs additional cost.

        :param version: the Version to match.
        :type version: Version
        :return: self.
        :raises IllegalArgumentException: raises the exception if version is not
            an instance of Version.
        """
        if not isinstance(version, Version):
            raise IllegalArgumentException('set_match_version requires an ' +
                                           'instance of Version as parameter.')
        if self.__option is None:
            self.__option = PutOption.IF_VERSION
        self.__match_version = version
        return self

    def get_match_version(self):
        """
        Returns the :py:class:`Version` used for a match on a conditional put.

        :return: the Version or None if not set.
        :rtype: Version
        """
        return self.__match_version

    def set_ttl(self, ttl):
        """
        Sets the :py:class:`TimeToLive` value, causing the time to live on the
        row to be set to the specified value on put. This value overrides any
        default time to live setting on the table.

        :param ttl: the time to live.
        :type ttl: TimeToLive
        :return: self.
        :raises IllegalArgumentException: raises the exception if ttl is not an
            instance of TimeToLive.
        """
        if ttl is not None and not isinstance(ttl, TimeToLive):
            raise IllegalArgumentException('set_ttl requires an instance of ' +
                                           'TimeToLive or None as parameter.')
        self.__ttl = ttl
        return self

    def get_ttl(self):
        """
        Returns the :py:class:`TimeToLive` value, if set.

        :return: the :py:class:`TimeToLive` if set, None otherwise.
        :rtype: TimeToLive
        """
        return self.__ttl

    def set_use_table_default_ttl(self, update_ttl):
        """
        If value is True, and there is an existing row, causes the operation
        to update the time to live (TTL) value of the row based on the Table's
        default TTL if set. If the table has no default TTL this state has no
        effect. By default updating an existing row has no effect on its TTL.

        :param update_ttl: True or False.
        :type update_ttl: bool
        :return: self.
        :raises IllegalArgumentException: raises the exception if update_ttl is
            not True or False.
        """
        CheckValue.check_boolean(update_ttl, 'update_ttl')
        self.__update_ttl = update_ttl
        return self

    def get_use_table_default_ttl(self):
        """
        Returns whether or not to update the row's time to live (TTL) based on a
        table default value if the row exists. By default updates of existing
        rows do not affect that row's TTL.

        :return: whether or not to update the row's TTL based on a table default
            value if the row exists.
        :rtype: bool
        """
        return self.__update_ttl

    def get_update_ttl(self):
        """
        Returns True if the operation should update the ttl.

        :return: True if the operation should update the ttl.
        :rtype: bool
        """
        return self.__update_ttl or self.__ttl is not None

    def set_timeout(self, timeout_ms):
        """
        Sets the request timeout value, in milliseconds. This overrides any
        default value set in :py:class:`NoSQLHandleConfig`. The value must be
        positive.

        :param timeout_ms: the timeout value, in milliseconds.
        :type timeout_ms: int
        :return: self.
        :raises IllegalArgumentException: raises the exception if the timeout
            value is less than or equal to 0.
        """
        super(PutRequest, self)._set_timeout_internal(timeout_ms)
        return self

    def get_timeout(self):
        """
        Returns the timeout to use for the operation, in milliseconds. A value
        of 0 indicates that the timeout has not been set.

        :return: the timeout value.
        :rtype: int
        """
        return super(PutRequest, self)._get_timeout_internal()

    def set_table_name(self, table_name):
        """
        Sets the table name to use for the operation.

        :param table_name: the table name.
        :type table_name: str
        :return: self.
        :raises IllegalArgumentException: raises the exception if table_name is
            not a string.
        """
        super(PutRequest, self).set_table_name_internal(table_name)
        return self

    def get_table_name(self):
        """
        Returns the table name for the operation.

        :return: the table name, or None if not set.
        :rtype: str
        """
        return super(PutRequest, self).get_table_name_internal()

    def set_return_row(self, return_row):
        """
        Sets whether information about the exist row should be returned on
        failure because of a version mismatch or failure of an "if absent"
        operation.

        :param return_row: set to True if information should be returned.
        :type return_row: bool
        :return: self.
        :raises IllegalArgumentException: raises the exception if return_row is
            not True or False.
        """
        super(PutRequest, self).set_return_row_internal(return_row)
        return self

    def get_return_row(self):
        """
        Returns whether information about the exist row should be returned on
        failure because of a version mismatch or failure of an "if absent"
        operation. If no option is set via :py:meth:`set_option` or the option
        is PutOption.IF_PRESENT the value of this parameter is ignored and there
        will not be any return information.

        :return: True if information should be returned.
        :rtype: bool
        """
        return super(PutRequest, self).get_return_row_internal()

    def validate(self):
        # Validates the state of the object when complete.
        super(PutRequest, self).validate_write_request('PutRequest')
        if self.__value is None:
            raise IllegalArgumentException('PutRequest requires a value')
        self.__validate_if_options()

    def __validate_if_options(self):
        # Ensures that only one of ifAbsent, ifPresent, or match_version is
        # set.
        if (self.__option == PutOption.IF_VERSION and
                self.__match_version is None):
            raise IllegalArgumentException(
                'PutRequest: match_version must be specified when ' +
                'PutOption.IF_VERSION is used.')
        if (self.__option != PutOption.IF_VERSION and
                self.__match_version is not None):
            raise IllegalArgumentException(
                'PutRequest: match_version is specified, the option is not ' +
                'PutOption.IF_VERSION.')
        if self.__update_ttl and self.__ttl is not None:
            raise IllegalArgumentException(
                'PutRequest: only one of set_use_table_default_ttl or set_ttl' +
                ' may be specified')

    def create_serializer(self):
        return serde.PutRequestSerializer()

    def create_deserializer(self):
        return serde.PutRequestSerializer(cls_result=PutResult)


class QueryRequest(Request):
    """
    A request that encapsulates a query. A query may be either a string query
    statement or a prepared query, which may include bind variables. A query
    request cannot have both a string statement and prepared query, but it must
    have one or the other.

    For performance reasons prepared queries are preferred for queries that may
    be reused. Prepared queries bypass compilation of the query. They also allow
    for parameterized queries using bind variables.

    The statement or prepared_statement is required parameter.
    """

    def __init__(self):
        super(QueryRequest, self).__init__()
        self.__limit = 0
        self.__max_read_kb = 0
        self.__continuation_key = None
        self.__consistency = None
        self.__statement = None
        self.__prepared_statement = None

    def set_limit(self, limit):
        """
        Sets the limit on number of items returned by the operation. This allows
        an operation to return less than the default amount of data.

        :param limit: the limit in terms of number of items returned.
        :type limit: int
        :return: self.
        :raises IllegalArgumentException: raises the exception if the limit is a
            negative number.
        """
        CheckValue.check_int_ge_zero(limit, 'limit')
        self.__limit = limit
        return self

    def get_limit(self):
        """
        Returns the limit on number of items returned by the operation. If not
        set by the application this value will be 0 which means no limit.

        :return: the limit, or 0 if not set.
        :rtype: int
        """
        return self.__limit

    def set_max_read_kb(self, max_read_kb):
        """
        Sets the limit on the total data read during this operation, in KB.
        This value can only reduce the system defined limit. An attempt to
        increase the limit beyond the system defined limit will cause
        IllegalArgumentException. This limit is independent of read units
        consumed by the operation.

        It is recommended that for tables with relatively low provisioned read
        throughput that this limit be reduced to less than or equal to one half
        of the provisioned throughput in order to avoid or reduce throttling
        exceptions.

        :param max_read_kb: the limit in terms of number of KB read during this
            operation.
        :type max_read_kb: int
        :return: self.
        :raises IllegalArgumentException: raises the exception if the maxReadKB
            value is less than 0 or beyond the system defined limit.
        :raises IllegalArgumentException: raises the exception if max_read_kb is
            a negative number or max_read_kb is greater than
            BinaryProtocol.READ_KB_LIMIT.
        """
        CheckValue.check_int_ge_zero(max_read_kb, 'max_read_kb')
        if max_read_kb > serde.BinaryProtocol.READ_KB_LIMIT:
            raise IllegalArgumentException(
                'max_read_kb can not exceed ' +
                str(serde.BinaryProtocol.READ_KB_LIMIT))
        self.__max_read_kb = max_read_kb
        return self

    def get_max_read_kb(self):
        """
        Returns the limit on the total data read during this operation, in KB.
        If not set by the application this value will be 0 which means no
        application-defined limit.

        :return: the limit, or 0 if not set.
        :rtype: int
        """
        return self.__max_read_kb

    def set_consistency(self, consistency):
        """
        Sets the consistency to use for the operation.

        :param consistency: the consistency.
        :type consistency: Consistency
        :return: self.
        :raises IllegalArgumentException: raises the exception if consistency
            is not Consistency.ABSOLUTE or Consistency.EVENTUAL.
        """
        if (consistency != Consistency.ABSOLUTE and
                consistency != Consistency.EVENTUAL):
            raise IllegalArgumentException(
                'set_consistency requires Consistency.ABSOLUTE or ' +
                'Consistency.EVENTUAL as parameter.')
        self.__consistency = consistency
        return self

    def get_consistency(self):
        """
        Returns the consistency set for this request, or None if not set.

        :return: the consistency
        :rtype: Consistency
        """
        return self.__consistency

    def set_continuation_key(self, continuation_key):
        """
        Sets the continuation key. This is used to continue an operation that
        returned this key in its :py:class:`QueryResult`.

        :param continuation_key: the key which should have been obtained from
            :py:meth:`QueryResult.get_continuation_key`.
        :type continuation_key: bytearray
        :return: self.
        :raises IllegalArgumentException: raises the exception if
            continuation_key is not a bytearray.
        """
        if not isinstance(continuation_key, bytearray):
            raise IllegalArgumentException(
                'set_continuation_key requires bytearray as parameter.')
        self.__continuation_key = continuation_key
        return self

    def get_continuation_key(self):
        """
        Returns the continuation key if set.

        :return: the key.
        :rtype: bytearray
        """
        return self.__continuation_key

    def set_statement(self, statement):
        """
        Sets the query statement.

        :param statement: the query statement.
        :type statement: str
        :return: self.
        :raises IllegalArgumentException: raises the exception if statement is
            not a string.
        """
        CheckValue.check_str(statement, 'statement')
        self.__statement = statement
        return self

    def get_statement(self):
        """
        Returns the query statement.

        :return: the statement, or None if it has not been set.
        :rtype: str
        """
        return self.__statement

    def set_prepared_statement(self, value):
        """
        Sets the prepared query statement.

        :param value: the prepared query statement or the result of a prepare
            request.
        :type value: PreparedStatement
        :return: self.
        :raises IllegalArgumentException: raises the exception if value is not
            an instance of PrepareResult or PreparedStatement.
        """
        if not (isinstance(value, PrepareResult) or
                isinstance(value, PreparedStatement)):
            raise IllegalArgumentException(
                'set_prepared_statement requires an instance of PrepareResult' +
                ' or PreparedStatement as parameter.')
        self.__prepared_statement = (
            value.get_prepared_statement() if isinstance(value, PrepareResult)
            else value)
        return self

    def get_prepared_statement(self):
        """
        Returns the prepared query statement.

        :return: the statement, or None if it has not been set.
        :rtype: PreparedStatement
        """
        return self.__prepared_statement

    def set_timeout(self, timeout_ms):
        """
        Sets the request timeout value, in milliseconds. This overrides any
        default value set in :py:class:`NoSQLHandleConfig`. The value must be
        positive.

        :param timeout_ms: the timeout value, in milliseconds.
        :type timeout_ms: int
        :return: self.
        :raises IllegalArgumentException: raises the exception if the timeout
            value is less than or equal to 0.
        """
        super(QueryRequest, self)._set_timeout_internal(timeout_ms)
        return self

    def get_timeout(self):
        """
        Returns the timeout to use for the operation, in milliseconds. A value
        of 0 indicates that the timeout has not been set.

        :return: the timeout value.
        :rtype: int
        """
        return super(QueryRequest, self)._get_timeout_internal()

    def set_defaults(self, cfg):
        super(QueryRequest, self).set_defaults(cfg)
        if self.__consistency is None:
            self.__consistency = cfg.get_default_consistency()
        return self

    def validate(self):
        if (self.__statement is not None and
                self.__prepared_statement is not None or
                self.__statement is None and
                self.__prepared_statement is None):
            raise IllegalArgumentException(
                'One of statement or prepared statement must be set.')

    def create_serializer(self):
        return serde.QueryRequestSerializer()

    def create_deserializer(self):
        return serde.QueryRequestSerializer(cls_result=QueryResult)


class TableRequest(Request):
    """
    TableRequest is used to create, modify, and drop tables. The operations
    allowed are those supported by the Data Definition Language (DDL) portion of
    the query language. The language provides for table creation and removal
    (drop), index add and drop, as well as schema evolution via alter table.
    Operations using DDL statements infer the table name from the query
    statement itself, e.g. "create table mytable(...)". Table creation requires
    a valid :py:class:`TableLimits` object to define the throughput desired for
    the table. If TableLimits is provided with any other type of query statement
    an exception is thrown.

    This request is also used to modify the limits of throughput and storage for
    an existing table. This case is handled by specifying a table name and
    limits without a query statement. If all three are specified it is an error.

    Execution of operations specified by this request is implicitly
    asynchronous. These are potentially long-running operations.
    :py:meth:`NoSQLHandle.table_request` returns a :py:class:`TableResult`
    instance that can be used to poll until the table reaches the desired state.

    The statement is required parameter.
    """

    def __init__(self):
        super(TableRequest, self).__init__()
        self.__statement = None
        self.__limits = None
        self.__table_name = None

    def __str__(self):
        return ('TableRequest: [name=' + str(self.__table_name) +
                ', statement=' + str(self.__statement) + ', limits=' +
                str(self.__limits))

    def set_statement(self, statement):
        """
        Sets the query statement to use for the operation. This parameter is
        required unless the operation is intended to change the limits of an
        existing table.

        :param statement: the statement.
        :return: self.
        :raises IllegalArgumentException: raises the exception if statement is
            not a string.
        """
        CheckValue.check_str(statement, 'statement')
        self.__statement = statement
        return self

    def get_statement(self):
        """
        Returns the statement, or None if not set.

        :return: the statement.
        :rtype: str
        """
        return self.__statement

    def set_table_limits(self, table_limits):
        """
        Sets the table limits to use for the operation. Limits are used in only
        2 cases -- table creation statements and limits modification operations.
        It is not used for other DDL operations.

        :param table_limits: the limits.
        :type table_limits: TableLimits
        :return: self.
        :raises IllegalArgumentException: raises the exception if table_limits
            is not an instance TableLimits.
        """
        if not isinstance(table_limits, TableLimits):
            raise IllegalArgumentException(
                'set_table_limits requires an instance of TableLimits as ' +
                'parameter.')
        self.__limits = table_limits
        return self

    def get_table_limits(self):
        """
        Returns the table limits, or None if not set.

        :return: the limits.
        :rtype: TableLimits
        """
        return self.__limits

    def set_table_name(self, table_name):
        """
        Sets the table name to use for the operation. The table name is only
        used to modify the limits of an existing table, and must not be set for
        any other operation.

        :param table_name: the name of the table.
        :type table_name: str
        :return: self.
        :raises IllegalArgumentException: raises the exception if table_name is
            not a string.
        """
        CheckValue.check_str(table_name, 'table_name')
        self.__table_name = table_name
        return self

    def get_table_name(self):
        """
        Returns the table name, or None if not set.

        :return: the table name.
        :rtype: str
        """
        return self.__table_name

    def set_timeout(self, timeout_ms):
        """
        Sets the request timeout value, in milliseconds. This overrides any
        default value set in :py:class:`NoSQLHandleConfig`. The value must be
        positive.

        :param timeout_ms: the timeout value, in milliseconds.
        :type timeout_ms: int
        :return: self.
        :raises IllegalArgumentException: raises the exception if the timeout
            value is less than or equal to 0.
        """
        super(TableRequest, self)._set_timeout_internal(timeout_ms)
        return self

    def get_timeout(self):
        """
        Returns the timeout to use for the operation, in milliseconds. A value
        of 0 indicates that the timeout has not been set.

        :return: the timeout value.
        :rtype: int
        """
        return super(TableRequest, self)._get_timeout_internal()

    def set_defaults(self, cfg):
        """
        Internal use only
        """
        if not isinstance(cfg, NoSQLHandleConfig):
            raise IllegalArgumentException(
                'set_defaults requires an instance of NoSQLHandleConfig as ' +
                'parameter.')
        if super(TableRequest, self)._get_timeout_internal() == 0:
            super(TableRequest, self)._set_timeout_internal(
                cfg.get_default_table_request_timeout())
        return self

    def should_retry(self):
        # Returns True if this request should be retried.
        return False

    def validate(self):
        if self.__statement is None and self.__table_name is None:
            raise IllegalArgumentException(
                'TableRequest requires statement or TableLimits and name.')
        if self.__statement is not None and self.__table_name is not None:
            raise IllegalArgumentException(
                'TableRequest cannot have both table name and statement.')

        if self.__limits is not None:
            self.__limits.validate()

    def create_serializer(self):
        return serde.TableRequestSerializer()

    def create_deserializer(self):
        return serde.TableRequestSerializer(TableResult)


class TableUsageRequest(Request):
    """
    Represents the argument of a :py:meth:`NoSQLHandle.get_table_usage`
    operation which returns dynamic information associated with a table, as
    returned in :py:class:`TableUsageResult`. This information includes a time
    series of usage snapshots, each indicating data such as read and write
    throughput, throttling events, etc, as found in
    :py:meth:`TableUsageResult.table_usage`.

    It is possible to return a range of usage records or, by default, only the
    most recent usage record. Usage records are created on a regular basis and
    maintained for a period of time. Only records for time periods that have
    completed are returned so that a user never sees changing data for a
    specific range.

    The table name is required parameter.
    """

    def __init__(self):
        super(TableUsageRequest, self).__init__()
        self.__table_name = None
        self.__start_time = 0
        self.__end_time = 0
        self.__limit = 0

    def set_table_name(self, table_name):
        """
        Sets the table name to use for the request. This is a required
        parameter.

        :param table_name: the table name.
        :type table_name: str
        :return: self.
        :raises IllegalArgumentException: raises the exception if table_name is
            not a string.
        """
        CheckValue.check_str(table_name, 'table_name')
        self.__table_name = table_name
        return self

    def get_table_name(self):
        """
        Gets the table name to use for the request.

        :return: the table name.
        :rtype: str
        """
        return self.__table_name

    def set_start_time(self, start_time):
        """
        Sets the start time to use for the request in milliseconds since the
        Epoch in UTC time or an ISO 8601 formatted string. If timezone is not
        specified it is interpreted as UTC.

        If no time range is set for this request the most
        recent complete usage record is returned.

        :param start_time: the start time.
        :type start_time: str
        :return: self.
        :raises IllegalArgumentException: raises the exception if start_time is
            a negative number and is not an ISO 8601 formatted string.
        """
        self.__check_time(start_time)
        if isinstance(start_time, str):
            start_time = self.__iso_time_to_timestamp(start_time)
        self.__start_time = start_time
        return self

    def get_start_time(self):
        """
        Returns the start time to use for the request in milliseconds since the
        Epoch.

        :return: the start time.
        :rtype: int
        """
        return self.__start_time

    def get_start_time_string(self):
        """
        Returns the start time as an ISO 8601 formatted string. If the start
        timestamp is not set, None is returned.

        :return: the start time, or None if not set.
        :rtype: str
        """

        if self.__start_time == 0:
            return None
        return datetime.fromtimestamp(
            float(self.__start_time) / 1000).isoformat()

    def set_end_time(self, end_time):
        """
        Sets the end time to use for the request in milliseconds since the Epoch
        in UTC time or an ISO 8601 formatted string. If timezone is not
        specified it is interpreted as UTC.

        If no time range is set for this request the most recent complete usage
        record is returned.

        :param end_time: the end time.
        :type end_time: str
        :return: self.
        :raises IllegalArgumentException: raises the exception if end_time is a
            negative number and is not an ISO 8601 formatted string.
        """
        self.__check_time(end_time)
        if isinstance(end_time, str):
            end_time = self.__iso_time_to_timestamp(end_time)
        self.__end_time = end_time
        return self

    def get_end_time(self):
        """
        Returns the end time to use for the request in milliseconds since the
        Epoch.

        :return: the end time.
        :rtype: int
        """
        return self.__end_time

    def get_end_time_string(self):
        """
        Returns the end time as an ISO 8601 formatted string. If the end
        timestamp is not set, None is returned.

        :return: the end time, or None if not set.
        :rtype: str
        """
        if self.__end_time == 0:
            return None
        return datetime.fromtimestamp(
            float(self.__end_time) / 1000).isoformat()

    def set_limit(self, limit):
        """
        Sets the limit to the number of usage records desired. If this value is
        0 there is no limit, but not all usage records may be returned in a
        single request due to size limitations.

        :param limit: the numeric limit.
        :type limit: int
        :return: self.
        :raises IllegalArgumentException: raises the exception if limit is a
            negative number.
        """
        CheckValue.check_int_ge_zero(limit, 'limit')
        self.__limit = limit
        return self

    def get_limit(self):
        """
        Returns the limit to the number of usage records desired.

        :return: the numeric limit.
        :rtype: int
        """
        return self.__limit

    def set_timeout(self, timeout_ms):
        """
        Sets the request timeout value, in milliseconds. This overrides any
        default value set in :py:class:`NoSQLHandleConfig`. The value must be
        positive.

        :param timeout_ms: the timeout value, in milliseconds.
        :type timeout_ms: int
        :return: self.
        :raises IllegalArgumentException: raises the exception if the timeout
            value is less than or equal to 0.
        """
        super(TableUsageRequest, self)._set_timeout_internal(timeout_ms)
        return self

    def get_timeout(self):
        """
        Returns the timeout to use for the operation, in milliseconds. A value
        of 0 indicates that the timeout has not been set.

        :return: the value.
        """
        return super(TableUsageRequest, self)._get_timeout_internal()

    def should_retry(self):
        # Returns True if this request should be retried.
        return False

    def validate(self):
        if self.__table_name is None:
            raise IllegalArgumentException(
                'TableUsageRequest requires a table name.')

    def create_serializer(self):
        return serde.TableUsageRequestSerializer()

    def create_deserializer(self):
        return serde.TableUsageRequestSerializer(TableUsageResult)

    def __check_time(self, dt):
        if (not (CheckValue.is_int(dt) or CheckValue.is_str(dt)) or
                CheckValue.is_int(dt) and dt < 0):
            raise IllegalArgumentException(
                'dt must be an integer that is not negative or an ISO ' +
                '8601 formatted string. Got:' + str(dt))

    def __iso_time_to_timestamp(self, dt):
        dt = datetime.strptime(dt, '%Y-%m-%dT%H:%M:%S.%f')
        return int(mktime(dt.timetuple()) * 1000)


class WriteMultipleRequest(Request):
    """
    Represents the input to a :py:meth:`NoSQLHandle.write_multiple` operation.

    This request can be used to perform a sequence of :py:class:`PutRequest` or
    :py:class:`DeleteRequest` operations associated with a table that share the
    same shard key portion of their primary keys, the WriteMultiple operation as
    whole is atomic. It is an efficient way to atomically modify multiple
    related rows.

    On a successful operation :py:meth:`WriteMultipleResult.get_success` returns
    True. The execution result of each operations can be retrieved using
    :py:meth:`WriteMultipleResult.get_results`.

    If the WriteMultiple operation is aborted because of the failure of an
    operation with abort_if_unsuccessful set to True, then
    :py:meth:`WriteMultipleResult.get_success` return False, the index of failed
    operation can be accessed using
    :py:meth:`WriteMultipleResult.get_failed_operation_index`, and the execution
    result of failed operation can be accessed using
    :py:meth:`WriteMultipleResult.get_failed_operation_result`.
    """

    def __init__(self):
        # Constructs an empty request.
        super(WriteMultipleRequest, self).__init__()
        self.__table_name = None
        self.__ops = list()

    def get_table_name(self):
        """
        Returns the tableName used for the operations.

        :return: the table name, or None if no operation.
        :rtype: str
        """
        return self.__table_name

    def add(self, request, abort_if_unsuccessful):
        """
        Adds a Request to the operation list, do validation check before adding
        it.

        :param request: the Request to add, either :py:class:`PutRequest` or
            :py:class:`DeleteRequest`.
        :type request: Request
        :param abort_if_unsuccessful: True if this operation should cause the
            entire WriteMultiple operation to abort when this operation fails.
        :type abort_if_unsuccessful: bool
        :return: self.
        :raises BatchOperationNumberLimitException: raises the exception if the
            number of requests exceeds the limit, or IllegalArgumentException if
            the request is neither a :py:class:`PutRequest` or
            :py:class:`DeleteRequest`. Or any invalid state of the Request.
        :raises IllegalArgumentException: raises the exception if parameters are
            not expected type.
        """
        if not isinstance(request, (PutRequest, DeleteRequest)):
            raise IllegalArgumentException(
                'Invalid request, requires an instance of PutRequest or ' +
                'DeleteRequest as parameter. Got: ' + str(request))
        CheckValue.check_boolean(abort_if_unsuccessful,
                                 'abort_if_unsuccessful')
        if len(self.__ops) == serde.BinaryProtocol.BATCH_OP_NUMBER_LIMIT:
            raise BatchOperationNumberLimitException(
                'The number of sub requests reached the max number of ' +
                str(serde.BinaryProtocol.BATCH_OP_NUMBER_LIMIT))
        if self.__table_name is None:
            self.__table_name = request.get_table_name_internal()
        else:
            if (request.get_table_name_internal().lower() !=
                    self.__table_name.lower()):
                raise IllegalArgumentException(
                    'The table_name used for the operation is different from ' +
                    'that of others: ' + self.__table_name)
        request.validate()
        self.__ops.append(self.OperationRequest(
            request, abort_if_unsuccessful))
        return self

    def get_request(self, index):
        """
        Returns the Request at the given position, it may be either a
        :py:class:`PutRequest` or :py:class:`DeleteRequest` object.

        :param index: the position of Request to get.
        :type index: int
        :return: the Request at the given position.
        :rtype: Request
        :raises IndexOutOfBoundsException: raises the exception if the position
            is negative or greater or equal to the number of Requests.
        :raises IllegalArgumentException: raises the exception if index is a
            negative number.
        """
        CheckValue.check_int_ge_zero(index, 'index')
        return self.__ops[index].get_request()

    def get_operations(self):
        # Returns the request lists, internal for now
        return self.__ops

    def get_num_operations(self):
        """
        Returns the number of Requests.

        :return: the number of Requests.
        :rtype: int
        """
        return len(self.__ops)

    def set_timeout(self, timeout_ms):
        """
        Sets the request timeout value, in milliseconds. This overrides any
        default value set in :py:class:`NoSQLHandleConfig`. The value must be
        positive.

        :param timeout_ms: the timeout value, in milliseconds.
        :type timeout_ms: int
        :return: self.
        :raises IllegalArgumentException: raises the exception if the timeout
            value is less than or equal to 0.
        """
        super(WriteMultipleRequest, self)._set_timeout_internal(timeout_ms)
        return self

    def get_timeout(self):
        """
        Returns the timeout to use for the operation, in milliseconds. A value
        of 0 indicates that the timeout has not been set.

        :return: the timeout value.
        :rtype: int
        """
        return super(WriteMultipleRequest, self)._get_timeout_internal()

    def clear(self):
        """
        Removes all of the operations from the WriteMultiple request.
        """
        self.__table_name = None
        self.__ops = list()

    def validate(self):
        if not self.__ops:
            raise IllegalArgumentException('The requests list is empty.')

    def create_serializer(self):
        return serde.WriteMultipleRequestSerializer()

    def create_deserializer(self):
        return serde.WriteMultipleRequestSerializer(
            WriteMultipleResult, OperationResult)

    class OperationRequest:
        # A wrapper of WriteRequest that contains an additional flag
        # abort_if_unsuccessful. Internal for now
        def __init__(self, request, abort_if_unsuccessful):
            self.__request = request
            self.__abort_if_unsuccessful = abort_if_unsuccessful

        def get_request(self):
            return self.__request

        def is_abort_if_unsuccessful(self):
            return self.__abort_if_unsuccessful


class Result(object):
    """
    Result is a base class for result classes for all supported operations.
    All state and methods are maintained by extending classes.
    """

    def __init__(self):
        """
        __read_units and __read_units will be different in the case of Absolute
        Consistency. _write_units and _write_kb will always be equal.
        """
        self.__read_kb = 0
        self.__read_units = 0
        self.__write_kb = 0

    def set_read_kb(self, read_kb):
        self.__read_kb = read_kb
        return self

    def _get_read_kb_internal(self):
        return self.__read_kb

    def set_read_units(self, read_units):
        self.__read_units = read_units
        return self

    def _get_read_units_internal(self):
        return self.__read_units

    def set_write_kb(self, write_kb):
        self.__write_kb = write_kb
        return self

    def _get_write_kb_internal(self):
        return self.__write_kb

    def _get_write_units_internal(self):
        return self.__write_kb


class WriteResult(Result):
    """
    A base class for results of single row modifying operations such as put and
    delete.
    """

    def __init__(self):
        super(WriteResult, self).__init__()
        self.__existing_version = None
        self.__existing_value = None

    def set_existing_version(self, existing_version):
        self.__existing_version = existing_version
        return self

    def get_existing_version_internal(self):
        return self.__existing_version

    def set_existing_value(self, existing_value):
        self.__existing_value = existing_value
        return self

    def get_existing_value_internal(self):
        return self.__existing_value


class DeleteResult(WriteResult):
    """
    Represents the result of a :py:meth:`NoSQLHandle.delete` operation.

    If the delete succeeded :py:meth:`get_success` returns True. Information
    about the existing row on failure may be available using
    :py:meth:`get_existing_value` and :py:meth:`get_existing_version`, depending
    on the use of :py:meth:`DeleteRequest.set_return_row`.
    """

    def __init__(self):
        super(DeleteResult, self).__init__()
        self.__success = False

    def __str__(self):
        return str(self.__success)

    def set_success(self, success):
        self.__success = success
        return self

    def get_success(self):
        """
        Returns True if the delete operation succeeded.

        :return: True if the operation succeeded.
        :rtype: bool
        """
        return self.__success

    def get_existing_value(self):
        """
        Returns the existing row value if available. It will be available if the
        target row exists and the operation failed because of a
        :py:class:`Version` mismatch and the corresponding
        :py:class:`DeleteRequest` the method
        :py:meth:`DeleteRequest.set_return_row` was called with a True value.
        :return: the value.
        :rtype: dict
        """
        return super(DeleteResult, self).get_existing_value_internal()

    def get_existing_version(self):
        """
        Returns the existing row :py:class:`Version` if available. It will be
        available if the target row exists and the operation failed because of a
        :py:class:`Version` mismatch and the corresponding
        :py:class:`DeleteRequest` the method
        :py:meth:`DeleteRequest.set_return_row` was called with a True value.
        :return: the version.
        :rtype Version
        """
        return super(DeleteResult, self).get_existing_version_internal()

    def get_read_kb(self):
        """
        Returns the read throughput consumed by this operation, in KBytes. This
        is the actual amount of data read by the operation. The number of read
        units consumed is returned by :py:meth:`get_read_units` which may be a
        larger number because this was an update operation.

        :return: the read KBytes consumed.
        :rtype: int
        """
        return super(DeleteResult, self)._get_read_kb_internal()

    def get_read_units(self):
        """
        Returns the read throughput consumed by this operation, in read units.
        This number may be larger than that returned by :py:meth:`get_read_kb`
        because it was an update operation.

        :return: the read units consumed.
        :rtype: int
        """
        return super(DeleteResult, self)._get_read_units_internal()

    def get_write_kb(self):
        """
        Returns the write throughput consumed by this operation, in KBytes.

        :return: the write KBytes consumed.
        :rtype: int
        """
        return super(DeleteResult, self)._get_write_kb_internal()

    def get_write_units(self):
        """
        Returns the write throughput consumed by this operation, in write units.

        :return: the write units consumed.
        :rtype: int
        """
        return super(DeleteResult, self)._get_write_units_internal()


class GetResult(Result):
    """
    Represents the result of a :py:meth:`NoSQLHandle.get` operation.

    On a successful operation the value of the row is available using
    :py:meth:`get_value` and the other state available in this class is valid.
    On failure that value is None and other state, other than consumed capacity,
    is undefined.
    """

    def __init__(self):
        super(GetResult, self).__init__()
        self.__value = None
        self.__version = None
        self.__expiration_time = 0

    def __str__(self):
        return 'None' if self.__value is None else str(self.__value)

    def set_value(self, value):
        # Sets the value of this object, internal.
        self.__value = value
        return self

    def get_value(self):
        """
        Returns the value of the returned row, or None if the row does not
        exist.

        :return: the value of the row, or None if it does not exist.
        :rtype: dict
        """
        return self.__value

    def set_version(self, version):
        # Sets the version, internal.
        self.__version = version
        return self

    def get_version(self):
        """
        Returns the :py:class:`Version` of the row if the operation was
        successful, or None if the row does not exist.

        :return: the version of the row, or None if the row does not exist.
        :rtype: Version
        """
        return self.__version

    def set_expiration_time(self, expiration_time):
        # Sets the expiration time, internal
        self.__expiration_time = expiration_time
        return self

    def get_expiration_time(self):
        """
        Returns the expiration time of the row. A zero value indicates that the
        row does not expire. This value is valid only if the operation
        successfully returned a row (:py:meth:`get_value` returns non-none).

        :return: the expiration time in milliseconds since January 1, 1970, or
            zero if the row never expires or the row does not exist.
        :rtype: int
        """
        return self.__expiration_time

    def get_read_kb(self):
        """
        Returns the read throughput consumed by this operation, in KBytes. This
        is the actual amount of data read by the operation. The number of read
        units consumed is returned by :py:meth:`get_read_units` which may be a
        larger number if the operation used Consistency.ABSOLUTE.

        :return: the read KBytes consumed.

        """
        return super(GetResult, self)._get_read_kb_internal()

    def get_read_units(self):
        """
        Returns the read throughput consumed by this operation, in read units.
        This number may be larger than that returned by :py:meth:`get_read_kb`
        if the operation used Consistency.ABSOLUTE.
        :return: the read units consumed.
        :rtype: int
        """
        return super(GetResult, self)._get_read_units_internal()

    def get_write_kb(self):
        """
        Returns the write throughput consumed by this operation, in KBytes.
        :return: the write KBytes consumed.
        :rtype: int
        """
        return super(GetResult, self)._get_write_kb_internal()

    def get_write_units(self):
        """
        Returns the write throughput consumed by this operation, in write units.
        :return: the write units consumed.
        :rtype: int
        """
        return super(GetResult, self)._get_write_units_internal()


class GetIndexesResult(Result):
    """
    Represents the result of a :py:meth:`NoSQLHandle.get_indexes` operation.

    On a successful operation the index information is returned in an array of
    IndexInfo.
    """

    def __init__(self):
        super(GetIndexesResult, self).__init__()
        self.__indexes = None

    def __str__(self):
        idxes = ''
        for index in range(len(self.__indexes)):
            idxes += str(self.__indexes[index])
            if index < len(self.__indexes) - 1:
                idxes += ','
        return '[' + idxes + ']'

    def set_indexes(self, indexes):
        self.__indexes = indexes
        return self

    def get_indexes(self):
        """
        Returns the list of index information returned by the operation.

        :return: the indexes information.
        :rtype: list(IndexInfo)
        """
        return self.__indexes


class ListTablesResult(Result):
    """
    Represents the result of a :py:meth:`NoSQLHandle.list_tables` operation.

    On a successful operation the table names are available as well as the
    index of the last returned table. Tables are returned in an array, sorted
    alphabetically.
    """

    def __init__(self):
        super(ListTablesResult, self).__init__()
        self.__tables = None
        self.__last_index_returned = 0

    def __str__(self):
        return '[' + ','.join(self.__tables) + ']'

    def set_tables(self, tables):
        self.__tables = tables
        return self

    def get_tables(self):
        """
        Returns the array of table names returned by the operation.

        :return: the table names.
        :rtype: list(str)
        """
        return self.__tables

    def set_last_index_returned(self, last_index_returned):
        self.__last_index_returned = last_index_returned
        return self

    def get_last_returned_index(self):
        """
        Returns the index of the last table name returned. This can be provided
        to :py:class:`ListTablesRequest` to be used as a starting point for
        listing tables.

        :return: the index.
        :rtype: int
        """
        return self.__last_index_returned


class MultiDeleteResult(Result):
    """
    Represents the result of a :py:meth:`NoSQLHandle.multi_delete` operation.

    On a successful operation the number of rows deleted is available using
    :py:meth:`get_num_deletions`. There is a limit to the amount of data
    consumed by a single call. If there are still more rows to delete, the
    continuation key can be get using :py:meth:`get_continuation_key`.
    """

    def __init__(self):
        super(MultiDeleteResult, self).__init__()
        self.__continuation_key = None
        self.__num_deleted = 0

    def __str__(self):
        return 'Deleted ' + str(self.__num_deleted) + ' rows.'

    def set_continuation_key(self, continuation_key):
        self.__continuation_key = continuation_key
        return self

    def get_continuation_key(self):
        """
        Returns the continuation key where the next MultiDelete request resume
        from.

        :return: the continuation key, or None if there are no more rows to
            delete.
        :rtype: bytearray
        """
        return self.__continuation_key

    def set_num_deletions(self, num_deleted):
        self.__num_deleted = num_deleted
        return self

    def get_num_deletions(self):
        """
        Returns the number of rows deleted from the table.

        :return: the number of rows deleted.
        :rtype: int
        """
        return self.__num_deleted

    def get_read_kb(self):
        """
        Returns the read throughput consumed by this operation, in KBytes. This
        is the actual amount of data read by the operation. The number of read
        units consumed is returned by :py:meth:`get_read_units` which may be a
        larger number because this was an update operation.

        :return: the read KBytes consumed.
        :rtype: int
        """
        return super(MultiDeleteResult, self)._get_read_kb_internal()

    def get_read_units(self):
        """
        Returns the read throughput consumed by this operation, in read units.
        This number may be larger than that returned by :py:meth:`get_read_kb`
        because it was an update operation.

        :return: the read units consumed.
        :rtype: int
        """
        return super(MultiDeleteResult, self)._get_read_units_internal()

    def get_write_kb(self):
        """
        Returns the write throughput consumed by this operation, in KBytes.

        :return: the write KBytes consumed.
        :rtype: int
        """
        return super(MultiDeleteResult, self)._get_write_kb_internal()

    def get_write_units(self):
        """
        Returns the write throughput consumed by this operation, in write units.

        :return: the write units consumed.
        :rtype: int
        """
        return super(MultiDeleteResult, self)._get_write_units_internal()


class PrepareResult(Result):
    """
    The result of a prepare operation. The returned
    :py:class:`PreparedStatement` can be re-used for query execution using
    :py:meth:`QueryRequest.set_prepared_statement`
    """

    def __init__(self):
        super(PrepareResult, self).__init__()
        self.__prepared_statement = None

    def set_prepared_statement(self, prepared_statement):
        # Sets the prepared statement.
        self.__prepared_statement = PreparedStatement(prepared_statement)
        return self

    def get_prepared_statement(self):
        """
        Returns the value of the prepared statement.

        :return: the value of the prepared statement.
        :rtype: PreparedStatement
        """
        return self.__prepared_statement

    def get_read_kb(self):
        """
        Returns the read throughput consumed by this operation, in KBytes. This
        is the actual amount of data read by the operation. The number of read
        units consumed is returned by :py:meth:`get_read_units` which may be a
        larger number if the operation used Consistency.ABSOLUTE.

        :return: the read KBytes consumed.
        :rtype: int
        """
        return super(PrepareResult, self)._get_read_kb_internal()

    def get_read_units(self):
        """
        Returns the read throughput consumed by this operation, in read units.
        This number may be larger than that returned by :py:meth:`get_read_kb`
        if the operation used Consistency.ABSOLUTE.

        :return: the read units consumed.
        :rtype: int
        """
        return super(PrepareResult, self)._get_read_units_internal()

    def get_write_kb(self):
        """
        Returns the write throughput consumed by this operation, in KBytes.

        :return: the write KBytes consumed.
        :rtype: int
        """
        return super(PrepareResult, self)._get_write_kb_internal()

    def get_write_units(self):
        """
        Returns the write throughput consumed by this operation, in write units.

        :return: the write units consumed.
        :rtype: int
        """
        return super(PrepareResult, self)._get_write_units_internal()


class PutResult(WriteResult):
    """
    Represents the result of a :py:meth:`NoSQLHandle.put` operation.

    On a successful operation the value returned by :py:meth:`get_version`
    is non-none. On failure that value is None. Information about the existing
    row on failure may be available using :py:meth:`get_existing_value` and
    :py:meth:`get_existing_version`, depending on the use of
    :py:meth:`PutRequest.set_return_row` and whether the put had an option set
    using :py:meth:`PutRequest.set_option`.
    """

    def __init__(self):
        super(PutResult, self).__init__()
        self.__version = None

    def __str__(self):
        return ('None Version' if self.__version is None else
                str(self.__version))

    def set_version(self, version):
        self.__version = version
        return self

    def get_version(self):
        """
        Returns the :py:class:`Version` of the new row if the operation was
        successful. If the operation failed None is returned.

        :return: the :py:class:`Version` on success, None on failure.
        :rtype: Version
        """
        return self.__version

    def get_existing_version(self):
        """
        Returns the existing row :py:class:`Version` if available. This value
        will only be available if the conditional put operation failed and the
        request specified that return information be returned using
        :py:meth:`PutRequest.set_return_row`.

        :return: the :py:class:`Version`.
        :rtype: Version
        """
        return super(PutResult, self).get_existing_version_internal()

    def get_existing_value(self):
        """
        Returns the existing row value if available. This value will only be
        available if the conditional put operation failed and the request
        specified that return information be returned using
        :py:meth:`PutRequest.set_return_row`.

        :return: the value.
        :rtype: dict
        """
        return super(PutResult, self).get_existing_value_internal()

    def get_read_kb(self):
        """
        Returns the read throughput consumed by this operation, in KBytes.
        This is the actual amount of data read by the operation. The number of
        read units consumed is returned by :py:meth:`get_read_units` which may
        be a larger number because this was an update operation.

        :return: the read KBytes consumed.
        :rtype: int
        """
        return super(PutResult, self)._get_read_kb_internal()

    def get_read_units(self):
        """
        Returns the read throughput consumed by this operation, in read units.
        This number may be larger than that returned by :py:meth:`get_read_kb`
        because it was an update operation.

        :return: the read units consumed.
        :rtype: int
        """
        return super(PutResult, self)._get_read_units_internal()

    def get_write_kb(self):
        """
        Returns the write throughput consumed by this operation, in KBytes.

        :return: the write KBytes consumed.
        :rtype: int
        """
        return super(PutResult, self)._get_write_kb_internal()

    def get_write_units(self):
        """
        Returns the write throughput consumed by this operation, in write units.

        :return: the write units consumed.
        :rtype: int
        """
        return super(PutResult, self)._get_write_units_internal()


class QueryResult(Result):
    """
    QueryResult comprises a list of dict instances representing the query
    results.

    The shape of the values is based on the schema implied by the query. For
    example a query such as "SELECT * FROM ..." that returns an intact row will
    return values that conform to the schema of the table. Projections return
    instances that conform to the schema implied by the statement. UPDATE
    queries either return values based on a RETURNING clause or, by default, the
    number of rows affected by the statement.

    If the value returned by :py:meth:`get_continuation_key` is not None there
    are additional results available. That value can be supplied to a new
    request using :py:meth:`QueryRequest.set_continuation_key` to continue the
    query. It is possible for a query to return no results in an empty list but
    still have a non-none continuation key. This happens if the query reads the
    maximum amount of data allowed in a single request without matching a query
    predicate. In this case, the continuation key must be used to get results,
    if any exist.
    """

    def __init__(self):
        super(QueryResult, self).__init__()
        self.__results = None
        self.__continuation_key = None

    def __str__(self):
        return 'Query, num results: ' + str(len(self.__results))

    def set_results(self, results):
        self.__results = results
        return self

    def get_results(self):
        """
        Returns a list of results for the query. It is possible to have an empty
        list and a non-none continuation key.

        :return: a list of results for the query.
        :rtype: list(dict)
        """
        return self.__results

    def set_continuation_key(self, continuation_key):
        self.__continuation_key = continuation_key
        return self

    def get_continuation_key(self):
        """
        Returns the continuation key that can be used to obtain more results
        if non-none.

        :return: the continuation key, or None if there are no further values
            to return.
        :rtype: bytearray
        """
        return self.__continuation_key

    def get_read_kb(self):
        """
        Returns the read throughput consumed by this operation, in KBytes. This
        is the actual amount of data read by the operation. The number of read
        units consumed is returned by :py:meth:`get_read_units` which may be a
        larger number if the operation used Consistency.ABSOLUTE.

        :return: the read KBytes consumed.
        :rtype: int
        """
        return super(QueryResult, self)._get_read_kb_internal()

    def get_read_units(self):
        """
        Returns the read throughput consumed by this operation, in read units.
        This number may be larger than that returned by :py:meth:`get_read_kb`
        if the operation used Consistency.ABSOLUTE.

        :return: the read units consumed.
        :rtype: int
        """
        return super(QueryResult, self)._get_read_units_internal()

    def get_write_kb(self):
        """
        Returns the write throughput consumed by this operation, in KBytes.

        :return: the write KBytes consumed.
        :rtype: int
        """
        return super(QueryResult, self)._get_write_kb_internal()

    def get_write_units(self):
        """
        Returns the write throughput consumed by this operation, in write units.

        :return: the write units consumed.
        :rtype: int
        """
        return super(QueryResult, self)._get_write_units_internal()


class TableResult(Result):
    """
    TableResult is returned from :py:meth:`NoSQLHandle.get_table` and
    :py:meth:`NoSQLHandle.table_request` operations. It encapsulates the state
    of the table that is the target of the request.

    Operations available in :py:meth:`NoSQLHandle.table_request` such as table
    creation, modification, and drop are asynchronous operations. When such an
    operation has been performed it is necessary to call
    :py:meth:`NoSQLHandle.get_table` until the status of the table is
    State.ACTIVE or there is an error condition. The method
    :py:meth:`wait_for_state` exists to perform this task and should be used
    whenever possible.

    :py:meth:`NoSQLHandle.get_table` is synchronous, returning static
    information about the table as well as its current state.
    """

    def __init__(self):
        super(TableResult, self).__init__()
        self.__table_name = None
        self.__state = None
        self.__limits = None
        self.__schema = None
        self.__operation_id = None

    def __str__(self):
        return ('table ' + str(self.__table_name) + '[' + str(self.__state) +
                '] ' + str(self.__limits) + ' schema [' + str(self.__schema) +
                '] operation_id = ' + str(self.__operation_id))

    def set_table_name(self, table_name):
        self.__table_name = table_name
        return self

    def get_table_name(self):
        """
        Returns the table name of the target table.

        :return: the table name.
        :rtype: str
        """
        return self.__table_name

    def set_state(self, state):
        self.__state = state
        return self

    def get_state(self):
        """
        Returns the table state. A table in state State.ACTIVE or State.UPDATING
        is usable for normal operation.

        :return: the state.
        :rtype: State
        """
        return self.__state

    def set_table_limits(self, limits):
        self.__limits = limits
        return self

    def get_table_limits(self):
        """
        Returns the throughput and capacity limits for the table.

        :return: the limits.
        :rtype: TableLimits
        """
        return self.__limits

    def set_schema(self, schema):
        self.__schema = schema
        return self

    def get_schema(self):
        """
        Returns the schema for the table.

        :return: the schema for the table.
        :rtype: str
        """
        return self.__schema

    def set_operation_id(self, operation_id):
        self.__operation_id = operation_id
        return self

    def get_operation_id(self):
        """
        Returns the operation id for an asynchronous operation. This is none if
        the request did not generate a new operation. The value can be used in
        :py:meth:`set_operation_id` to find potential errors resulting from the
        operation.

        :return: the operation id for an asynchronous operation.
        :rtype: int
        """
        return self.__operation_id

    def wait_for_state_with_res(self, handle, state, wait_millis, delay_millis,
                                operation_id=None):
        """
        Waits for the specified table to reach the desired state. This is a
        blocking, polling style wait that delays for the specified number of
        milliseconds between each polling operation. The state of State.DROPPED
        is treated specially in that it will be returned as success, even if the
        table does not exist. Other states will throw an exception if the table
        is not found.

        :param handle: the NoSQLHandle to use.
        :type handle: NoSQLHandle
        :param state: the desired state.
        :type state: State
        :param wait_millis: the total amount of time to wait, in milliseconds.
            This value must be non-zero and greater than delay_millis.
        :type wait_millis: int
        :param delay_millis: the amount of time to wait between polling
            attempts, in milliseconds. If 0 it will default to 500.
        :type delay_millis: int
        :param operation_id: optional operation id.
        :type operation_id: int
        :return: the TableResult representing the table at the desired state.
        :rtype: TableResult
        :raises IllegalArgumentException: raises the exception if the operation
            times out or the parameters are not valid.
        :raises NoSQLException: raises the exception if the operation id used is
            not None that the operation has failed for some reason.
        """
        return self.wait_for_state(handle, self.get_table_name(), state,
                                   wait_millis, delay_millis,
                                   operation_id)

    @staticmethod
    def wait_for_state(handle, table_name, state, wait_millis, delay_millis,
                       operation_id=None):
        """
        Waits for the specified table to reach the desired state. This is a
        blocking, polling style wait that delays for the specified number of
        milliseconds between each polling operation. The state of State.DROPPED
        is treated specially in that it will be returned as success, even if the
        table does not exist. Other states will throw an exception if the table
        is not found.

        :param handle: the NoSQLHandle to use.
        :type handle: NoSQLHandle
        :param table_name: the table name.
        :type table_name: str
        :param state: the desired state.
        :type state: State
        :param wait_millis: the total amount of time to wait, in milliseconds.
            This value must be non-zero and greater than delay_millis.
        :type wait_millis: int
        :param delay_millis: the amount of time to wait between polling
            attempts, in milliseconds. If 0 it will default to 500.
        :type delay_millis: int
        :param operation_id: optional operation id.
        :type operation_id: int
        :return: the TableResult representing the table at the desired state.
        :rtype: TableResult
        :raises IllegalArgumentException: raises the exception if the operation
            times out or the parameters are not valid.
        :raises NoSQLException: raises the exception if the operation id used is
            not None that the operation has failed for some reason.
        """
        default_delay = 500
        delay_ms = delay_millis if delay_millis != 0 else default_delay
        if wait_millis < delay_millis:
            raise IllegalArgumentException(
                'Wait milliseconds must be a minimum of ' + str(default_delay) +
                ' and greater than delay milliseconds')
        start_time = int(round(time() * 1000))
        delay_s = delay_ms // 1000
        if delay_s == 0:
            delay_s = 1
        get_table = GetTableRequest().set_table_name(
            table_name).set_operation_id(operation_id)
        res = None
        while True:
            cur_time = int(round(time() * 1000))
            if cur_time - start_time > wait_millis:
                raise RequestTimeoutException(
                    'Expected state for table ' + table_name + ' not reached.',
                    wait_millis)
            # delay.
            try:
                if res is not None:
                    # only delay after the first get_table.
                    sleep(delay_s)
                res = handle.get_table(get_table)
                # If using operation_id, re-acquire from the current result. It
                # can change.
                if operation_id is not None:
                    get_table.set_operation_id(res.get_operation_id())
            except TableNotFoundException as tnf:
                # table not found is == DROPPED.
                if state == State.DROPPED:
                    return TableResult().set_table_name(table_name).set_state(
                        State.DROPPED)
                raise tnf
            if state == res.get_state():
                break
        return res


class TableUsageResult(Result):
    """
    TableUsageResult is returned from :py:meth:`NoSQLHandle.get_table_usage`.
    It encapsulates the dynamic state of the requested table.
    """

    def __init__(self):
        super(TableUsageResult, self).__init__()
        self.__table_name = None
        self.__usage_records = None

    def __str__(self):
        if self.__usage_records is None:
            records_str = 'None'
        else:
            records_str = ''
            for index in range(len(self.__usage_records)):
                records_str += str(self.__usage_records[index])
                if index < len(self.__usage_records) - 1:
                    records_str += ', '
        return ('TableUsageResult [table=' + str(self.__table_name) +
                '] [table_usage=[' + records_str + ']]')

    def set_table_name(self, table_name):
        self.__table_name = table_name
        return self

    def get_table_name(self):
        """
        Returns the table name used by the operation.

        :return: the table name.
        :rtype: str
        """
        return self.__table_name

    def set_usage_records(self, records):
        self.__usage_records = records
        return self

    def get_usage_records(self):
        """
        Returns a list of usage records based on the parameters of the
        :py:class:`TableUsageRequest` used.

        :return: an list of usage records.
        :type: list(TableUsage)
        """
        return self.__usage_records


class WriteMultipleResult(Result):
    """
    Represents the result of a :py:meth:`NoSQLHandle.write_multiple` operation.

    If the WriteMultiple succeeds, the execution result of each sub operation
    can be retrieved using :py:meth:`get_results`.

    If the WriteMultiple operation is aborted because of the failure of an
    operation with abort_if_unsuccessful set to True, then the index of failed
    operation can be accessed using :py:meth:`get_failed_operation_index`, and
    the execution result of failed operation can be accessed using
    :py:meth:`get_failed_operation_result`.
    """

    def __init__(self):
        super(WriteMultipleResult, self).__init__()
        self.__results = list()
        self.__failed_operation_index = -1

    def __str__(self):
        if self.get_success():
            return 'WriteMultiple, num results: ' + str(len(self.__results))
        return ('WriteMultiple aborted, the failed operation index: ' +
                str(self.__failed_operation_index))

    def add_result(self, result):
        self.__results.append(result)

    def get_results(self):
        """
        Returns the list of execution results for the operations.

        :return: the list of execution results.
        :rtype: list(OperationResult)
        """
        return self.__results

    def get_failed_operation_result(self):
        """
        Returns the result of the operation that results in the entire
        WriteMultiple operation aborting.

        :return: the result of the operation, None if not set.
        """
        if self.__failed_operation_index == -1 or not self.__results:
            return None
        return self.__results[0]

    def set_failed_operation_index(self, index):
        self.__failed_operation_index = index

    def get_failed_operation_index(self):
        """
        Returns the index of failed operation that results in the entire
        WriteMultiple operation aborting.

        :return: the index of operation, -1 if not set.
        :rtype: int
        """
        return self.__failed_operation_index

    def get_success(self):
        """
        Returns True if the WriteMultiple operation succeeded, or False if the
        operation is aborted due to the failure of a sub operation.

        The failed operation index can be accessed using
        :py:meth:`get_failed_operation_index` and its result can be accessed
        using :py:meth:`get_failed_operation_result`.

        :return: True if the operation succeeded.
        :rtype: bool
        """
        return self.__failed_operation_index == -1

    def size(self):
        """
        Returns the number of results.

        :return: the number of results.
        """
        return len(self.__results)

    def get_read_kb(self):
        """
        Returns the read throughput consumed by this operation, in KBytes. This
        is the actual amount of data read by the operation. The number of read
        units consumed is returned by :py:meth:`get_read_units` which may be a
        larger number because this was an update operation.

        :return: the read KBytes consumed.
        :rtype: int
        """
        return super(WriteMultipleResult, self)._get_read_kb_internal()

    def get_read_units(self):
        """
        Returns the read throughput consumed by this operation, in read units.
        This number may be larger than that returned by :py:meth:`get_read_kb`
        because it was an update operation.

        :return: the read units consumed.
        :rtype: int
        """
        return super(WriteMultipleResult, self)._get_read_units_internal()

    def get_write_kb(self):
        """
        Returns the write throughput consumed by this operation, in KBytes.

        :return: the write KBytes consumed.
        :rtype: int
        """
        return super(WriteMultipleResult, self)._get_write_kb_internal()

    def get_write_units(self):
        """
        Returns the write throughput consumed by this operation, in write units.

        :return: the write units consumed.
        :rtype: int
        """
        return super(WriteMultipleResult, self)._get_write_units_internal()


class OperationResult(WriteResult):
    """
    A single Result associated with the execution of an individual operation in
    a :py:meth:`NoSQLHandle.write_multiple` request. A list of OperationResult
    is contained in :py:meth:`WriteMultipleResult` and obtained using
    :py:meth:`WriteMultipleResult.get_results`.
    """

    def __init__(self):
        super(OperationResult, self).__init__()
        self.__version = None
        self.__success = False

    def __str__(self):
        return ('Success: ' + str(self.__success) + ', version: ' +
                str(self.__version) + ', existing version: ' +
                str(self.get_existing_version()) + ', existing value: ' +
                str(self.get_existing_value()))

    def set_version(self, version):
        self.__version = version
        return self

    def get_version(self):
        """
        Returns the version of the new row for put operation, or None if put
        operations did not succeed or the operation is delete operation.

        :return: the version.
        :rtype: Version
        """
        return self.__version

    def set_success(self, success):
        self.__success = success
        return self

    def get_success(self):
        """
        Returns the flag indicates whether the operation succeeded. A put or
        delete operation may be unsuccessful if the condition is not
        matched.

        :return: True if the operation succeeded.
        :rtype: bool
        """
        return self.__success

    def get_existing_version(self):
        """
        Returns the existing row version associated with the key if
        available.

        :return: the existing row version associated with the key if
            available.
        :rtype: Version
        """
        return super(OperationResult, self).get_existing_version_internal()

    def get_existing_value(self):
        """
        Returns the previous row value associated with the key if available.

        :return: the previous row value associated with the key if
            available.
        :rtype: dict
        """
        return super(OperationResult, self).get_existing_value_internal()
