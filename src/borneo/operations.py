#
# Copyright (c) 2018, 2022 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#
from abc import abstractmethod
from datetime import datetime
from dateutil import parser, tz
from decimal import Context, ROUND_HALF_EVEN
from json import loads
from time import mktime, sleep, time

from .common import (
    CheckValue, Consistency, Durability, FieldRange, PreparedStatement,
    PutOption, State, SystemState, TableLimits, TimeToLive, Version,
    deprecated)
from .exception import (
    IllegalArgumentException, RequestTimeoutException)
from .http import RateLimiter
from .serde import (
    BinaryProtocol, DeleteRequestSerializer, GetIndexesRequestSerializer,
    GetRequestSerializer, GetTableRequestSerializer,
    ListTablesRequestSerializer, MultiDeleteRequestSerializer,
    PrepareRequestSerializer, PutRequestSerializer, QueryRequestSerializer,
    SystemRequestSerializer, SystemStatusRequestSerializer,
    TableRequestSerializer, TableUsageRequestSerializer,
    WriteMultipleRequestSerializer)
try:
    from . import config
except ImportError:
    import config


class Request(object):
    """
    A request is a class used as a base for all requests types. Public state and
    methods are implemented by extending classes.
    """

    def __init__(self):
        self._check_request_size = True
        # Cloud service only.
        self._compartment = None
        self._read_rate_limiter = None
        self._retry_stats = None
        self._start_time_ms = 0
        self._table_name = None
        self._timeout_ms = 0
        self._write_rate_limiter = None
        self._rate_limit_delayed_ms = 0

    def add_retry_delay_ms(self, millis):
        """
        Internal use only.

        This adds time to the total time spent processing retries during a
        single request processing operation.

        :param millis: millis time to add to retry delay value
        :type millis: int
        """
        if self._retry_stats is None:
            self._retry_stats = RetryStats()
        self._retry_stats.add_delay_ms(millis)

    def add_retry_exception(self, re):
        """
        Internal use only.

        This adds (or increments) a class type to the list of exceptions that
        were processed during retries of a single request operation.

        :param re: class of exception to add to retry stats.
        :type re: Exception
        """
        if self._retry_stats is None:
            self._retry_stats = RetryStats()
        self._retry_stats.add_exception(re)

    def does_reads(self):
        """
        Internal use only.

        :returns: True if the request expects to do reads (incur read units).
        :rtype: boolean
        """
        return False

    def does_writes(self):
        """
        Internal use only.

        :returns: True if the request expects to do writes (incur write units).
        :rtype: boolean
        """
        return False

    def get_check_request_size(self):
        # Internal use only.
        return self._check_request_size

    def get_compartment(self):
        """
        Cloud service only.

        Get the compartment id or name if set for the request.

        :returns: compartment id or name if set for the request, otherwise None
            if not set.
        :rtype: str
        """
        return self._compartment

    def get_num_retries(self):
        """
        Internal use only.

        :returns: number of retries.
        :rtype: int
        """
        if self._retry_stats is None:
            return 0
        return self._retry_stats.get_retries()

    def get_read_rate_limiter(self):
        """
        Cloud service only.

        Returns the read rate limiter instance used during this request.

        This will be the value supplied via :py:meth:`set_read_rate_limiter`, or
        if that was not called, it may be an instance of an internal rate
        limiter that was configured internally during request processing.

        This is supplied for stats and tracing/debugging only. The returned
        limiter should be treated as read-only.

        :returns: the rate limiter instance used for read operations, or None if
            no limiter was used.
        :rtype: RateLimiter
        """
        return self._read_rate_limiter

    def get_retry_delay_ms(self):
        """
        Internal use only.

        :returns: the time spent in retries, in milliseconds.
        :rtype: int
        """
        if self._retry_stats is None:
            return 0
        return self._retry_stats.get_delay_ms()

    def get_retry_stats(self):
        """
        Returns a stats object with information about retries. This may be used
        during a retry handler or after a request has completed or thrown an
        exception.

        :returns: stats object with retry information, or None if no retries
        were performed.
        :rtype: RetryStats
        """
        return self._retry_stats

    def get_start_time_ms(self):
        """
        Internal use only.

        :returns: the start time of request processing.
        :returns: int
        """
        return self._start_time_ms

    def get_table_name(self):
        """
        Returns the table name to use for the operation.

        :returns: the table name, or None if not set.
        :returns: str
        """
        return self._table_name

    def get_timeout(self):
        return self._timeout_ms

    def get_write_rate_limiter(self):
        """
        Cloud service only.

        Returns the write rate limiter instance used during this request.

        This will be the value supplied via :py:meth:`set_write_rate_limiter`,
        or if that was not called, it may be an instance of an internal rate
        limiter that was configured internally during request processing.

        This is supplied for stats and tracing/debugging only. The returned
        limiter should be treated as read-only.

        :returns: the rate limiter instance used for write operations, or None
            if no limiter was used.
        :rtype: RateLimiter
        """
        return self._write_rate_limiter

    def is_query_request(self):
        return False

    def increment_retries(self):
        """
        Internal use only.

        Increments the number of retries during the request operation.
        """
        if self._retry_stats is None:
            self._retry_stats = RetryStats()
        self._retry_stats.increment_retries()

    def set_check_request_size(self, check_request_size):
        # Internal use only.
        self._check_request_size = check_request_size
        return self

    def set_compartment_internal(self, compartment):
        """
        Internal use only.

        Sets the compartment id or name to use for the operation.

        :param compartment: the compartment name or id.
        :type compartment: str or None.
        :raises IllegalArgumentException: raises the exception if compartment
            is not a str or None.
        """
        CheckValue.check_str(compartment, 'compartment', True)
        self._compartment = compartment

    def set_defaults(self, cfg):
        """
        Internal use only.

        Sets default values in a request based on the specified config object.
        This will typically be overridden by subclasses.

        :param cfg: the configuration object to use to get default values.
        :type cfg: NoSQLHandleConfig
        :returns: self.
        :raises IllegalArgumentException: raises the exception if cfg is not an
            instance of NoSQLHandleConfig.
        """
        self._check_config(cfg)
        if self._timeout_ms == 0:
            self._timeout_ms = cfg.get_default_timeout()
        return self

    def set_read_rate_limiter(self, rate_limiter):
        """
        Cloud service only.

        Sets a read rate limiter to use for this request.

        This will override any internal rate limiter that may have otherwise
        been used during request processing, and it will be used regardless of
        any rate limiter config.

        :param rate_limiter: the rate limiter instance to use for read
            operations.
        :type rate_limiter: RateLimiter
        :returns: self.
        :raises IllegalArgumentException: raises the exception if rate_limiter
            is not an instance of RateLimiter.
        """
        self._check_rate_limiter(rate_limiter, 'set_read_rate_limiter')
        self._read_rate_limiter = rate_limiter
        return self

    def set_retry_stats(self, retry_stats):
        """
        Internal use only.

        This is typically set by internal request processing when the first
        retry is attempted. It is used/updated thereafter on subsequent retry
        attempts.

        :param retry_stats: the stats object to use.
        :type retry_stats: RetryStats
        :returns: self.
        :raises IllegalArgumentException: raises the exception if retry_stats is
            not an instance of RetryStats.
        """
        if retry_stats is not None and not isinstance(retry_stats, RetryStats):
            raise IllegalArgumentException(
                'set_retry_stats requires an instance of RetryStats as ' +
                'parameter.')
        self._retry_stats = retry_stats
        return self

    def set_start_time_ms(self, start_time_ms):
        """
        Internal use only.

        :param start_time_ms: the start time of request processing.
        :type start_time_ms: int
        """
        self._start_time_ms = start_time_ms

    def set_write_rate_limiter(self, rate_limiter):
        """
        Cloud service only.

        Sets a write rate limiter to use for this request.

        This will override any internal rate limiter that may have otherwise
        been used during request processing, and it will be used regardless of
        any rate limiter config.

        :param rate_limiter: the rate limiter instance to use for write
            operations.
        :type rate_limiter: RateLimiter
        :returns: self.
        :raises IllegalArgumentException: raises the exception if rate_limiter
            is not an instance of RateLimiter.
        """
        self._check_rate_limiter(rate_limiter, 'set_write_rate_limiter')
        self._write_rate_limiter = rate_limiter
        return self

    def should_retry(self):
        # Returns True if this request should be retried.
        return True

    def set_table_name(self, table_name):
        """
        Internal use only.

        Sets the table name to use for the operation.

        :param table_name: the table name.
        :type table_name: str or None
        :raises IllegalArgumentException: raises the exception if table_name is
            not a string.
        """
        CheckValue.check_str(table_name, 'table_name', True)
        self._table_name = table_name

    def _set_timeout(self, timeout_ms):
        CheckValue.check_int_gt_zero(timeout_ms, 'timeout_ms')
        self._timeout_ms = timeout_ms

    def get_rate_limit_delayed_ms(self):
        """
        Get the time the operation was delayed due to rate limiting. Cloud only.
        If rate limiting is in place, this value will represent the number of
        milliseconds that the operation was delayed due to rate limiting. If the
        value is zero, rate limiting did not apply or the operation did not need
        to wait for rate limiting.

        :returns: delay time in milliseconds.
        """
        return self._rate_limit_delayed_ms

    def set_rate_limit_delayed_ms(self, delay_ms):
        """
        Set the time the operation was delayed due to rate limiting.
        :param delay_ms: the delay in milliseconds.
        :type delay_ms: int
        :returns: self.
        """
        self._rate_limit_delayed_ms = delay_ms
        return self

    @staticmethod
    def _check_config(cfg):
        if not isinstance(cfg, config.NoSQLHandleConfig):
            raise IllegalArgumentException(
                'set_defaults requires an instance of NoSQLHandleConfig as ' +
                'parameter.')

    @staticmethod
    def _check_rate_limiter(rate_limiter, name):
        if (rate_limiter is not None and
                not isinstance(rate_limiter, RateLimiter)):
            raise IllegalArgumentException(
                name + ' requires an instance of RateLimiter as parameter.')

    @abstractmethod
    def get_request_name(self):
        # type () -> str
        pass


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
        self._return_row = False
        self._durability = None

    def __str__(self):
        return 'WriteRequest'

    def __str__(self):
        return 'WriteRequest'

    def does_writes(self):
        return True

    def _set_return_row(self, return_row):
        CheckValue.check_boolean(return_row, 'return_row')
        self._return_row = return_row

    def _get_return_row(self):
        return self._return_row

    def _set_durability(self, dur):
        if dur is None:
            self._durability = None
            return
        if not isinstance(dur, Durability):
            raise IllegalArgumentException('set_durability requires an ' +
                                           'instance of Durability as parameter.')
        dur.validate()
        self._durability = dur

    def _get_durability(self):
        return self._durability

    def _validate_write_request(self, request_name):
        if self.get_table_name() is None:
            raise IllegalArgumentException(
                 "{} requires table name".format(request_name))

    def get_durability(self):
        pass

    def get_request_name(self):
        # type: () -> str
        return "Write"

    def get_type_name(self):
        # type: () -> str
        return "Write"


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
        self._consistency = None

    def __str__(self):
        return 'ReadRequest'

    def does_reads(self):
        return True

    def set_defaults(self, cfg):
        super(ReadRequest, self).set_defaults(cfg)
        if self._consistency is None:
            self._set_consistency(cfg.get_default_consistency())
        return self

    def _set_consistency(self, consistency):
        if (consistency != Consistency.ABSOLUTE and
                consistency != Consistency.EVENTUAL):
            raise IllegalArgumentException(
                'Consistency must be Consistency.ABSOLUTE or ' +
                'Consistency.EVENTUAL')
        self._consistency = consistency

    def _get_consistency(self):
        return self._consistency

    def _validate_read_request(self, request_name):
        if self.get_table_name() is None:
            raise IllegalArgumentException(
                request_name + ' requires table name.')

    def get_request_name(self):
        # type: () -> str
        return "Read"


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
        self._key = None
        self._match_version = None

    def __str__(self):
        return 'DeleteRequest'

    def set_key(self, key):
        """
        Sets the key to use for the delete operation. This is a required field.

        :param key: the key value.
        :type key: dict
        :returns: self.
        :raises IllegalArgumentException: raises the exception if key is not a
            dictionary.
        """
        CheckValue.check_dict(key, 'key')
        self._key = key
        return self

    def set_key_from_json(self, json_key):
        """
        Sets the key to use for the delete operation based on a JSON string.
        The string is parsed for validity and stored internally as a dict.

        :param json_key: the key as a JSON string.
        :type json_key: str
        :returns: self.
        :raises IllegalArgumentException: raises the exception if json_key is
            not a string.
        """
        CheckValue.check_str(json_key, 'json_key')
        self._key = loads(json_key)
        return self

    def get_key(self):
        """
        Returns the key of the row to be deleted.

        :returns: the key value, or None if not set.
        :rtype: dict
        """
        return self._key

    def set_compartment(self, compartment):
        """
        Cloud service only.

        Sets the name or id of a compartment to be used for this operation.

        The compartment may be specified as either a name (or path for nested
        compartments) or as an id (OCID). A name (vs id) can only be used when
        authenticated using a specific user identity. It is *not* available if
        authenticated as an Instance Principal which can be done when calling
        the service from a compute instance in the Oracle Cloud Infrastructure.
        See
        :py:meth:`borneo.iam.SignatureProvider.create_with_instance_principal`.

        :param compartment: the compartment name or id. If using a nested
            compartment, specify the full compartment path
            compartmentA.compartmentB, but exclude the name of the root
            compartment (tenant).
        :type compartment: str
        :returns: self.
        :raises IllegalArgumentException: raises the exception if compartment
            is not a str.
        """
        self.set_compartment_internal(compartment)
        return self

    def set_match_version(self, version):
        """
        Sets the :py:class:`Version` to use for a conditional delete operation.
        The Version is usually obtained from :py:meth:`GetResult.get_version` or
        other method that returns a Version. When set, the delete operation will
        succeed only if the row exists and its Version matches the one
        specified. Using this option will incur additional cost.

        :param version: the :py:class:`Version` to match.
        :type version: Version
        :returns: self.
        :raises IllegalArgumentException: raises the exception if version is not
            an instance of Version.
        """
        if not isinstance(version, Version):
            raise IllegalArgumentException('set_match_version requires an ' +
                                           'instance of Version as parameter.')
        self._match_version = version
        return self

    def get_match_version(self):
        """
        Returns the :py:class:`Version` used for a match on a conditional
        delete.

        :returns: the Version or None if not set.
        :rtype: Version
        """
        return self._match_version

    def set_timeout(self, timeout_ms):
        """
        Sets the optional request timeout value, in milliseconds. This overrides
        any default value set in :py:class:`NoSQLHandleConfig`. The value must
        be positive.

        :param timeout_ms: the timeout value, in milliseconds.
        :type timeout_ms: int
        :returns: self.
        :raises IllegalArgumentException: raises the exception if the timeout
            value is less than or equal to 0.
        """
        self._set_timeout(timeout_ms)
        return self

    def get_timeout(self):
        """
        Returns the timeout to use for the operation, in milliseconds. A value
        of 0 indicates that the timeout has not been set.

        :returns: the timeout value.
        :rtype: int
        """
        return super(DeleteRequest, self).get_timeout()

    def set_table_name(self, table_name):
        """
        Sets the table name to use for the operation. This is a required
        parameter.

        :param table_name: the table name.
        :type table_name: str
        :returns: self.
        :raises IllegalArgumentException: raises the exception if table_name is
            not a string.
        """
        super(DeleteRequest, self).set_table_name(table_name)
        return self

    def set_return_row(self, return_row):
        """
        Sets whether information about the existing row should be returned on
        failure because of a version mismatch. If a match version has not been
        set via :py:meth:`set_match_version` this parameter is ignored and there
        will be no return information. This parameter is optional and defaults
        to False. It's use may incur additional cost.

        :param return_row: set to True if information should be returned.
        :type return_row: bool
        :returns: self.
        :raises IllegalArgumentException: raises the exception if return_row is
            not True or False.
        """
        self._set_return_row(return_row)
        return self

    def get_return_row(self):
        """
        Returns whether information about the existing row should be returned on
        failure because of a version mismatch.

        :returns: True if information should be returned.
        :rtype: bool
        """
        return self._get_return_row()

    def set_durability(self, durability):
        """
        On-premise only. Sets the durability to use for the operation.

        :param durability: the Durability to use
        :type durability: Durability
        :returns: self.
        :raises IllegalArgumentException: raises the exception if Durability
            is not valid
        :versionadded: 5.3.0
        """
        self._set_durability(durability)
        return self

    def get_durability(self):
        """
        On-premise only. Gets the durability to use for the operation or
        None if not set
        :returns: the Durability
        :versionadded: 5.3.0
        """
        return self._get_durability()

    def does_reads(self):
        return self._match_version is not None or self.get_return_row()

    def validate(self):
        # Validates the state of the object when complete.
        self._validate_write_request('DeleteRequest')
        if self._key is None:
            raise IllegalArgumentException('DeleteRequest requires a key.')

    @staticmethod
    def create_serializer():
        return DeleteRequestSerializer()

    def get_request_name(self):
        # type: () -> str
        return "Delete"


class GetIndexesRequest(Request):
    """
    Represents the argument of a :py:meth:`NoSQLHandle.get_indexes` operation
    which returns the information of a specific index or all indexes of the
    specified table, as returned in :py:class:`GetIndexesResult`.

    The table name is a required parameter.
    """

    def __init__(self):
        super(GetIndexesRequest, self).__init__()
        self._index_name = None

    def __str__(self):
        return 'GetIndexesRequest'

    def set_table_name(self, table_name):
        """
        Sets the table name to use for the request.

        :param table_name: the table name. This is a required parameter.
        :type table_name: str
        :returns: self.
        :raises IllegalArgumentException: raises the exception if table_name is
            not a string.
        """
        super(GetIndexesRequest, self).set_table_name(table_name)
        return self

    def set_compartment(self, compartment):
        """
        Cloud service only.

        Sets the name or id of a compartment to be used for this operation.

        The compartment may be specified as either a name (or path for nested
        compartments) or as an id (OCID). A name (vs id) can only be used when
        authenticated using a specific user identity. It is *not* available if
        authenticated as an Instance Principal which can be done when calling
        the service from a compute instance in the Oracle Cloud Infrastructure.
        See
        :py:meth:`borneo.iam.SignatureProvider.create_with_instance_principal`.

        :param compartment: the compartment name or id. If using a nested
            compartment, specify the full compartment path
            compartmentA.compartmentB, but exclude the name of the root
            compartment (tenant).
        :type compartment: str
        :returns: self.
        :raises IllegalArgumentException: raises the exception if compartment
            is not a str.
        """
        self.set_compartment_internal(compartment)
        return self

    def set_index_name(self, index_name):
        """
        Sets the index name to use for the request. If not set, this request
        will return all indexes of the table.

        :param index_name: the index name.
        :type index_name: str
        :returns: self.
        :raises IllegalArgumentException: raises the exception if index_name is
            not a string.
        """
        CheckValue.check_str(index_name, 'index_name')
        self._index_name = index_name
        return self

    def get_index_name(self):
        """
        Gets the index name to use for the request.

        :returns: the index name.
        :rtype: str
        """
        return self._index_name

    def set_timeout(self, timeout_ms):
        """
        Sets the request timeout value, in milliseconds. This overrides any
        default value set in :py:class:`NoSQLHandleConfig`. The value must be
        positive.

        :param timeout_ms: the timeout value, in milliseconds.
        :type timeout_ms: int
        :returns: self.
        :raises IllegalArgumentException: raises the exception if the timeout
            value is less than or equal to 0.
        """
        self._set_timeout(timeout_ms)
        return self

    def get_timeout(self):
        """
        Returns the timeout to use for the operation, in milliseconds. A value
        of 0 indicates that the timeout has not been set.

        :returns: the timeout value.
        :rtype: int
        """
        return super(GetIndexesRequest, self).get_timeout()

    def should_retry(self):
        # Returns True if this request should be retried.
        return False

    def validate(self):
        if self.get_table_name() is None:
            raise IllegalArgumentException(
                'GetIndexesRequest requires a table name.')

    @staticmethod
    def create_serializer():
        return GetIndexesRequestSerializer()

    def get_request_name(self):
        # type: () -> str
        return "GetIndexes"


class GetRequest(ReadRequest):
    """
    Represents the input to a :py:meth:`NoSQLHandle.get` operation which returns
    a single row based on the specified key.

    The table name and key are required parameters.
    """

    def __init__(self):
        super(GetRequest, self).__init__()
        self._key = None

    def __str__(self):
        return 'GetRequest'

    def set_key(self, key):
        """
        Sets the primary key used for the get operation. This is a required
        parameter.

        :param key: the primary key.
        :type key: dict
        :returns: self.
        :raises IllegalArgumentException: raises the exception if key is not a
            dictionary.
        """
        CheckValue.check_dict(key, 'key')
        self._key = key
        return self

    def set_key_from_json(self, json_key):
        """
        Sets the key to use for the get operation based on a JSON string.

        :param json_key: the key as a JSON string.
        :type json_key: str
        :returns: self.
        :raises IllegalArgumentException: raises the exception if json_key is
            not a string.
        """
        CheckValue.check_str(json_key, 'json_key')
        self._key = loads(json_key)
        return self

    def get_key(self):
        """
        Returns the primary key used for the operation. This is a required
        parameter.

        :returns: the key.
        :rtype: dict
        """
        return self._key

    def set_table_name(self, table_name):
        """
        Sets the table name to use for the operation. This is a required
        parameter.

        :param table_name: the table name.
        :type table_name: str
        :returns: self.
        :raises IllegalArgumentException: raises the exception if table_name is
            not a string.
        """
        super(GetRequest, self).set_table_name(table_name)
        return self

    def set_compartment(self, compartment):
        """
        Cloud service only.

        Sets the name or id of a compartment to be used for this operation.

        The compartment may be specified as either a name (or path for nested
        compartments) or as an id (OCID). A name (vs id) can only be used when
        authenticated using a specific user identity. It is *not* available if
        authenticated as an Instance Principal which can be done when calling
        the service from a compute instance in the Oracle Cloud Infrastructure.
        See
        :py:meth:`borneo.iam.SignatureProvider.create_with_instance_principal`.

        :param compartment: the compartment name or id. If using a nested
            compartment, specify the full compartment path
            compartmentA.compartmentB, but exclude the name of the root
            compartment (tenant).
        :type compartment: str
        :returns: self.
        :raises IllegalArgumentException: raises the exception if compartment
            is not a str.
        """
        self.set_compartment_internal(compartment)
        return self

    def set_consistency(self, consistency):
        """
        Sets the consistency to use for the operation. This parameter is
        optional and if not set the default consistency configured for the
        :py:class:`NoSQLHandle` is used.

        :param consistency: the consistency.
        :type consistency: Consistency
        :returns: self.
        :raises IllegalArgumentException: raises the exception if consistency
            is not Consistency.ABSOLUTE or Consistency.EVENTUAL.
        """
        self._set_consistency(consistency)
        return self

    def get_consistency(self):
        """
        Returns the consistency set for this request, or None if not set.

        :returns: the consistency
        :rtype: Consistency
        """
        return self._get_consistency()

    def set_timeout(self, timeout_ms):
        """
        Sets the request timeout value, in milliseconds. This overrides any
        default value set in :py:class:`NoSQLHandleConfig`. The value must be
        positive.

        :param timeout_ms: the timeout value, in milliseconds.
        :type timeout_ms: int
        :returns: self.
        :raises IllegalArgumentException: raises the exception if the timeout
            value is less than or equal to 0.
        """
        self._set_timeout(timeout_ms)
        return self

    def get_timeout(self):
        """
        Returns the timeout to use for the operation, in milliseconds. A value
        of 0 indicates that the timeout has not been set.

        :returns: the timeout value.
        :rtype: int
        """
        return super(GetRequest, self).get_timeout()

    def validate(self):
        # Validates the state of the members of this class for use.
        self._validate_read_request('GetRequest')
        if self._key is None:
            raise IllegalArgumentException('GetRequest requires a key.')

    @staticmethod
    def create_serializer():
        return GetRequestSerializer()

    def get_request_name(self):
        # type: () -> str
        return "Get"


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
        self._operation_id = None

    def __str__(self):
        return 'GetTableRequest'

    def set_table_name(self, table_name):
        """
        Sets the table name to use for the request.

        :param table_name: the table name. This is a required parameter.
        :type table_name: str
        :returns: self.
        :raises IllegalArgumentException: raises the exception if table_name is
            not a string.
        """
        super(GetTableRequest, self).set_table_name(table_name)
        return self

    def set_compartment(self, compartment):
        """
        Cloud service only.

        Sets the name or id of a compartment to be used for this operation.

        The compartment may be specified as either a name (or path for nested
        compartments) or as an id (OCID). A name (vs id) can only be used when
        authenticated using a specific user identity. It is *not* available if
        authenticated as an Instance Principal which can be done when calling
        the service from a compute instance in the Oracle Cloud Infrastructure.
        See
        :py:meth:`borneo.iam.SignatureProvider.create_with_instance_principal`.

        :param compartment: the compartment name or id. If using a nested
            compartment, specify the full compartment path
            compartmentA.compartmentB, but exclude the name of the root
            compartment (tenant).
        :type compartment: str
        :returns: self.
        :raises IllegalArgumentException: raises the exception if compartment
            is not a str.
        """
        self.set_compartment_internal(compartment)
        return self

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
        :type operation_id: str
        :returns: self.
        :raises IllegalArgumentException: raises the exception if operation_id
            is a negative number.
        """
        if operation_id is not None and not CheckValue.is_str(operation_id):
            raise IllegalArgumentException(
                'operation_id must be a string type.')
        self._operation_id = operation_id
        return self

    def get_operation_id(self):
        """
        Returns the operation id to use for the request, None if not set.

        :returns: the operation id.
        :rtype: str
        """
        return self._operation_id

    def set_timeout(self, timeout_ms):
        """
        Sets the request timeout value, in milliseconds. This overrides any
        default value set in :py:class:`NoSQLHandleConfig`. The value must be
        positive.

        :param timeout_ms: the timeout value, in milliseconds.
        :type timeout_ms: int
        :returns: self.
        :raises IllegalArgumentException: raises the exception if the timeout
            value is less than or equal to 0.
        """
        self._set_timeout(timeout_ms)
        return self

    def get_timeout(self):
        """
        Returns the timeout to use for the operation, in milliseconds. A value
        of 0 indicates that the timeout has not been set.

        :returns: the timeout value.
        :rtype: int
        """
        return super(GetTableRequest, self).get_timeout()

    def should_retry(self):
        # Returns True if this request should be retried.
        return False

    def validate(self):
        if self.get_table_name() is None:
            raise IllegalArgumentException(
                'GetTableRequest requires a table name.')

    @staticmethod
    def create_serializer():
        return GetTableRequestSerializer()

    def get_request_name(self):
        # type: () -> str
        return "GetTable"


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
        self._start_index = 0
        self._limit = 0
        self._namespace = None

    def __str__(self):
        return 'ListTablesRequest'

    def set_compartment(self, compartment):
        """
        Cloud service only.

        Sets the name or id of a compartment to be used for this operation.

        The compartment may be specified as either a name (or path for nested
        compartments) or as an id (OCID). A name (vs id) can only be used when
        authenticated using a specific user identity. It is *not* available if
        authenticated as an Instance Principal which can be done when calling
        the service from a compute instance in the Oracle Cloud Infrastructure.
        See
        :py:meth:`borneo.iam.SignatureProvider.create_with_instance_principal`.

        :param compartment: the compartment name or id. If using a nested
            compartment, specify the full compartment path
            compartmentA.compartmentB, but exclude the name of the root
            compartment (tenant).
        :type compartment: str
        :returns: self.
        :raises IllegalArgumentException: raises the exception if compartment
            is not a str.
        """
        self.set_compartment_internal(compartment)
        return self

    def set_start_index(self, start_index):
        """
        Sets the index to use to start returning table names. This is related to
        the :py:meth:`ListTablesResult.get_last_returned_index` from a previous
        request and can be used to page table names. If not set, the list starts
        at index 0.

        :param start_index: the start index.
        :type start_index: int
        :returns: self.
        :raises IllegalArgumentException: raises the exception if start_index is
            a negative number.
        """
        CheckValue.check_int_ge_zero(start_index, 'start_index')
        self._start_index = start_index
        return self

    def get_start_index(self):
        """
        Returns the index to use to start returning table names. This is related
        to the :py:meth:`ListTablesResult.get_last_returned_index` from a
        previous request and can be used to page table names. If not set, the
        list starts at index 0.

        :returns: the start index.
        :rtype: int
        """
        return self._start_index

    def set_limit(self, limit):
        """
        Sets the maximum number of table names to return in the operation. If
        not set (0) there is no limit.

        :param limit: the maximum number of tables.
        :type limit: int
        :returns: self.
        :raises IllegalArgumentException: raises the exception if limit is a
            negative number.
        """
        CheckValue.check_int_ge_zero(limit, 'limit')
        self._limit = limit
        return self

    def get_limit(self):
        """
        Returns the maximum number of table names to return in the operation. If
        not set (0) there is no application-imposed limit.

        :returns: the maximum number of tables to return in a single request.
        :rtype: int
        """
        return self._limit

    def set_namespace(self, namespace):
        """
        On-premise only.

        Sets the namespace to use for the list. If not set, all tables
        accessible to the user will be returned. If set, only tables in the
        namespace provided are returned.

        :param namespace: the namespace to use.
        :type namespace: str
        :returns: self.
        :raises IllegalArgumentException: raises the exception if namespace is
            not a string.
        """
        CheckValue.check_str(namespace, 'namespace')
        self._namespace = namespace
        return self

    def get_namespace(self):
        """
        On-premise only.

        Returns the namespace to use for the list or None if not set.

        :returns: the namespace.
        :rtype: str
        """
        return self._namespace

    def set_timeout(self, timeout_ms):
        """
        Sets the request timeout value, in milliseconds. This overrides any
        default value set in :py:class:`NoSQLHandleConfig`. The value must be
        positive.

        :param timeout_ms: the timeout value, in milliseconds.
        :type timeout_ms: int
        :returns: self.
        :raises IllegalArgumentException: raises the exception if the timeout
            value is less than or equal to 0.
        """
        self._set_timeout(timeout_ms)
        return self

    def get_timeout(self):
        """
        Returns the timeout to use for the operation, in milliseconds. A value
        of 0 indicates that the timeout has not been set.

        :returns: the timeout value.
        :rtype: int
        """
        return super(ListTablesRequest, self).get_timeout()

    def should_retry(self):
        # Returns True if this request should be retried.
        return False

    def validate(self):
        if self._start_index < 0 or self._limit < 0:
            raise IllegalArgumentException(
                'ListTables: start index and number of tables must be ' +
                'non-negative.')

    @staticmethod
    def create_serializer():
        return ListTablesRequestSerializer()

    def get_request_name(self):
        # type: () -> str
        return "ListTables"


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
        self._key = None
        self._continuation_key = None
        self._range = None
        self._max_write_kb = 0
        self._durability = None

    def __str__(self):
        return 'MultiDeleteRequest'

    def __str__(self):
        return 'MultiDeleteRequest'

    def set_table_name(self, table_name):
        """
        Sets the table name to use for the operation. This is a required
        parameter.

        :param table_name: the table name.
        :type table_name: str
        :returns: self.
        :raises IllegalArgumentException: raises the exception if table_name is
            not a string.
        """
        super(MultiDeleteRequest, self).set_table_name(table_name)
        return self

    def set_key(self, key):
        """
        Sets the key to be used for the operation. This is a required parameter
        and must completely specify the target table's shard key.

        :param key: the key.
        :type key: dict
        :returns: self.
        :raises IllegalArgumentException: raises the exception if key is not a
            dictionary.
        """
        CheckValue.check_dict(key, 'key')
        self._key = key
        return self

    def get_key(self):
        """
        Returns the key to be used for the operation.

        :returns: the key.
        :rtype: dict
        """
        return self._key

    def set_compartment(self, compartment):
        """
        Cloud service only.

        Sets the name or id of a compartment to be used for this operation.

        The compartment may be specified as either a name (or path for nested
        compartments) or as an id (OCID). A name (vs id) can only be used when
        authenticated using a specific user identity. It is *not* available if
        authenticated as an Instance Principal which can be done when calling
        the service from a compute instance in the Oracle Cloud Infrastructure.
        See
        :py:meth:`borneo.iam.SignatureProvider.create_with_instance_principal`.

        :param compartment: the compartment name or id. If using a nested
            compartment, specify the full compartment path
            compartmentA.compartmentB, but exclude the name of the root
            compartment (tenant).
        :type compartment: str
        :returns: self.
        :raises IllegalArgumentException: raises the exception if compartment
            is not a str.
        """
        self.set_compartment_internal(compartment)
        return self

    def set_continuation_key(self, continuation_key):
        """
        Sets the continuation key.

        :param continuation_key: the key which should have been obtained from
            :py:meth:`MultiDeleteResult.get_continuation_key`.
        :type continuation_key: bytearray
        :returns: self.
        :raises IllegalArgumentException: raises the exception if
            continuation_key is not a bytearray.
        """
        if (continuation_key is not None and
                not isinstance(continuation_key, bytearray)):
            raise IllegalArgumentException(
                'set_continuation_key requires bytearray as parameter.')
        self._continuation_key = continuation_key
        return self

    def get_continuation_key(self):
        """
        Returns the continuation key if set.

        :returns: the continuation key.
        :rtype: bytearray
        """
        return self._continuation_key

    def set_max_write_kb(self, max_write_kb):
        """
        Sets the limit on the total KB write during this operation, 0 means no
        application-defined limit. This value can only reduce the system defined
        limit.

        :param max_write_kb: the limit in terms of number of KB write during
            this operation.
        :type max_write_kb: int
        :returns: self.
        :raises IllegalArgumentException: raises the exception if the
            max_write_kb value is less than 0.
        """
        CheckValue.check_int_ge_zero(max_write_kb, 'max_write_kb')
        self._max_write_kb = max_write_kb
        return self

    def get_max_write_kb(self):
        """
        Returns the limit on the total KB write during this operation. If not
        set by the application this value will be 0 which means the default
        system limit is used.

        :returns: the limit, or 0 if not set.
        :rtype: int
        """
        return self._max_write_kb

    def set_range(self, field_range):
        """
        Sets the :py:class:`FieldRange` to be used for the operation. This
        parameter is optional, but required to delete a specific range of rows.

        :param field_range: the field range.
        :type field_range: FieldRange
        :returns: self.
        :raises IllegalArgumentException: raises the exception if field_range is
            not an instance of FieldRange.
        """
        if field_range is not None and not isinstance(field_range, FieldRange):
            raise IllegalArgumentException(
                'set_range requires an instance of FieldRange or None as ' +
                'parameter.')
        self._range = field_range
        return self

    def get_range(self):
        """
        Returns the :py:class:`FieldRange` to be used for the operation if set.

        :returns: the range, None if no range is to be used.
        :rtype: FieldRange
        """
        return self._range

    def set_timeout(self, timeout_ms):
        """
        Sets the request timeout value, in milliseconds. This overrides any
        default value set in :py:class:`NoSQLHandleConfig`. The value must be
        positive.

        :param timeout_ms: the timeout value, in milliseconds.
        :type timeout_ms: int
        :returns: self.
        :raises IllegalArgumentException: raises the exception if the timeout
            value is less than or equal to 0.
        """
        self._set_timeout(timeout_ms)
        return self

    def get_timeout(self):
        """
        Returns the timeout to use for the operation, in milliseconds. A value
        of 0 indicates that the timeout has not been set.

        :returns: the timeout value.
        :rtype: int
        """
        return super(MultiDeleteRequest, self).get_timeout()

    def set_durability(self, durability):
        """
        On-premise only. Sets the durability to use for the operation.

        :param durability: the Durability to use
        :type durability: Durability
        :returns: self.
        :raises IllegalArgumentException: raises the exception if Durability
            is not valid
        :versionadded: 5.3.0
        """
        if durability is None:
            self._durability = None
            return
        if not isinstance(durability, Durability):
            raise IllegalArgumentException('set_durability requires an ' +
                                           'instance of Durability as parameter.')
        durability.validate()
        self._durability = durability
        return self

    def get_durability(self):
        """
        On-premise only. Gets the durability to use for the operation or
        None if not set
        :returns: the Durability
        :versionadded: 5.3.0
        """
        return self._durability

    def does_reads(self):
        return True

    def does_writes(self):
        return True

    def validate(self):
        if self.get_table_name() is None:
            raise IllegalArgumentException(
                'MultiDeleteRequest requires table name.')
        if self._key is None:
            raise IllegalArgumentException(
                'MultiDeleteRequest requires a key.')
        if self._range is not None:
            self._range.validate()

    @staticmethod
    def create_serializer():
        return MultiDeleteRequestSerializer()

    def get_request_name(self):
        # type: () -> str
        return "MultiDelete"


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
        self._statement = None
        self._get_query_plan = False

    def __str__(self):
        return 'PrepareRequest'

    def set_table_name(self, table_name):
        """
        Sets the table name for a query operation. This is used by rate limiting
        logic to manage internal rate limiters.

        :param table_name: the name (or OCID) of the table.
        :type table_name: str
        :returns: self.
        :raises IllegalArgumentException: raises the exception if table_name is
            not a string.
        """
        super(PrepareRequest, self).set_table_name(table_name)
        return self

    def set_statement(self, statement):
        """
        Sets the query statement.

        :param statement: the query statement.
        :type statement: str
        :returns: self.
        :raises IllegalArgumentException: raises the exception if statement is
            not a string.
        """
        CheckValue.check_str(statement, 'statement')
        self._statement = statement
        return self

    def get_statement(self):
        """
        Returns the query statement.

        :returns: the statement, or None if it has not been set.
        :rtype: str
        """
        return self._statement

    def set_compartment(self, compartment):
        """
        Cloud service only.

        Sets the name or id of a compartment to be used for this operation.

        The compartment may be specified as either a name (or path for nested
        compartments) or as an id (OCID). A name (vs id) can only be used when
        authenticated using a specific user identity. It is *not* available if
        authenticated as an Instance Principal which can be done when calling
        the service from a compute instance in the Oracle Cloud Infrastructure.
        See
        :py:meth:`borneo.iam.SignatureProvider.create_with_instance_principal`.

        :param compartment: the compartment name or id. If using a nested
            compartment, specify the full compartment path
            compartmentA.compartmentB, but exclude the name of the root
            compartment (tenant).
        :type compartment: str
        :returns: self.
        :raises IllegalArgumentException: raises the exception if compartment
            is not a str.
        """
        self.set_compartment_internal(compartment)
        return self

    def set_get_query_plan(self, get_query_plan):
        """
        Sets whether a printout of the query execution plan should be included
        in the :py:class:`PrepareResult`.

        :param get_query_plan: True if a printout of the query execution plan
            should be included in the :py:class:`PrepareResult`. False
            otherwise.
        :type get_query_plan: bool
        :returns: self.
        :raises IllegalArgumentException: raises the exception if get_query_plan
            is not a boolean.
        """
        CheckValue.check_boolean(get_query_plan, 'get_query_plan')
        self._get_query_plan = get_query_plan
        return self

    def get_query_plan(self):
        """
        Returns whether a printout of the query execution plan should be include
        in the :py:class:`PrepareResult`.

        :returns: whether a printout of the query execution plan should be
            include in the :py:class:`PrepareResult`.
        :rtype: bool
        """
        return self._get_query_plan

    def set_timeout(self, timeout_ms):
        """
        Sets the request timeout value, in milliseconds. This overrides any
        default value set in :py:class:`NoSQLHandleConfig`. The value must be
        positive.

        :param timeout_ms: the timeout value, in milliseconds.
        :type timeout_ms: int
        :returns: self.
        :raises IllegalArgumentException: raises the exception if the timeout
            value is less than or equal to 0.
        """
        self._set_timeout(timeout_ms)
        return self

    def get_timeout(self):
        """
        Returns the timeout to use for the operation, in milliseconds. A value
        of 0 indicates that the timeout has not been set.

        :returns: the value.
        :rtype: int
        """
        return super(PrepareRequest, self).get_timeout()

    def validate(self):
        if self._statement is None:
            raise IllegalArgumentException(
                'PrepareRequest requires a statement.')

    @staticmethod
    def create_serializer():
        return PrepareRequestSerializer()

    def get_request_name(self):
        # type: () -> str
        return "Prepare"


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
        self._value = None
        self._option = None
        self._match_version = None
        self._ttl = None
        self._update_ttl = False
        self._exact_match = False
        self._identity_cache_size = 0

    def __str__(self):
        return 'PutRequest'

    def set_value(self, value):
        """
        Sets the value to use for the put operation. This is a required
        parameter and must be set using this method or
        :py:meth:`set_value_from_json`

        :param value: the row value.
        :type value: dict
        :returns: self.
        :raises IllegalArgumentException: raises the exception if value is not
            a dictionary.
        """
        CheckValue.check_dict(value, 'value')
        self._value = value
        return self

    def set_value_from_json(self, json_value):
        """
        Sets the value to use for the put operation based on a JSON string. The
        string is parsed for validity and stored internally as a dict. This is
        a required parameter and must be set using this method or
        :py:meth:`set_value`

        :param json_value: the row value as a JSON string.
        :type json_value: str
        :returns: self.
        :raises IllegalArgumentException: raises the exception if json_value is
            not a string.
        """
        CheckValue.check_str(json_value, 'json_value')
        self._value = loads(json_value)
        return self

    def get_value(self):
        """
        Returns the value of the row to be used.

        :returns: the value, or None if not set.
        :rtype: dict
        """
        return self._value

    def set_compartment(self, compartment):
        """
        Cloud service only.

        Sets the name or id of a compartment to be used for this operation.

        The compartment may be specified as either a name (or path for nested
        compartments) or as an id (OCID). A name (vs id) can only be used when
        authenticated using a specific user identity. It is *not* available if
        authenticated as an Instance Principal which can be done when calling
        the service from a compute instance in the Oracle Cloud Infrastructure.
        See
        :py:meth:`borneo.iam.SignatureProvider.create_with_instance_principal`.

        :param compartment: the compartment name or id. If using a nested
            compartment, specify the full compartment path
            compartmentA.compartmentB, but exclude the name of the root
            compartment (tenant).
        :type compartment: str
        :returns: self.
        :raises IllegalArgumentException: raises the exception if compartment
            is not a str.
        """
        self.set_compartment_internal(compartment)
        return self

    def set_option(self, option):
        """
        Sets the option for the put.

        :param option: the option to set.
        :type option: PutOption
        :returns: self.
        """
        self._option = option
        return self

    def get_option(self):
        """
        Returns the option specified for the put.

        :returns: the option specified.
        :rtype: PutOption
        """
        return self._option

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
        :returns: self.
        :raises IllegalArgumentException: raises the exception if version is not
            an instance of Version.
        """
        if not isinstance(version, Version):
            raise IllegalArgumentException('set_match_version requires an ' +
                                           'instance of Version as parameter.')
        if self._option is None:
            self._option = PutOption.IF_VERSION
        self._match_version = version
        return self

    def get_match_version(self):
        """
        Returns the :py:class:`Version` used for a match on a conditional put.

        :returns: the Version or None if not set.
        :rtype: Version
        """
        return self._match_version

    def set_ttl(self, ttl):
        """
        Sets the :py:class:`TimeToLive` value, causing the time to live on the
        row to be set to the specified value on put. This value overrides any
        default time to live setting on the table.

        :param ttl: the time to live.
        :type ttl: TimeToLive
        :returns: self.
        :raises IllegalArgumentException: raises the exception if ttl is not an
            instance of TimeToLive.
        """
        if ttl is not None and not isinstance(ttl, TimeToLive):
            raise IllegalArgumentException('set_ttl requires an instance of ' +
                                           'TimeToLive or None as parameter.')
        self._ttl = ttl
        return self

    def get_ttl(self):
        """
        Returns the :py:class:`TimeToLive` value, if set.

        :returns: the :py:class:`TimeToLive` if set, None otherwise.
        :rtype: TimeToLive
        """
        return self._ttl

    def set_use_table_default_ttl(self, update_ttl):
        """
        If value is True, and there is an existing row, causes the operation
        to update the time to live (TTL) value of the row based on the Table's
        default TTL if set. If the table has no default TTL this state has no
        effect. By default updating an existing row has no effect on its TTL.

        :param update_ttl: True or False.
        :type update_ttl: bool
        :returns: self.
        :raises IllegalArgumentException: raises the exception if update_ttl is
            not True or False.
        """
        CheckValue.check_boolean(update_ttl, 'update_ttl')
        self._update_ttl = update_ttl
        return self

    def get_use_table_default_ttl(self):
        """
        Returns whether or not to update the row's time to live (TTL) based on a
        table default value if the row exists. By default updates of existing
        rows do not affect that row's TTL.

        :returns: whether or not to update the row's TTL based on a table default
            value if the row exists.
        :rtype: bool
        """
        return self._update_ttl

    def get_update_ttl(self):
        """
        Returns True if the operation should update the ttl.

        :returns: True if the operation should update the ttl.
        :rtype: bool
        """
        return self._update_ttl or self._ttl is not None

    def set_exact_match(self, exact_match):
        """
        If True the value must be an exact match for the table schema or the
        operation will fail. An exact match means that there are no required
        fields missing and that there are no extra, unknown fields. The default
        behavior is to not require an exact match.

        :param exact_match: True or False.
        :type exact_match: bool
        :returns: self.
        :raises IllegalArgumentException: raises the exception if exact_match is
            not True or False.
        """
        CheckValue.check_boolean(exact_match, 'exact_match')
        self._exact_match = exact_match
        return self

    def get_exact_match(self):
        """
        Returns whether the value must be an exact match to the table schema or
        not.

        :returns: the value.
        :rtype: bool
        """
        return self._exact_match

    def set_identity_cache_size(self, identity_cache_size):
        """
        Sets the number of generated identity values that are requested from the
        server during a put. This takes precedence over the DDL identity CACHE
        option set during creation of the identity column.

        Any value equal or less than 0 means that the DDL identity CACHE value
        is used.

        :param identity_cache_size: the size.
        :type identity_cache_size: int
        :returns: self.
        :raises IllegalArgumentException: raises the exception if
            identity_cache_size is not an integer.
        """
        CheckValue.check_int(identity_cache_size, 'identity_cache_size')
        self._identity_cache_size = identity_cache_size
        return self

    def get_identity_cache_size(self):
        """
        Gets the number of generated identity values that are requested from the
        server during a put if set in this request.

        :returns: the identity cache size.
        :rtype: int
        """
        return self._identity_cache_size

    def set_timeout(self, timeout_ms):
        """
        Sets the request timeout value, in milliseconds. This overrides any
        default value set in :py:class:`NoSQLHandleConfig`. The value must be
        positive.

        :param timeout_ms: the timeout value, in milliseconds.
        :type timeout_ms: int
        :returns: self.
        :raises IllegalArgumentException: raises the exception if the timeout
            value is less than or equal to 0.
        """
        self._set_timeout(timeout_ms)
        return self

    def get_timeout(self):
        """
        Returns the timeout to use for the operation, in milliseconds. A value
        of 0 indicates that the timeout has not been set.

        :returns: the timeout value.
        :rtype: int
        """
        return super(PutRequest, self).get_timeout()

    def set_table_name(self, table_name):
        """
        Sets the table name to use for the operation.

        :param table_name: the table name.
        :type table_name: str
        :returns: self.
        :raises IllegalArgumentException: raises the exception if table_name is
            not a string.
        """

        super(PutRequest, self).set_table_name(table_name)
        return self

    def set_return_row(self, return_row):
        """
        Sets whether information about the exist row should be returned on
        failure because of a version mismatch or failure of an "if absent"
        operation.

        :param return_row: set to True if information should be returned.
        :type return_row: bool
        :returns: self.
        :raises IllegalArgumentException: raises the exception if return_row is
            not True or False.
        """
        self._set_return_row(return_row)
        return self

    def get_return_row(self):
        """
        Returns whether information about the exist row should be returned on
        failure because of a version mismatch or failure of an "if absent"
        operation. If no option is set via :py:meth:`set_option` or the option
        is PutOption.IF_PRESENT the value of this parameter is ignored and there
        will not be any return information.

        :returns: True if information should be returned.
        :rtype: bool
        """
        return self._get_return_row()

    def set_durability(self, durability):
        """
        On-premise only. Sets the durability to use for the operation.

        :param durability: the Durability to use
        :type durability: Durability
        :returns: self.
        :raises IllegalArgumentException: raises the exception if Durability
            is not valid
        :versionadded: 5.3.0
        """
        self._set_durability(durability)
        return self

    def get_durability(self):
        """
        On-premise only. Gets the durability to use for the operation or
        None if not set
        :returns: the Durability
        :versionadded: 5.3.0
        """
        return self._get_durability()

    def does_reads(self):
        return self._option is not None or self.get_return_row()

    def validate(self):
        # Validates the state of the object when complete.
        self._validate_write_request('PutRequest')
        if self._value is None:
            raise IllegalArgumentException('PutRequest requires a value')
        self._validate_if_options()

    def _validate_if_options(self):
        # Ensures that only one of ifAbsent, ifPresent, or match_version is
        # set.
        if (self._option == PutOption.IF_VERSION and
                self._match_version is None):
            raise IllegalArgumentException(
                'PutRequest: match_version must be specified when ' +
                'PutOption.IF_VERSION is used.')
        if (self._option != PutOption.IF_VERSION and
                self._match_version is not None):
            raise IllegalArgumentException(
                'PutRequest: match_version is specified, the option is not ' +
                'PutOption.IF_VERSION.')
        if self._update_ttl and self._ttl is not None:
            raise IllegalArgumentException(
                'PutRequest: only one of set_use_table_default_ttl or set_ttl' +
                ' may be specified')

    @staticmethod
    def create_serializer():
        return PutRequestSerializer()

    def get_request_name(self):
        # type: () -> str
        return "Put"


class QueryRequest(Request):
    """
    A request that represents a query. A query may be specified as either a
    textual SQL statement (a String) or a prepared query (an instance of
    :py:class:`PreparedStatement`), which may include bind variables.

    For performance reasons prepared queries are preferred for queries that may
    be reused. This is because prepared queries bypass query compilation. They
    also allow for parameterized queries using bind variables.

    To compute and retrieve the full result set of a query, the same
    QueryRequest instance will, in general, have to be executed multiple times
    (via :py:meth:`NoSQLHandle.query`). Each execution returns a
    :py:class:`QueryResult`, which contains a subset of the result set. The
    following code snippet illustrates a typical query execution:

    .. code-block:: pycon

        handle = ...
        request = QueryRequest().set_statement('SELECT * FROM foo')
        while True:
            result = handle.query(request)
            results = result.get_results()
            # do something with the results
            if request.is_done():
                break

    Notice that a batch of results returned by a QueryRequest execution may be
    empty. This is because during each execution the query is allowed to read or
    write a maximum number of bytes. If this maximum is reached, execution
    stops. This can happen before any result was generated (for example, if none
    of the rows read satisfied the query conditions).

    If an application wishes to terminate query execution before retrieving all
    of the query results, it should call :py:meth:`close` in order to release
    any local resources held by the query. This also allows the application to
    reuse the QueryRequest instance to run the same query from the beginning or
    a different query.

    QueryRequest instances are not thread-safe. That is, if two or more
    application threads need to run the same query concurrently, they must
    create and use their own QueryRequest instances.

    The statement or prepared_statement is required parameter.
    """

    def __init__(self):
        super(QueryRequest, self).__init__()
        self._trace_level = 0
        self._limit = 0
        self._max_read_kb = 0
        self._max_write_kb = 0
        self._max_memory_consumption = 1024 * 1024 * 1024
        self._math_context = Context(prec=7, rounding=ROUND_HALF_EVEN)
        self._consistency = None
        self._statement = None
        self._prepared_statement = None
        self._continuation_key = None
        # If shardId is >= 0, the QueryRequest should be executed only at the
        # shard with this id. This is the case only for advanced queries that do
        # sorting.
        self._shard_id = -1
        # The QueryDriver, for advanced queries only.
        self.driver = None
        # An "internal" request is one created and submitted for execution by
        # the ReceiveIter.
        self.is_internal = False

    def __str__(self):
        return 'QueryRequest'

    def copy_internal(self):
        # Creates an internal QueryRequest out of the application-provided
        # request.
        internal_req = QueryRequest()
        internal_req.set_compartment(self.get_compartment())
        internal_req.set_table_name(self.get_table_name())
        internal_req.set_timeout(self.get_timeout())
        internal_req.set_trace_level(self._trace_level)
        internal_req.set_limit(self._limit)
        internal_req.set_max_read_kb(self._max_read_kb)
        internal_req.set_max_write_kb(self._max_write_kb)
        internal_req.set_max_memory_consumption(self._max_memory_consumption)
        internal_req.set_math_context(self._math_context)
        internal_req.set_consistency(self._consistency)
        internal_req.set_prepared_statement(self._prepared_statement)
        internal_req.driver = self.driver
        internal_req.is_internal = True
        return internal_req

    def close(self):
        """
        Terminates the query execution and releases any memory consumed by the
        query at the driver. An application should use this method if it wishes
        to terminate query execution before retrieving all of the query results.
        """
        self.set_cont_key(None)

    def is_done(self):
        """
        Returns True if the query execution is finished, i.e., there are no more
        query results to be generated. Otherwise False.

        :returns: Whether the query execution is finished or not.
        :rtype: bool
        """
        return self._continuation_key is None

    def get_table_name(self):
        if self._prepared_statement is None:
            return None
        return self._prepared_statement.get_table_name()

    def set_compartment(self, compartment):
        """
        Cloud service only.

        Sets the name or id of a compartment to be used for this operation.

        The compartment may be specified as either a name (or path for nested
        compartments) or as an id (OCID). A name (vs id) can only be used when
        authenticated using a specific user identity. It is *not* available if
        authenticated as an Instance Principal which can be done when calling
        the service from a compute instance in the Oracle Cloud Infrastructure.
        See
        :py:meth:`borneo.iam.SignatureProvider.create_with_instance_principal`.

        :param compartment: the compartment name or id. If using a nested
            compartment, specify the full compartment path
            compartmentA.compartmentB, but exclude the name of the root
            compartment (tenant).
        :type compartment: str
        :returns: self.
        :raises IllegalArgumentException: raises the exception if compartment
            is not a str.
        """
        self.set_compartment_internal(compartment)
        return self

    def set_trace_level(self, trace_level):
        CheckValue.check_int_ge_zero(trace_level, 'trace_level')
        if trace_level > 32:
            raise IllegalArgumentException('trace level must be <= 32')
        self._trace_level = trace_level
        return self

    def get_trace_level(self):
        return self._trace_level

    def set_limit(self, limit):
        """
        Sets the limit on number of items returned by the operation. This allows
        an operation to return less than the default amount of data.

        :param limit: the limit in terms of number of items returned.
        :type limit: int
        :returns: self.
        :raises IllegalArgumentException: raises the exception if the limit is a
            negative number.
        """
        CheckValue.check_int_ge_zero(limit, 'limit')
        self._limit = limit
        return self

    def get_limit(self):
        """
        Returns the limit on number of items returned by the operation. If not
        set by the application this value will be 0 which means no limit.

        :returns: the limit, or 0 if not set.
        :rtype: int
        """
        return self._limit

    def set_max_read_kb(self, max_read_kb):
        """
        Sets the limit on the total data read during this operation, in KB.
        This value can only reduce the system defined limit. This limit is
        independent of read units consumed by the operation.

        It is recommended that for tables with relatively low provisioned read
        throughput that this limit be reduced to less than or equal to one half
        of the provisioned throughput in order to avoid or reduce throttling
        exceptions.

        :param max_read_kb: the limit in terms of number of KB read during this
            operation.
        :type max_read_kb: int
        :returns: self.
        :raises IllegalArgumentException: raises the exception if the
            max_read_kb value is less than 0.
        """
        CheckValue.check_int_ge_zero(max_read_kb, 'max_read_kb')
        self._max_read_kb = max_read_kb
        return self

    def get_max_read_kb(self):
        """
        Returns the limit on the total data read during this operation, in KB.
        If not set by the application this value will be 0 which means no
        application-defined limit.

        :returns: the limit, or 0 if not set.
        :rtype: int
        """
        return self._max_read_kb

    def set_max_write_kb(self, max_write_kb):
        """
        Sets the limit on the total data written during this operation, in KB.
        This limit is independent of write units consumed by the operation.

        :param max_write_kb: the limit in terms of number of KB written during
            this operation.
        :type max_write_kb: int
        :returns: self.
        :raises IllegalArgumentException: raises the exception if the
            max_write_kb value is less than 0.
        """
        CheckValue.check_int_ge_zero(max_write_kb, 'max_write_kb')
        self._max_write_kb = max_write_kb
        return self

    def get_max_write_kb(self):
        """
        Returns the limit on the total data written during this operation, in
        KB. If not set by the application this value will be 0 which means no
        application-defined limit.

        :returns: the limit, or 0 if not set.
        :rtype: int
        """
        return self._max_write_kb

    def set_max_memory_consumption(self, memory_consumption):
        """
        Sets the maximum number of memory bytes that may be consumed by the
        statement at the driver for operations such as duplicate elimination
        (which may be required due to the use of an index on a list or map)
        and sorting. Such operations may consume a lot of memory as they need to
        cache the full result set or a large subset of it at the client memory.
        The default value is 1GB.

        :param memory_consumption: the maximum number of memory bytes that may
            be consumed by the statement at the driver for blocking operations.
        :type memory_consumption: long
        :returns: self.
        :raises IllegalArgumentException: raises the exception if
            memory_consumption is a negative number or 0.
        """
        CheckValue.check_int_ge_zero(memory_consumption, 'memory_consumption')
        self._max_memory_consumption = memory_consumption
        return self

    def get_max_memory_consumption(self):
        """
        Returns the maximum number of memory bytes that may be consumed by the
        statement at the driver for operations such as duplicate elimination
        (which may be required due to the use of an index on a list or map)
        and sorting (sorting by distance when a query contains a geo_near()
        function). Such operations may consume a lot of memory as they need to
        cache the full result set at the client memory.
        The default value is 100MB.

        :returns: the maximum number of memory bytes.
        :rtype: long
        """
        return self._max_memory_consumption

    def set_math_context(self, math_context):
        """
        Sets the Context used for Decimal operations.

        :param math_context: the Context used for Decimal operations.
        :type math_context: Context
        :returns: self.
        :raises IllegalArgumentException: raises the exception if math_context
            is not an instance of Context.
        """
        if not isinstance(math_context, Context):
            raise IllegalArgumentException(
                'set_math_context requires an instance of decimal.Context as ' +
                'parameter.')
        self._math_context = math_context
        return self

    def get_math_context(self):
        """
        Returns the Context used for Decimal operations.

        :returns: the Context used for Decimal operations.
        :rtype: Context
        """
        return self._math_context

    def set_consistency(self, consistency):
        """
        Sets the consistency to use for the operation.

        :param consistency: the consistency.
        :type consistency: Consistency
        :returns: self.
        :raises IllegalArgumentException: raises the exception if consistency
            is not Consistency.ABSOLUTE or Consistency.EVENTUAL.
        """
        if (consistency != Consistency.ABSOLUTE and
                consistency != Consistency.EVENTUAL):
            raise IllegalArgumentException(
                'set_consistency requires Consistency.ABSOLUTE or ' +
                'Consistency.EVENTUAL as parameter.')
        self._consistency = consistency
        return self

    def get_consistency(self):
        """
        Returns the consistency set for this request, or None if not set.

        :returns: the consistency
        :rtype: Consistency
        """
        return self._consistency

    @deprecated
    def set_continuation_key(self, continuation_key):
        """
        Sets the continuation key. This is used to continue an operation that
        returned this key in its :py:class:`QueryResult`.

        :param continuation_key: the key which should have been obtained from
            :py:meth:`QueryResult.get_continuation_key`.
        :type continuation_key: bytearray or None
        :returns: self.
        :raises IllegalArgumentException: raises the exception if
            continuation_key is not a bytearray.
        :deprecated: There is no reason to use this method anymore, because
            setting the continuation key is now done internally.
        """
        if (continuation_key is not None and
                not isinstance(continuation_key, bytearray)):
            raise IllegalArgumentException(
                'set_continuation_key requires bytearray as parameter.')
        return self.set_cont_key(continuation_key)

    @deprecated
    def get_continuation_key(self):
        """
        Returns the continuation key if set.

        :returns: the key.
        :rtype: bytearray
        :deprecated: There is no reason to use this method anymore, because
            getting the continuation key is now done internally.
        """
        return self._continuation_key

    def set_cont_key(self, continuation_key):
        self._continuation_key = continuation_key
        if (self.driver is not None and not self.is_internal and
                self._continuation_key is None):
            self.driver.close()
            self.driver = None
        return self

    def get_cont_key(self):
        return self._continuation_key

    def set_statement(self, statement):
        """
        Sets the query statement.

        :param statement: the query statement.
        :type statement: str
        :returns: self.
        :raises IllegalArgumentException: raises the exception if statement is
            not a string.
        """
        CheckValue.check_str(statement, 'statement')
        if (self._prepared_statement is not None and
                statement != self._prepared_statement.get_sql_text()):
            raise IllegalArgumentException(
                'The query text is not equal to the prepared one.')
        self._statement = statement
        return self

    def get_statement(self):
        """
        Returns the query statement.

        :returns: the statement, or None if it has not been set.
        :rtype: str
        """
        return self._statement

    def set_prepared_statement(self, value):
        """
        Sets the prepared query statement.

        :param value: the prepared query statement or the result of a prepare
            request.
        :type value: PreparedStatement
        :returns: self.
        :raises IllegalArgumentException: raises the exception if value is not
            an instance of PrepareResult or PreparedStatement.
        """
        if not (isinstance(value, PrepareResult) or
                isinstance(value, PreparedStatement)):
            raise IllegalArgumentException(
                'set_prepared_statement requires an instance of PrepareResult' +
                ' or PreparedStatement as parameter.')
        if (isinstance(value, PreparedStatement) and self._statement is not None
                and self._statement != value.get_sql_text()):
            raise IllegalArgumentException(
                'The query text is not equal to the prepared one.')
        self._prepared_statement = (value.get_prepared_statement() if
                                    isinstance(value, PrepareResult) else value)
        return self

    def get_prepared_statement(self):
        """
        Returns the prepared query statement.

        :returns: the statement, or None if it has not been set.
        :rtype: PreparedStatement
        """
        return self._prepared_statement

    def set_shard_id(self, shard_id):
        self._shard_id = shard_id

    def get_shard_id(self):
        return self._shard_id

    def set_driver(self, driver):
        if self.driver is not None:
            raise IllegalArgumentException(
                'QueryRequest is already bound to a QueryDriver')
        self.driver = driver
        return self

    def get_driver(self):
        return self.driver

    def set_timeout(self, timeout_ms):
        """
        Sets the request timeout value, in milliseconds. This overrides any
        default value set in :py:class:`NoSQLHandleConfig`. The value must be
        positive.

        :param timeout_ms: the timeout value, in milliseconds.
        :type timeout_ms: int
        :returns: self.
        :raises IllegalArgumentException: raises the exception if the timeout
            value is less than or equal to 0.
        """
        self._set_timeout(timeout_ms)
        return self

    def get_timeout(self):
        """
        Returns the timeout to use for the operation, in milliseconds. A value
        of 0 indicates that the timeout has not been set.

        :returns: the timeout value.
        :rtype: int
        """
        return super(QueryRequest, self).get_timeout()

    def set_defaults(self, cfg):
        super(QueryRequest, self).set_defaults(cfg)
        if self._consistency is None:
            self._consistency = cfg.get_default_consistency()
        return self

    def does_reads(self):
        """
        Just about every permutation of query does reads.
        """
        return True

    def does_writes(self):
        if self._prepared_statement is None:
            return False
        return self._prepared_statement.does_writes()

    def has_driver(self):
        return self.driver is not None

    def is_prepared(self):
        return self._prepared_statement is not None

    def is_query_request(self):
        return not self.is_internal

    def is_simple_query(self):
        return self._prepared_statement.is_simple_query()

    def topology_info(self):
        return (None if self._prepared_statement is None else
                self._prepared_statement.topology_info())

    def topology_seq_num(self):
        return (-1 if self._prepared_statement is None else
                self._prepared_statement.topology_seq_num())

    def validate(self):
        if self._statement is None and self._prepared_statement is None:
            raise IllegalArgumentException(
                'Either statement or prepared statement should be set.')

    @staticmethod
    def create_serializer():
        return QueryRequestSerializer()

    def get_request_name(self):
        # type: () -> str
        return "Query"


class SystemRequest(Request):
    """
    On-premise only.

    SystemRequest is an on-premise-only request used to perform any
    table-independent administrative operation such as create/drop of namespaces
    and security-relevant operations (create/drop users and roles). These
    operations are asynchronous and completion needs to be checked.

    Examples of statements used in this object include:

        CREATE NAMESPACE mynamespace\n
        CREATE USER some_user IDENTIFIED BY password\n
        CREATE ROLE some_role\n
        GRANT ROLE some_role TO USER some_user

    Execution of operations specified by this request is implicitly
    asynchronous. These are potentially long-running operations.
    :py:meth:`NoSQLHandle.system_request` returns a :py:class:`SystemResult`
    instance that can be used to poll until the operation succeeds or fails.
    """

    def __init__(self):
        super(SystemRequest, self).__init__()
        self._statement = None

    def __str__(self):
        return 'SystemRequest: [statement=' + self._statement + ']'

    def set_statement(self, statement):
        """
        Sets the statement to use for the operation.

        :param statement: the statement. This is a required parameter.
        :type statement: str
        :returns: self.
        :raises IllegalArgumentException: raises the exception if statement is
            not a string.
        """
        CheckValue.check_str(statement, 'statement')
        self._statement = statement
        return self

    def get_statement(self):
        """
        Returns the statement, or None if not set.

        :returns: the statement.
        :rtype: str
        """
        return self._statement

    def set_timeout(self, timeout_ms):
        """
        Sets the request timeout value, in milliseconds. This overrides any
        default value set in :py:class:`NoSQLHandleConfig`. The value must be
        positive.

        :param timeout_ms: the timeout value, in milliseconds.
        :type timeout_ms: int
        :returns: self.
        :raises IllegalArgumentException: raises the exception if the timeout
            value is less than or equal to 0.
        """
        self._set_timeout(timeout_ms)
        return self

    def get_timeout(self):
        """
        Returns the timeout to use for the operation, in milliseconds. A value
        of 0 indicates that the timeout has not been set.

        :returns: the timeout value.
        :rtype: int
        """
        return super(SystemRequest, self).get_timeout()

    def set_defaults(self, cfg):
        # Use the default request timeout if not set.
        self._check_config(cfg)
        if self.get_timeout() == 0:
            self._set_timeout(cfg.get_default_table_request_timeout())
        return self

    def should_retry(self):
        return False

    def validate(self):
        if self._statement is None:
            raise IllegalArgumentException(
                'SystemRequest requires a statement.')

    @staticmethod
    def create_serializer():
        return SystemRequestSerializer()

    def get_request_name(self):
        # type: () -> str
        return "System"


class SystemStatusRequest(Request):
    """
    On-premise only.

    SystemStatusRequest is an on-premise-only request used to check the status
    of an operation started using a :py:class:`SystemRequest`.
    """

    def __init__(self):
        super(SystemStatusRequest, self).__init__()
        self._statement = None
        self._operation_id = None

    def __str__(self):
        return ('SystemStatusRequest [statement=' + self._statement +
                ', operation_id=' + self._operation_id + ']')

    def set_operation_id(self, operation_id):
        """
        Sets the operation id to use for the request. The operation id can be
        obtained via :py:meth:`SystemResult.get_operation_id`. This parameter is
        not optional and represents an asynchronous operation that may be in
        progress. It is used to examine the result of the operation and if the
        operation has failed an exception will be thrown in response to a
        :py:meth:`NoSQLHandle.system_status` operation. If the operation is in
        progress or has completed successfully, the state of the operation is
        returned.

        :param operation_id: the operation id.
        :type operation_id: str
        :returns: self.
        :raises IllegalArgumentException: raises the exception if operation_id
            is a negative number.
        """
        CheckValue.check_str(operation_id, 'operation_id')
        self._operation_id = operation_id
        return self

    def get_operation_id(self):
        """
        Returns the operation id to use for the request, None if not set.

        :returns: the operation id.
        :rtype: str
        """
        return self._operation_id

    def set_statement(self, statement):
        """
        Sets the statement that was used for the operation. This is optional and
        is not used in any significant way. It is returned, unmodified, in the
        :py:class:`SystemResult` for convenience.

        :param statement: the statement. This is a optional parameter.
        :type statement: str
        :returns: self.
        :raises IllegalArgumentException: raises the exception if statement is
            not a string.
        """
        CheckValue.check_str(statement, 'statement')
        self._statement = statement
        return self

    def get_statement(self):
        """
        Returns the statement set by :py:meth:`set_statement`, or None if not
        set.

        :returns: the statement.
        :rtype: str
        """
        return self._statement

    def set_timeout(self, timeout_ms):
        """
        Sets the request timeout value, in milliseconds. This overrides any
        default value set in :py:class:`NoSQLHandleConfig`. The value must be
        positive.

        :param timeout_ms: the timeout value, in milliseconds.
        :type timeout_ms: int
        :returns: self.
        :raises IllegalArgumentException: raises the exception if the timeout
            value is less than or equal to 0.
        """
        self._set_timeout(timeout_ms)
        return self

    def get_timeout(self):
        """
        Returns the timeout to use for the operation, in milliseconds. A value
        of 0 indicates that the timeout has not been set.

        :returns: the timeout value.
        :rtype: int
        """
        return super(SystemStatusRequest, self).get_timeout()

    def set_defaults(self, cfg):
        # Use the default request timeout if not set.
        self._check_config(cfg)
        if self.get_timeout() == 0:
            self._set_timeout(cfg.get_default_table_request_timeout())
        return self

    def should_retry(self):
        return True

    def validate(self):
        if self._operation_id is None:
            raise IllegalArgumentException(
                'SystemStatusRequest requires an operation id.')

    @staticmethod
    def create_serializer():
        return SystemStatusRequestSerializer()

    def get_request_name(self):
        # type: () -> str
        return "SystemStatus"


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
        self._statement = None
        self._limits = None

    def __str__(self):
        return ('TableRequest: [name=' + str(self.get_table_name()) +
                ', statement=' + str(self._statement) + ', limits=' +
                str(self._limits) + ']')

    def set_statement(self, statement):
        """
        Sets the query statement to use for the operation. This parameter is
        required unless the operation is intended to change the limits of an
        existing table.

        :param statement: the statement.
        :type statement: str
        :returns: self.
        :raises IllegalArgumentException: raises the exception if statement is
            not a string.
        """
        CheckValue.check_str(statement, 'statement')
        self._statement = statement
        return self

    def get_statement(self):
        """
        Returns the statement, or None if not set.

        :returns: the statement.
        :rtype: str
        """
        return self._statement

    def set_compartment(self, compartment):
        """
        Cloud service only.

        Sets the name or id of a compartment to be used for this operation.

        The compartment may be specified as either a name (or path for nested
        compartments) or as an id (OCID). A name (vs id) can only be used when
        authenticated using a specific user identity. It is *not* available if
        authenticated as an Instance Principal which can be done when calling
        the service from a compute instance in the Oracle Cloud Infrastructure.
        See
        :py:meth:`borneo.iam.SignatureProvider.create_with_instance_principal`.

        :param compartment: the compartment name or id. If using a nested
            compartment, specify the full compartment path
            compartmentA.compartmentB, but exclude the name of the root
            compartment (tenant).
        :type compartment: str
        :returns: self.
        :raises IllegalArgumentException: raises the exception if compartment
            is not a str.
        """
        self.set_compartment_internal(compartment)
        return self

    def set_table_limits(self, table_limits):
        """
        Cloud service only.

        Sets the table limits to use for the operation. Limits are used in only
        2 cases -- table creation statements and limits modification operations.
        It is not used for other DDL operations.

        If limits are set for an on-premise service they are silently ignored.

        :param table_limits: the limits.
        :type table_limits: TableLimits
        :returns: self.
        :raises IllegalArgumentException: raises the exception if table_limits
            is not an instance TableLimits.
        """
        if not isinstance(table_limits, TableLimits):
            raise IllegalArgumentException(
                'set_table_limits requires an instance of TableLimits as ' +
                'parameter.')
        self._limits = table_limits
        return self

    def get_table_limits(self):
        """
        Returns the table limits, or None if not set.

        :returns: the limits.
        :rtype: TableLimits
        """
        return self._limits

    def set_table_name(self, table_name):
        """
        Sets the table name to use for the operation. The table name is only
        used to modify the limits of an existing table, and must not be set for
        any other operation.

        :param table_name: the name of the table.
        :type table_name: str
        :returns: self.
        :raises IllegalArgumentException: raises the exception if table_name is
            not a string.
        """
        super(TableRequest, self).set_table_name(table_name)
        return self

    def set_timeout(self, timeout_ms):
        """
        Sets the request timeout value, in milliseconds. This overrides any
        default value set in :py:class:`NoSQLHandleConfig`. The value must be
        positive.

        :param timeout_ms: the timeout value, in milliseconds.
        :type timeout_ms: int
        :returns: self.
        :raises IllegalArgumentException: raises the exception if the timeout
            value is less than or equal to 0.
        """
        self._set_timeout(timeout_ms)
        return self

    def get_timeout(self):
        """
        Returns the timeout to use for the operation, in milliseconds. A value
        of 0 indicates that the timeout has not been set.

        :returns: the timeout value.
        :rtype: int
        """
        return super(TableRequest, self).get_timeout()

    def set_defaults(self, cfg):
        """
        Internal use only
        """
        # Use the default request timeout if not set.
        self._check_config(cfg)
        if self.get_timeout() == 0:
            self._set_timeout(cfg.get_default_table_request_timeout())

        # Use the default compartment if not set
        if self.get_compartment() is None:
            self.set_compartment_internal(cfg.get_default_compartment())
        return self

    def should_retry(self):
        # Returns True if this request should be retried.
        return False

    def validate(self):
        table_name = self.get_table_name()
        if self._statement is None and table_name is None:
            raise IllegalArgumentException(
                'TableRequest requires statement or TableLimits and name.')
        if self._statement is not None and table_name is not None:
            raise IllegalArgumentException(
                'TableRequest cannot have both table name and statement.')

        if self._limits is not None:
            self._limits.validate()

    @staticmethod
    def create_serializer():
        return TableRequestSerializer()

    def get_request_name(self):
        # type: () -> str
        return "Table"


class TableUsageRequest(Request):
    """
    Cloud service only.

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
        self._start_time = 0
        self._end_time = 0
        self._limit = 0

    def set_table_name(self, table_name):
        """
        Sets the table name to use for the request. This is a required
        parameter.

        :param table_name: the table name.
        :type table_name: str
        :returns: self.
        :raises IllegalArgumentException: raises the exception if table_name is
            not a string.
        """
        super(TableUsageRequest, self).set_table_name(table_name)
        return self

    def set_compartment(self, compartment):
        """
        Cloud service only.

        Sets the name or id of a compartment to be used for this operation.

        The compartment may be specified as either a name (or path for nested
        compartments) or as an id (OCID). A name (vs id) can only be used when
        authenticated using a specific user identity. It is *not* available if
        authenticated as an Instance Principal which can be done when calling
        the service from a compute instance in the Oracle Cloud Infrastructure.
        See
        :py:meth:`borneo.iam.SignatureProvider.create_with_instance_principal`.

        :param compartment: the compartment name or id. If using a nested
            compartment, specify the full compartment path
            compartmentA.compartmentB, but exclude the name of the root
            compartment (tenant).
        :type compartment: str
        :returns: self.
        :raises IllegalArgumentException: raises the exception if compartment
            is not a str.
        """
        self.set_compartment_internal(compartment)
        return self

    def set_start_time(self, start_time):
        """
        Sets the start time to use for the request in milliseconds since the
        Epoch in UTC time or an ISO 8601 formatted string accurate to
        milliseconds. If timezone is not specified it is interpreted as UTC.

        If no time range is set for this request the most recent complete usage
        record is returned.

        :param start_time: the start time.
        :type start_time: str
        :returns: self.
        :raises IllegalArgumentException: raises the exception if start_time is
            a negative number and is not an ISO 8601 formatted string.
        """
        self._check_time(start_time)
        if isinstance(start_time, str):
            start_time = self._iso_time_to_timestamp(start_time)
        self._start_time = start_time
        return self

    def get_start_time(self):
        """
        Returns the start time to use for the request in milliseconds since the
        Epoch.

        :returns: the start time.
        :rtype: int
        """
        return self._start_time

    def get_start_time_string(self):
        """
        Returns the start time as an ISO 8601 formatted string. If the start
        timestamp is not set, None is returned.

        :returns: the start time, or None if not set.
        :rtype: str
        """

        if self._start_time == 0:
            return None
        return datetime.fromtimestamp(
            float(self._start_time) / 1000).replace(tzinfo=tz.UTC).isoformat()

    def set_end_time(self, end_time):
        """
        Sets the end time to use for the request in milliseconds since the Epoch
        in UTC time or an ISO 8601 formatted string accurate to milliseconds. If
        timezone is not specified it is interpreted as UTC.

        If no time range is set for this request the most recent complete usage
        record is returned.

        :param end_time: the end time.
        :type end_time: str
        :returns: self.
        :raises IllegalArgumentException: raises the exception if end_time is a
            negative number and is not an ISO 8601 formatted string.
        """
        self._check_time(end_time)
        if isinstance(end_time, str):
            end_time = self._iso_time_to_timestamp(end_time)
        self._end_time = end_time
        return self

    def get_end_time(self):
        """
        Returns the end time to use for the request in milliseconds since the
        Epoch.

        :returns: the end time.
        :rtype: int
        """
        return self._end_time

    def get_end_time_string(self):
        """
        Returns the end time as an ISO 8601 formatted string. If the end
        timestamp is not set, None is returned.

        :returns: the end time, or None if not set.
        :rtype: str
        """
        if self._end_time == 0:
            return None
        return datetime.fromtimestamp(
            float(self._end_time) / 1000).replace(tzinfo=tz.UTC).isoformat()

    def set_limit(self, limit):
        """
        Sets the limit to the number of usage records desired. If this value is
        0 there is no limit, but not all usage records may be returned in a
        single request due to size limitations.

        :param limit: the numeric limit.
        :type limit: int
        :returns: self.
        :raises IllegalArgumentException: raises the exception if limit is a
            negative number.
        """
        CheckValue.check_int_ge_zero(limit, 'limit')
        self._limit = limit
        return self

    def get_limit(self):
        """
        Returns the limit to the number of usage records desired.

        :returns: the numeric limit.
        :rtype: int
        """
        return self._limit

    def set_timeout(self, timeout_ms):
        """
        Sets the request timeout value, in milliseconds. This overrides any
        default value set in :py:class:`NoSQLHandleConfig`. The value must be
        positive.

        :param timeout_ms: the timeout value, in milliseconds.
        :type timeout_ms: int
        :returns: self.
        :raises IllegalArgumentException: raises the exception if the timeout
            value is less than or equal to 0.
        """
        self._set_timeout(timeout_ms)
        return self

    def get_timeout(self):
        """
        Returns the timeout to use for the operation, in milliseconds. A value
        of 0 indicates that the timeout has not been set.

        :returns: the value.
        :rtype: int
        """
        return super(TableUsageRequest, self).get_timeout()

    def should_retry(self):
        # Returns True if this request should be retried.
        return False

    def validate(self):
        if self.get_table_name() is None:
            raise IllegalArgumentException(
                'TableUsageRequest requires a table name.')
        if self._start_time > self._end_time > 0:
            raise IllegalArgumentException(
                'TableUsageRequest: end time must be greater than start time.')

    @staticmethod
    def create_serializer():
        return TableUsageRequestSerializer()

    @staticmethod
    def _check_time(dt):
        if (not (CheckValue.is_int(dt) or CheckValue.is_long(dt) or
                 CheckValue.is_str(dt)) or
                not CheckValue.is_str(dt) and dt < 0):
            raise IllegalArgumentException(
                'dt must be an integer that is not negative or an ISO ' +
                '8601 formatted string. Got:' + str(dt))

    @staticmethod
    def _iso_time_to_timestamp(dt):
        dt = parser.parse(dt)
        if dt.tzinfo is not None:
            dt = dt.astimezone(tz.UTC)
        return int(mktime(dt.timetuple()) * 1000) + dt.microsecond // 1000

    def get_request_name(self):
        # type: () -> str
        return "TableUsage"


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
        self._ops = list()
        self._durability = None

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
        :returns: self.
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
        table_name = self.get_table_name()
        if table_name is None:
            self.set_table_name(request.get_table_name())
        else:
            if request.get_table_name().lower() != table_name.lower():
                raise IllegalArgumentException(
                    'The table_name used for the operation is different from ' +
                    'that of others: ' + table_name)
        request.validate()
        self._ops.append(self.OperationRequest(request, abort_if_unsuccessful))
        return self

    def set_compartment(self, compartment):
        """
        Cloud service only.

        Sets the name or id of a compartment to be used for this operation.

        The compartment may be specified as either a name (or path for nested
        compartments) or as an id (OCID). A name (vs id) can only be used when
        authenticated using a specific user identity. It is *not* available if
        authenticated as an Instance Principal which can be done when calling
        the service from a compute instance in the Oracle Cloud Infrastructure.
        See
        :py:meth:`borneo.iam.SignatureProvider.create_with_instance_principal`.

        :param compartment: the compartment name or id. If using a nested
            compartment, specify the full compartment path
            compartmentA.compartmentB, but exclude the name of the root
            compartment (tenant).
        :type compartment: str
        :returns: self.
        :raises IllegalArgumentException: raises the exception if compartment
            is not a str.
        """
        self.set_compartment_internal(compartment)
        return self

    def get_request(self, index):
        """
        Returns the Request at the given position, it may be either a
        :py:class:`PutRequest` or :py:class:`DeleteRequest` object.

        :param index: the position of Request to get.
        :type index: int
        :returns: the Request at the given position.
        :rtype: Request
        :raises IndexOutOfBoundsException: raises the exception if the position
            is negative or greater or equal to the number of Requests.
        :raises IllegalArgumentException: raises the exception if index is a
            negative number.
        """
        CheckValue.check_int_ge_zero(index, 'index')
        return self._ops[index].get_request()

    def get_operations(self):
        # Returns the request lists, internal for now
        return self._ops

    def get_num_operations(self):
        """
        Returns the number of Requests.

        :returns: the number of Requests.
        :rtype: int
        """
        return len(self._ops)

    def set_timeout(self, timeout_ms):
        """
        Sets the request timeout value, in milliseconds. This overrides any
        default value set in :py:class:`NoSQLHandleConfig`. The value must be
        positive.

        :param timeout_ms: the timeout value, in milliseconds.
        :type timeout_ms: int
        :returns: self.
        :raises IllegalArgumentException: raises the exception if the timeout
            value is less than or equal to 0.
        """
        self._set_timeout(timeout_ms)
        return self

    def get_timeout(self):
        """
        Returns the timeout to use for the operation, in milliseconds. A value
        of 0 indicates that the timeout has not been set.

        :returns: the timeout value.
        :rtype: int
        """
        return super(WriteMultipleRequest, self).get_timeout()

    def clear(self):
        """
        Removes all of the operations from the WriteMultiple request.
        """
        self.set_table_name(None)
        self._ops = list()

    def set_durability(self, durability):
        """
        On-premise only. Sets the durability to use for the operation.

        :param durability: the Durability to use
        :type durability: Durability
        :returns: self.
        :raises IllegalArgumentException: raises the exception if Durability
            is not valid
        :versionadded: 5.3.0
        """
        if durability is None:
            self._durability = None
            return
        if not isinstance(durability, Durability):
            raise IllegalArgumentException('set_durability requires an ' +
                                           'instance of Durability as parameter.')
        durability.validate()
        self._durability = durability
        return self

    def get_durability(self):
        """
        On-premise only. Gets the durability to use for the operation or
        None if not set
        :returns: the Durability
        :versionadded: 5.3.0
        """
        return self._durability

    def does_reads(self):
        for op in self._ops:
            req = op.get_request()
            if req.does_reads():
                return True
        return False

    def does_writes(self):
        return True

    def validate(self):
        if not self._ops:
            raise IllegalArgumentException('The requests list is empty.')

    @staticmethod
    def create_serializer():
        return WriteMultipleRequestSerializer()

    class OperationRequest(object):

        # A wrapper of WriteRequest that contains an additional flag
        # abort_if_unsuccessful. Internal for now
        def __init__(self, request, abort_if_unsuccessful):
            self._request = request
            self._abort_if_unsuccessful = abort_if_unsuccessful

        def get_request(self):
            return self._request

        def is_abort_if_unsuccessful(self):
            return self._abort_if_unsuccessful

    def get_request_name(self):
        # type: () -> str
        return "WriteMultiple"


class Result(object):
    """
    Result is a base class for result classes for all supported operations.
    All state and methods are maintained by extending classes.
    """

    def __init__(self):
        """
        read_kb and read_units will be different in the case of Absolute
        Consistency. write_kb and write_units will always be equal.
        """
        self._rate_limit_delayed_ms = 0
        self._read_kb = 0
        self._read_units = 0
        self._retry_stats = None
        self._write_kb = 0

    def get_rate_limit_delayed_ms(self):
        """
        Get the time the operation was delayed due to rate limiting. Cloud only.
        If rate limiting is in place, this value will represent the number of
        milliseconds that the operation was delayed due to rate limiting. If the
        value is zero, rate limiting did not apply or the operation did not need
        to wait for rate limiting.

        :returns: delay time in milliseconds.
        """
        return self._rate_limit_delayed_ms

    def get_read_units(self):
        # Internal use only.
        return self._read_units

    def get_retry_stats(self):
        """
        Returns a stats object with information about retries.

        :returns: stats object with retry information, or None if no retries
            were performed.
        """
        return self._retry_stats

    def get_write_units(self):
        # Internal use only.
        return self._write_kb

    def set_rate_limit_delayed_ms(self, delay_ms):
        """
        :param delay_ms: the delay in milliseconds.
        :type delay_ms: int
        :returns: self.
        """
        self._rate_limit_delayed_ms = delay_ms
        return self

    def set_read_kb(self, read_kb):
        self._read_kb = read_kb
        return self

    def set_read_units(self, read_units):
        self._read_units = read_units
        return self

    def set_retry_stats(self, retry_stats):
        """
        Internal use only.

        :param retry_stats: the stats object to use.
        :type retry_stats: RetryStats
        """
        self._retry_stats = retry_stats

    def set_write_kb(self, write_kb):
        self._write_kb = write_kb
        return self

    def _get_read_kb(self):
        return self._read_kb

    def _get_write_kb(self):
        return self._write_kb


class WriteResult(Result):
    """
    A base class for results of single row modifying operations such as put and
    delete.
    """

    def __init__(self):
        super(WriteResult, self).__init__()
        self._existing_version = None
        self._existing_value = None
        self._existing_modification_time = 0

    def set_existing_value(self, existing_value):
        self._existing_value = existing_value
        return self

    def set_existing_version(self, existing_version):
        self._existing_version = existing_version
        return self

    def set_existing_modification_time(self, existing_modification_time):
        self._existing_modification_time = existing_modification_time
        return self

    def _get_existing_value(self):
        return self._existing_value

    def _get_existing_version(self):
        return self._existing_version

    def _get_existing_modification_time(self):
        return self._existing_modification_time


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
        self._success = False

    def __str__(self):
        return str(self._success)

    def set_success(self, success):
        self._success = success
        return self

    def get_success(self):
        """
        Returns True if the delete operation succeeded.

        :returns: True if the operation succeeded.
        :rtype: bool
        """
        return self._success

    def get_existing_value(self):
        """
        Returns the existing row value if available. It will be available if the
        target row exists and the operation failed because of a
        :py:class:`Version` mismatch and the corresponding
        :py:class:`DeleteRequest` the method
        :py:meth:`DeleteRequest.set_return_row` was called with a True value.

        :returns: the value.
        :rtype: dict
        """
        return self._get_existing_value()

    def get_existing_version(self):
        """
        Returns the existing row :py:class:`Version` if available. It will be
        available if the target row exists and the operation failed because of a
        :py:class:`Version` mismatch and the corresponding
        :py:class:`DeleteRequest` the method
        :py:meth:`DeleteRequest.set_return_row` was called with a True value.

        :returns: the version.
        :rtype: Version
        """
        return self._get_existing_version()

    def get_existing_modification_time(self):
        """
        Returns the existing row modification time if available. It will be
        available if the target row exists and the operation failed because of a
        :py:class:`Version` mismatch and the corresponding
        :py:class:`DeleteRequest` the method
        :py:meth:`DeleteRequest.set_return_row` was called with a True value.

        :returns: the modification time in milliseconds since January 1, 1970
        :rtype: int
        :versionadded: 5.3.0
        """
        return self._get_existing_modification_time()

    def get_read_kb(self):
        """
        Returns the read throughput consumed by this operation, in KBytes. This
        is the actual amount of data read by the operation. The number of read
        units consumed is returned by :py:meth:`get_read_units` which may be a
        larger number because this was an update operation.

        :returns: the read KBytes consumed.
        :rtype: int
        """
        return self._get_read_kb()

    def get_read_units(self):
        """
        Returns the read throughput consumed by this operation, in read units.
        This number may be larger than that returned by :py:meth:`get_read_kb`
        because it was an update operation.

        :returns: the read units consumed.
        :rtype: int
        """
        return super(DeleteResult, self).get_read_units()

    def get_write_kb(self):
        """
        Returns the write throughput consumed by this operation, in KBytes.

        :returns: the write KBytes consumed.
        :rtype: int
        """
        return self._get_write_kb()

    def get_write_units(self):
        """
        Returns the write throughput consumed by this operation, in write units.

        :returns: the write units consumed.
        :rtype: int
        """
        return super(DeleteResult, self).get_write_units()


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
        self._value = None
        self._version = None
        self._expiration_time = 0
        self._modification_time = 0

    def __str__(self):
        return 'None' if self._value is None else str(self._value)

    def set_value(self, value):
        # Sets the value of this object, internal.
        self._value = value
        return self

    def get_value(self):
        """
        Returns the value of the returned row, or None if the row does not
        exist.

        :returns: the value of the row, or None if it does not exist.
        :rtype: dict
        """
        return self._value

    def set_version(self, version):
        # Sets the version, internal.
        self._version = version
        return self

    def get_version(self):
        """
        Returns the :py:class:`Version` of the row if the operation was
        successful, or None if the row does not exist.

        :returns: the version of the row, or None if the row does not exist.
        :rtype: Version
        """
        return self._version

    def set_expiration_time(self, expiration_time):
        # Sets the expiration time, internal
        self._expiration_time = expiration_time
        return self

    def get_expiration_time(self):
        """
        Returns the expiration time of the row. A zero value indicates that the
        row does not expire. This value is valid only if the operation
        successfully returned a row (:py:meth:`get_value` returns non-none).

        :returns: the expiration time in milliseconds since January 1, 1970, or
            zero if the row never expires or the row does not exist.
        :rtype: int
        """
        return self._expiration_time

    def set_modification_time(self, modification_time):
        # Sets the modification time, internal
        self._modification_time = modification_time
        return self

    def get_modification_time(self):
        """
        Returns the modification time of the row. This value is valid only if
        the operation successfully returned a row (:py:meth:`get_value` returns non-none).

        :returns: the modification time in milliseconds since January 1, 1970
        :rtype: int
        """
        return self._modification_time

    def get_read_kb(self):
        """
        Returns the read throughput consumed by this operation, in KBytes. This
        is the actual amount of data read by the operation. The number of read
        units consumed is returned by :py:meth:`get_read_units` which may be a
        larger number if the operation used Consistency.ABSOLUTE.

        :returns: the read KBytes consumed.
        :rtype: int
        """
        return self._get_read_kb()

    def get_read_units(self):
        """
        Returns the read throughput consumed by this operation, in read units.
        This number may be larger than that returned by :py:meth:`get_read_kb`
        if the operation used Consistency.ABSOLUTE.

        :returns: the read units consumed.
        :rtype: int
        """
        return super(GetResult, self).get_read_units()

    def get_write_kb(self):
        """
        Returns the write throughput consumed by this operation, in KBytes.

        :returns: the write KBytes consumed.
        :rtype: int
        """
        return self._get_write_kb()

    def get_write_units(self):
        """
        Returns the write throughput consumed by this operation, in write units.

        :returns: the write units consumed.
        :rtype: int
        """
        return super(GetResult, self).get_write_units()


class GetIndexesResult(Result):
    """
    Represents the result of a :py:meth:`NoSQLHandle.get_indexes` operation.

    On a successful operation the index information is returned in a list of
    IndexInfo.
    """

    def __init__(self):
        super(GetIndexesResult, self).__init__()
        self._indexes = None

    def __str__(self):
        idxes = ''
        for index in range(len(self._indexes)):
            idxes += str(self._indexes[index])
            if index < len(self._indexes) - 1:
                idxes += ','
        return '[' + idxes + ']'

    def set_indexes(self, indexes):
        self._indexes = indexes
        return self

    def get_indexes(self):
        """
        Returns the list of index information returned by the operation.

        :returns: the indexes information.
        :rtype: list(IndexInfo)
        """
        return self._indexes


class ListTablesResult(Result):
    """
    Represents the result of a :py:meth:`NoSQLHandle.list_tables` operation.

    On a successful operation the table names are available as well as the
    index of the last returned table. Tables are returned in a list, sorted
    alphabetically.
    """

    def __init__(self):
        super(ListTablesResult, self).__init__()
        self._tables = None
        self._last_index_returned = 0

    def __str__(self):
        return '[' + ','.join(self._tables) + ']'

    def set_tables(self, tables):
        self._tables = tables
        return self

    def get_tables(self):
        """
        Returns the array of table names returned by the operation.

        :returns: the table names.
        :rtype: list(str)
        """
        return self._tables

    def set_last_index_returned(self, last_index_returned):
        self._last_index_returned = last_index_returned
        return self

    def get_last_returned_index(self):
        """
        Returns the index of the last table name returned. This can be provided
        to :py:class:`ListTablesRequest` to be used as a starting point for
        listing tables.

        :returns: the index.
        :rtype: int
        """
        return self._last_index_returned


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
        self._continuation_key = None
        self._num_deleted = 0

    def __str__(self):
        return 'Deleted ' + str(self._num_deleted) + ' rows.'

    def set_continuation_key(self, continuation_key):
        self._continuation_key = continuation_key
        return self

    def get_continuation_key(self):
        """
        Returns the continuation key where the next MultiDelete request resume
        from.

        :returns: the continuation key, or None if there are no more rows to
            delete.
        :rtype: bytearray
        """
        return self._continuation_key

    def set_num_deletions(self, num_deleted):
        self._num_deleted = num_deleted
        return self

    def get_num_deletions(self):
        """
        Returns the number of rows deleted from the table.

        :returns: the number of rows deleted.
        :rtype: int
        """
        return self._num_deleted

    def get_read_kb(self):
        """
        Returns the read throughput consumed by this operation, in KBytes. This
        is the actual amount of data read by the operation. The number of read
        units consumed is returned by :py:meth:`get_read_units` which may be a
        larger number because this was an update operation.

        :returns: the read KBytes consumed.
        :rtype: int
        """
        return self._get_read_kb()

    def get_read_units(self):
        """
        Returns the read throughput consumed by this operation, in read units.
        This number may be larger than that returned by :py:meth:`get_read_kb`
        because it was an update operation.

        :returns: the read units consumed.
        :rtype: int
        """
        return super(MultiDeleteResult, self).get_read_units()

    def get_write_kb(self):
        """
        Returns the write throughput consumed by this operation, in KBytes.

        :returns: the write KBytes consumed.
        :rtype: int
        """
        return self._get_write_kb()

    def get_write_units(self):
        """
        Returns the write throughput consumed by this operation, in write units.

        :returns: the write units consumed.
        :rtype: int
        """
        return super(MultiDeleteResult, self).get_write_units()


class PrepareResult(Result):
    """
    The result of a prepare operation. The returned
    :py:class:`PreparedStatement` can be re-used for query execution using
    :py:meth:`QueryRequest.set_prepared_statement`
    """

    def __init__(self):
        super(PrepareResult, self).__init__()
        self._prepared_statement = None

    def set_prepared_statement(self, prepared_statement):
        # Sets the prepared statement.
        self._prepared_statement = prepared_statement
        return self

    def get_prepared_statement(self):
        """
        Returns the value of the prepared statement.

        :returns: the value of the prepared statement.
        :rtype: PreparedStatement
        """
        return self._prepared_statement

    def get_read_kb(self):
        """
        Returns the read throughput consumed by this operation, in KBytes. This
        is the actual amount of data read by the operation. The number of read
        units consumed is returned by :py:meth:`get_read_units` which may be a
        larger number if the operation used Consistency.ABSOLUTE.

        :returns: the read KBytes consumed.
        :rtype: int
        """
        return self._get_read_kb()

    def get_read_units(self):
        """
        Returns the read throughput consumed by this operation, in read units.
        This number may be larger than that returned by :py:meth:`get_read_kb`
        if the operation used Consistency.ABSOLUTE.

        :returns: the read units consumed.
        :rtype: int
        """
        return super(PrepareResult, self).get_read_units()

    def get_write_kb(self):
        """
        Returns the write throughput consumed by this operation, in KBytes.

        :returns: the write KBytes consumed.
        :rtype: int
        """
        return self._get_write_kb()

    def get_write_units(self):
        """
        Returns the write throughput consumed by this operation, in write units.

        :returns: the write units consumed.
        :rtype: int
        """
        return super(PrepareResult, self).get_write_units()


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
        self._version = None
        self._generated_value = None

    def __str__(self):
        return 'None Version' if self._version is None else str(self._version)

    def set_version(self, version):
        self._version = version
        return self

    def get_version(self):
        """
        Returns the :py:class:`Version` of the new row if the operation was
        successful. If the operation failed None is returned.

        :returns: the :py:class:`Version` on success, None on failure.
        :rtype: Version
        """
        return self._version

    def set_generated_value(self, value):
        self._generated_value = value
        return self

    def get_generated_value(self):
        """
        Returns the value generated if the operation created a new value. This
        can happen if the table contains an identity column or string column
        declared as a generated UUID. If the table has no such columns this
        value is None. If a value was generated for the operation, it is
        non-None.

        :returns: the generated value.
        """
        return self._generated_value

    def get_existing_version(self):
        """
        Returns the existing row :py:class:`Version` if available. This value
        will only be available if the conditional put operation failed and the
        request specified that return information be returned using
        :py:meth:`PutRequest.set_return_row`.

        :returns: the :py:class:`Version`.
        :rtype: Version
        """
        return self._get_existing_version()

    def get_existing_value(self):
        """
        Returns the existing row value if available. This value will only be
        available if the conditional put operation failed and the request
        specified that return information be returned using
        :py:meth:`PutRequest.set_return_row`.

        :returns: the value.
        :rtype: dict
        """
        return self._get_existing_value()

    def get_existing_modification_time(self):
        """
        Returns the existing row modification time if available. It will be
        available if the conditional put operation failed and the request
        specified that return information be returned using
        :py:meth:`PutRequest.set_return_row`. A value of -1 indicates this
        feature is not available at the connected server.

        :returns: the modification time in milliseconds since January 1, 1970
        :rtype: int
        :versionadded: 5.3.0
        """
        return self._get_existing_modification_time()

    def get_read_kb(self):
        """
        Returns the read throughput consumed by this operation, in KBytes.
        This is the actual amount of data read by the operation. The number of
        read units consumed is returned by :py:meth:`get_read_units` which may
        be a larger number because this was an update operation.

        :returns: the read KBytes consumed.
        :rtype: int
        """
        return self._get_read_kb()

    def get_read_units(self):
        """
        Returns the read throughput consumed by this operation, in read units.
        This number may be larger than that returned by :py:meth:`get_read_kb`
        because it was an update operation.

        :returns: the read units consumed.
        :rtype: int
        """
        return super(PutResult, self).get_read_units()

    def get_write_kb(self):
        """
        Returns the write throughput consumed by this operation, in KBytes.

        :returns: the write KBytes consumed.
        :rtype: int
        """
        return self._get_write_kb()

    def get_write_units(self):
        """
        Returns the write throughput consumed by this operation, in write units.

        :returns: the write units consumed.
        :rtype: int
        """
        return super(PutResult, self).get_write_units()


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

    A single QueryResult does not imply that all results for the query have been
    returned. If the value returned by :py:meth:`QueryRequest.is_done` is False
    there are additional results available. This can happen even if there are no
    values in the returned QueryResult. The best way to use
    :py:class:`QueryRequest` and :py:class:`QueryResult` is to perform
    operations in a loop, for example:

    .. code-block:: pycon

        handle = ...
        request = QueryRequest().set_statement('SELECT * FROM foo')
        while True:
            result = handle.query(request)
            results = result.get_results()
            # do something with the results
            if request.is_done():
                break

    Modification queries either return values based on a RETURNING clause or, by
    default, return the number of rows affected by the statement in a
    dictionary. INSERT queries with no RETURNING clause return a dictionary
    indicating the number of rows inserted, for example {'NumRowsInserted': 5}.
    UPDATE queries with no RETURNING clause return a dictionary indicating the
    number of rows updated, for example {'NumRowsUpdated': 3}. DELETE queries
    with no RETURNING clause return a dictionary indicating the number of rows
    deleted, for example {'numRowsDeleted': 2}.
    """

    def __init__(self, request, computed=True):
        super(QueryResult, self).__init__()
        self._request = request
        self._results = None
        self._continuation_key = None
        # The following 6 fields are used only for "internal" QueryResults,
        # i.e., those received and processed by the ReceiveIter.

        self._reached_limit = False
        self._is_computed = computed
        # The following 4 fields are used during phase 1 of a sorting
        # ALL_PARTITIONS query. In this case, self._results may store results
        # from multiple partitions. If so, self._results are grouped by
        # partition and self._pids, self._num_results_per_pid, and
        # self._continuation_keys fields store the partition id, the number of
        # results, and the continuation key per partition. Finally, the
        # self._is_in_phase1 specifies whether phase 1 is done.
        self._is_in_phase1 = False
        self._num_results_per_pid = None
        self._continuation_keys = None
        self._pids = None

    def __str__(self):
        self._compute()
        if self._results is None:
            return None
        res = 'Number of query results: ' + str(len(self._results))
        for result in self._results:
            res += '\n' + str(result)
        return res + '\n'

    def set_results(self, results):
        self._results = results
        return self

    def get_results(self):
        """
        Returns a list of results for the query. It is possible to have an empty
        list and a non-none continuation key.

        :returns: a list of results for the query.
        :rtype: list(dict)
        """
        self._compute()
        return self._results

    def get_results_internal(self):
        return self._results

    def set_continuation_key(self, continuation_key):
        self._continuation_key = continuation_key
        return self

    def get_continuation_key(self):
        """
        Returns the continuation key that can be used to obtain more results
        if non-none.

        :returns: the continuation key, or None if there are no further values
            to return.
        :rtype: bytearray
        """
        self._compute()
        return self._continuation_key

    def set_reached_limit(self, reached_limit):
        self._reached_limit = reached_limit
        return self

    def reached_limit(self):
        return self._reached_limit

    def get_request(self):
        return self._request

    def set_computed(self, computed):
        self._is_computed = computed
        return self

    def set_is_in_phase1(self, is_in_phase1):
        self._is_in_phase1 = is_in_phase1

    def is_in_phase1(self):
        return self._is_in_phase1

    def set_num_results_per_pid(self, num_results_per_pid):
        self._num_results_per_pid = num_results_per_pid

    def get_num_partition_results(self, i):
        return self._num_results_per_pid[i]

    def set_partition_cont_keys(self, continuation_keys):
        self._continuation_keys = continuation_keys

    def get_partition_cont_key(self, i):
        return self._continuation_keys[i]

    def set_pids(self, pids):
        self._pids = pids

    def get_num_pids(self):
        return 0 if self._pids is None else len(self._pids)

    def get_pid(self, i):
        return self._pids[i]

    def get_read_kb(self):
        """
        Returns the read throughput consumed by this operation, in KBytes. This
        is the actual amount of data read by the operation. The number of read
        units consumed is returned by :py:meth:`get_read_units` which may be a
        larger number if the operation used Consistency.ABSOLUTE.

        :returns: the read KBytes consumed.
        :rtype: int
        """
        self._compute()
        return self._get_read_kb()

    def get_read_units(self):
        """
        Returns the read throughput consumed by this operation, in read units.
        This number may be larger than that returned by :py:meth:`get_read_kb`
        if the operation used Consistency.ABSOLUTE.

        :returns: the read units consumed.
        :rtype: int
        """
        self._compute()
        return super(QueryResult, self).get_read_units()

    def get_write_kb(self):
        """
        Returns the write throughput consumed by this operation, in KBytes.

        :returns: the write KBytes consumed.
        :rtype: int
        """
        self._compute()
        return self._get_write_kb()

    def get_write_units(self):
        """
        Returns the write throughput consumed by this operation, in write units.

        :returns: the write units consumed.
        :rtype: int
        """
        self._compute()
        return super(QueryResult, self).get_write_units()

    def _compute(self):
        if self._is_computed:
            return
        driver = self._request.get_driver()
        driver.compute(self)
        self._is_computed = True
        # If the original request specified rate limiting, apply the used
        # read/write units to the limiter(s) here.
        if self._request is not None:
            read_limiter = self._request.get_read_rate_limiter()
            if read_limiter is not None:
                print(self.get_read_units())
                read_limiter.consume_units_unconditionally(
                    self.get_read_units())
            write_limiter = self._request.get_write_rate_limiter()
            if write_limiter is not None:
                write_limiter.consume_units_unconditionally(
                    self.get_write_units())


class SystemResult(Result):
    """
    On-premise only.

    SystemResult is returned from :py:meth:`NoSQLHandle.system_status` and
    :py:meth:`NoSQLHandle.system_request` operations. It encapsulates the state
    of the operation requested.

    Some operations performed by :py:meth:`NoSQLHandle.system_request` are
    asynchronous. When such an operation has been performed it is necessary to
    call :py:meth:`NoSQLHandle.system_status` until the status of the operation
    is known. The method :py:meth:`wait_for_completion` exists to perform this
    task and should be used whenever possible.

    Asynchronous operations (e.g. create namespace) can be distinguished from
    synchronous system operations in this way:

        Asynchronous operations may return a non-none operation id.\n
        Asynchronous operations modify state, while synchronous operations are
        read-only.\n
        Synchronous operations return a state of STATE.COMPLETE and have a
        non-none result string.

    :py:meth:`NoSQLHandle.system_status` is synchronous, returning the known
    state of the operation. It should only be called if the operation was
    asynchronous and returned a non-none operation id.
    """

    def __init__(self):
        super(SystemResult, self).__init__()
        self._operation_id = None
        self._result_string = None
        self._state = 0
        self._statement = None

    def __str__(self):
        return ('SystemResult [statement=' + self._statement + ', state=' +
                BinaryProtocol.get_operation_state(self._state) +
                ', operation_id=' + self._operation_id + ', result_string=' +
                self._result_string + ']')

    def set_operation_id(self, operation_id):
        # Sets the operation id for the operation.
        self._operation_id = operation_id
        return self

    def get_operation_id(self):
        """
        Returns the operation id for the operation if it was asynchronous. This
        is None if the request did not generate a new operation and/or the
        operation state is SystemState.COMPLETE. The value can be used in
        :py:meth:`SystemStatusRequest.set_operation_id` to get status and find
        potential errors resulting from the operation.

        This method is only useful for the result of asynchronous operations.

        :returns: the operation id.
        :rtype: str
        """
        return self._operation_id

    def set_state(self, state):
        # Sets the operation state.
        self._state = state
        return self

    def get_operation_state(self):
        """
        Returns the operation state.

        :returns: the state.
        :rtype: int
        """
        return self._state

    def set_result_string(self, result_string):
        # Sets the result string for the operation.
        self._result_string = result_string
        return self

    def get_result_string(self):
        """
        Returns the result string for the operation. This is None if the request
        was asynchronous or did not return an actual result. For example the
        "show" operations return a non-none result string, but "create, drop,
        grant, etc" operations return a none result string.

        :returns: the result string.
        :rtype: str
        """
        return self._result_string

    def set_statement(self, statement):
        # Sets the statement to use for the operation.
        self._statement = statement
        return self

    def get_statement(self):
        """
        Returns the statement used for the operation.

        :returns: the statement.
        :rtype: str
        """
        return self._statement

    def wait_for_completion(self, handle, wait_millis, delay_millis):
        """
        Waits for the operation to be complete. This is a blocking, polling
        style wait that delays for the specified number of milliseconds between
        each polling operation.

        This instance is modified with any changes in state.

        :param handle: the NoSQLHandle to use. This is required.
        :type handle: NoSQLHandle
        :param wait_millis: the total amount of time to wait, in milliseconds.
            This value must be non-zero and greater than delay_millis. This is
            required.
        :type wait_millis: int
        :param delay_millis: the amount of time to wait between polling
            attempts, in milliseconds. If 0 it will default to 500. This is
            required.
        :type delay_millis: int
        :raises IllegalArgumentException: raises the exception if the operation
            times out or the parameters are not valid.
        """
        if self._state == SystemState.COMPLETE:
            return
        default_delay = 500
        delay_ms = delay_millis if delay_millis != 0 else default_delay
        if wait_millis < delay_millis:
            raise IllegalArgumentException(
                'Wait milliseconds must be a minimum of ' + str(default_delay) +
                ' and greater than delay milliseconds.')
        start_time = int(round(time() * 1000))
        delay_s = float(delay_ms) / 1000
        system_status = SystemStatusRequest().set_operation_id(
            self._operation_id)
        res = None
        while True:
            cur_time = int(round(time() * 1000))
            if cur_time - start_time > wait_millis:
                raise RequestTimeoutException(
                    'Operation not completed within timeout: ' +
                    self._statement, wait_millis)
            if res is not None:
                # only delay after the first get_table.
                sleep(delay_s)
            res = handle.system_status(system_status)
            # do partial copy of new state.
            # statement and operation_id are not changed.
            self._result_string = res.get_result_string()
            self._state = res.get_operation_state()
            if self._state == SystemState.COMPLETE:
                break


class TableResult(Result):
    """
    TableResult is returned from :py:meth:`NoSQLHandle.get_table` and
    :py:meth:`NoSQLHandle.table_request` operations. It encapsulates the state
    of the table that is the target of the request.

    Operations available in :py:meth:`NoSQLHandle.table_request` such as table
    creation, modification, and drop are asynchronous operations. When such an
    operation has been performed, it is necessary to call
    :py:meth:`NoSQLHandle.get_table` until the status of the table is
    State.ACTIVE, State.DROPPED or there is an error condition. The method
    :py:meth:`wait_for_completion` exists to perform this task and should
    be used to wait for an operation to complete.

    :py:meth:`NoSQLHandle.get_table` is synchronous, returning static
    information about the table as well as its current state.
    """

    def __init__(self):
        super(TableResult, self).__init__()
        self._compartment_id = None
        self._table_name = None
        self._state = None
        self._limits = None
        self._schema = None
        self._operation_id = None

    def __str__(self):
        return ('table ' + str(self._table_name) + '[' + self._state + '] ' +
                str(self._limits) + ' schema [' + str(self._schema) +
                '] operation_id = ' + str(self._operation_id))

    def set_compartment_id(self, compartment_id):
        # Internal use only.
        self._compartment_id = compartment_id
        return self

    def get_compartment_id(self):
        """
        Cloud service only.

        Returns compartment id of the target table.

        :returns: compartment id.
        :rtype: str
        """
        return self._compartment_id

    def set_table_name(self, table_name):
        self._table_name = table_name
        return self

    def get_table_name(self):
        """
        Returns the table name of the target table.

        :returns: the table name.
        :rtype: str
        """
        return self._table_name

    def set_state(self, state):
        self._state = state
        return self

    def get_state(self):
        """
        Returns the table state. A table in state State.ACTIVE or State.UPDATING
        is usable for normal operation.

        :returns: the state.
        :rtype: State
        """
        return self._state

    def set_table_limits(self, limits):
        self._limits = limits
        return self

    def get_table_limits(self):
        """
        Returns the throughput and capacity limits for the table. Limits from an
        on-premise service will always be None.

        :returns: the limits.
        :rtype: TableLimits
        """
        return self._limits

    def set_schema(self, schema):
        self._schema = schema
        return self

    def get_schema(self):
        """
        Returns the schema for the table.

        :returns: the schema for the table.
        :rtype: str
        """
        return self._schema

    def set_operation_id(self, operation_id):
        self._operation_id = operation_id
        return self

    def get_operation_id(self):
        """
        Returns the operation id for an asynchronous operation. This is none if
        the request did not generate a new operation. The value can be used in
        :py:meth:`set_operation_id` to find potential errors resulting from the
        operation.

        :returns: the operation id for an asynchronous operation.
        :rtype: str
        """
        return self._operation_id

    def wait_for_completion(self, handle, wait_millis, delay_millis):
        """
        Waits for a table operation to complete. Table operations are
        asynchronous. This is a blocking, polling style wait that delays for the
        specified number of milliseconds between each polling operation. This
        call returns when the table reaches a *terminal* state, which is either
        State.ACTIVE or State.DROPPED.

        This instance must be the return value of a previous
        :py:meth:`NoSQLHandle.table_request` and contain a non-none operation id
        representing the in-progress operation unless the operation has already
        completed.

        This instance is modified with any change in table state or metadata.

        :param handle: the NoSQLHandle to use.
        :type handle: NoSQLHandle
        :param wait_millis: the total amount of time to wait, in milliseconds.
            This value must be non-zero and greater than delay_millis.
        :type wait_millis: int
        :param delay_millis: the amount of time to wait between polling
            attempts, in milliseconds. If 0 it will default to 500.
        :type delay_millis: int
        :raises IllegalArgumentException: raises the exception if the parameters
            are not valid.
        :raises RequestTimeoutException: raises the exception if the operation
            times out.
        """
        terminal = [State.ACTIVE, State.DROPPED]
        if self._state in terminal:
            return
        if self._operation_id is None:
            raise IllegalArgumentException('Operation id must not be none.')
        default_delay = 500
        delay_ms = delay_millis if delay_millis != 0 else default_delay
        if wait_millis < delay_millis:
            raise IllegalArgumentException(
                'Wait milliseconds must be a minimum of ' + str(default_delay) +
                ' and greater than delay milliseconds')
        start_time = int(round(time() * 1000))
        delay_s = float(delay_ms) / 1000
        get_table = GetTableRequest().set_table_name(
            self._table_name).set_operation_id(
            self._operation_id).set_compartment(self._compartment_id)
        res = None
        while True:
            cur_time = int(round(time() * 1000))
            if cur_time - start_time > wait_millis:
                raise RequestTimeoutException(
                    'Operation not completed in expected time', wait_millis)
            if res is not None:
                # only delay after the first get_table.
                sleep(delay_s)
            res = handle.get_table(get_table)
            # partial "copy" of possibly modified state. Don't modify
            # operationId as that is what we are waiting to complete.
            self._state = res.get_state()
            self._limits = res.get_table_limits()
            self._schema = res.get_schema()
            if self._state in terminal:
                break


class TableUsageResult(Result):
    """
    Cloud service only.

    TableUsageResult is returned from :py:meth:`NoSQLHandle.get_table_usage`.
    It encapsulates the dynamic state of the requested table.
    """

    def __init__(self):
        super(TableUsageResult, self).__init__()
        self._table_name = None
        self._usage_records = None

    def __str__(self):
        if self._usage_records is None:
            records_str = 'None'
        else:
            records_str = ''
            for index in range(len(self._usage_records)):
                records_str += str(self._usage_records[index])
                if index < len(self._usage_records) - 1:
                    records_str += ', '
        return ('TableUsageResult [table=' + str(self._table_name) +
                '] [table_usage=[' + records_str + ']]')

    def set_table_name(self, table_name):
        self._table_name = table_name
        return self

    def get_table_name(self):
        """
        Returns the table name used by the operation.

        :returns: the table name.
        :rtype: str
        """
        return self._table_name

    def set_usage_records(self, records):
        self._usage_records = records
        return self

    def get_usage_records(self):
        """
        Returns a list of usage records based on the parameters of the
        :py:class:`TableUsageRequest` used.

        :returns: an list of usage records.
        :type: list(TableUsage)
        """
        return self._usage_records


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
        self._results = list()
        self._failed_operation_index = -1

    def __str__(self):
        if self.get_success():
            return 'WriteMultiple, num results: ' + str(len(self._results))
        return ('WriteMultiple aborted, the failed operation index: ' +
                str(self._failed_operation_index))

    def add_result(self, result):
        self._results.append(result)

    def get_results(self):
        """
        Returns the list of execution results for the operations.

        :returns: the list of execution results.
        :rtype: list(OperationResult)
        """
        return self._results

    def get_failed_operation_result(self):
        """
        Returns the result of the operation that results in the entire
        WriteMultiple operation aborting.

        :returns: the result of the operation, None if not set.
        :rtype: OperationResult or None
        """
        if self._failed_operation_index == -1 or not self._results:
            return None
        return self._results[0]

    def set_failed_operation_index(self, index):
        self._failed_operation_index = index

    def get_failed_operation_index(self):
        """
        Returns the index of failed operation that results in the entire
        WriteMultiple operation aborting.

        :returns: the index of operation, -1 if not set.
        :rtype: int
        """
        return self._failed_operation_index

    def get_success(self):
        """
        Returns True if the WriteMultiple operation succeeded, or False if the
        operation is aborted due to the failure of a sub operation.

        The failed operation index can be accessed using
        :py:meth:`get_failed_operation_index` and its result can be accessed
        using :py:meth:`get_failed_operation_result`.

        :returns: True if the operation succeeded.
        :rtype: bool
        """
        return self._failed_operation_index == -1

    def size(self):
        """
        Returns the number of results.

        :returns: the number of results.
        :rtype: int
        """
        return len(self._results)

    def get_read_kb(self):
        """
        Returns the read throughput consumed by this operation, in KBytes. This
        is the actual amount of data read by the operation. The number of read
        units consumed is returned by :py:meth:`get_read_units` which may be a
        larger number because this was an update operation.

        :returns: the read KBytes consumed.
        :rtype: int
        """
        return self._get_read_kb()

    def get_read_units(self):
        """
        Returns the read throughput consumed by this operation, in read units.
        This number may be larger than that returned by :py:meth:`get_read_kb`
        because it was an update operation.

        :returns: the read units consumed.
        :rtype: int
        """
        return super(WriteMultipleResult, self).get_read_units()

    def get_write_kb(self):
        """
        Returns the write throughput consumed by this operation, in KBytes.

        :returns: the write KBytes consumed.
        :rtype: int
        """
        return self._get_write_kb()

    def get_write_units(self):
        """
        Returns the write throughput consumed by this operation, in write units.

        :returns: the write units consumed.
        :rtype: int
        """
        return super(WriteMultipleResult, self).get_write_units()


class OperationResult(WriteResult):
    """
    A single Result associated with the execution of an individual operation
    in a :py:meth:`NoSQLHandle.write_multiple` request. A list of
    OperationResult is contained in :py:class:`WriteMultipleResult` and obtained
    using :py:meth:`WriteMultipleResult.get_results`.
    """

    def __init__(self):
        super(OperationResult, self).__init__()
        self._version = None
        self._success = False
        self._generated_value = None

    def __str__(self):
        return ('Success: ' + str(self._success) + ', version: ' +
                str(self._version) + ', existing version: ' +
                str(self.get_existing_version()) + ', existing value: ' +
                str(self.get_existing_value()) + ', generated value: ' +
                str(self._generated_value))

    def set_version(self, version):
        self._version = version
        return self

    def get_version(self):
        """
        Returns the version of the new row for put operation, or None if put
        operations did not succeed or the operation is delete operation.

        :returns: the version.
        :rtype: Version
        """
        return self._version

    def set_success(self, success):
        self._success = success
        return self

    def get_success(self):
        """
        Returns the flag indicates whether the operation succeeded. A put or
        delete operation may be unsuccessful if the condition is not
        matched.

        :returns: True if the operation succeeded.
        :rtype: bool
        """
        return self._success

    def set_generated_value(self, value):
        self._generated_value = value
        return self

    def get_generated_value(self):
        """
        Returns the value generated if the operation created a new value. This
        can happen if the table contains an identity column or string column
        declared as a generated UUID. If the table has no such columns this
        value is None. If a value was generated for the operation, it is
        non-None.

        This value is only valid for a put operation on a table with an identity
        column or string as uuid column.

        :returns: the generated value.
        """
        return self._generated_value

    def get_existing_version(self):
        """
        Returns the existing row version associated with the key if
        available.

        :returns: the existing row version
        :rtype: Version
        """
        return self._get_existing_version()

    def get_existing_value(self):
        """
        Returns the previous row value associated with the key if available.

        :returns: the previous row value
        :rtype: dict
        """
        return self._get_existing_value()

    def get_existing_modification_time(self):
        """
        Returns the existing row modification time if available.

        :returns: the modification time in milliseconds since January 1, 1970
        :rtype: int
        :versionadded: 5.3.0
        """
        return self._get_existing_modification_time()


class RetryStats(object):
    """
    A class that maintains stats on retries during a request.

    This object tracks statistics about retries performed during requests. It
    can be accessed from within retry handlers (see :py:class:`RetryHandler`) or
    after a request is finished by calling :py:meth:`Request.get_retry_stats`.
    """

    def __init__(self):
        self._delay_ms = 0
        self._exception_map = dict()
        self._retries = 0

    def __str__(self):
        return ('retries=' + str(self._retries) + ', delay_ms=' +
                str(self._delay_ms) + ', exception_map=' +
                str(self._exception_map))

    def add_delay_ms(self, delay_ms):
        """
        Internal use only.

        Adds time to the overall delay time spent.

        :param delay_ms: the number of milliseconds to add to the delay total.
        :type delay_ms: int
        """
        self._delay_ms += delay_ms

    def add_exception(self, e):
        """
        Internal use only.

        Adds an exception class to the stats object.

        This increments the exception count and adds to the count of this type
        of exception class.

        :param e: the exception class.
        :type e: Exception
        """
        num = self.get_num_exceptions(e) + 1
        self._exception_map[e] = num

    def clear(self):
        """
        Internal use only.

        Clears the stats object.
        """
        self._delay_ms = 0
        self._exception_map.clear()
        self._retries = 0

    def get_delay_ms(self):
        """
        Returns the total time delayed (slept) between retry events.

        :returns: the time delayed during retries, in milliseconds. This is only
            the time spent locally in sleep() between retry events.
        :rtype: int
        """
        return self._delay_ms

    def get_num_exceptions(self, e):
        """
        Returns the number of exceptions of a particular class. If no exceptions
        of this class were added to this stats object, the return value is zero.

        :param e: the class of exception to query.
        :type e: Exception
        :returns: the number of exceptions of this class
        :rtype: int
        """
        num = self._exception_map.get(e)
        if num is None:
            return 0
        return num

    def get_retries(self):
        """
        Returns the number of retry events.

        :returns: number of retry events.
        :rtype: int
        """
        return self._retries

    def increment_retries(self):
        """
        Internal use only.

        Increments the number of retries.
        """
        self._retries += 1
