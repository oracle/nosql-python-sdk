#
# Copyright (C) 2018, 2019 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
#
# Please see LICENSE.txt file included in the top-level directory of the
# appropriate download for a copy of the license and additional information.
#

from logging import FileHandler, WARNING, getLogger
from os import mkdir, path, sep
from sys import argv

from .idcs import DefaultAccessTokenProvider
from .client import Client
from .config import NoSQLHandleConfig
from .exception import IllegalArgumentException, IllegalStateException
from .operations import (
    DeleteRequest, GetIndexesRequest, GetRequest, GetTableRequest,
    ListTablesRequest, MultiDeleteRequest, PrepareRequest, PutRequest,
    QueryRequest, TableRequest, TableUsageRequest, WriteMultipleRequest)


class NoSQLHandle:
    """
    NoSQLHandle is a handle that can be used to access Oracle NoSQL tables. To
    create a connection represented by NoSQLHandle, request an instance using
    :py:class:`NoSQLHandleConfig`, which allows an application to specify
    default values and other configuration information to be used by the handle.

    A handle has memory and network resources associated with it. Consequently,
    the :py:meth:`close` method must be invoked to free up the resources when
    the application is done using the handle. To minimize network activity as
    well as resource allocation and deallocation overheads, it's best to avoid
    repeated creation and closing of handles. For example, creating and closing
    a handle around each operation, would incur large resource allocation
    overheads resulting in poor application performance.

    A handle permits concurrent operations, so a single handle is sufficient to
    access tables in a multi-threaded application. The creation of multiple
    handles incurs additional resource overheads without providing any
    performance benefit.

    With the exception of :py:meth:`close` the operations on this interface
    follow a similar pattern. They accept a Request object containing
    parameters, both required and optional. They return a Result object
    containing results. Operation failures throw exceptions. Unique subclasses
    of Request and Result exist for most operations, containing information
    specific to the operation. All of these operations result in remote calls
    across a network.

    All Request instances support specification of parameters for the operation
    as well as the ability to override default parameters which may have been
    specified in :py:class:`NoSQLHandleConfig`, such as request timeouts, etc.

    Objects returned by methods of this interface can only be used safely by
    one thread at a time unless synchronized externally. Request objects are not
    copied and must not be modified by the application while a method on this
    interface is using them.

    For Error and Exception Handling, on success all methods in this interface
    return Result objects. Errors are thrown as exceptions. Exceptions that may
    be retried may succeed on retry. These are instances of
    :py:class:`RetryableException`. Exceptions that may not be retried and if
    retried, will fail again. Exceptions that may be retried return true for
    :py:meth:`RetryableException.ok_to_retry` while those that may not will
    return False. Examples of retryable exceptions are those which indicate
    resource consumption violations such as
    :py:class:`OperationThrottlingException`. Examples of exceptions that should
    not be retried are :py:class:`IllegalArgumentException`,
    :py:class:`TableNotFoundException`, and any other exception indicating a
    syntactic or semantic error.

    Instances of NoSQLHandle are thread-safe and expected to be shared among
    threads.

    :param config: an instance of NoSQLHandleConfig.
    :raises IllegalArgumentException: raises the exception if config is not an
        instance of NoSQLHandleConfig.
    """

    def __init__(self, config):
        if not isinstance(config, NoSQLHandleConfig):
            raise IllegalArgumentException(
                'config must be an instance of NoSQLHandleConfig.')
        logger = self.__get_logger(config)
        self.__config_default_at_handler_logging(config, logger)
        self.__client = Client(config, logger)

    def delete(self, request):
        """
        Deletes a row from a table. The row is identified using a primary key
        value supplied in :py:meth:`DeleteRequest.set_key`.

        By default a delete operation is unconditional and will succeed if the
        specified row exists. Delete operations can be made conditional based
        on whether the :py:class:`Version` of an existing row matches that
        supplied by :py:meth:`DeleteRequest.set_match_version`.

        It is also possible, on failure, to return information about the
        existing row. The row, including it's :py:class:`Version` can be
        optionally returned if a delete operation fails because of a Version
        mismatch. The existing row information will only be returned if
        :py:meth:`DeleteRequest.set_return_row` is True and the operation fails
        because :py:meth:`DeleteRequest.set_match_version` is used and the
        operation fails because the row exists and its version does not match.
        Use of :py:meth:`DeleteRequest.set_return_row` may result in additional
        consumed read capacity. If the operation is successful there will be no
        information returned about the previous row.

        :param request: the input parameters for the operation.
        :returns: the result of the operation.
        :raises IllegalArgumentException: raises the exception if request is not
            an instance of :py:class:`DeleteRequest`.
        :raises NoSQLException: raises the exception if the operation cannot be
            performed for any other reason.
        """
        if not isinstance(request, DeleteRequest):
            raise IllegalArgumentException(
                'The parameter should be an instance of DeleteRequest.')
        self.__check_client()
        return self.__client.execute(request)

    def get(self, request):
        """
        Gets the row associated with a primary key. On success the value of the
        row is available using the :py:meth:`GetResult.get_value` operation. If
        there are no matching rows that method will return None.

        The default consistency used for the operation is Consistency.EVENTUAL
        unless an explicit value has been set using
        :py:meth:`NoSQLHandleConfig.set_consistency` or
        :py:meth:`GetRequest.set_consistency`. Use of Consistency.ABSOLUTE may
        affect latency of the operation and may result in additional cost for
        the operation.

        :param request: the input parameters for the operation.
        :returns: the result of the operation.
        :raises IllegalArgumentException: raises the exception if request is not
            an instance of :py:class:`GetRequest`.
        :raises NoSQLException: raises the exception if the operation cannot be
            performed for any other reason.
        """
        if not isinstance(request, GetRequest):
            raise IllegalArgumentException(
                'The parameter should be an instance of GetRequest.')
        self.__check_client()
        return self.__client.execute(request)

    def get_indexes(self, request):
        """
        Returns information about and index, or indexes on a table. If no index
        name is specified in the :py:class:`GetIndexesRequest`, then information
        on all indexes is returned.

        :param request: the input parameters for the operation.
        :returns: the result of the operation.
        :raises IllegalArgumentException: raises the exception if request is not
            an instance of :py:class:`GetIndexesRequest`.
        :raises NoSQLException: raises the exception if the operation cannot be
            performed for any other reason.
        """
        if not isinstance(request, GetIndexesRequest):
            raise IllegalArgumentException(
                'The parameter should be an instance of GetIndexesRequest.')
        self.__check_client()
        return self.__client.execute(request)

    def get_table(self, request):
        """
        Gets static information about the specified table including its
        provisioned throughput and capacity and schema. Dynamic information such
        as usage is obtained using :py:meth:`get_table_usage`.

        :param request: the input parameters for the operation.
        :returns: the result of the operation.
        :raises IllegalArgumentException: raises the exception if request is not
            an instance of :py:class:`GetTableRequest`.
        :raises TableNotFoundException: raises the exception if the specified
            table does not exist.
        :raises NoSQLException: raises the exception if the operation cannot be
            performed for any other reason.
        """
        if not isinstance(request, GetTableRequest):
            raise IllegalArgumentException(
                'The parameter should be an instance of GetTableRequest.')
        self.__check_client()
        return self.__client.execute(request)

    def get_table_usage(self, request):
        """
        Gets dynamic information about the specified table such as the current
        throughput usage. Usage information is collected in time slices and
        returned in individual usage records. It is possible to specify a
        time-based range of usage records using input parameters.

        :param request: the input parameters for the operation.
        :returns: the result of the operation.
        :raises IllegalArgumentException: raises the exception if request is not
            an instance of :py:class:`TableUsageRequest`.
        :raises TableNotFoundException: raises the exception if the specified
            table does not exist.
        :raises NoSQLException: raises the exception if the operation cannot be
            performed for any other reason.
        """
        if not isinstance(request, TableUsageRequest):
            raise IllegalArgumentException(
                'The parameter should be an instance of TableUsageRequest.')
        self.__check_client()
        return self.__client.execute(request)

    def list_tables(self, request):
        """
        Lists tables, returning table names. If further information about a
        specific table is desired the :py:meth:`get_table` interface may be
        used. If a given identity has access to a large number of tables the
        list may be paged using input parameters.

        :param request: the input parameters for the operation.
        :returns: the result of the operation.
        :raises IllegalArgumentException: raises the exception if request is not
            an instance of :py:class:`ListTablesRequest`.
        :raises NoSQLException: raises the exception if the operation cannot be
            performed for any other reason.
        """
        if not isinstance(request, ListTablesRequest):
            raise IllegalArgumentException(
                'The parameter should be an instance of ListTablesRequest.')
        self.__check_client()
        return self.__client.execute(request)

    def multi_delete(self, request):
        """
        Deletes multiple rows from a table in an atomic operation. The key used
        may be partial but must contain all of the fields that are in the shard
        key. A range may be specified to delete a range of keys.

        :param request: the input parameters for the operation.
        :returns: the result of the operation.
        :raises IllegalArgumentException: raises the exception if request is not
            an instance of :py:class:`MultiDeleteRequest`.
        :raises NoSQLException: raises the exception if the operation cannot be
            performed for any other reason.
        """
        if not isinstance(request, MultiDeleteRequest):
            raise IllegalArgumentException(
                'The parameter should be an instance of MultiDeleteRequest.')
        self.__check_client()
        return self.__client.execute(request)

    def prepare(self, request):
        """
        Prepares a query for execution and reuse. See :py:meth:`query` for
        general information and restrictions. It is recommended that prepared
        queries are used when the same query will run multiple times as
        execution is much more efficient than starting with a query string every
        time. The query language and API support query variables to assist with
        re-use.

        :param request: the input parameters for the operation.
        :returns: the result of the operation.
        :raises IllegalArgumentException: raises the exception if request is not
            an instance of :py:class:`PrepareRequest`.
        :raises NoSQLException: raises the exception if the operation cannot be
            performed for any other reason.
        """
        if not isinstance(request, PrepareRequest):
            raise IllegalArgumentException(
                'The parameter should be an instance of PrepareRequest.')
        self.__check_client()
        return self.__client.execute(request)

    def put(self, request):
        """
        Puts a row into a table. This method creates a new row or overwrites
        an existing row entirely. The value used for the put is in the
        :py:class:`PutRequest` object and must contain a complete primary key
        and all required fields.

        It is not possible to put part of a row.
        Any fields that are not provided will be defaulted, overwriting any
        existing value. Fields that are not noneable or defaulted must be
        provided or an exception will be thrown.

        By default a put operation is unconditional, but put operations can be
        conditional based on existence, or not, of a previous value as well as
        conditional on the :py:class:`Version` of the existing value.

            Use PutOption.IF_ABSENT to do a put only if there is no existing row
            that matches the primary key.\n
            Use PutOption.IF_PRESENT to do a put only if there is an existing
            row that matches the primary key.\n
            Use PutOption.IF_VERSION to do a put only if there is an existing
            row that matches the primary key and its :py:class:`Version` matches
            that provided.

        It is also possible, on failure, to return information about the
        existing row. The row, including it's :py:class:`Version` can be
        optionally returned if a put operation fails because of a Version
        mismatch or if the operation fails because the row already exists.
        The existing row information will only be returned if
        :py:meth:`PutRequest.set_return_row` is True and one of the following
        occurs:

            The PutOption.IF_ABSENT is used and the operation fails because the
            row already exists.\n
            The PutOption.IF_VERSION is used and the operation fails because the
            row exists and its version does not match.

        Use of :py:meth:`PutRequest.set_return_row` may result in additional
        consumed read capacity. If the operation is successful there will be no
        information returned about the previous row.

        :param request: the input parameters for the operation.
        :returns: the result of the operation.
        :raises IllegalArgumentException: raises the exception if request is not
            an instance of :py:class:`PutRequest`.
        :raises NoSQLException: raises the exception if the operation cannot be
            performed for any other reason.
        """
        if not isinstance(request, PutRequest):
            raise IllegalArgumentException(
                'The parameter should be an instance of PutRequest.')
        self.__check_client()
        return self.__client.execute(request)

    def query(self, request):
        """
        Queries a table based on the query statement specified in the
        :py:class:`QueryRequest`. There are limitations on types of queries that
        can be supported in a multi-tenant cloud environment. In general,
        queries that must visit multiple shards are supported except in the
        following conditions:

        The query includes an "ORDER BY" clause. Distributed sorting is not
        available. Sorted queries will work if a shard key is supplied in the
        query.

        The query includes a "GROUP BY" clause. Distributed grouping is not
        available. Grouped queries will work if a shard key is supplied in the
        query.

        Queries that include a full shard key will execute much more efficiently
        than more distributed queries that must go to multiple shards.

        DDL-style queries such as "CREATE TABLE ..." or "DROP TABLE .." are not
        supported by this interfaces. Those operations must be performed using
        :py:meth:`table_request`.

        The amount of data read by a single query request is limited by a system
        default and can be further limited using
        :py:meth:`QueryRequest.set_max_read_kb`. This limits the amount of data
        *read* and not the amount of data *returned*, which means that a query
        can return zero results but still have more data to read. This situation
        is detected by checking if the :py:class:`QueryRequest` has a
        continuation key, using :py:meth:`QueryRequest.get_continuation_key`.
        For this reason queries should always operate in a loop, acquiring more
        results, until the continuation key is null, indicating that the query
        is done. Inside the loop the continuation key is applied to the
        :py:class:`QueryRequest` using
        :py:meth:`QueryRequest.et_continuation_key`.

        :param request: the input parameters for the operation.
        :returns: the result of the operation.
        :raises IllegalArgumentException: raises the exception if request is not
            an instance of :py:class:`QueryRequest`.
        :raises NoSQLException: raises the exception if the operation cannot be
            performed for any other reason.
        """
        if not isinstance(request, QueryRequest):
            raise IllegalArgumentException(
                'The parameter should be an instance of QueryRequest.')
        self.__check_client()
        return self.__client.execute(request)

    def table_request(self, request):
        """
        Performs a DDL operation on a table. This method is used for creating
        and dropping tables and indexes as well as altering tables. Only one
        operation is allowed on a table at any one time.

        This operation is implicitly asynchronous. The caller must poll using
        methods on :py:class:`TableResult` to determine when it has completed.

        :param request: the input parameters for the operation.
        :returns: the result of the operation.
        :raises IllegalArgumentException: raises the exception if request is not
            an instance of :py:class:`TableRequest`.
        :raises NoSQLException: raises the exception if the operation cannot be
            performed for any other reason.
        """
        if not isinstance(request, TableRequest):
            raise IllegalArgumentException(
                'The parameter should be an instance of TableRequest.')
        self.__check_client()
        return self.__client.execute(request)

    def write_multiple(self, request):
        """
        Executes a sequence of operations associated with a table that share the
        same shard key portion of their primary keys, all the specified
        operations are executed within the scope of a single transaction.

        There are some size-based limitations on this operation:

            The max number of individual operations (put, delete) in a single
            WriteMultipleRequest is 50.\n
            The total request size is limited to 25MB.

        :param request: the input parameters for the operation.
        :returns: the result of the operation.
        :raises IllegalArgumentException: raises the exception if request is not
            an instance of :py:class:`WriteMultipleRequest`.
        :raises RowSizeLimitException: raises the exception if data size in an
            operation exceeds the limit.
        :raises BatchOperationNumberLimitException: raises the exception if the
            number of operations exceeds this limit.
        :raises NoSQLException: raises the exception if the operation cannot be
            performed for any other reason.
        """
        if not isinstance(request, WriteMultipleRequest):
            raise IllegalArgumentException(
                'The parameter should be an instance of WriteMultipleRequest.')
        self.__check_client()
        return self.__client.execute(request)

    def close(self):
        """
        Close the NoSQLHandle.
        """
        self.__check_client()
        self.__client.shut_down()
        self.__client = None

    def __check_client(self):
        # Ensure that the client exists and hasn't been closed.
        if self.__client is None:
            raise IllegalStateException('NoSQLHandle has been closed.')

    def __config_default_at_handler_logging(self, config, logger):
        provider = config.get_authorization_provider()
        if isinstance(provider, DefaultAccessTokenProvider):
            if provider.get_logger() is None:
                provider.set_logger(logger)

    def __get_logger(self, config):
        """
        Returns the logger used for the driver. If no logger is specified,
        create one based on this class name.
        """
        if config.get_logger() is not None:
            logger = config.get_logger()
        else:
            logger = getLogger(self.__class__.__name__)
            logger.setLevel(WARNING)
            log_dir = (path.abspath(path.dirname(argv[0])) + sep + 'logs')
            if not path.exists(log_dir):
                mkdir(log_dir)
            logger.addHandler(FileHandler(log_dir + sep + 'driver.log'))
        return logger
