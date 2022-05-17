#
# Copyright (c) 2018, 2022 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#

from json import loads
from logging import FileHandler, Formatter, WARNING, getLogger
from os import mkdir, path
from ssl import SSLContext, SSLError, create_default_context
from sys import argv

from .client import Client
from .common import CheckValue, UserInfo
from .config import NoSQLHandleConfig
from .exception import IllegalArgumentException, IllegalStateException
from .iam import SignatureProvider
from .kv import StoreAccessTokenProvider
from .operations import (
    DeleteRequest, GetIndexesRequest, GetRequest, GetTableRequest,
    ListTablesRequest, MultiDeleteRequest, PrepareRequest, PutRequest,
    QueryRequest, SystemRequest, SystemStatusRequest, TableRequest,
    TableUsageRequest, WriteMultipleRequest)


class NoSQLHandle(object):
    """
    NoSQLHandle is a handle that can be used to access Oracle NoSQL tables. To
    create a connection represented by NoSQLHandle, request an instance using
    :py:class:`NoSQLHandleConfig`, which allows an application to specify
    default values and other configuration information to be used by the handle.

    The same interface is available to both users of the Oracle NoSQL Database
    Cloud Service and the on-premise Oracle NoSQL Database; however, some
    methods and/or parameters are specific to each environment. The
    documentation has notes about whether a class, method, or parameter is
    environment-specific. Unless otherwise noted they are applicable to both
    environments.

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
    retried, will fail again. Exceptions that may be retried return True for
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
    :type config: NoSQLHandleConfig
    :raises IllegalArgumentException: raises the exception if config is not an
        instance of NoSQLHandleConfig.
    """

    def __init__(self, config):
        if not isinstance(config, NoSQLHandleConfig):
            raise IllegalArgumentException(
                'config must be an instance of NoSQLHandleConfig.')
        logger = self._get_logger(config)
        # config SSLContext first, on-prem authorization provider will reuse the
        # context in NoSQLHandleConfig
        self._config_ssl_context(config)
        self._config_auth_provider(config, logger)
        self._client = Client(config, logger)

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
        :type request: DeleteRequest
        :returns: the result of the operation.
        :rtype: DeleteResult
        :raises IllegalArgumentException: raises the exception if request is not
            an instance of :py:class:`DeleteRequest`.
        :raises NoSQLException: raises the exception if the operation cannot be
            performed for any other reason.
        """
        if not isinstance(request, DeleteRequest):
            raise IllegalArgumentException(
                'The parameter should be an instance of DeleteRequest.')
        return self._execute(request)

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
        :type request: GetRequest
        :returns: the result of the operation.
        :rtype: GetResult
        :raises IllegalArgumentException: raises the exception if request is not
            an instance of :py:class:`GetRequest`.
        :raises NoSQLException: raises the exception if the operation cannot be
            performed for any other reason.
        """
        if not isinstance(request, GetRequest):
            raise IllegalArgumentException(
                'The parameter should be an instance of GetRequest.')
        return self._execute(request)

    def get_indexes(self, request):
        """
        Returns information about and index, or indexes on a table. If no index
        name is specified in the :py:class:`GetIndexesRequest`, then information
        on all indexes is returned.

        :param request: the input parameters for the operation.
        :type request: GetIndexesRequest
        :returns: the result of the operation.
        :rtype: GetIndexesResult
        :raises IllegalArgumentException: raises the exception if request is not
            an instance of :py:class:`GetIndexesRequest`.
        :raises NoSQLException: raises the exception if the operation cannot be
            performed for any other reason.
        """
        if not isinstance(request, GetIndexesRequest):
            raise IllegalArgumentException(
                'The parameter should be an instance of GetIndexesRequest.')
        return self._execute(request)

    def get_table(self, request):
        """
        Gets static information about the specified table including its state,
        provisioned throughput and capacity and schema. Dynamic information such
        as usage is obtained using :py:meth:`get_table_usage`. Throughput,
        capacity and usage information is only available when using the Cloud
        Service and will be None or not defined on-premise.

        :param request: the input parameters for the operation.
        :type request: GetTableRequest
        :returns: the result of the operation.
        :rtype: TableResult
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
        res = self._execute(request)
        # Update rate limiters, if table has limits.
        self._client.update_rate_limiters(
            res.get_table_name(), res.get_table_limits())
        return res

    def get_table_usage(self, request):
        """
        Cloud service only.

        Gets dynamic information about the specified table such as the current
        throughput usage. Usage information is collected in time slices and
        returned in individual usage records. It is possible to specify a
        time-based range of usage records using input parameters.

        :param request: the input parameters for the operation.
        :type request: TableUsageRequest
        :returns: the result of the operation.
        :rtype: TableUsageResult
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
        return self._execute(request)

    def list_tables(self, request):
        """
        Lists tables, returning table names. If further information about a
        specific table is desired the :py:meth:`get_table` interface may be
        used. If a given identity has access to a large number of tables the
        list may be paged using input parameters.

        :param request: the input parameters for the operation.
        :type request: ListTablesRequest
        :returns: the result of the operation.
        :rtype: ListTablesResult
        :raises IllegalArgumentException: raises the exception if request is not
            an instance of :py:class:`ListTablesRequest`.
        :raises NoSQLException: raises the exception if the operation cannot be
            performed for any other reason.
        """
        if not isinstance(request, ListTablesRequest):
            raise IllegalArgumentException(
                'The parameter should be an instance of ListTablesRequest.')
        return self._execute(request)

    def multi_delete(self, request):
        """
        Deletes multiple rows from a table in an atomic operation. The key used
        may be partial but must contain all of the fields that are in the shard
        key. A range may be specified to delete a range of keys.

        :param request: the input parameters for the operation.
        :type request: MultiDeleteRequest
        :returns: the result of the operation.
        :rtype: MultiDeleteResult
        :raises IllegalArgumentException: raises the exception if request is not
            an instance of :py:class:`MultiDeleteRequest`.
        :raises NoSQLException: raises the exception if the operation cannot be
            performed for any other reason.
        """
        if not isinstance(request, MultiDeleteRequest):
            raise IllegalArgumentException(
                'The parameter should be an instance of MultiDeleteRequest.')
        return self._execute(request)

    def prepare(self, request):
        """
        Prepares a query for execution and reuse. See :py:meth:`query` for
        general information and restrictions. It is recommended that prepared
        queries are used when the same query will run multiple times as
        execution is much more efficient than starting with a query string every
        time. The query language and API support query variables to assist with
        re-use.

        :param request: the input parameters for the operation.
        :type request: PrepareRequest
        :returns: the result of the operation.
        :rtype: PrepareResult
        :raises IllegalArgumentException: raises the exception if request is not
            an instance of :py:class:`PrepareRequest`.
        :raises NoSQLException: raises the exception if the operation cannot be
            performed for any other reason.
        """
        if not isinstance(request, PrepareRequest):
            raise IllegalArgumentException(
                'The parameter should be an instance of PrepareRequest.')
        return self._execute(request)

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
        :type request: PutRequest
        :returns: the result of the operation.
        :rtype: PutResult
        :raises IllegalArgumentException: raises the exception if request is not
            an instance of :py:class:`PutRequest`.
        :raises NoSQLException: raises the exception if the operation cannot be
            performed for any other reason.
        """
        if not isinstance(request, PutRequest):
            raise IllegalArgumentException(
                'The parameter should be an instance of PutRequest.')
        return self._execute(request)

    def query(self, request):
        """
        Queries a table based on the query statement specified in the
        :py:class:`QueryRequest`.

        Queries that include a full shard key will execute much more efficiently
        than more distributed queries that must go to multiple shards.

        Table and system-style queries such as "CREATE TABLE ..." or "DROP TABLE
        ..." are not supported by this interfaces. Those operations must be
        performed using :py:meth:`table_request` or :py:meth:`system_request` as
        appropriate.

        The amount of data read by a single query request is limited by a system
        default and can be further limited using
        :py:meth:`QueryRequest.set_max_read_kb`. This limits the amount of data
        *read* and not the amount of data *returned*, which means that a query
        can return zero results but still have more data to read. This situation
        is detected by checking if the :py:class:`QueryRequest` is done using
        :py:meth:`QueryRequest.is_done`. For this reason queries should always
        operate in a loop, acquiring more results, until
        :py:meth:`QueryRequest.is_done` returns True, indicating that the query
        is done.

        :param request: the input parameters for the operation.
        :type request: QueryRequest
        :returns: the result of the operation.
        :rtype: QueryResult
        :raises IllegalArgumentException: raises the exception if request is not
            an instance of :py:class:`QueryRequest`.
        :raises NoSQLException: raises the exception if the operation cannot be
            performed for any other reason.
        """
        if not isinstance(request, QueryRequest):
            raise IllegalArgumentException(
                'The parameter should be an instance of QueryRequest.')
        return self._execute(request)

    def system_request(self, request):
        """
        On-premise only.

        Performs a system operation on the system, such as administrative
        operations that don't affect a specific table. For table-specific
        operations use :py:meth:`table_request` or :py:meth:`do_table_request`.

        Examples of statements in the :py:class:`SystemRequest` passed to this
        method include:

            CREATE NAMESPACE mynamespace\n
            CREATE USER some_user IDENTIFIED BY password\n
            CREATE ROLE some_role\n
            GRANT ROLE some_role TO USER some_user

        This operation is implicitly asynchronous. The caller must poll using
        methods on :py:class:`SystemResult` to determine when it has completed.

        :param request: the input parameters for the operation.
        :type request: SystemRequest
        :returns: the result of the operation.
        :rtype: SystemResult
        :raises IllegalArgumentException: raises the exception if request is not
            an instance of :py:class:`SystemRequest`.
        :raises NoSQLException: raises the exception if the operation cannot be
            performed for any other reason.
        """
        if not isinstance(request, SystemRequest):
            raise IllegalArgumentException(
                'The parameter should be an instance of SystemRequest.')
        return self._execute(request)

    def system_status(self, request):
        """
        On-premise only.

        Checks the status of an operation previously performed using
        :py:meth:`system_request`.

        :param request: the input parameters for the operation.
        :type request: SystemStatusRequest
        :returns: the result of the operation.
        :rtype: SystemResult
        :raises IllegalArgumentException: raises the exception if request is not
            an instance of :py:class:`SystemStatusRequest`.
        :raises NoSQLException: raises the exception if the operation cannot be
            performed for any other reason.
        """
        if not isinstance(request, SystemStatusRequest):
            raise IllegalArgumentException(
                'The parameter should be an instance of SystemStatusRequest.')
        return self._execute(request)

    def table_request(self, request):
        """
        Performs an operation on a table. This method is used for creating and
        dropping tables and indexes as well as altering tables. Only one
        operation is allowed on a table at any one time.

        This operation is implicitly asynchronous. The caller must poll using
        methods on :py:class:`TableResult` to determine when it has completed.

        :param request: the input parameters for the operation.
        :type request: TableRequest
        :returns: the result of the operation.
        :rtype: TableResult
        :raises IllegalArgumentException: raises the exception if request is not
            an instance of :py:class:`TableRequest`.
        :raises NoSQLException: raises the exception if the operation cannot be
            performed for any other reason.
        """
        if not isinstance(request, TableRequest):
            raise IllegalArgumentException(
                'The parameter should be an instance of TableRequest.')
        res = self._execute(request)
        # Update rate limiters, if table has limits.
        self._client.update_rate_limiters(
            res.get_table_name(), res.get_table_limits())
        return res

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
        :type request: WriteMultipleRequest
        :returns: the result of the operation.
        :rtype: WriteMultipleResult
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
        return self._execute(request)

    def close(self):
        """
        Close the NoSQLHandle.
        """
        if self._client is not None:
            self._client.shut_down()
            self._client = None

    def do_system_request(self, statement, timeout_ms=30000,
                          poll_interval_ms=1000):
        """
        On-premise only.

        A convenience method that performs a SystemRequest and waits for
        completion of the operation. This is the same as calling
        :py:meth:`system_request` then calling
        :py:meth:`SystemResult.wait_for_completion`. If the operation fails an
        exception is thrown.

        System requests are those related to namespaces and security and are
        generally independent of specific tables. Examples of statements include

            CREATE NAMESPACE mynamespace\n
            CREATE USER some_user IDENTIFIED BY password\n
            CREATE ROLE some_role\n
            GRANT ROLE some_role TO USER some_user

        :param statement: the system statement for the operation.
        :type statement: str
        :param timeout_ms: the amount of time to wait for completion, in
            milliseconds.
        :type timeout_ms: int
        :param poll_interval_ms: the polling interval for the wait operation.
        :type poll_interval_ms: int
        :returns: the result of the system request.
        :rtype: SystemResult
        :raises IllegalArgumentException: raises the exception if any of the
            parameters are invalid or required parameters are missing.
        :raises RequestTimeoutException: raises the exception if the operation
            times out.
        :raises NoSQLException: raises the exception if the operation cannot be
            performed for any other reason.
        """
        CheckValue.check_str(statement, 'statement')
        req = SystemRequest().set_statement(statement)
        res = self.system_request(req)
        res.wait_for_completion(self, timeout_ms, poll_interval_ms)
        return res

    def do_table_request(self, request, timeout_ms, poll_interval_ms):
        """
        A convenience method that performs a TableRequest and waits for
        completion of the operation. This is the same as calling
        :py:meth:`table_request` then calling
        :py:meth:`TableResult.wait_for_completion`. If the operation fails an
        exception is thrown. All parameters are required.

        :param request: the :py:class:`TableRequest` to perform.
        :type request: TableRequest
        :param timeout_ms: the amount of time to wait for completion, in
            milliseconds.
        :type timeout_ms: int
        :param poll_interval_ms: the polling interval for the wait operation.
        :type poll_interval_ms: int
        :returns: the result of the table request.
        :rtype: TableResult
        :raises IllegalArgumentException: raises the exception if any of the
            parameters are invalid or required parameters are missing.
        :raises RequestTimeoutException: raises the exception if the operation
            times out.
        :raises NoSQLException: raises the exception if the operation cannot be
            performed for any other reason.
        """
        if not isinstance(request, TableRequest):
            raise IllegalArgumentException(
                'The parameter should be an instance of TableRequest.')
        res = self.table_request(request)
        res.wait_for_completion(self, timeout_ms, poll_interval_ms)
        return res

    def list_namespaces(self):
        """
        On-premise only.

        Returns the namespaces in a store as a list of string.

        :returns: the namespaces, or None if none are found.
        :rtype: list(str)
        """
        res = self.do_system_request('show as json namespaces')
        json_res = res.get_result_string()
        if json_res is None:
            return None
        root = loads(json_res)
        namespaces = root.get('namespaces')
        if namespaces is None:
            return None
        results = list()
        for namespace in namespaces:
            results.append(namespace)
        return results

    def list_roles(self):
        """
        On-premise only.

        Returns the roles in a store as a list of string.

        :returns: the list of roles, or None if none are found.
        :rtype: list(str)
        """
        res = self.do_system_request('show as json roles')
        json_res = res.get_result_string()
        if json_res is None:
            return None
        root = loads(json_res)
        roles = root.get('roles')
        if roles is None:
            return None
        results = list()
        for role in roles:
            results.append(role['name'])
        return results

    def list_users(self):
        """
        On-premise only.

        Returns the users in a store as a list of :py:class:`UserInfo`.

        :returns: the list of users, or None if none are found.
        :rtype: list(UserInfo)
        """
        res = self.do_system_request('show as json users')
        json_res = res.get_result_string()
        if json_res is None:
            return None
        root = loads(json_res)
        users = root.get('users')
        if users is None:
            return None
        results = list()
        for user in users:
            results.append(UserInfo(user['id'], user['name']))
        return results

    def get_client(self):
        # For testing use
        return self._client

    def _execute(self, request):
        # Ensure that the client exists and hasn't been closed.
        if self._client is None:
            raise IllegalStateException('NoSQLHandle has been closed.')
        return self._client.execute(request)

    def _get_logger(self, config):
        """
        Returns the logger used for the driver. If no logger is specified,
        create one based on this class name.
        """
        if config.get_logger() is None and config.is_default_logger():
            logger = getLogger(self.__class__.__name__)
            logger.setLevel(WARNING)
            log_dir = path.join(path.abspath(path.dirname(argv[0])), 'logs')
            if not path.exists(log_dir):
                mkdir(log_dir)
            handler = FileHandler(path.join(log_dir, 'driver.log'))
            formatter = Formatter('%(asctime)s [%(levelname)s] %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        else:
            logger = config.get_logger()
        return logger

    @staticmethod
    def _config_auth_provider(config, logger):
        provider = config.get_authorization_provider()
        if provider.get_logger() is None:
            provider.set_logger(logger)
        if (isinstance(provider, StoreAccessTokenProvider) and
                provider.is_secure()):
            if provider.get_endpoint() is None:
                endpoint = config.get_service_url().geturl()
                if endpoint.endswith('/'):
                    endpoint = endpoint[:len(endpoint) - 1]
                provider.set_endpoint(endpoint)
            provider.set_ssl_context(config.get_ssl_context())
        elif isinstance(provider, SignatureProvider):
            provider.set_service_url(config)

    @staticmethod
    def _config_ssl_context(config):
        if config.get_ssl_context() is not None:
            return
        if config.get_service_url().scheme == 'https':
            try:
                if config.get_ssl_protocol() is None:
                    ctx = create_default_context()
                else:
                    ctx = SSLContext(config.get_ssl_protocol())
                if config.get_ssl_cipher_suites() is not None:
                    ctx.set_ciphers(config.get_ssl_cipher_suites())
                if config.get_ssl_ca_certs() is not None:
                    ctx.load_verify_locations(config.get_ssl_ca_certs())
                config.set_ssl_context(ctx)
            except (SSLError, ValueError) as err:
                raise IllegalArgumentException(str(err))

    def get_stats_control(self):
        return self._client.get_stats_control()
