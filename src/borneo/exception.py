#
# Copyright (c) 2018, 2022 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#


class IllegalArgumentException(RuntimeError):
    """
    Exception class that is used when an invalid argument was passed, this could
    mean that the type is not the expected or the value is not valid for the
    specific case.
    """

    def __init__(self, message=None, cause=None):
        self._message = message
        self._cause = cause

    def __str__(self):
        return self._message

    def get_cause(self):
        """
        Get the cause of the exception.

        :returns: the cause of the exception.
        :rtype: RuntimeError
        """
        return self._cause


class IllegalStateException(RuntimeError):
    """
    Exception that is thrown when a method has been invoked at an illegal or
    inappropriate time.
    """

    def __init__(self, message=None, cause=None):
        self._message = message
        self._cause = cause

    def __str__(self):
        return self._message

    def get_cause(self):
        """
        Get the cause of the exception.

        :returns: the cause of the exception.
        :rtype: RuntimeError
        """
        return self._cause


class NoSQLException(RuntimeError):
    """
    A base class for most exceptions thrown by the NoSQL driver.
    """

    def __init__(self, message, cause=None):
        self._message = message
        self._cause = cause

    def __str__(self):
        return self._message

    def get_cause(self):
        """
        Get the cause of the exception.

        :returns: the cause of the exception.
        :rtype: RuntimeError
        """
        return self._cause

    def ok_to_retry(self):
        """
        Returns whether this exception can be retried with a reasonable
        expectation that it may succeed. Instances of
        :py:class:`RetryableException` will return True for this method.
        """
        return False


class QueryException(RuntimeError):
    """
    A class to hold query exceptions indicating syntactic or semantic problems
    at the driver side during query execution. It is internal use only and it
    will be caught, and rethrown as IllegalArgumentException to the application.
    It includes location information. When converted to an IAE, the location
    info is put into the message created for the IAE.
    """

    def __init__(self, message=None, cause=None, location=None):
        self._message = message
        self._cause = cause
        self._location = location

    def __str__(self):
        return ('Error:' + ('' if self._location is None else ' at (' +
                            str(self._location.get_start_line()) + ', ' +
                            str(self._location.get_start_column()) + ')') +
                ' ' + self._message)

    def get_illegal_argument(self):
        # Get this exception as a simple IAE, not wrapped. This is used on the
        # client side.
        raise IllegalArgumentException(str(self))

    def get_location(self):
        # Returns the location associated with this exception. May be None if
        # not available.
        return self._location

    class Location(object):
        """
        Location of an expression in the query. It contains both start and end,
        line and column info.
        """

        def __init__(self, start_line, start_column, end_line, end_column):
            self._start_line = start_line
            self._start_column = start_column
            self._end_line = end_line
            self._end_column = end_column
            assert start_line >= 0
            assert start_column >= 0
            assert end_line >= 0
            assert end_column >= 0

        def __str__(self):
            return (str(self._start_line) + ':' + str(self._start_column) +
                    '-' + str(self._end_line) + ':' + str(self._end_column))

        def get_end_column(self):
            # Returns the end column as its char position in line.
            return self._end_column

        def get_end_line(self):
            # Returns the end line.
            return self._end_line

        def get_start_column(self):
            # Returns the start column as its char position in line.
            return self._start_column

        def get_start_line(self):
            # Returns the start line.
            return self._start_line


class QueryStateException(IllegalStateException):
    """
    An internal class that encapsulates illegal states in the query engine. The
    query engine operates inside clients and servers and cannot safely throw
    IllegalStateException as that can crash the server. This exception is used
    to indicate problems in the engine that are most likely query engine bugs
    but are not otherwise fatal to the system.
    """

    def __init__(self, message):
        super(QueryStateException, self).__init__(
            'Unexpected state in query engine:\n' + message)


class InvalidAuthorizationException(NoSQLException):
    """
    The exception is thrown if the application presents an invalid authorization
    string in a request.
    """

    def __init__(self, message):
        super(InvalidAuthorizationException, self).__init__(message)


class OperationNotSupportedException(NoSQLException):
    """
    The operation attempted is not supported. This may be related to on-premise
    vs cloud service configurations.
    """

    def __init__(self, message):
        super(OperationNotSupportedException, self).__init__(message)


class UnsupportedProtocolException(NoSQLException):
    """
    The protocol serial version is not supported by the connected server. The
    client should decrement its serial version (if possible) and retry.
    """

    def __init__(self, message):
        super(UnsupportedProtocolException, self).__init__(message)


class RequestTimeoutException(NoSQLException):
    """
    Thrown when a request cannot be processed because the configured timeout
    interval is exceeded. If a retry handler is configured it is possible that
    the request has been retried a number of times before the timeout occurs.
    """

    def __init__(self, message, timeout_ms=0, cause=None):
        super(RequestTimeoutException, self).__init__(message, cause)
        self._timeout_ms = timeout_ms

    def __str__(self):
        msg = super(RequestTimeoutException, self).__str__()
        if self._timeout_ms != 0:
            msg += '  Timeout: ' + str(self._timeout_ms) + ' ms.'
        cause = self.get_cause()
        if cause is not None:
            msg += ('\nCaused by: ' + cause.__class__.__name__ + ': ' +
                    str(cause))
        return msg

    def get_timeout_ms(self):
        """
        Returns the timeout that was in effect for the operation.

        :returns: the timeout that was in effect for the operation, in
            milliseconds.
        :rtype: int
        """
        return self._timeout_ms


class ResourceExistsException(NoSQLException):
    """
    The operation attempted to create a resource but it already exists.
    """

    def __init__(self, message):
        super(ResourceExistsException, self).__init__(message)


class ResourceLimitException(NoSQLException):
    """
    Cloud service only.

    This is a base class for exceptions that result from reaching a limit for a
    particular resource, such as number of tables, indexes, or a size limit on
    data. It is never thrown directly.
    """

    def __init__(self, message):
        super(ResourceLimitException, self).__init__(message)


class ResourceNotFoundException(NoSQLException):
    """
    The operation attempted to access a resource that does not exist or is not
    in a visible state.
    """

    def __init__(self, message):
        super(ResourceNotFoundException, self).__init__(message)


class RetryableException(NoSQLException):
    """
    A base class for all exceptions that may be retried with a reasonable
    expectation that they may succeed on retry.
    """

    def __init__(self, message):
        super(RetryableException, self).__init__(message)

    def ok_to_retry(self):
        return True


class TableSizeException(NoSQLException):
    """
    An exception indicating a table size limit has been exceeded by writing more
    data than the table can support. This exception is not retryable because the
    conditions that lead to it being thrown, while potentially transient,
    typically require user intervention.
    """

    def __init__(self, message):
        super(TableSizeException, self).__init__(message)


class UnauthorizedException(NoSQLException):
    """
    The exception is thrown if an application does not have sufficient
    permission to perform a request.
    """

    def __init__(self, message):
        super(UnauthorizedException, self).__init__(message)


class IndexExistsException(ResourceExistsException):
    """
    The operation attempted to create an index for a table but the named index
    already exists.
    """

    def __init__(self, message):
        super(IndexExistsException, self).__init__(message)


class TableExistsException(ResourceExistsException):
    """
    The operation attempted to create a table but the named table already
    exists.
    """

    def __init__(self, message):
        super(TableExistsException, self).__init__(message)


class EvolutionLimitException(ResourceLimitException):
    """
    Cloud service only.

    Thrown to indicate that an attempt has been made to evolve the schema of a
    table more times than allowed by the system defined limit.
    """

    def __init__(self, message):
        super(EvolutionLimitException, self).__init__(message)


class DeploymentException(ResourceLimitException):
    """
    Cloud service only.

    Thrown to indicate that an attempt has been made to create or modify a table
    using limits that exceed the maximum allowed for a single table or that
    cause the tenant's aggregate resources to exceed the maximum allowed for a
    tenant. These are system-defined limits.
    """

    def __init__(self, message):
        super(DeploymentException, self).__init__(message)


class IndexLimitException(ResourceLimitException):
    """
    Cloud service only.

    Thrown to indicate that an attempt has been made to create more indexes on a
    table than the system defined limit.
    """

    def __init__(self, message):
        super(IndexLimitException, self).__init__(message)


class KeySizeLimitException(ResourceLimitException):
    """
    Cloud service only.

    Thrown to indicate that an attempt has been made to create a row with a
    primary key or index key size that exceeds the system defined limit.
    """

    def __init__(self, message):
        super(KeySizeLimitException, self).__init__(message)


class RowSizeLimitException(ResourceLimitException):
    """
    Cloud service only.

    Thrown to indicate that an attempt has been made to create a row with a size
    that exceeds the system defined limit.
    """

    def __init__(self, message):
        super(RowSizeLimitException, self).__init__(message)


class TableLimitException(ResourceLimitException):
    """
    Cloud service only.

    Thrown to indicate that an attempt has been made to create a number of
    tables that exceeds the system defined limit.
    """

    def __init__(self, message):
        super(TableLimitException, self).__init__(message)


class BatchOperationNumberLimitException(ResourceLimitException):
    """
    Cloud service only.

    Thrown to indicate that the number of operations included in
    :py:meth:`NoSQLHandle.write_multiple` operation exceeds the system defined
    limit.
    """

    def __init__(self, message):
        super(BatchOperationNumberLimitException, self).__init__(message)


class RequestSizeLimitException(ResourceLimitException):
    """
    Cloud service only.

    Thrown to indicate that the size of a Request exceeds the system defined
    limit.
    """

    def __init__(self, message):
        super(RequestSizeLimitException, self).__init__(message)


class IndexNotFoundException(ResourceNotFoundException):
    """
    The operation attempted to access a index that does not exist or is not in
    a visible state.
    """

    def __init__(self, message):
        super(IndexNotFoundException, self).__init__(message)


class TableNotFoundException(ResourceNotFoundException):
    """
    The operation attempted to access a table that does not exist or is not in
    a visible state.
    """

    def __init__(self, message):
        super(TableNotFoundException, self).__init__(message)


class SecurityInfoNotReadyException(RetryableException):
    """
    Cloud service only.

    An exception that is thrown when security information is not ready in the
    system. This exception will occur as the system acquires security
    information and must be retried in order for authorization to work properly.
    """

    def __init__(self, message):
        super(SecurityInfoNotReadyException, self).__init__(message)


class SystemException(RetryableException):
    """
    An exception that is thrown when there is an internal system problem.
    Most system problems are temporary, so this is a retryable exception.
    """

    def __init__(self, message):
        super(SystemException, self).__init__(message)


class ThrottlingException(RetryableException):
    """
    Cloud service only.

    ThrottlingException is a base class for exceptions that indicate the
    application has exceeded a provisioned or implicit limit in terms of size
    of data accessed or frequency of operation.

    Operations resulting in this exception can be retried but it is recommended
    that callers use a delay before retrying in order to minimize the chance
    that a retry will also be throttled.

    It is recommended that applications use rate limiting to avoid these
    exceptions.
    """

    def __init__(self, message):
        super(ThrottlingException, self).__init__(message)


class OperationThrottlingException(ThrottlingException):
    """
    Cloud service only.

    An exception that is thrown when a non-data operation is throttled. This can
    happen if an application attempts too many control operations such as table
    creation, deletion, or similar methods. Such operations do not use
    throughput or capacity provisioned for a given table but they consume system
    resources and their use is limited.

    Operations resulting in this exception can be retried but it is recommended
    that callers use a relatively large delay before retrying in order to
    minimize the chance that a retry will also be throttled.
    """

    def __init__(self, message):
        super(OperationThrottlingException, self).__init__(message)


class ReadThrottlingException(ThrottlingException):
    """
    Cloud service only.

    This exception indicates that the provisioned read throughput has been
    exceeded.

    Operations resulting in this exception can be retried but it is recommended
    that callers use a delay before retrying in order to minimize the chance
    that a retry will also be throttled. Applications should attempt to avoid
    throttling exceptions by rate limiting themselves to the degree possible.
    """

    def __init__(self, message):
        super(ReadThrottlingException, self).__init__(message)


class WriteThrottlingException(ThrottlingException):
    """
    Cloud service only.

    This exception indicates that the provisioned write throughput has been
    exceeded.

    Operations resulting in this exception can be retried but it is recommended
    that callers use a delay before retrying in order to minimize the chance
    that a retry will also be throttled. Applications should attempt to avoid
    throttling exceptions by rate limiting themselves to the degree possible.
    """

    def __init__(self, message):
        super(WriteThrottlingException, self).__init__(message)
