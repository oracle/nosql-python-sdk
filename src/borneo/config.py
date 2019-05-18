#
# Copyright (C) 2018, 2019 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
#
# Please see LICENSE.txt file included in the top-level directory of the
# appropriate download for a copy of the license and additional information.
#

from abc import ABCMeta, abstractmethod
from copy import deepcopy
from random import random
from time import sleep

from .common import CheckValue, Consistency
from .exception import (
    IllegalArgumentException, OperationThrottlingException, RetryableException,
    SecurityInfoNotReadyException)
try:
    import auth
    import operations
except ImportError:
    from . import auth
    from . import operations


class RetryHandler(object):
    """
    RetryHandler is called by the request handling system when a
    :py:class:`RetryableException` is thrown. It controls the number of retries
    as well as frequency of retries using a delaying algorithm. A default
    RetryHandler is always configured on a :py:class:`NoSQLHandle` instance and
    can be controlled or overridden using
    :py:meth:`NoSQLHandleConfig.set_retry_handler` and
    :py:meth:`NoSQLHandleConfig.configure_default_retry_handler`.

    It is not recommended that applications rely on a RetryHandler for
    regulating provisioned throughput. It is best to add rate-limiting to the
    application based on a table's capacity and access patterns to avoid
    throttling exceptions.

    Instances of this class must be immutable so they can be shared among
    threads.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_num_retries(self):
        """
        Returns the number of retries that this handler instance will allow
        before the exception is thrown to the application.

        :returns: the number of retries.
        """
        pass

    @abstractmethod
    def do_retry(self, request, num_retried, re):
        """
        This method is called when a :py:class:`RetryableException` is thrown
        and the handler determines whether to perform a retry or not based on
        the parameters.

        Default behavior is to *not* retry OperationThrottlingException because
        the retry time is likely much longer than normal because they are DDL
        operations. In addition, *not* retry any requests that should not be
        retired: TableRequest, ListTablesRequest, GetTableRequest,
        TableUsageRequest, GetIndexesRequest.

        Always retry SecurityInfoNotReadyException until exceed the request
        timeout. It's not restrained by the maximum retries configured for this
        handler, the driver with retry handler with 0 retry setting would still
        retry this exception.

        :param request: the request that has triggered the exception.
        :param num_retried: the number of retries that have occurred for the
            operation.
        :param re: the exception that was thrown.
        :returns: True if the operation should be retried, False if not, causing
            the exception to be thrown to the application.
        :raises IllegalArgumentException: raises the exception if num_retried is
            not a positive number.
        """
        pass

    @abstractmethod
    def delay(self, num_retried, re):
        """
        This method is called when a :py:class:`RetryableException` is thrown
        and it is determined that the request will be retried based on the
        return value if :py:meth:`do_retry`. It provides a delay between
        retries. Most implementations will sleep for some period of time. The
        method should not return until the desired delay period has passed.
        Implementations should not busy-wait in a tight loop.

        If delayMS is non-zero, use it. Otherwise, use a exponential backoff
        algorithm to compute the time of delay.

        If retry-able exception is SecurityInfoNotReadyException, delay for
        SEC_RETRY_DELAY_MS when number of retries is smaller than 10. Otherwise,
        use the exponential backoff algorithm to compute the time of delay.

        :param num_retried: the number of retries that have occurred for the
            operation.
        :param re: the exception that was thrown.
        :raises IllegalArgumentException: raises the exception if num_retried is
            not a positive number.
        """
        pass


class DefaultRetryHandler(RetryHandler):
    """
    A default instance of :py:class:`RetryHandler`
    """
    # Base time of delay between retries for security info unavailable.
    _SEC_ERROR_DELAY_MS = 100

    def __init__(self, num_retries=10, delay_s=1):
        CheckValue.check_int_ge_zero(num_retries, 'num_retries')
        CheckValue.check_int_ge_zero(delay_s, 'delay_s')
        self.__num_retries = num_retries
        self.__delay_s = delay_s

    def get_num_retries(self):
        return self.__num_retries

    def do_retry(self, request, num_retried, re):
        self.__check_request(request)
        CheckValue.check_int_gt_zero(num_retried, 'num_retried')
        self.__check_retryable_exception(re)
        if isinstance(re, OperationThrottlingException):
            return False
        elif isinstance(re, SecurityInfoNotReadyException):
            # always retry if security info is not read.
            return True
        elif not request.should_retry():
            return False
        return num_retried < self.__num_retries

    def delay(self, num_retried, re):
        CheckValue.check_int_gt_zero(num_retried, 'num_retried')
        self.__check_retryable_exception(re)
        sec = self.__delay_s
        if sec == 0:
            sec = self.__compute_backoff_delay(num_retried, 1000)
        if isinstance(re, SecurityInfoNotReadyException):
            sec = self.__sec_info_not_ready_delay(num_retried)
        sleep(sec)

    def __check_request(self, request):
        if not isinstance(request, operations.Request):
            raise IllegalArgumentException(
                'The parameter request should be an instance of Request.')

    def __check_retryable_exception(self, re):
        if not isinstance(re, RetryableException):
            raise IllegalArgumentException(
                're must be an instance of RetryableException.')

    def __compute_backoff_delay(self, num_retried, base_delay):
        """
        Use an exponential backoff algorithm to compute time of delay.

        Assumption: numRetries starts with 1
        sec = (2^(num_retried-1) + random MS (0-1000)) / 1000
        """
        msec = (1 << (num_retried - 1)) * base_delay
        msec += (random() * base_delay)
        return msec // 1000

    def __sec_info_not_ready_delay(self, num_retried):
        """
        Handle security information not ready retries. If number of retries is
        smaller than 10, delay for DefaultRetryHandler._SEC_ERROR_DELAY_MS.
        Otherwise, use the backoff algorithm to compute the time of delay.
        """
        msec = DefaultRetryHandler._SEC_ERROR_DELAY_MS
        if num_retried > 10:
            msec = self.__compute_backoff_delay(
                num_retried - 10, DefaultRetryHandler._SEC_ERROR_DELAY_MS)
        return msec // 1000


class NoSQLHandleConfig:
    """
    An instance of this class is required by :py:class:`NoSQLHandle`.

    NoSQLHandleConfig groups parameters used to configure a
    :py:class:`NoSQLHandle`. It also provides a way to default common parameters
    for use by :py:class:`NoSQLHandle` methods. When creating a
    :py:class:`NoSQLHandle`, the NoSQLHandleConfig instance is copied
    so modification operations on the instance have no effect on existing
    handles which are immutable. NoSQLHandle state with default values can be
    overridden in individual operations.

    Most of the configuration parameters are optional and have default values if
    not specified. The only required configuration is the endpoint required by
    the constructor. The endpoint is used to connect to the service. Endpoints
    must include the target address, and may include protocol and port. The
    valid syntax is [http[s]://]host[:port], For example, these are valid
    endpoint arguments:

     * ndcs.uscom-east-1.oracle.cloud.com
     * localhost:8080 - used for connecting to a Cloud Simulator instance
     * https\://ndcs.eucom-central-1.oraclecloud.com:443

    If port is omitted, the endpoint uses 8080 if protocol is http, and 443 in
    all other cases.

    If protocol is omitted, the endpoint uses http if the port is 8080, and
    https in all other cases.

    See the documentation online for the complete set of available regions.

    :param endpoint: The endpoint to use to connect to the service. Required.
    :type endpoint: str
    :raises IllegalArgumentException: raises the exception if the endpoint is
        None or malformed.
    """

    # The default value for request, and table request timeouts in milliseconds,
    # if not configured.
    _DEFAULT_TIMEOUT = 5000
    _DEFAULT_TABLE_REQ_TIMEOUT = 10000
    # The default value for timeouts in milliseconds while waiting for security
    # information is available if it is not configure.
    _DEFAULT_SEC_INFO_TIMEOUT = 10000
    _DEFAULT_CONSISTENCY = Consistency.EVENTUAL

    def __init__(self, endpoint):
        # Inits a NoSQLHandleConfig object.
        CheckValue.check_str(endpoint, 'endpoint')
        self.__endpoint = endpoint
        self.__parse_endpoint()
        self.__timeout = 0
        self.__table_request_timeout = 0
        self.__sec_info_timeout = NoSQLHandleConfig._DEFAULT_SEC_INFO_TIMEOUT
        self.__consistency = None
        self.__pool_connections = 2
        self.__pool_maxsize = 10
        self.__max_content_length = 1024 * 1024
        self.__retry_handler = None
        self.__auth_provider = None
        self.__proxy_host = None
        self.__proxy_port = 0
        self.__proxy_username = None
        self.__proxy_password = None
        self.__logger = None

    def get_endpoint(self):
        """
        Returns the endpoint string used to connect to the server

        :returns: the endpoint.
        """
        return self.__endpoint

    def get_protocol(self):
        """
        Returns the protocol that is used to connect to the server.

        :returns: the protocol that used to connect to the server.
        """
        return self.__protocol

    def get_host(self):
        """
        Returns the host string used to connect to the server.

        :returns: the host.
        """
        return self.__host

    def get_port(self):
        """
        Returns the port that is used to connect to the service.

        :returns: the port.
        """
        return self.__port

    def get_default_timeout(self):
        """
        Returns the default value for request timeout in milliseconds. If there
        is no configured timeout or it is configured as 0, a "default" value of
        5000 milliseconds is used.

        :returns: the default timeout, in milliseconds.
        """
        return (NoSQLHandleConfig._DEFAULT_TIMEOUT if self.__timeout == 0 else
                self.__timeout)

    def get_default_table_request_timeout(self):
        """
        Returns the default value for a table request timeout. If there is no
        configured timeout or it is configured as 0, a "default" default value
        of 10000 milliseconds is used.

        :returns: the default timeout, in milliseconds.
        """
        return (NoSQLHandleConfig._DEFAULT_TABLE_REQ_TIMEOUT if
                self.__table_request_timeout == 0 else
                self.__table_request_timeout)

    def get_default_consistency(self):
        """
        Returns the default consistency. If there is a configured consistency it
        is returned. If not a default value of Consistency.EVENTUAL is used.

        :returns: the default consistency.
        """
        return (NoSQLHandleConfig._DEFAULT_CONSISTENCY if
                self.__consistency is None else self.__consistency)

    def set_timeout(self, timeout):
        """
        Sets the default request timeout in milliseconds, the default timeout is
        5 seconds.

        :param timeout: the timeout value, in milliseconds.
        :returns: self.
        :raises IllegalArgumentException: raises the exception if timeout is a
            negative number.
        """
        CheckValue.check_int_gt_zero(timeout, 'timeout')
        self.__timeout = timeout
        return self

    def get_timeout(self):
        """
        Returns the configured request timeout value, in milliseconds, 0 if it
        has not been set.

        :returns: the timeout, in milliseconds, or 0 if it has not been set.
        """
        return self.__timeout

    def set_table_request_timeout(self, table_request_timeout):
        """
        Sets the default table request timeout. The default timeout is 5
        seconds. The table request timeout can be specified independently of
        that specified by :py:meth:`set_request_timeout` because table requests
        can take longer and justify longer timeouts. The default timeout is 10
        seconds (10000 milliseconds).

        :param table_request_timeout: the timeout value, in milliseconds.
        :returns: self.
        :raises IllegalArgumentException: raises the exception if
            table_request_timeout is a negative number.
        """
        CheckValue.check_int_gt_zero(table_request_timeout,
                                     'table_request_timeout')
        self.__table_request_timeout = table_request_timeout
        return self

    def get_table_request_timeout(self):
        """
        Returns the configured table request timeout value, in milliseconds.
        The table request timeout default can be specified independently to
        allow it to be larger than a typical data request. If it is not
        specified the default table request timeout of 10000 is used.

        :returns: the timeout, in milliseconds, or 0 if it has not been set.
        """
        return self.__table_request_timeout

    def set_sec_info_timeout(self, sec_info_timeout):
        """
        Sets the timeout of waiting security information to be available. The
        default timeout is 10 seconds.

        :param sec_info_timeout: the timeout value, in milliseconds.
        :returns: self.
        :raises IllegalArgumentException: raises the exception if
            sec_info_timeout is a negative number.
        """
        CheckValue.check_int_gt_zero(sec_info_timeout, 'sec_info_timeout')
        self.__sec_info_timeout = sec_info_timeout
        return self

    def get_sec_info_timeout(self):
        """
        Returns the configured timeout value for waiting security information
        to be available, in milliseconds.

        :returns: the timeout, in milliseconds, or 0 if it has not been set.
        """
        return self.__sec_info_timeout

    def set_consistency(self, consistency):
        """
        Sets the default request :py:class:`Consistency`. The default
        consistency is Consistency.EVENTUAL.

        :param consistency: the consistency.
        :returns: self.
        :raises IllegalArgumentException: raises the exception if consistency
            is not Consistency.ABSOLUTE or Consistency.EVENTUAL.
        """
        if (consistency != Consistency.ABSOLUTE and
                consistency != Consistency.EVENTUAL):
            raise IllegalArgumentException(
                'Consistency must be Consistency.ABSOLUTE or ' +
                'Consistency.EVENTUAL')
        self.__consistency = consistency
        return self

    def get_consistency(self):
        """
        Returns the configured default :py:class:`Consistency`, None if it has
        not been configured.

        :returns: the consistency, or None if it has not been configured.
        """
        return self.__consistency

    def set_pool_connections(self, pool_connections):
        """
        Sets the number of connection pools to cache.

        :param pool_connections: the number of connection pools.
        :returns: self.
        :raises IllegalArgumentException: raises the exception if
            pool_connections is not a positive number.
        """
        CheckValue.check_int_gt_zero(pool_connections, 'pool_connections')
        self.__pool_connections = pool_connections
        return self

    def get_pool_connections(self):
        """
        Returns the number of connection pools to cache.

        :returns: the number of connection pools.
        """
        return self.__pool_connections

    def set_pool_maxsize(self, pool_maxsize):
        """
        Sets the maximum number of individual connections to use to connect to
        to the service. Each request/response pair uses a connection. The pool
        exists to allow concurrent requests and will bound the number of
        concurrent requests. Additional requests will wait for a connection to
        become available.

        :param pool_maxsize: the pool size.
        :returns: self.
        :raises IllegalArgumentException: raises the exception if pool_maxsize
            is not a positive number.
        """
        CheckValue.check_int_gt_zero(pool_maxsize, 'pool_maxsize')
        self.__pool_maxsize = pool_maxsize
        return self

    def get_pool_maxsize(self):
        """
        Returns the maximum number of individual connections to use to connect
        to the service. Each request/response pair uses a connection. The pool
        exists to allow concurrent requests and will bound the number of
        concurrent requests. Additional requests will wait for a connection to
        become available.

        :returns: the pool size.
        """
        return self.__pool_maxsize

    def get_max_content_length(self):
        """
        Returns the maximum size, in bytes, of a request operation payload. Not
        currently user-configurable.

        :returns: the size.
        """
        return self.__max_content_length

    def set_retry_handler(self, retry_handler):
        """
        Sets the :py:class:`RetryHandler` to use for the handle. If no handler
        is configured a default is used. The handler must be safely usable by
        multiple threads.

        :param retry_handler: the handler.
        :returns: self.
        :raises IllegalArgumentException: raises the exception if retry_handler
            is not an instance of :py:class:`RetryHandler`.
        """
        if not isinstance(retry_handler, RetryHandler):
            raise IllegalArgumentException(
                'retry_handler must be an instance of RetryHandler.')
        self.__retry_handler = retry_handler
        return self

    def configure_default_retry_handler(self, num_retries, delay_s):
        """
        Sets the :py:class:`RetryHandler` using a default retry handler
        configured with the specified number of retries and a static delay, in
        seconds. 0 retries means no retries. A delay of 0 means "use the
        default delay algorithm" which is an random delay time. A non-zero delay
        will work but is not recommended for production systems as it is not
        flexible.

        The default retry handler will not retry exceptions of type
        :py:class:`OperationThrottlingException`. The reason is that these
        operations are long-running operations, and while technically they can
        be retried, an immediate retry is unlikely to succeed because of the low
        rates allowed for these operations.

        :param num_retries: the number of retries to perform automatically.
            This parameter may be 0 for no retries.
        :param delay_s: the delay, in seconds. Use 0 to use the default delay
            algorithm.
        :returns: self.
        :raises IllegalArgumentException: raises the exception if num_retries or
            delay_s is a negative number.
        """
        self.__retry_handler = DefaultRetryHandler(num_retries, delay_s)
        return self

    def get_retry_handler(self):
        """
        Returns the :py:class:`RetryHandler` configured for the handle, or None
        if None is set.

        :returns: the handler.
        """
        return self.__retry_handler

    def set_authorization_provider(self, provider):
        """
        Sets the :py:class:`AuthorizationProvider` to use for the handle. The
        provider must be safely usable by multiple threads.

        :param provider: the AuthorizationProvider.
        :returns: self.
        :raises IllegalArgumentException: raises the exception if provider is
            not an instance of :py:class:`AuthorizationProvider`.
        """
        if not isinstance(provider, auth.AuthorizationProvider):
            raise IllegalArgumentException(
                'provider must be an instance of AuthorizationProvider.')
        self.__auth_provider = provider
        return self

    def get_authorization_provider(self):
        """
        Returns the :py:class:`auth.AuthorizationProvider` configured for
        the handle, or None.

        :returns: the AuthorizationProvider.
        """
        return self.__auth_provider

    def set_proxy_host(self, proxy_host):
        """
        Sets an HTTP proxy host to be used for the session. If a proxy host is
        specified a proxy port must also be specified, using
        :py:meth:`set_proxy_port`.

        :param proxy_host: the proxy host.
        :returns: self.
        :raises IllegalArgumentException: raises the exception if proxy_host is
            not a string.
        """
        CheckValue.check_str(proxy_host, 'proxy_host')
        self.__proxy_host = proxy_host
        return self

    def get_proxy_host(self):
        """
        Returns a proxy host, or None if not configured.

        :returns: the host, or None.
        """
        return self.__proxy_host

    def set_proxy_port(self, proxy_port):
        """
        Sets an HTTP proxy port to be used for the session. If a proxy port is
        specified a proxy host must also be specified, using
        :py:meth:`set_proxy_host`.

        :param proxy_port: the proxy port.
        :returns: self.
        :raises IllegalArgumentException: raises the exception if proxy_port is
            a negative number.
        """
        CheckValue.check_int_ge_zero(proxy_port, 'proxy_port')
        self.__proxy_port = proxy_port
        return self

    def get_proxy_port(self):
        """
        Returns a proxy port, or 0 if not configured.

        :returns: the proxy port.
        """
        return self.__proxy_port

    def set_proxy_username(self, proxy_username):
        """
        Sets an HTTP proxy user name if the configured proxy host requires
        authentication. If a proxy host is not configured this configuration is
        ignored.

        :param proxy_username: the user name.
        :returns: self.
        :raises IllegalArgumentException: raises the exception if proxy_username
            is not a string.
        """
        CheckValue.check_str(proxy_username, 'proxy_username')
        self.__proxy_username = proxy_username
        return self

    def get_proxy_username(self):
        """
        Returns a proxy user name, or None if not configured.

        :returns: the user name, or None.
        """
        return self.__proxy_username

    def set_proxy_password(self, proxy_password):
        """
        Sets an HTTP proxy password if the configured proxy host requires
        authentication. If a proxy user name is not configured this
        configuration is ignored.

        :param proxy_password: the password.
        :returns: self.
        :raises IllegalArgumentException: raises the exception if proxy_password
            is not a string.
        """
        CheckValue.check_str(proxy_password, 'proxy_password')
        self.__proxy_password = proxy_password
        return self

    def get_proxy_password(self):
        """
        Returns a proxy password, or None if not configured.

        :returns: the password, or None.
        """
        return self.__proxy_password

    def set_logger(self, logger):
        """
        Sets the logger used for the driver.

        :param logger: the logger.
        :returns: self.
        :raises IllegalArgumentException: raises the exception if logger is not
            an instance of Logger.
        """
        CheckValue.check_logger(logger, 'logger')
        self.__logger = logger
        return self

    def get_logger(self):
        """
        Returns the logger, or None if not configured by user.

        :returns: the logger.
        """
        return self.__logger

    def clone(self):
        """
        All the configurations will be copied.

        :returns: the copy of the instance.
        """
        auth_provider = self.__auth_provider
        logger = self.__logger
        self.__auth_provider = None
        self.__logger = None
        clone_config = deepcopy(self)
        clone_config.set_authorization_provider(
            auth_provider).set_logger(logger)
        self.__logger = logger
        self.__auth_provider = auth_provider
        return clone_config

    #
    # Parse the endpoint, which has the following format:
    #   [proto:]host[:port]
    def __parse_endpoint(self):
        """
        Parse the endpoint string into host, port, protocol
        """

        # defaults
        self.__protocol = 'https'
        self.__port = 443

        parts = self.__endpoint.split(':')

        if len(parts) > 3:
            raise IllegalArgumentException(
                'Invalid endpoint: ' + self.__endpoint)

        if len(parts) == 1:
            # 1 part means only host
            self.__host = self.__endpoint
        elif len(parts) == 2:
            # 2 parts:
            #  proto:[//]host (default port based on proto)
            #  host:port (default proto)
            if parts[0].lower().startswith('http'):
                # proto:[//]host
                self.__protocol = parts[0].lower()
                self.__host = parts[1]
                # infer port
                if self.__protocol == 'http':
                    self.__port = 8080
            else:
                # host:port
                self.__host = parts[0]
                self.__port = self.__validate_port(parts[1])
                # in this path infer proto from port
                if self.__port != 443:
                    self.__protocol = 'http'
        elif len(parts) == 3:
            # 3 parts: proto:[//]host:port
            self.__protocol = parts[0].lower()
            self.__host = parts[1]
            self.__port = self.__validate_port(parts[2])

        # strip '//' if present in host
        if self.__host.startswith('//'):
            self.__host = self.__host[2:]

        if self.__protocol != 'http' and self.__protocol != 'https':
            raise IllegalArgumentException(
                'Invalid endpoint, protocol must be http or https: ' +
                self.__endpoint)

    def __validate_port(self, portstring):
        try:
            port = int(portstring)
            CheckValue.check_int_ge_zero(port, 'port')
        except ValueError:
            raise IllegalArgumentException(
                'Invalid endpoint: ' + self.__endpoint)
        return port
