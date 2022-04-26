#
# Copyright (c) 2018, 2022 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#

from logging import DEBUG
from multiprocessing import pool
from platform import python_version
from requests import Session
from sys import version_info
from threading import Lock
from time import time

from .common import (
    ByteOutputStream, CheckValue, HttpConstants, LogUtils, SSLAdapter,
    TableLimits, synchronized)
from .config import DefaultRetryHandler
from .exception import (IllegalArgumentException,
                        OperationNotSupportedException, RequestSizeLimitException)
from .http import RateLimiterMap, RequestUtils
from .kv import StoreAccessTokenProvider
from .operations import GetTableRequest, QueryResult, TableRequest, WriteRequest
from .query import QueryDriver
from .serde import BinaryProtocol
from .version import __version__
from .stats import StatsControl


class Client(object):
    DEFAULT_MAX_CONTENT_LENGTH = 32 * 1024 * 1024
    LIMITER_REFRESH_NANOS = 600000000000
    TRACE_LEVEL = 0

    # The HTTP driver client.
    def __init__(self, config, logger):
        self._logutils = LogUtils(logger)
        self._config = config
        self._url = config.get_service_url()
        self._request_uri = self._url.geturl() + HttpConstants.NOSQL_DATA_PATH
        self._pool_connections = config.get_pool_connections()
        self._pool_maxsize = config.get_pool_maxsize()
        max_content_length = config.get_max_content_length()
        self._max_content_length = (
            Client.DEFAULT_MAX_CONTENT_LENGTH if max_content_length == 0
            else max_content_length)
        self._request_id = 1
        self._proxy_host = config.get_proxy_host()
        self._proxy_port = config.get_proxy_port()
        self._proxy_username = config.get_proxy_username()
        self._proxy_password = config.get_proxy_password()
        self._retry_handler = config.get_retry_handler()
        if self._retry_handler is None:
            self._retry_handler = DefaultRetryHandler()
        self._shut_down = False
        self._user_agent = self._make_user_agent()
        self._auth_provider = config.get_authorization_provider()
        if self._auth_provider is None:
            raise IllegalArgumentException(
                'Must configure AuthorizationProvider.')
        self._sess = Session()
        self._session_cookie = None

        ssl_ctx = None
        url_scheme = self._url.scheme
        if url_scheme == 'https':
            ssl_ctx = config.get_ssl_context()
            if ssl_ctx is None:
                raise IllegalArgumentException(
                    'Unable to configure https: SSLContext is missing from ' +
                    'config.')

        # Session uses a urllib3 PoolManager for pooling connections. This is
        # configured using requests.adapter.HTPPAdapter (see SSLAdapter in
        # common.py)
        #
        # pool_connections: applies to the number of
        #   pools to keep, where a pool applies to a single host.
        # pool_maxsize: how many connections to reuse. More than this can
        #   be created but will be dropped once used
        # pool_block: if True once pool_maxsize connections are created new
        #   calls will be blocked until a connection is released, defaults to
        #   False in urllib3
        # max_retries: internal retries in urllib3 because of network/system
        #   issues, defaults to 0 in urllib3
        #
        adapter = SSLAdapter(
            ssl_ctx, pool_connections=self._pool_connections,
            pool_maxsize=self._pool_maxsize, max_retries=5, pool_block=True)
        self._sess.mount(self._url.scheme + '://', adapter)
        if self._proxy_host is not None:
            self._check_and_set_proxy(self._sess)
        self.serial_version = BinaryProtocol.DEFAULT_SERIAL_VERSION
        # StoreAccessTokenProvider means onprem
        self._is_cloud = not isinstance(self._auth_provider, StoreAccessTokenProvider)
        if config.get_rate_limiting_enabled() and self._is_cloud:
            self._logutils.log_debug(
                'Starting client with rate limiting enabled')
            self._rate_limiter_map = RateLimiterMap()
            self._table_limit_update_map = dict()
            self._threadpool = pool.ThreadPool(1)
        else:
            self._logutils.log_debug('Starting client with no rate limiting')
            self._rate_limiter_map = None
            self._table_limit_update_map = None
            self._threadpool = None
        self.lock = Lock()
        self._ratelimiter_duration_seconds = 30
        self._one_time_messages = {}
        self._stats_control = StatsControl(config,
                                           logger,
                                           config.get_rate_limiting_enabled())

    @synchronized
    def background_update_limiters(self, table_name):
        # Query table limits and create rate limiters for a table in a
        # short-lived background thread.
        if not self._table_needs_refresh(table_name):
            return
        self._set_table_needs_refresh(table_name, False)
        try:
            self._threadpool.map(self._update_table_limiters, ['table_name'])
        except RuntimeError:
            self._set_table_needs_refresh(table_name, True)

    def enable_rate_limiting(self, enable, use_percent):
        """
        Internal use only.

        Allow tests to enable/disable rate limiting. This method is not thread
        safe, and should only be executed by one thread when no other operations
        are in progress.
        """
        self._config.set_default_rate_limiting_percentage(use_percent)
        if enable and self._rate_limiter_map is None:
            self._rate_limiter_map = RateLimiterMap()
            self._table_limit_update_map = dict()
            self._threadpool = pool.ThreadPool(1)
        elif not enable and self._rate_limiter_map is not None:
            self._rate_limiter_map.clear()
            self._rate_limiter_map = None
            self._table_limit_update_map.clear()
            self._table_limit_update_map = None
            if self._threadpool is not None:
                self._threadpool.close()
                self._threadpool = None

    def execute(self, request):
        """
        Execute the KV request and return the response. This is the top-level
        method for request execution.

        This method handles exceptions to distinguish between what can be
        retried and what cannot, making sure that root cause exceptions are
        kept. Examples:

            can't connect (host, port, etc)\n
            throttling exceptions\n
            general networking issues, IOError\n

        RequestTimeoutException needs a cause, or at least needs to include the
        message from the causing exception.

        :param request: the request to be executed by the server.
        :type request: Request
        :returns: the result of the request.
        :rtype: Result
        :raises IllegalArgumentException: raises the exception if request is
            None.
        """
        CheckValue.check_not_none(request, 'request')
        request.set_defaults(self._config)
        request.validate()
        if request.is_query_request():
            self._stats_control.observe_query(request)

            """
            The following 'if' may be True for advanced queries only. For such
            queries, the 'if' will be True (i.e., the QueryRequest will be bound
            with a QueryDriver) if and only if this is not the 1st execute()
            call for this query. In this case we just return a new, empty
            QueryResult. Actual computation of a result batch will take place
            when the app calls get_results() on the QueryResult.
            """
            if request.has_driver():
                self._trace('QueryRequest has QueryDriver', 2)
                return QueryResult(request, False)
            """
            If it is an advanced query and we are here, then this must be the
            1st execute() call for the query. If the query has been prepared
            before, we create a QueryDriver and bind it with the QueryRequest.
            Then, we create and return an empty QueryResult. Actual computation
            of a result batch will take place when the app calls get_results()
            on the QueryResult.
            """
            if request.is_prepared() and not request.is_simple_query():
                self._trace(
                    'QueryRequest has no QueryDriver, but is prepared', 2)
                driver = QueryDriver(request)
                driver.set_client(self)
                driver.set_topology_info(request.topology_info())
                return QueryResult(request, False)
            """
            If we are here, then this is either (a) a simple query or (b) an
            advanced query that has not been prepared already, which also
            implies that this is the 1st execute() call on this query. For a
            non-prepared advanced query, the effect of this 1st execute() call
            is to send the query to the proxy for compilation, get back the
            prepared query, but no query results, create a QueryDriver, and bind
            it with the QueryRequest (see QueryRequestSerializer.deserialize()),
            and return an empty QueryResult.
            """
            self._trace(
                'QueryRequest has no QueryDriver and is not prepared', 2)
        timeout_ms = request.get_timeout()
        headers = {'Host': self._url.hostname,
                   'Content-Type': 'application/octet-stream',
                   'Connection': 'keep-alive',
                   'Accept': 'application/octet-stream',
                   'User-Agent': self._user_agent}

        # set the session cookie if available
        if self._session_cookie is not None:
            headers['Cookie'] = self._session_cookie

        # We expressly check size limit below based on onprem versus cloud. Set
        # the request to not check size limit inside self._write_content().
        request.set_check_request_size(False)
        content = self.serialize_request(request, headers)
        content_len = len(content)
        # If on-premise the auth_provider will always be a
        # StoreAccessTokenProvider. If so, check against configurable limit.
        # Otherwise check against internal hardcoded cloud limit.
        if isinstance(self._auth_provider, StoreAccessTokenProvider):
            if content_len > self._max_content_length:
                raise RequestSizeLimitException(
                    'The request size of ' + str(content_len) + ' exceeded ' +
                    'the limit of ' + str(self._max_content_length))
        else:
            request.set_check_request_size(True)
            BinaryProtocol.check_request_size_limit(request, content_len)
        if request.get_compartment() is None:
            request.set_compartment_internal(
                self._config.get_default_compartment())
        if self._logutils.is_enabled_for(DEBUG):
            self._logutils.log_debug('Request: ' + request.__class__.__name__)
        request_id = self._next_request_id()
        headers[HttpConstants.REQUEST_ID_HEADER] = request_id
        self.check_request(request)
        # TODO: look at avoiding creating this object on each request
        request_utils = RequestUtils(
            self._sess, self._logutils, request, self._retry_handler, self,
            self._rate_limiter_map)
        return request_utils.do_post_request(self._request_uri, headers,
            content, timeout_ms, self._stats_control)

    # set the session cookie if in return headers (see RequestUtils in http.py)
    @synchronized
    def set_session_cookie(self, cookie):
        # only grab the "session=value" portion of the header
        value = cookie.partition(';')[0]
        if self._logutils.is_enabled_for(DEBUG):
            self._logutils.log_debug(
                'Set cookie value: ' + value)

        self._session_cookie = value

    def check_request(self, request):
        # warn if using features not implemented at the connected server
        # currently cloud does not support Durability
        if self.serial_version < 3 or self._is_cloud:
            if isinstance(request, WriteRequest) and request.get_durability() is not None:
                self.one_time_message('The requested feature is not supported ' +
                                      'by the connected server: Durability')
        # ondemand tables are not available in V2 or onprem
        if self.serial_version < 3 or not self._is_cloud:
            if (isinstance(request, TableRequest) and
                    request.get_table_limits() is not None and
                    request.get_table_limits().get_mode() ==
                    TableLimits.CAPACITY_MODE.ON_DEMAND):
                raise OperationNotSupportedException(
                    'The requested feature is not supported ' +
                    'by the connected server: on demand capacity table')

    @synchronized
    def _next_request_id(self):
        """
        Get the next client-scoped request id. It really needs to be combined
        with a client id to obtain a globally unique scope but is sufficient
        for most purposes
        """
        self._request_id += 1
        return str(self._request_id)

    def get_auth_provider(self):
        return self._auth_provider

    # for test use
    def get_is_cloud(self):
        return self._is_cloud

    @synchronized
    def one_time_message(self, message):
        val = self._one_time_messages.get(message)
        if val is None:
            self._one_time_messages[message] = "1"
            self._logutils.log_warning(message)

    def reset_rate_limiters(self, table_name):
        """
        Internal use only.

        Allow tests to reset limiters in map.

        :param table_name: name or OCID of the table.
        :type table_name: str
        """
        if self._rate_limiter_map is not None:
            self._rate_limiter_map.reset(table_name)

    def set_ratelimiter_duration_seconds(self, duration_seconds):
        # Allow tests to override this hardcoded setting
        self._ratelimiter_duration_seconds = duration_seconds

    def shut_down(self):
        # Shutdown the client.
        self._logutils.log_debug('Shutting down driver http client')
        if self._shut_down:
            return
        self._shut_down = True
        if self._auth_provider is not None:
            self._auth_provider.close()
        if self._sess is not None:
            self._sess.close()
        if self._threadpool is not None:
            self._threadpool.close()
        if self._stats_control is not None:
            self._stats_control.shutdown()

    def update_rate_limiters(self, table_name, limits):
        """
        Add or update rate limiters for a table.
        Cloud only.

        :param table_name: the table name or OCID of table.
        :type table_name: str
        :param limits: read/write limits for table.
        :type limits: TableLimits
        :returns: whether the update is succeed.
        """
        if self._rate_limiter_map is None:
            return False
        self._set_table_needs_refresh(table_name, False)
        if (limits is None or limits.get_read_units() <= 0 and
                limits.get_write_units() <= 0):
            self._rate_limiter_map.remove(table_name)
            self._logutils.log_info(
                'Removing rate limiting from table: ' + table_name)
            return False
        """
        Create or update rate limiters in map
        Note: NoSQL cloud service has a "burst" availability of 300 seconds. But
        we don't know if or how many other clients may have been using this
        table, and a duration of 30 seconds allows for more predictable usage.
        Also, it's better to use a reasonable hardcoded value here than to try
        to explain the subtleties of it in docs for configuration. In the end
        this setting is probably fine for all uses.
        """
        read_units = limits.get_read_units()
        write_units = limits.get_write_units()
        # If there's a specified rate limiter percentage, use that.
        rl_percent = self._config.get_default_rate_limiting_percentage()
        if rl_percent > 0.0:
            read_units = read_units * rl_percent / 100.0
            write_units = write_units * rl_percent / 100.0
        self._rate_limiter_map.update(
            table_name, float(read_units), float(write_units),
            self._ratelimiter_duration_seconds)
        msg = str.format('Updated table "{0}" to have RUs={1} and WUs={2} ' +
                         'per second.', table_name, str(read_units),
                         str(write_units))
        self._logutils.log_info(msg)
        return True

    def _check_and_set_proxy(self, sess):
        if (self._proxy_host is not None and self._proxy_port == 0 or
                self._proxy_host is None and self._proxy_port != 0):
            raise IllegalArgumentException(
                'To configure an HTTP proxy, both host and port are required.')
        if (self._proxy_username is not None and self._proxy_password is None or
                self._proxy_username is None and
                self._proxy_password is not None):
            raise IllegalArgumentException(
                'To configure HTTP proxy authentication, both user name and ' +
                'password are required')
        if self._proxy_host is not None:
            if self._proxy_username is None:
                proxy_url = (
                        'http://' + self._proxy_host + ':' + str(self._proxy_port))
            else:
                assert self._proxy_password is not None
                proxy_url = (
                        'http://' + self._proxy_username + ':' +
                        self._proxy_password + '@' + self._proxy_host + ':' +
                        str(self._proxy_port))
            sess.proxies = {'http': proxy_url, 'https': proxy_url}

    @staticmethod
    def _make_user_agent():
        if version_info.major >= 3:
            pyversion = python_version()
        else:
            pyversion = '%s.%s.%s' % (version_info.major, version_info.minor,
                                      version_info.micro)
        return '%s/%s (Python %s)' % ('NoSQL-PythonSDK', __version__, pyversion)

    def _set_table_needs_refresh(self, table_name, needs_refresh):
        # set the status of a table needing limits refresh now.
        if self._table_limit_update_map is None:
            return
        then = self._table_limit_update_map.get(table_name)
        now_nanos = int(round(time() * 1000000000))
        if then is not None:
            if needs_refresh:
                self._table_limit_update_map[table_name] = now_nanos - 1
            else:
                self._table_limit_update_map[table_name] = (
                        now_nanos + Client.LIMITER_REFRESH_NANOS)
            return
        if needs_refresh:
            self._table_limit_update_map[table_name] = now_nanos - 1
        else:
            self._table_limit_update_map[table_name] = (
                    now_nanos + Client.LIMITER_REFRESH_NANOS)

    def _table_needs_refresh(self, table_name):
        # Return True if table needs limits refresh.
        if self._table_limit_update_map is None:
            return False
        then = self._table_limit_update_map.get(table_name)
        now_nanos = int(round(time() * 1000000000))
        if then is not None and then > now_nanos:
            return False
        return True

    @staticmethod
    def _trace(msg, level):
        if level <= Client.TRACE_LEVEL:
            print('DRIVER: ' + msg)

    def _update_table_limiters(self, table_name):
        # This is meant to be run in a background thread.
        req = GetTableRequest().set_table_name(table_name).set_timeout(1000)
        res = None
        try:
            self._logutils.log_debug(
                'Starting GetTableRequest for table "' + table_name + '"')
            res = self.execute(req)
        except Exception as e:
            self._logutils.log_error(
                'GetTableRequest for table "' + table_name +
                '" returned exception: ' + str(e))
        if res is None:
            # table doesn't exist? other error?
            self._logutils.log_error(
                'GetTableRequest for table "' + table_name + '" returned None')
            then = self._table_limit_update_map.get(table_name)
            if then is not None:
                # Allow retry after 100ms.
                self._table_limit_update_map[table_name] = (
                        int(round(time() * 1000000000)) + 100000000)
            return
        self._logutils.log_debug(
            'GetTableRequest completed for table "' + table_name + '"')
        # Update/add rate limiters for table.
        if self.update_rate_limiters(table_name, res.get_table_limits()):
            self._logutils.log_info(
                'Background thread added limiters for table "' + table_name +
                '"')

    def decrement_serial_version(self):
        """
        Decrements the serial version, if it is greater than the minimum.
        For internal use only.

        The current minimum value is 2.
        :returns: true if the version was decremented, false otherwise.
        :rtype: bool
        """
        if self.serial_version > 2:
            self.serial_version -= 1
            return True
        return False

    def _write_content(self, request):
        """
        Serializes the request payload, sent as http content.

        :param request: the request to be executed by the server.
        :type request: Request
        :returns: the bytearray that contains the content.
        :rtype: bytearray
        """
        content = bytearray()
        bos = ByteOutputStream(content)
        BinaryProtocol.write_serial_version(bos, self.serial_version)
        request.create_serializer().serialize(
            request, bos, self.serial_version)
        return content

    def serialize_request(self, request, headers):
        """
        Serializes the request payload and sets the Content-Length
        header to the correct value.

        :param request: the request to be executed by the server.
        :type request: Request
        :param headers: the http headers
        :type headers: Dictionary
        :returns: the bytearray that contains the content.
        :rtype: bytearray
        """
        content = self._write_content(request)
        headers.update({'Content-Length': str(len(content))})
        return content

    def get_stats_control(self):
        return self._stats_control
