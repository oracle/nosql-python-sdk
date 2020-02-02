#
# Copyright (C) 2018, 2020 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
#
# Please see LICENSE.txt file included in the top-level directory of the
# appropriate download for a copy of the license and additional information.
#

from logging import DEBUG
from platform import python_version
from requests import Session, adapters
from sys import version_info

from .common import ByteOutputStream, CheckValue, HttpConstants, LogUtils
from .config import DefaultRetryHandler
from .exception import IllegalArgumentException
from .http import RequestUtils
from .operations import QueryResult
from .query import QueryDriver
from .serde import BinaryProtocol
from .version import __version__


class Client(object):
    TRACE_LEVEL = 0

    # The HTTP driver client.
    def __init__(self, config, logger):
        self._logutils = LogUtils(logger)
        self._config = config
        self._url = config.get_service_url()
        self._request_uri = self._url.geturl() + HttpConstants.NOSQL_DATA_PATH
        self._pool_connections = config.get_pool_connections()
        self._pool_maxsize = config.get_pool_maxsize()
        self._max_request_id = 1
        self._proxy_host = config.get_proxy_host()
        self._proxy_port = config.get_proxy_port()
        self._proxy_username = config.get_proxy_username()
        self._proxy_password = config.get_proxy_password()
        self._retry_handler = config.get_retry_handler()
        if self._retry_handler is None:
            self._retry_handler = DefaultRetryHandler()
        self._sec_info_timeout = config.get_sec_info_timeout()
        self._shut_down = False
        self._user_agent = self._make_user_agent()
        self._auth_provider = config.get_authorization_provider()
        if self._auth_provider is None:
            raise IllegalArgumentException(
                'Must configure AuthorizationProvider.')
        self._sess = Session()
        adapter = adapters.HTTPAdapter(pool_connections=self._pool_connections,
                                       pool_maxsize=self._pool_maxsize,
                                       max_retries=5, pool_block=True)
        self._sess.mount(self._url.scheme + '://', adapter)
        if self._proxy_host is not None:
            self._check_and_set_proxy(self._sess)

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
        content = self._write_content(request)
        BinaryProtocol.check_request_size_limit(request, len(content))
        headers = {'Host': self._url.hostname,
                   'Content-Type': 'application/octet-stream',
                   'Connection': 'keep-alive',
                   'Accept': 'application/octet-stream',
                   'Content-Length': str(len(content)),
                   'User-Agent': self._user_agent}
        if request.get_compartment() is None:
            request.set_compartment_internal(
                self._config.get_default_compartment())
        if self._logutils.is_enabled_for(DEBUG):
            self._logutils.log_debug('Request: ' + request.__class__.__name__)
        request_utils = RequestUtils(
            self._sess, self._logutils, request, self._retry_handler, self)
        return request_utils.do_post_request(
            self._request_uri, headers, content, timeout_ms,
            self._sec_info_timeout)

    def get_auth_provider(self):
        return self._auth_provider

    def shut_down(self):
        # Shutdown the client.
        self._logutils.log_info('Shutting down driver http client')
        if self._shut_down:
            return
        self._shut_down = True
        if self._auth_provider is not None:
            self._auth_provider.close()
        if self._sess is not None:
            self._sess.close()

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

    @staticmethod
    def _trace(msg, level):
        if level <= Client.TRACE_LEVEL:
            print('DRIVER: ' + msg)

    @staticmethod
    def _write_content(request):
        """
        Serializes the request payload, sent as http content.

        :param request: the request to be executed by the server.
        :type request: Request
        :returns: the bytearray that contains the content.
        :rtype: bytearray
        """
        content = bytearray()
        bos = ByteOutputStream(content)
        BinaryProtocol.write_serial_version(bos)
        request.create_serializer().serialize(
            request, bos, BinaryProtocol.SERIAL_VERSION)
        return content
