#
# Copyright (C) 2018, 2019 Oracle and/or its affiliates. All rights reserved.
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
from .serde import BinaryProtocol
from .version import __version__


class Client:
    # The HTTP driver client.
    def __init__(self, config, logger):
        self.__logutils = LogUtils(logger)
        self.__config = config
        self.__protocol = config.get_protocol()
        self.__host = config.get_host()
        self.__port = config.get_port()
        self.__request_uri = self.__generate_uri(self.__protocol, self.__host,
                                                 self.__port)
        self.__pool_connections = config.get_pool_connections()
        self.__pool_maxsize = config.get_pool_maxsize()
        self.__max_request_id = 1
        self.__proxy_host = config.get_proxy_host()
        self.__proxy_port = config.get_proxy_port()
        self.__proxy_username = config.get_proxy_username()
        self.__proxy_password = config.get_proxy_password()
        self.__retry_handler = config.get_retry_handler()
        if self.__retry_handler is None:
            self.__retry_handler = DefaultRetryHandler()
        self.__sec_info_timeout = config.get_sec_info_timeout()
        self.__shut_down = False
        self.__user_agent = self.__make_user_agent()
        self.__auth_provider = config.get_authorization_provider()
        if self.__auth_provider is None:
            raise IllegalArgumentException(
                'Must configure AuthorizationProvider.')
        self.__sess = Session()
        adapter = adapters.HTTPAdapter(pool_connections=self.__pool_connections,
                                       pool_maxsize=self.__pool_maxsize,
                                       max_retries=5, pool_block=True)
        self.__sess.mount(self.__protocol + '://', adapter)
        if self.__proxy_host is not None:
            self.__check_and_set_proxy(self.__sess)

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
        :returns: the result of the request.
        :raises IllegalArgumentException: raises the exception if request is
            None.
        """
        CheckValue.check_not_none(request, 'request')
        request.set_defaults(self.__config)
        request.validate()
        timeout_ms = request.get_timeout()
        auth_string = self.__auth_provider.get_authorization_string(request)
        if auth_string is None:
            raise IllegalArgumentException(
                'Configured AuthorizationProvider acquired an unexpected ' +
                'None authorization string.')
        content = bytearray()
        self.__write_content(request, content)
        BinaryProtocol.check_request_size_limit(request, len(content))
        headers = {'Host': self.__host,
                   'Content-Type': 'application/octet-stream',
                   'Connection': 'keep-alive',
                   'Accept': 'application/octet-stream',
                   'Authorization': auth_string,
                   'Content-Length': str(len(content)),
                   'User-Agent': self.__user_agent}
        if self.__logutils.is_enabled_for(DEBUG):
            self.__logutils.log_trace('Request: ' + request.__class__.__name__)
        request_utils = RequestUtils(
            self.__sess, self.__logutils, request, self.__retry_handler)
        return request_utils.do_post_request(
            self.__request_uri, headers, content, timeout_ms,
            self.__sec_info_timeout)

    def shut_down(self):
        # Shutdown the client.
        self.__logutils.log_info('Shutting down driver http client')
        if self.__shut_down:
            return
        self.__shut_down = True
        if self.__auth_provider is not None:
            self.__auth_provider.close()
        if self.__sess is not None:
            self.__sess.close()

    def __check_and_set_proxy(self, sess):
        if (self.__proxy_host is not None and self.__proxy_port == 0 or
                self.__proxy_host is None and self.__proxy_port != 0):
            raise IllegalArgumentException(
                'To configure an HTTP proxy, both host and port are required.')
        if (self.__proxy_username is not None and self.__proxy_password is None
                or self.__proxy_username is None and
                self.__proxy_password is not None):
            raise IllegalArgumentException(
                'To configure HTTP proxy authentication, both user name and ' +
                'password are required')
        if self.__proxy_host is not None:
            if self.__proxy_username is None:
                proxy_url = ('http://' + self.__proxy_host + ':' +
                             str(self.__proxy_port))
                sess.proxies = {'http': proxy_url, 'https': proxy_url}
            elif self.__proxy_username is not None:
                proxy_url = (self.__proxy_username + ':' +
                             self.__proxy_password + '@http://' +
                             self.__proxy_host + ':' + str(self.__proxy_port))
                sess.proxies = {'http': proxy_url, 'https': proxy_url}

    def __generate_uri(self, protocol, host, port):
        # Generate the uri for request.
        return (protocol + '://' + host + ':' + str(port) + '/' +
                HttpConstants.NOSQL_DATA_PATH)

    def __make_user_agent(self):
        if version_info.major >= 3:
            pyversion = python_version()
        else:
            pyversion = '%s.%s.%s' % (version_info.major, version_info.minor,
                                      version_info.micro)
        return '%s/%s (Python %s)' % ('NoSQL-PythonSDK', __version__, pyversion)

    def __write_content(self, request, content):
        """
        Serializes the request payload, sent as http content.

        :param request: the request to be executed by the server.
        :returns: the bytes contain the content.
        """
        bos = ByteOutputStream(content)
        BinaryProtocol.write_serial_version(bos)
        return request.create_serializer().serialize(request, bos)
