#
# Copyright (C) 2018, 2020 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
#
# Please see LICENSE.txt file included in the top-level directory of the
# appropriate download for a copy of the license and additional information.
#

from logging import DEBUG
from requests import ConnectionError, Timeout, codes
from threading import Lock
from time import time

from .common import ByteInputStream, HttpConstants, synchronized
from .exception import (
    IllegalStateException, NoSQLException, RequestTimeoutException,
    RetryableException, SecurityInfoNotReadyException)
from .kv import AuthenticationException, StoreAccessTokenProvider
from .serde import BinaryProtocol


class HttpResponse(object):
    # Class to package HTTP response output and status code.
    def __init__(self, content, status_code):
        self._content = content
        self._status_code = status_code

    def __str__(self):
        return ('HttpResponse [content=' + self._content + ', status_code=' +
                str(self._status_code) + ']')

    def get_content(self):
        return self._content

    def get_status_code(self):
        return self._status_code


class RequestUtils(object):
    # Utility to issue http request.
    def __init__(self, sess, logutils, request=None, retry_handler=None,
                 client=None):
        """
        Init the RequestUtils.

        :param sess: the session.
        :type sess: Session
        :param logutils: contains the logging methods.
        :type logutils: LogUtils
        :param request: request to execute.
        :type request: Request
        :param retry_handler: the retry handler.
        :type retry_handler: RetryHandler
        """
        self._sess = sess
        self._logutils = logutils
        self._request = request
        self._retry_handler = retry_handler
        self._client = client
        self._max_request_id = 1
        self._auth_provider = (
            client.get_auth_provider() if client is not None else None)
        self.lock = Lock()

    def do_delete_request(self, uri, headers, timeout_ms):
        """
        Issue HTTP DELETE request with retries and general error handling.

        It retries upon seeing following exceptions and response codes:

            IOException\n
            HTTP response with status code larger than 500\n
            Other throwable excluding RuntimeException, InterruptedException,
            ExecutionException and TimeoutException.

        :param uri: the request URI.
        :type uri: str
        :param headers: HTTP headers of this request.
        :type headers: dict
        :param timeout_ms: request timeout in milliseconds.
        :type timeout_ms: int
        :returns: HTTP response, a object encapsulate status code and response.
        :rtype: HttpResponse or Result
        """
        return self._do_request('DELETE', uri, headers, None, timeout_ms,
                                sec_timeout_ms=0)

    def do_get_request(self, uri, headers, timeout_ms):
        """
        Issue HTTP GET request with retries and general error handling.

        It retries upon seeing following exceptions and response codes:

            IOException\n
            HTTP response with status code larger than 500\n
            Other throwable excluding RuntimeException, InterruptedException,
            ExecutionException and TimeoutException.

        :param uri: the request URI.
        :type uri: str
        :param headers: HTTP headers of this request.
        :type headers: dict
        :param timeout_ms: request timeout in milliseconds.
        :type timeout_ms: int
        :returns: HTTP response, a object encapsulate status code and response.
        :rtype: HttpResponse or Result
        """
        return self._do_request('GET', uri, headers, None, timeout_ms,
                                sec_timeout_ms=0)

    def do_post_request(self, uri, headers, payload, timeout_ms,
                        sec_timeout_ms=0):
        """
        Issue HTTP POST request with retries and general error handling.

        It retries upon seeing following exceptions and response codes:

            IOException\n
            HTTP response with status code larger than 500\n
            Other throwable excluding RuntimeException, InterruptedException,
            ExecutionException and TimeoutException

        :param uri: the request URI.
        :type uri: str
        :param headers: HTTP headers of this request.
        :type headers: dict
        :param payload: payload in string.
        :type payload: str
        :param timeout_ms: request timeout in milliseconds.
        :type timeout_ms: int
        :param sec_timeout_ms: the timeout of waiting security information to be
            available in milliseconds.
        :type sec_timeout_ms: int
        :returns: HTTP response, a object encapsulate status code and response.
        :rtype: HttpResponse or Result
        """
        return self._do_request('POST', uri, headers, payload, timeout_ms,
                                sec_timeout_ms)

    def do_put_request(self, uri, headers, payload, timeout_ms,
                       sec_timeout_ms=0):
        """
        Issue HTTP PUT request with retries and general error handling.

        It retries upon seeing following exceptions and response codes:

            IOException\n
            HTTP response with status code larger than 500\n
            Other throwable excluding RuntimeException, InterruptedException,
            ExecutionException and TimeoutException

        :param uri: the request URI.
        :type uri: str
        :param headers: HTTP headers of this request.
        :type headers: dict
        :param payload: payload in string.
        :type payload: str
        :param timeout_ms: request timeout in milliseconds.
        :type timeout_ms: int
        :param sec_timeout_ms: the timeout of waiting security information to be
            available in milliseconds.
        :type sec_timeout_ms: int
        :returns: HTTP response, a object encapsulate status code and response.
        :rtype: HttpResponse or Result
        """
        return self._do_request('PUT', uri, headers, payload, timeout_ms,
                                sec_timeout_ms)

    def _do_request(self, method, uri, headers, payload, timeout_ms,
                    sec_timeout_ms):
        start_ms = int(round(time() * 1000))
        timeout_s = float(timeout_ms) / 1000
        throttle_retried = 0
        num_retried = 0
        exception = None
        while True:
            if self._auth_provider is not None:
                auth_string = self._auth_provider.get_authorization_string(
                    self._request)
                self._auth_provider.validate_auth_string(auth_string)
                self._auth_provider.set_required_headers(
                    self._request, auth_string, headers)
            if num_retried > 0:
                self._log_retried(num_retried, exception)
            response = None
            try:
                if self._request is not None:
                    request_id = str(self._next_request_id())
                    headers[HttpConstants.REQUEST_ID_HEADER] = request_id
                elif payload is not None:
                    payload = payload.encode()
                if payload is None:
                    response = self._sess.request(
                        method, uri, headers=headers, timeout=timeout_s)
                else:
                    # wrap the payload to make it compatible with pyOpenSSL,
                    # there maybe small cost to wrap it.
                    response = self._sess.request(
                        method, uri, headers=headers, data=memoryview(payload),
                        timeout=timeout_s)
                if self._logutils.is_enabled_for(DEBUG):
                    self._logutils.log_debug(
                        'Response: ' + self._request.__class__.__name__ +
                        ', status: ' + str(response.status_code))
                if self._request is not None:
                    return self._process_response(
                        self._request, response.content, response.status_code)
                else:
                    res = HttpResponse(response.content.decode(),
                                       response.status_code)
                    """
                    Retry upon status code larger than 500, in general, this
                    indicates server internal error.
                    """
                    if res.get_status_code() >= codes.server_error:
                        self._logutils.log_debug(
                            'Remote server temporarily unavailable, status ' +
                            'code ' + str(res.get_status_code()) +
                            ' , response ' + res.get_content())
                        num_retried += 1
                        continue
                    return res
            except AuthenticationException as ae:
                if (self._auth_provider is not None and isinstance(
                        self._auth_provider, StoreAccessTokenProvider)):
                    self._auth_provider.bootstrap_login()
                    exception = ae
                    num_retried += 1
                    continue
                self._logutils.log_info(
                    'Unexpected authentication exception: ' + str(ae))
                raise NoSQLException('Unexpected exception: ' + str(ae), ae)
            except RetryableException as re:
                self._logutils.log_debug('Retryable exception: ' + str(re))
                """
                Handle automatic retries. If this does not throw an error, then
                the delay (if any) will have been performed and the request
                should be retried.

                If there have been too many retries this method will throw the
                original exception.
                """
                retried = self._handle_retry(
                    re, self._request, throttle_retried)
                """
                Don't count retries for security info not ready as throttle
                retires.
                """
                if not isinstance(re, SecurityInfoNotReadyException):
                    throttle_retried = retried
                exception = re
                num_retried += 1
            except NoSQLException as nse:
                self._logutils.log_error(
                    'Client execution NoSQLException: ' + str(nse))
                raise nse
            except RuntimeError as re:
                self._logutils.log_error(
                    'Client execution RuntimeError: ' + str(re))
                raise re
            except ConnectionError as ce:
                self._logutils.log_error(
                    'HTTP request execution ConnectionError: ' + str(ce))
                raise ce
            except Timeout as t:
                if self._request is not None:
                    self._logutils.log_error('Timeout exception: ' + str(t))
                    break  # fall through to exception below
                raise RuntimeError('Timeout exception: ' + str(t))
            finally:
                if response is not None:
                    response.close()
            if self._timeout_request(
                    start_ms, timeout_ms, sec_timeout_ms, exception):
                break
        actual_timeout = timeout_ms
        if (self._request is not None and
                isinstance(exception, SecurityInfoNotReadyException)):
            actual_timeout = sec_timeout_ms
        raise RequestTimeoutException(
            'Request timed out after ' + str(num_retried) +
            (' retry.' if num_retried == 0 or num_retried == 1
             else ' retries.'), actual_timeout, exception)

    def _handle_retry(self, re, request, throttle_retried):
        throttle_retried += 1
        msg = ('Retry for request ' + request.__class__.__name__ + ', num ' +
               'retries: ' + str(throttle_retried) + ', exception: ' + str(re))
        self._logutils.log_debug(msg)
        handler = self._retry_handler
        if not handler.do_retry(request, throttle_retried, re):
            self._logutils.log_debug(
                'Operation not retry-able or too many retries.')
            raise re
        handler.delay(throttle_retried, re)
        return throttle_retried

    def _log_retried(self, num_retried, exception):
        msg = ('Client, doing retry: ' + str(num_retried) +
               ('' if exception is None else ', exception: ' + str(exception)))
        if (self._request is not None and
                isinstance(exception, SecurityInfoNotReadyException)):
            self._logutils.log_debug(msg)
        else:
            self._logutils.log_info(msg)

    @synchronized
    def _next_request_id(self):
        """
        Get the next client-scoped request id. It needs to be combined with the
        client id to obtain a globally unique scope.
        """
        self._max_request_id += 1
        return self._max_request_id

    def _process_response(self, request, content, status):
        """
        Convert the url_response into a suitable return value.

        :param request: the request executed by the server.
        :type request: Request
        :param content: the content of the response from the server.
        :type content: bytes for python 3 and str for python 2
        :param status: the status code of the response from the server.
        :type status: int
        :returns: the programmatic response object.
        :rtype: Result
        """
        if status == codes.ok:
            bis = ByteInputStream(bytearray(content))
            return self._process_ok_response(bis, request)
        self._process_not_ok_response(content, status)
        raise IllegalStateException('Unexpected http response status: ' +
                                    str(status))

    def _process_ok_response(self, bis, request):
        """
        Process an OK response.

        :param bis: the byte input stream created from the content of response
            get from the server.
        :type bis: ByteInputStream
        :param request: the request executed by the server.
        :type request: Request
        :returns: the result of processing the successful request.
        :rtype: Result
        """
        code = bis.read_byte()
        if code == 0:
            res = request.create_serializer().deserialize(
                request, bis, BinaryProtocol.SERIAL_VERSION)
            if request.is_query_request():
                if not request.is_simple_query():
                    request.get_driver().set_client(self._client)
            return res

        """
        Operation failed. Handle the failure and throw an appropriate
        exception.
        """
        err = BinaryProtocol.read_string(bis)
        raise BinaryProtocol.map_exception(code, err)

    @staticmethod
    def _process_not_ok_response(content, status):
        """
        Process not OK response. The method typically throws an appropriate
        exception. A normal return indicates that the method declined to handle
        the response and it's the caller's responsibility to take appropriate
        action.

        :param content: content of the response from the server.
        :type content: bytes for python 3 and str for python 2
        :param status: the status code of the response from the server.
        :type status: int
        """
        if status == codes.bad:
            length = len(content)
            err_msg = (content if length > 0 else str(status))
            raise NoSQLException('Error response: ' + err_msg)
        raise NoSQLException('Error response = ' + str(status))

    @staticmethod
    def _timeout_request(start_time, request_timeout, sec_timeout_ms,
                         exception):
        """
        Determine if the request should be timed out. If the last exception if
        the SecurityInfoNotReadyException, use its specific timeout to
        determine. Otherwise, check if the request exceed the timeout given.

        :param start_time: when the request starts.
        :type start_time: int
        :param request_timeout: the default timeout of this request.
        :type request_timeout: int
        :param sec_timeout_ms: the timeout of waiting security information to be
            available in milliseconds.
        :type sec_timeout_ms: int
        :param exception: the last exception.
        :type exception: RuntimeError
        :returns: True if the request need to be timed out.
        :rtype: bool
        """
        if isinstance(exception, SecurityInfoNotReadyException):
            return int(round(time() * 1000)) - start_time >= sec_timeout_ms
        return int(round(time() * 1000)) - start_time >= request_timeout
