#
# Copyright (C) 2018, 2019 Oracle and/or its affiliates. All rights reserved.
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

from .common import ByteInputStream
from .exception import (
    IllegalStateException, NoSQLException, RequestTimeoutException,
    RetryableException, SecurityInfoNotReadyException)
from .serde import BinaryProtocol


class HttpResponse:
    # Class to package HTTP response output and status code.
    def __init__(self, content, status_code):
        self.__content = content
        self.__status_code = status_code

    def __str__(self):
        return ('HttpResponse [content=' + self.__content + ', status_code=' +
                str(self.__status_code) + ']')

    def get_content(self):
        return self.__content

    def get_status_code(self):
        return self.__status_code


class RequestUtils:
    # Utility to issue http request.
    def __init__(self, sess, logutils, request=None, retry_handler=None):
        """
        Init the RequestUtils.

        :param sess: the session.
        :param logutils: contains the logging methods.
        :param request: request to execute.
        :param retry_handler: the retry handler.
        """
        self.__sess = sess
        self.__logutils = logutils
        self.__request = request
        self.__retry_handler = retry_handler
        self.__lock = Lock()
        self.__max_request_id = 1

    def do_delete_request(self, uri, headers, timeout_ms):
        """
        Issue HTTP DELETE request with retries and general error handling.

        It retries upon seeing following exceptions and response codes:

            IOException\n
            HTTP response with status code larger than 500\n
            Other throwable excluding RuntimeException, InterruptedException,
            ExecutionException and TimeoutException.

        :param uri: the request URI.
        :param headers: HTTP headers of this request.
        :param timeout_ms: request timeout in milliseconds.
        :returns: HTTP response, a object encapsulate status code and response.
        """
        return self.__do_request('DELETE', uri, headers, None, timeout_ms,
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
        :param headers: HTTP headers of this request.
        :param timeout_ms: request timeout in milliseconds.
        :returns: HTTP response, a object encapsulate status code and response.
        """
        return self.__do_request('GET', uri, headers, None, timeout_ms,
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
        :param headers: HTTP headers of this request.
        :param payload: payload in string.
        :param timeout_ms: request timeout in milliseconds.
        :param sec_timeout_ms: the timeout of waiting security information to be
            available in milliseconds.
        :returns: HTTP response, a object encapsulate status code and response.
        """
        return self.__do_request('POST', uri, headers, payload, timeout_ms,
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
        :param headers: HTTP headers of this request.
        :param payload: payload in string.
        :param timeout_ms: request timeout in milliseconds.
        :param sec_timeout_ms: the timeout of waiting security information to be
            available in milliseconds.
        :returns: HTTP response, a object encapsulate status code and response.
        """
        return self.__do_request('PUT', uri, headers, payload, timeout_ms,
                                 sec_timeout_ms)

    def __do_request(self, method, uri, headers, payload, timeout_ms,
                     sec_timeout_ms):
        start_ms = int(time() * 1000)
        timeout_s = timeout_ms // 1000
        if timeout_s == 0:
            timeout_s = 1
        throttle_retried = 0
        num_retried = 0
        exception = None
        while True:
            if num_retried > 0:
                self.__log_retried(num_retried, exception)
            response = None
            try:
                if self.__request is not None:
                    request_id = str(self.__next_request_id())
                    headers['x-nosql-request-id'] = request_id
                elif payload is not None:
                    payload = payload.encode()
                if payload is None:
                    response = self.__sess.request(
                        method, uri, headers=headers, timeout=timeout_s)
                else:
                    # wrap the payload to make it compatible with pyOpenSSL,
                    # there maybe small cost to wrap it.
                    response = self.__sess.request(
                        method, uri, headers=headers,
                        data=memoryview(payload), timeout=timeout_s)
                if self.__logutils.is_enabled_for(DEBUG):
                    self.__logutils.log_trace(
                        'Response: ' + self.__request.__class__.__name__ +
                        ', status: ' + str(response.status_code))
                if self.__request is not None:
                    return self.__process_response(
                        self.__request, response.content, response.status_code)
                else:
                    res = HttpResponse(response.content.decode(),
                                       response.status_code)
                    """
                    Retry upon status code larger than 500, in general, this
                    indicates server internal error.
                    """
                    if res.get_status_code() >= codes.server_error:
                        self.__logutils.log_debug(
                            'Remote server temporarily unavailable, status ' +
                            'code ' + str(res.get_status_code()) +
                            ' , response ' + res.get_content())
                        num_retried += 1
                        continue
                    return res
            except RetryableException as re:
                self.__logutils.log_debug('Retryable exception: ' + str(re))
                """
                Handle automatic retries. If this returns True then the delay
                (if any) will be performed and the request should be retried.
                If there have been too many retries this method will throw the
                original exception.
                """
                retried = self.__handle_retry(re, self.__request,
                                              throttle_retried)
                """
                Don't count retries for security info not ready as throttle
                retires.
                """
                if not isinstance(re, SecurityInfoNotReadyException):
                    throttle_retried = retried
                exception = re
                num_retried += 1
            except NoSQLException as nse:
                self.__logutils.log_error(
                    'Client execution NoSQLException: ' + str(nse))
                raise nse
            except RuntimeError as re:
                self.__logutils.log_error(
                    'Client execution RuntimeError: ' + str(re))
                raise re
            except ConnectionError as ce:
                self.__logutils.log_error(
                    'HTTP request execution ConnectionError: ' + str(ce))
                raise ce
            except Timeout as t:
                if self.__request is not None:
                    self.__logutils.log_error('Timeout exception: ' + str(t))
                    break  # fall through to exception below
                raise RuntimeError('Timeout exception: ' + str(t))
            finally:
                if response is not None:
                    response.close()
            if self.__timeout_request(
                    start_ms, timeout_ms, sec_timeout_ms, exception):
                break
        actual_timeout = timeout_ms
        if (self.__request is not None and
                isinstance(exception, SecurityInfoNotReadyException)):
            actual_timeout = sec_timeout_ms
        raise RequestTimeoutException(
            'Request timed out after ' + str(num_retried) +
            (' retry.' if num_retried == 0 or num_retried == 1
             else ' retries.'), actual_timeout, exception)

    def __handle_retry(self, re, request, throttle_retried):
        throttle_retried += 1
        msg = ('Retry for request ' + request.__class__.__name__ + ', num ' +
               'retries: ' + str(throttle_retried) + ', exception: ' + str(re))
        self.__logutils.log_debug(msg)
        handler = self.__retry_handler
        if not handler.do_retry(request, throttle_retried, re):
            self.__logutils.log_debug(
                'Operation not retry-able or too many retries.')
            raise re
        handler.delay(throttle_retried, re)
        return throttle_retried

    def __log_retried(self, num_retried, exception):
        msg = ('Client, doing retry: ' + str(num_retried) +
               ('' if exception is None else ', exception: ' + str(exception)))
        if (self.__request is not None and
                isinstance(exception, SecurityInfoNotReadyException)):
            self.__logutils.log_debug(msg)
        else:
            self.__logutils.log_info(msg)

    def __next_request_id(self):
        """
        Get the next client-scoped request id. It needs to be combined with the
        client id to obtain a globally unique scope.
        """
        with self.__lock:
            self.__max_request_id += 1
            return self.__max_request_id

    def __process_response(self, request, content, status):
        """
        Convert the url_response into a suitable return value.

        :param request: the request executed by the server.
        :param content: the content of the response from the server.
        :param status: the status code of the response from the server.
        :returns: the programmatic response object.
        """
        if status == codes.ok:
            bis = ByteInputStream(bytearray(content))
            return self.__process_ok_response(bis, request)
        self.__process_not_ok_response(content, status)
        raise IllegalStateException('Unexpected http response status: ' +
                                    str(status))

    def __process_ok_response(self, bis, request):
        """
        Process an OK response.

        :param bis: the byte input stream created from the content of response
            get from the server.
        :param request: the request executed by the server.
        :returns: the result of processing the successful request.
        """
        code = bis.read_byte()
        if code == 0:
            return request.create_deserializer().deserialize(bis)

        """
        Operation failed. Handle the failure and throw an appropriate
        exception.
        """
        err = BinaryProtocol.read_string(bis)
        raise BinaryProtocol.map_exception(code, err)

    def __process_not_ok_response(self, content, status):
        """
        Process not OK response. The method typically throws an appropriate
        exception. A normal return indicates that the method declined to handle
        the response and it's the caller's responsibility to take appropriate
        action.

        :param content: content of the response from the server.
        :param status: the status code of the response from the server.
        """
        if status == codes.bad:
            length = len(content)
            err_msg = (content if length > 0 else status)
            raise NoSQLException('Error response: ' + err_msg)
        raise NoSQLException('Error response = ' + str(status))

    def __timeout_request(self, start_time, request_timeout, sec_timeout_ms,
                          exception):
        """
        Determine if the request should be timed out. If the last exception if
        the SecurityInfoNotReadyException, use its specific timeout to
        determine. Otherwise, check if the request exceed the timeout given.

        :param start_time: when the request starts.
        :param request_timeout: the default timeout of this request.
        :param sec_timeout_ms: the timeout of waiting security information to be
            available in milliseconds.
        :param exception: the last exception.
        :returns: True if the request need to be timed out.
        """
        if isinstance(exception, SecurityInfoNotReadyException):
            return int(time() * 1000) - start_time >= sec_timeout_ms
        return int(time() * 1000) - start_time >= request_timeout
