#
# Copyright (c) 2018, 2022 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#

from abc import ABCMeta, abstractmethod
from io import UnsupportedOperation
from logging import DEBUG
from threading import Lock
from time import sleep, time

from requests import ConnectionError, Timeout, codes

from .common import ByteInputStream, CheckValue, synchronized
from .exception import (
    IllegalStateException, NoSQLException,
    ReadThrottlingException, RequestTimeoutException, RetryableException,
    SecurityInfoNotReadyException, UnsupportedProtocolException,
    WriteThrottlingException)

try:
    from . import config
    from . import kv
    from . import operations
    from . import serde
except ImportError:
    import config
    import kv
    import operations
    import serde


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
    SEC_ERROR_DELAY_MS = 100

    # Utility to issue http request.
    def __init__(self, sess, logutils, request=None, retry_handler=None,
                 client=None, rate_limiter_map=None):
        """
        Init the RequestUtils. There are 2 users of this class:
        1. Normal requests to the proxy. In this case a new instance of
        this class is created for every request
        2. KV-specific HTTP requests for login/logout/etc when using
        a secure store. In this case there is no request and the same
        RequestUtils instance is reused for all requests

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
        self._rate_limiter_map = rate_limiter_map
        self._auth_provider = (
            None if client is None else client.get_auth_provider())
        self.lock = Lock()

    def do_delete_request(self, uri, headers, timeout_ms):
        """
        Issue HTTP DELETE request with retries and general error handling.

        It retries upon seeing following exceptions and response codes:

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
        return self._do_request('DELETE', uri, headers, None, timeout_ms, None)

    def do_get_request(self, uri, headers, timeout_ms):
        """
        Issue HTTP GET request with retries and general error handling.

        It retries upon seeing following exceptions and response codes:

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
        return self._do_request('GET', uri, headers, None, timeout_ms, None)

    def do_post_request(self, uri, headers, payload, timeout_ms, stats_config):
        """
        Issue HTTP POST request with retries and general error handling.

        It retries upon seeing following exceptions and response codes:

            HTTP response with status code larger than 500\n
            Other throwable excluding RuntimeException, InterruptedException,
            ExecutionException and TimeoutException

        :param uri: the request URI.
        :type uri: str
        :param headers: HTTP headers of this request.
        :type headers: dict
        :param payload: payload in string.
        :type payload: bytearray
        :param timeout_ms: request timeout in milliseconds.
        :type timeout_ms: int
        :returns: HTTP response, a object encapsulate status code and response.
        :rtype: HttpResponse or Result
        """
        return self._do_request('POST', uri, headers, payload, timeout_ms,
            stats_config)

    def do_put_request(self, uri, headers, payload, timeout_ms):
        """
        Issue HTTP PUT request with retries and general error handling.

        It retries upon seeing following exceptions and response codes:

            HTTP response with status code larger than 500\n
            Other throwable excluding RuntimeException, InterruptedException,
            ExecutionException and TimeoutException

        :param uri: the request URI.
        :type uri: str
        :param headers: HTTP headers of this request.
        :type headers: dict
        :param payload: payload in string.
        :type payload: bytearray
        :param timeout_ms: request timeout in milliseconds.
        :type timeout_ms: int
        :returns: HTTP response, a object encapsulate status code and response.
        :rtype: HttpResponse or Result
        """
        return self._do_request('PUT', uri, headers, payload, timeout_ms, None)

    def _do_request(self, method, uri, headers, payload, timeout_ms,
                    stats_config):
        exception = None
        start_ms = int(round(time() * 1000))
        num_retried = 0
        # Clear any retry stats that may exist on this request object
        rate_delayed_ms = 0
        check_read_units = False
        check_write_units = False
        read_limiter = None
        write_limiter = None
        if self._request is not None:
            self._request.set_retry_stats(None)
            # If the request itself specifies rate limiters, use them
            read_limiter = self._request.get_read_rate_limiter()
            if read_limiter is not None:
                check_read_units = True
            write_limiter = self._request.get_write_rate_limiter()
            if write_limiter is not None:
                check_write_units = True
            # If not, see if we have limiters in our map for the given table
            if (self._rate_limiter_map is not None and read_limiter is None and
                    write_limiter is None):
                table_name = self._request.get_table_name()
                if table_name is not None:
                    read_limiter = self._rate_limiter_map.get_read_limiter(
                        table_name)
                    write_limiter = self._rate_limiter_map.get_write_limiter(
                        table_name)
                    if read_limiter is None and write_limiter is None:
                        if (self._request.does_reads() or
                                self._request.does_writes()):
                            self._client.background_update_limiters(table_name)
                    else:
                        check_read_units = self._request.does_reads()
                        self._request.set_read_rate_limiter(read_limiter)
                        check_write_units = self._request.does_writes()
                        self._request.set_write_rate_limiter(write_limiter)
            start_ms = int(round(time() * 1000))
            self._request.set_start_time_ms(start_ms)

        while True:
            this_time = int(round(time() * 1000))
            this_iteration_timeout_ms = timeout_ms - (this_time - start_ms)
            this_iteration_timeout_s = (float(this_iteration_timeout_ms) / 1000)
            if self._request is not None:
                self._client.check_request(self._request)
                """
                Check rate limiters before executing the request. Wait for read
                and/or write limiters to be below their limits before
                continuing. Be aware of the timeout given.
                """
                if read_limiter is not None and check_read_units:
                    try:
                        # This may sleep for a while, up to
                        # this_iteration_timeout_ms and may throw
                        # TimeoutException.
                        rate_delayed_ms += (
                            read_limiter.consume_units_with_timeout(
                                0, this_iteration_timeout_ms, False))
                    except Exception as e:
                        exception = e
                        break
                if write_limiter is not None and check_write_units:
                    try:
                        # This may sleep for a while, up to
                        # this_iteration_timeout_ms and may throw
                        # TimeoutException.
                        rate_delayed_ms += (
                            write_limiter.consume_units_with_timeout(
                                0, this_iteration_timeout_ms, False))
                    except Exception as e:
                        exception = e
                        break
                # Ensure limiting didn't throw us over the timeout
                if self._timeout_request(start_ms, timeout_ms):
                    break
                if self._auth_provider is not None:
                    auth_string = self._auth_provider.get_authorization_string(
                        self._request)
                    self._auth_provider.validate_auth_string(auth_string)
                    self._auth_provider.set_required_headers(
                        self._request, auth_string, headers)
                num_retried = self._request.get_num_retries()
            if num_retried > 0:
                self._log_retried(num_retried, exception)
            response = None
            network_time = 0
            if stats_config is not None:
                time()
            req_size = 0
            if payload is not None:
                req_size = len(payload)

            try:
                # this logic is accounting for the fact that there may
                # be kv requests that do not have a request instance, and
                # only contain a payload
                if self._request is None and payload is not None:
                    payload, req_size = payload.encode()
                if payload is None:
                    response = self._sess.request(
                        method, uri, headers=headers,
                        timeout=this_iteration_timeout_s)
                else:
                    # wrap the payload to make it compatible with pyOpenSSL,
                    # there maybe small cost to wrap it.
                    response = self._sess.request(
                        method, uri, headers=headers, data=memoryview(payload),
                        timeout=this_iteration_timeout_s)
                if stats_config is not None:
                    network_time = int(round(
                        (time() - network_time) * 1000000)) / 1000
                if self._logutils.is_enabled_for(DEBUG):
                    self._logutils.log_debug(
                        'Response: ' + self._request.__class__.__name__ +
                        ', status: ' + str(response.status_code))
                if self._request is not None:
                    res = self._process_response(
                        self._request, response.content, response.status_code)
                    if (isinstance(res, operations.TableResult) and
                            self._rate_limiter_map is not None):
                        # Update rate limiter settings for table.
                        tl = res.get_table_limits()
                        self._client.update_rate_limiters(
                            res.get_table_name(), tl)
                    if (self._rate_limiter_map is not None and
                            read_limiter is None):
                        read_limiter = self._get_query_rate_limiter(True)
                    if (self._rate_limiter_map is not None and
                            write_limiter is None):
                        write_limiter = self._get_query_rate_limiter(False)

                    # Consume rate limiter units based on actual usage.
                    rate_delayed_ms += RequestUtils._consume_limiter_units(
                        read_limiter, res.get_read_units(),
                        this_iteration_timeout_ms)
                    rate_delayed_ms += RequestUtils._consume_limiter_units(
                        write_limiter, res.get_write_units(),
                        this_iteration_timeout_ms)
                    res.set_rate_limit_delayed_ms(rate_delayed_ms)
                    self._request.set_rate_limit_delayed_ms(rate_delayed_ms)
                    # Copy retry stats to Result on successful operation.
                    res.set_retry_stats(self._request.get_retry_stats())
                    if stats_config is not None:
                        stats_config.observe(self._request, req_size,
                                             len(response.content),
                                             network_time)

                    # check for a Set-Cookie header
                    cookie = response.headers.get('Set-Cookie', None)
                    if cookie is not None and cookie.startswith('session='):
                        self._client.set_session_cookie(cookie)
                    return res
                else:
                    res = HttpResponse(response.content.decode(),
                                       response.status_code)
                    if stats_config is not None:
                        network_time = int(round(
                            (time() - network_time) * 1000000)) / 1000
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
                    if stats_config is not None:
                        stats_config.observe(None, 0, len(response.content),
                                             network_time)
                    return res
            except kv.AuthenticationException as ae:
                if (self._auth_provider is not None and isinstance(
                        self._auth_provider, kv.StoreAccessTokenProvider)):
                    self._auth_provider.bootstrap_login()
                    self._request.add_retry_exception(ae.__class__.__name__)
                    self._request.increment_retries()
                    exception = ae
                    continue
                self._logutils.log_error(
                    'Unexpected authentication exception: ' + str(ae))
                if stats_config is not None:
                    stats_config.observe_error(self._request)
                raise NoSQLException('Unexpected exception: ' + str(ae), ae)
            except SecurityInfoNotReadyException as se:
                self._request.add_retry_exception(se.__class__.__name__)
                delay_ms = RequestUtils.SEC_ERROR_DELAY_MS
                if self._request.get_num_retries() > 10:
                    delay_ms = config.DefaultRetryHandler.compute_backoff_delay(
                        self._request, 0)
                    if delay_ms <= 0:
                        break
                sleep(float(delay_ms) / 1000)
                self._request.add_retry_delay_ms(delay_ms)
                self._request.increment_retries()
                exception = se
                continue
            except RetryableException as re:
                if (isinstance(re, WriteThrottlingException) and
                        write_limiter is not None):
                    # Ensure we check write limits next loop.
                    check_write_units = True
                    # Set limiter to its limit, if not over already.
                    if write_limiter.get_current_rate() < 100.0:
                        write_limiter.set_current_rate(100.0)
                if (isinstance(re, ReadThrottlingException) and
                        read_limiter is not None):
                    # Ensure we check read limits next loop.
                    check_read_units = True
                    # Set limiter to its limit, if not over already.
                    if read_limiter.get_current_rate() < 100.0:
                        read_limiter.set_current_rate(100.0)
                self._logutils.log_debug('Retryable exception: ' + str(re))
                """
                Handle automatic retries. If this does not throw an error, then
                the delay (if any) will have been performed and the request
                should be retried.

                If there have been too many retries this method will throw the
                original exception.
                """
                self._request.add_retry_exception(re.__class__.__name__)
                self._handle_retry(re, self._request)
                self._request.increment_retries()
                exception = re
                continue
            except UnsupportedProtocolException as upe:
                if self._client.decrement_serial_version():
                    if self._request is not None:
                        payload = self._client.serialize_request(self._request,
                                                                 headers)
                    self._request.increment_retries()
                    exception = upe
                    continue
                self._logutils.log_error(
                    'Client execution UnsupportedProtocolException: ' + str(upe))
                raise upe
            except NoSQLException as nse:
                self._logutils.log_error(
                    'Client execution NoSQLException: ' + str(nse))
                if stats_config is not None:
                    stats_config.observe_error(self._request)
                raise nse
            except RuntimeError as re:
                self._logutils.log_error(
                    'Client execution RuntimeError: ' + str(re))
                if stats_config is not None:
                    stats_config.observe_error(self._request)
                raise re
            except ConnectionError as ce:
                self._logutils.log_error(
                    'HTTP request execution ConnectionError: ' + str(ce))
                if stats_config is not None:
                    stats_config.observe_error(self._request)
                raise ce
            except Timeout as t:
                if self._request is not None:
                    self._logutils.log_error('Timeout exception: ' + str(t))
                    break  # fall through to exception below
                if stats_config is not None:
                    stats_config.observe_error(self._request)
                raise RuntimeError('Timeout exception: ' + str(t))
            finally:
                if response is not None:
                    response.close()
            if self._timeout_request(start_ms, timeout_ms):
                break
        retry_stats = ''
        if self._request is not None:
            retry_stats = self._request.get_retry_stats()
            num_retried = self._request.get_num_retries()
        if stats_config is not None:
            stats_config.observe_error(self._request)
        raise RequestTimeoutException(
            'Request timed out after ' + str(num_retried) +
            (' retry.' if num_retried == 0 or num_retried == 1
             else ' retries. ') + str(retry_stats), timeout_ms, exception)

    @staticmethod
    def _consume_limiter_units(rl, units, timeout_ms):
        """
        Consume rate limiter units after successful operation. Returns the
        number of milliseconds delayed due to rate limiting.
        """
        if rl is None or units <= 0:
            return 0
        """
        The logic consumes units (and potentially delays) _after_ a successful
        operation for a couple reasons:

        * We don't know the actual number of units an op uses until after the
          operation successfully finishes.
        * Delaying after the op keeps the application from immediately trying
          the next op and ending up waiting along with other client threads
          until the rate goes below the limit, at which time all client threads
          would continue at once. By waiting after a successful op, client
          threads will get staggered better to avoid spikes in throughput and
          oscillation that can result from it.
        """
        try:
            return rl.consume_units_with_timeout(units, timeout_ms, False)
        except Timeout:
            # Don't throw - operation succeeded. Just return timeout_ms.
            return timeout_ms

    def _get_query_rate_limiter(self, read):
        """
        Returns a rate limiter for a query operation, if the query op has a
        prepared statement and a limiter exists in the rate limiter map for the
        query table.
        """
        if (self._rate_limiter_map is None or
                not isinstance(self._request, operations.QueryRequest)):
            return None
        # If we're asked for a write limiter, and the request doesn't do writes,
        # return None
        if not read and not self._request.does_writes():
            return None
        # We sometimes may only get a prepared statement after the first query
        # response is returned. In this case, we can get the table_name from the
        # request and apply rate limiting.
        table_name = self._request.get_table_name()
        if table_name is None or table_name == '':
            return None
        if read:
            return self._rate_limiter_map.get_read_limiter(table_name)
        return self._rate_limiter_map.get_write_limiter(table_name)

    def _handle_retry(self, re, request):
        num_retries = self._request.get_num_retries()
        msg = ('Retry for request ' + request.__class__.__name__ + ', num ' +
               'retries: ' + str(num_retries) + ', exception: ' + str(re))
        self._logutils.log_debug(msg)
        handler = self._retry_handler
        if not handler.do_retry(request, num_retries, re):
            self._logutils.log_debug(
                'Operation not retry-able or too many retries.')
            raise re
        handler.delay(request, num_retries, re)

    def _log_retried(self, num_retried, exception):
        msg = ('Client, doing retry: ' + str(num_retried) +
               ('' if exception is None else ', exception: ' + str(exception)))
        self._logutils.log_debug(msg)

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
                request, bis, self._client.serial_version)
            if request.is_query_request():
                if not request.is_simple_query():
                    request.get_driver().set_client(self._client)
            return res

        """
        Operation failed. Handle the failure and throw an appropriate
        exception.
        """
        err = serde.BinaryProtocol.read_string(bis)
        raise serde.BinaryProtocol.map_exception(code, err)

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
    def _timeout_request(start_time, request_timeout):
        """
        Determine if the request should be timed out.
        Check if the request exceed the timeout given.

        :param start_time: when the request starts.
        :type start_time: int
        :param request_timeout: the default timeout of this request.
        :type request_timeout: int
        :returns: True if the request need to be timed out.
        :rtype: bool
        """
        return int(round(time() * 1000)) - start_time >= request_timeout


class RateLimiter(object):
    """
    RateLimiter provides default methods that all rate limiters  must implement.

    In NoSQL Cloud, rate limiters are used internally in :py:class:`NoSQLHandle`
    operations when enabled using
    :py:meth:`NoSQLHandleConfig.set_rate_limiting_enabled`.

    **Typical usage:**

    The simplest use of the rate limiter is to consume a number of units,
    blocking until they are successfully consumed:

    .. code-block:: pycon

        # delay_ms indicates how long the consume delayed
        delay_ms = rate_limiter.consume_units(units)

    To poll a limiter to see if it is currently over the limit:

    .. code-block:: pycon

        if rate_limiter.try_consume_units(0):
            # Limiter is below its limit.

    To attempt to consume units, only if they can be immediately consumed
    without waiting:

    .. code-block:: pycon

        if rate_limiter.try_consume_units(units):
            # Successful consume.
        else:
            # Could not consume units without waiting.

    Usages that involve waiting with timeouts:

    In cases where the number of units an operation will consume is already
    known before the operation, a simple one-shot method can be used:

    .. code-block:: pycon

        units = (known units the operation will use)
        try:
            # Don't consume if we time out.
            always_consume=False
            delay_ms = rate_limiter.consume_units_with_timeout(
                units, timeout_ms, always_consume)
            # We waited delay_ms for the units to be consumed, and the consume
            # was successful.
            # do operation
        except Timeout as e:
            # We could not do the operation within the given timeframe.
            # skip operation

    In cases where the number of units an operation will consume is not known
    before the operation, typically two rate limiter calls would be used: one to
    wait till the limiter is below it limit, and a second to update the limiter
    with used units:

    .. code-block:: pycon

        # Wait until we're under the limit.
        try:
            # Note here we don't consume units if we time out.
            delay_ms = rate_limiter.consume_units_with_timeout(
                0, timeout_ms, False)
        except Timeout as e:
            # We could not go below the limit within the given timeframe.
            # skip operation
        # We waited delayMs to be under the limit, and were successful.
        units = ...do operation, get number of units used...
        # Update rate limiter with consumed units. Next operation on this
        # limiter may delay due to it being over its limit.
        rate_limiter.consume_units(units)

    Alternately, the operation could be always performed, and then the limiter
    could try to wait for the units to be consumed:

    .. code-block:: pycon

        units = ...do operation, get number of units used...
        try:
            # Consume, even if we time out.
            always_consume=True
            delay_ms = rate_limiter.consume_units_with_timeout(
                units, timeout_ms, always_consume)
            # We waited delayMs for the units to be consumed, and the consume
            # was successful.
        except Timeout:
            # The rate limiter could not consume the units and go below the
            # limit in the given timeframe. Units are consumed anyway, and the
            # next call to this limiter will likely delay.

    **Limiter duration:**

    Implementing rate limiters should support a configurable "duration". This is
    sometimes referred to as a "burst mode", or a "window time", or "burst
    duration". This will allow consumes of units that were not consumed in the
    recent past. For example, if a limiter allows for 100 units per second, and
    is not used for 5 seconds, it should allow an immediate consume of 500 units
    with no delay upon a consume call, assuming that the limiter's duration is
    set to at least 5 seconds.

    The maximum length of time for this duration should be configurable. In all
    cases a limiter should set a reasonable minimum duration, such that a call
    to try_consume_units(1) has a chance of succeeding. It is up to the limiter
    implementation to determine if units from the past before the limiter was
    created or reset are available for use. If a limiter implementation does not
    allow setting a duration, it must throw an UnsupportedOperation when its
    set_duration() method is called.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def consume_units(self, units):
        """
        Consumes a number of units, blocking until the units are available.

        :param units: the number of units to consume. This can be a negative
            value to "give back" units.
        :type units: int
        :returns: the amount of time blocked in milliseconds. If not blocked, 0
            is returned.
        :rtype: int
        :raises IllegalArgumentException: raises the exception if units is not
            an integer.
        """
        pass

    @abstractmethod
    def consume_units_unconditionally(self, units):
        """
        Consumes units without checking or waiting.

        The internal amount of units consumed will be updated by the given
        amount, regardless of its current over/under limit state.

        :param units: the number of units to consume (may be negative to give
            back units)
        :type units: int
        :raises IllegalArgumentException: raises the exception if units is not
            an integer.
        """
        pass

    @abstractmethod
    def consume_units_with_timeout(self, units, timeout_ms, always_consume):
        """
        Attempts to consume a number of units, blocking until the units are
        available or the specified timeout expires.

        :param units: the number of units to consume. This can be a negative
            value to "give back" units.
        :type units: int
        :param timeout_ms: the timeout in milliseconds. Pass 0 to block
            indefinitely. To poll if the limiter is currently over its limit,
            use :py:meth:`try_consume_units` instead.
        :type timeout_ms: int
        :param always_consume: if True, consume units even on timeout.
        :type always_consume: bool
        :returns: the amount of time blocked in milliseconds. If not blocked, 0
            is returned.
        :rtype: int
        :raises Timeout: if the timeout expires before the units can be acquired
            by the limiter.
        :raises IllegalArgumentException: raises the exception if units is not
            an integer, timeout_ms is a negative number or always_consume is not
            True or False.
        """
        pass

    @abstractmethod
    def get_current_rate(self):
        """
        Returns the current rate as a percentage of current limit.

        :returns: the rate as of this instant in time.
        :rtype: float
        """
        pass

    def get_duration(self):
        """
        Returns the duration configured for this rate limiter instance.

        :returns: the duration in seconds.
        :rtype: float
        :raises UnsupportedOperation: raises this if duration is not supported
            by this limiter.
        """
        raise UnsupportedOperation('Duration not implemented')

    @abstractmethod
    def get_limit_per_second(self):
        """
        Returns the number of units configured for this rate limiter instance.

        :returns: the max number of units per second this limiter allows.
        :rtype: float
        """
        pass

    @abstractmethod
    def reset(self):
        """
        Resets the rate limiter as if it was newly constructed.

        Allows reuse.
        """
        pass

    @abstractmethod
    def set_current_rate(self, rate_to_set):
        """
        Sets the current rate as a percentage of current limit.

        This modifies the internal limiter values; it does not modify the rate
        limit.

        :param rate_to_set: percentage to set the current rate to. This may be
            greater than 100.0 to set the limiter to "over its limit".
        :type rate_to_set: float
        :raises IllegalArgumentException: raises the exception if rate_to_set is
            not a non-negative float.
        """
        pass

    def set_duration(self, duration_secs):
        """
        Sets the duration for this rate limiter instance.

        The duration specifies how far back in time the limiter will go to
        consume previously unused units.

        For example, if a limiter had a limit of 1000 units and a 5 seconds, and
        had no units consumed for at least 5 seconds, a call to
        try_consume_units(5000) will succeed immediately with no waiting.

        :param duration_secs: the duration in seconds.
        :type duration_secs: float
        :raises UnsupportedOperation: raises this if duration is not supported
            by this limiter.
        """
        raise UnsupportedOperation('Duration not implemented')

    @abstractmethod
    def set_limit_per_second(self, rate_limit_per_second):
        """
        Sets a new limit (units per second) on the limiter.

        Note that implementing rate limiters should fully support non-integer
        (float) values internally, to avoid issues when the limits are set very
        low.

        Changing the limit may lead to unexpected spiky behavior, and may affect
        other threads currently operating on the same limiter instance.

        :param rate_limit_per_second: the new number of units to allow.
        :type rate_limit_per_second: float
        :raises IllegalArgumentException: raises the exception if
            rate_limit_per_second is not a non-negative float.
        """
        pass

    @abstractmethod
    def try_consume_units(self, units):
        """
        Consumes the specified number of units if they can be returned
        immediately without waiting.

        :param units: the number of units to consume. Pass zero to poll if the
            limiter is currently over its limit. Pass negative values to "give
            back" units (same as calling :py:meth:`consume_units` with a
            negative value).
        :type units: int
        :returns: True if the units were consumed, False if they were not
            immediately available. If units was zero, True means the limiter is
            currently below its limit.
        :rtype: bool
        :raises IllegalArgumentException: raises the exception if units is not
            an integer.
        """
        pass


class RateLimiterMap(object):
    """
    A map of table names to RateLimiter instances.
    Each entry in the map has both a read and write rate limiter instance.
    """

    def __init__(self):
        self._limiter_map = dict()
        self.lock = Lock()

    def clear(self):
        # Clear all rate limiters from map.
        self._limiter_map.clear()

    def get_read_limiter(self, table_name):
        """
        Get a Read RateLimiter instance from the map.

        :param table_name: name or OCID of the table.
        :type table_name: str
        :returns: the RateLimiter instance, or null if it does not exist in the
            map.
        :rtype: RateLimiter
        """
        rle = self._limiter_map.get(table_name.lower())
        if rle is None:
            return None
        return rle.read_limiter

    def get_write_limiter(self, table_name):
        """
        Get a Write RateLimiter instance from the map.

        :param table_name: name or OCID of the table.
        :type table_name: str
        :returns: the RateLimiter instance, or null if it does not exist in the
            map.
        :rtype: RateLimiter
        """
        rle = self._limiter_map.get(table_name.lower())
        if rle is None:
            return None
        return rle.write_limiter

    def limiters_exist(self, table_name):
        """
        Return True if a pair of limiters exist for given table. This can be
        used to accelerate non-limited operations (skip if not doing rate
        limiting).
        """
        return self._limiter_map.get(table_name.lower()) is not None

    def remove(self, table_name):
        # Remove limiters from the map based on table name.
        self._limiter_map.pop(table_name.lower(), None)

    @synchronized
    def reset(self, table_name):
        """
        Internal use only.

        Allow tests to reset limiters in map.

        :param table_name: name or OCID of the table.
        :type table_name: str
        """
        lower_table = table_name.lower()
        rle = self._limiter_map.get(lower_table)
        if rle is not None:
            rle.read_limiter.reset()
            rle.write_limiter.reset()

    @synchronized
    def update(self, table_name, read_units, write_units, duration_seconds):
        """
        Put a new Entry into the map if it does not exist.
        If the specified rate limiter already exists, its units will be updated.

        :param table_name: name or OCID of the table.
        :type table_name: str
        :param read_units: number of read units per second.
        :type read_units: float
        :param write_units: number of write units per second.
        :type write_units: float
        :param duration_seconds: duration in seconds.
        :type duration_seconds: float
        """
        if read_units <= 0 and write_units <= 0:
            self.remove(table_name)
            return
        lower_table = table_name.lower()
        rle = self._limiter_map.get(lower_table)
        if rle is None:
            rrl = SimpleRateLimiter(read_units, duration_seconds)
            wrl = SimpleRateLimiter(write_units, duration_seconds)
            self._limiter_map[lower_table] = RateLimiterMap.Entry(rrl, wrl)
        else:
            # Set existing limiters to new values. If the new values result in a
            # different rate than previous, reset the limiters.
            prev_rus = rle.read_limiter.get_limit_per_second()
            prev_wus = rle.write_limiter.get_limit_per_second()
            rle.read_limiter.set_limit_per_second(read_units)
            rle.write_limiter.set_limit_per_second(write_units)
            if rle.read_limiter.get_limit_per_second() != prev_rus:
                rle.read_limiter.reset()
            if rle.write_limiter.get_limit_per_second() != prev_wus:
                rle.write_limiter.reset()

    class Entry(object):

        def __init__(self, read_limiter, write_limiter):
            self.read_limiter = read_limiter
            self.write_limiter = write_limiter


class SimpleRateLimiter(RateLimiter):
    """
    An implementation of RateLimiter using a simple time-based mechanism.

    This limiter keeps a single "last_nano" time and a "nanos_per_unit".
    Together these represent a number of units available based on the current
    time.

    When units are consumed, the last_nano value is incremented by
    (units * nanos_per_unit). If the result is greater than the current time, a
    single sleep() is called to wait for the time difference.

    This method inherently "queues" the consume calls, since each consume will
    increment the last_nano time. For example, a request for a small number of
    units will have to wait for a previous request for a large number of units.
    In this way, small requests can never "starve" large requests.

    This limiter allows for a specified number of seconds of "duration" to be
    used: if units have not been used in the past N seconds, they can be used
    now. The minimum duration is internally bound such that a consume of 1 unit
    will always have a chance of succeeding without waiting.

    Note that "giving back" (returning) previously consumed units will only
    affect consume calls made after the return. Currently sleeping consume calls
    will not have their sleep times shortened.
    """
    NANOS = 1000000000.0

    def __init__(self, rate_limit_per_sec, duration_secs=1.0):
        """
        Creates a simple time-based rate limiter.

        :param rate_limit_per_sec: the maximum number of units allowed per
            second.
        :type rate_limit_per_sec: float
        :param duration_secs: maximum amount of time to consume unused units
            from the past, default is 1 second.
        :type duration_secs: float
        """
        self._duration_nanos = 0
        self._last_nano = 0
        self._nanos_per_unit = 0
        self.lock = Lock()
        self.set_limit_per_second(rate_limit_per_sec)
        self.set_duration(duration_secs)
        self.reset()

    def __str__(self):
        return ('last_nano=' + str(self._last_nano) + ', nanos_per_unit=' +
                str(self._nanos_per_unit) + ', duration_nanos=' +
                str(self._duration_nanos) + ', limit=' +
                str(self.get_limit_per_second()) + ', capacity=' +
                str(self.get_capacity()) + ', rate=' +
                str(self.get_current_rate()))

    def consume_externally(self, units):
        """
        Consumes units and returns the time to sleep.

        Note this method returns immediately in all cases. It returns the number
        of milliseconds to sleep.

        :param units: number of units to attempt to consume.
        :type units: int
        :returns: number of milliseconds to sleep. If the return value is zero,
            the consume succeeded under the limit and no sleep is necessary.
        """
        # If disabled, just return success.
        if self._nanos_per_unit <= 0:
            return 0
        return self._consume(
            units, 0, True, int(round(time() * SimpleRateLimiter.NANOS)))

    def consume_units(self, units):
        """
        Call internal logic, get the time we need to sleep to complete the
        consume. note this call immediately consumes the units
        """
        ms_to_sleep = self._consume(
            units, 0, False, int(round(time() * SimpleRateLimiter.NANOS)))
        # Sleep for the requested time.
        sleep(float(ms_to_sleep) / 1000)
        # Return the amount of time slept
        return ms_to_sleep

    def consume_units_unconditionally(self, units):
        # Consume units, ignore amount of time to sleep.
        self._consume(
            units, 0, True, int(round(time() * SimpleRateLimiter.NANOS)))

    def consume_units_with_timeout(self, units, timeout_ms, always_consume):
        CheckValue.check_int_ge_zero(timeout_ms, 'timeout_ms')
        # call internal logic, get the time we need to sleep to complete the
        # consume.
        ms_to_sleep = self._consume(
            units, timeout_ms, always_consume,
            int(round(time() * SimpleRateLimiter.NANOS)))
        if ms_to_sleep == 0:
            return 0
        """
        If the time required to consume is greater than our timeout, sleep up to
        the timeout then throw a timeout exception. Note the units may have
        already been consumed if always_consume is True.
        """
        if 0 < timeout_ms <= ms_to_sleep:
            sleep(float(timeout_ms) / 1000)
            raise Timeout('Timed out waiting ' + str(timeout_ms) + 'ms for ' +
                          str(units) + ' units in rate limiter.')
        # Sleep for the requested time.
        sleep(float(ms_to_sleep) / 1000)
        # Return the amount of time slept.
        return ms_to_sleep

    def get_capacity(self):
        # Ensure we never use more from the past than duration allows.
        now_nanos = int(round(time() * SimpleRateLimiter.NANOS))
        max_past = now_nanos - self._duration_nanos
        if self._last_nano > max_past:
            max_past = self._last_nano
        return float(now_nanos - max_past) / self._nanos_per_unit

    def get_current_rate(self):
        # See comment in set_current_rate()
        cap = self.get_capacity()
        limit = self.get_limit_per_second()
        rate = 100.0 - cap * 100.0 / limit
        if rate < 0.0:
            return 0.0
        return rate

    def get_duration(self):
        return self._duration_nanos / SimpleRateLimiter.NANOS

    def get_limit_per_second(self):
        if self._nanos_per_unit == 0:
            return 0.0
        return SimpleRateLimiter.NANOS / self._nanos_per_unit

    def reset(self):
        self._last_nano = int(round(time() * SimpleRateLimiter.NANOS))

    def set_current_rate(self, percent):
        """
        Note that "rate" isn't really clearly defined in this type of limiter,
        because there is no inherent "time period". So all "rate" operations
        just assume "for 1 second".
        """
        now_nanos = int(round(time() * SimpleRateLimiter.NANOS))
        if percent == 100.0:
            self._last_nano = now_nanos
            return
        percent -= 100.0
        self._last_nano = (now_nanos +
                           int(percent / 100.0 * SimpleRateLimiter.NANOS))

    def set_duration(self, duration_secs):
        self._duration_nanos = int(duration_secs * SimpleRateLimiter.NANOS)
        self._enforce_minimum_duration()

    @synchronized
    def set_limit_per_second(self, rate_limit_per_sec):
        if rate_limit_per_sec <= 0.0:
            self._nanos_per_unit = 0
        else:
            self._nanos_per_unit = int(
                SimpleRateLimiter.NANOS / rate_limit_per_sec)
        self._enforce_minimum_duration()

    def try_consume_units(self, units):
        if self._consume(units, 1, False,
                         int(round(time() * SimpleRateLimiter.NANOS))) == 0:
            return True
        return False

    @synchronized
    def _consume(self, units, timeout_ms, always_consume, now_nanos):
        """
        Returns the time to sleep to consume units.

        Note this method returns immediately in all cases. It returns the number
        of milliseconds to sleep.

        This is the only method that actually "consumes units", i.e. updates the
        last_nano value.

        :param units: number of units to attempt to consume.
        :type units: int
        :param timeout_ms: max time to allow for consumption. Pass zero for no
            timeout (infinite wait).
        :type timeout_ms: int
        :param always_consume: if True, units will be consumed regardless of
            return value.
        :type always_consume: bool
        :param now_nanos: current time in nanos.
        :type now_nanos: int
        :returns: number of milliseconds to sleep. If timeout_ms is positive,
            and the return value is greater than or equal to timeout_ms, consume
            failed and the app should just sleep for timeout_ms then throw an
            exception. If the return value is zero, the consume succeeded under
            the limit and no sleep is necessary.
        :rtype: int
        """
        # If disabled, just return success.
        if self._nanos_per_unit <= 0:
            return 0
        # Determine how many nanos we need to add based on units requested.
        nanos_needed = units * self._nanos_per_unit
        # Ensure we never use more from the past than duration allows.
        max_past = now_nanos - self._duration_nanos
        if self._last_nano < max_past:
            self._last_nano = max_past
        # Compute the new "last nano used".
        new_last = self._last_nano + nanos_needed
        # If units < 0, we're "returning" them.
        if units < 0:
            # Consume the units.
            self._last_nano = new_last
            return 0

        # If the limiter is currently under its limit, the consume succeeds
        # immediately (no sleep required).
        if self._last_nano < now_nanos:
            # Consume the units.
            self._last_nano = new_last
            return 0
        """
        Determine the amount of time that the caller needs to sleep for this
        limiter to go below its limit. Note that the limiter is not guaranteed
        to be below the limit after this time, as other consume calls may come
        in after this one and push the "at the limit time" further out.
        """
        sleep_ms = (self._last_nano - now_nanos) / 1000000
        if sleep_ms == 0:
            sleep_ms = 1
        if always_consume or timeout_ms == 0 or sleep_ms < timeout_ms:
            """
            If we're told to always consume the units no matter what, consume
            the units. Or if the timeout is zero, consume the units. Or if the
            given timeout is more than the amount of time to sleep, consume the
            units.
            """
            self._last_nano = new_last
        return sleep_ms

    def _enforce_minimum_duration(self):
        """
        Force duration_nanos such that the limiter can always be capable of
        consuming at least 1 unit without waiting.
        """
        if self._duration_nanos < self._nanos_per_unit:
            self._duration_nanos = self._nanos_per_unit
