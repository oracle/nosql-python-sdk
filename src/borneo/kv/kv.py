#
# Copyright (C) 2018, 2019 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
#
# Please see LICENSE.txt file included in the top-level directory of the
# appropriate download for a copy of the license and additional information.
#

from base64 import b64encode
from requests import Session, codes
from threading import Lock, Timer
from time import time
from traceback import format_exc
try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

import borneo.http
from borneo.auth import AuthorizationProvider
from borneo.common import ByteInputStream, CheckValue, LogUtils
from borneo.config import NoSQLHandleConfig
from borneo.exception import (
    IllegalArgumentException, InvalidAuthorizationException, NoSQLException)
from borneo.operations import Request


class StoreAccessTokenProvider(AuthorizationProvider):
    """
    StoreAccessTokenProvider is an :py:class:`borneo.AuthorizationProvider` that
    performs the following functions:

        Initial (bootstrap) login to store, using credentials provided.\n
        Storage of bootstrap login token for re-use.\n
        Optionally renews the login token before it expires.\n
        Logs out of the store when closed.

    To access to a store without security enabled, no parameter need to be set
    to the constructor.

    To access to a secure store, the constructor requires an endpoint string
    which is used to locate the entity responsible for proving the required
    access token. This is usually the same as the server used to access the
    proxy service. Endpoints must include the target address, and may include
    protocol and port. The valid syntax is [http[s]://]host[:port].

    For example, these are valid endpoint arguments:

        localhost\n
        https\://localhost\n
        https\://localhost:443

    If protocol is omitted, the endpoint uses https in all cases except for the
    port is 8080. So please don't use 8080 as security proxy port. If protocol
    is provided, it must be https.\n
    If port is omitted, the endpoint defaults to 443.

    Apart from endpoint, user_name and password are also required parameters.

    :param endpoint: the endpoint string to use for the login operation.
    :param user_name: the user name to use for the store.
    :param password: the password for the user.
    :raises IllegalArgumentException: raises the exception if one or more of the
        parameters is malformed or a required parameter is missing.
    """
    # Used when we send user:password pair.
    _BASIC_PREFIX = 'Basic '
    # The general prefix for the login token.
    _BEARER_PREFIX = 'Bearer '
    # Login service end point name.
    _LOGIN_SERVICE = '/login'
    # Login token renew service end point name.
    _RENEW_SERVICE = '/renew'
    # Logout service end point name.
    _LOGOUT_SERVICE = '/logout'
    # Default timeout when sending http request to server
    _HTTP_TIMEOUT_MS = 30000

    def __init__(self, endpoint=None, user_name=None, password=None):
        self.__auth_string = None
        self.__auto_renew = True
        self.__disable_ssl_hook = False
        self.__is_closed = False
        # The base path for security related services.
        self.__base_path = '/V0/nosql/security'
        self.__logger = None
        self.__logutils = LogUtils(self.__logger)
        self.__sess = Session()
        self.__request_utils = borneo.http.RequestUtils(
            self.__sess, self.__logutils)
        self.__lock = Lock()
        self.__timer = None

        if endpoint is None and user_name is None and password is None:
            # Used to access to a store without security enabled.
            self.__is_secure = False
        else:
            if endpoint is None or user_name is None or password is None:
                raise IllegalArgumentException('Invalid input arguments.')
            CheckValue.check_str(endpoint, 'endpoint')
            CheckValue.check_str(user_name, 'user_name')
            CheckValue.check_str(password, 'password')
            self.__is_secure = True
            # The url to reach the authenticating entity (proxy).
            self.__url = NoSQLHandleConfig.create_url(endpoint, '')
            if self.__url.scheme.lower() != 'https':
                raise IllegalArgumentException(
                    'StoreAccessTokenProvider requires use of https')
            self.__user_name = user_name
            self.__password = password

    def bootstrap_login(self):
        # Bootstrap login using the provided credentials.
        if not self.__is_secure or self.__is_closed:
            return
        # Convert the username:password pair in base 64 format.
        pair = self.__user_name + ':' + self.__password
        try:
            encoded_pair = b64encode(pair)
        except TypeError:
            encoded_pair = b64encode(pair.encode()).decode()
        try:
            # Send request to server for login token.
            response = self.__send_request(
                StoreAccessTokenProvider._BASIC_PREFIX + encoded_pair,
                StoreAccessTokenProvider._LOGIN_SERVICE)
            content = response.get_content()
            # Login fail
            if response.get_status_code() != codes.ok:
                raise InvalidAuthorizationException(
                    'Fail to login to service: ' + content)
            if self.__is_closed:
                return
            # Generate the authentication string using login token.
            self.__auth_string = (StoreAccessTokenProvider._BEARER_PREFIX +
                                  content)
            # Schedule login token renew thread.
            self.__schedule_refresh()
        except InvalidAuthorizationException as iae:
            print(format_exc())
            raise iae
        except Exception as e:
            print(format_exc())
            raise NoSQLException('Bootstrap login fail.', e)

    def close(self):
        """
        Close the provider, releasing resources such as a stored login token.
        """
        # Don't do anything for non-secure case.
        if not self.__is_secure or self.__is_closed:
            return
        # Send request for logout.
        try:
            response = self.__send_request(
                self.__auth_string, StoreAccessTokenProvider._LOGOUT_SERVICE)
            if response.get_status_code() != codes.ok:
                self.__logutils.log_info(
                    'Failed to logout user ' + self.__user_name + ': ' +
                    response.get_content())
        except Exception as e:
            self.__logutils.log_info(
                'Failed to logout user ' + self.__user_name + ': ' + str(e))

        # Clean up.
        self.__is_closed = True
        self.__auth_string = None
        self.__password = None
        if self.__timer is not None:
            self.__timer.cancel()
            self.__timer = None
        if self.__sess is not None:
            self.__sess.close()

    def get_authorization_string(self, request=None):
        if request is not None and not isinstance(request, Request):
            raise IllegalArgumentException(
                'get_authorization_string requires an instance of Request or ' +
                'None as parameter.')
        if not self.__is_secure or self.__is_closed:
            return None
        # If there is no cached auth string, re-authentication to retrieve the
        # login token and generate the auth string.
        if self.__auth_string is None:
            self.bootstrap_login()
        return self.__auth_string

    def is_secure(self):
        """
        Returns whether the provider is accessing a secured store.

        :return: True if accessing a secure store, otherwise False.
        :rtype: bool
        """
        return self.__is_secure

    def set_auto_renew(self, auto_renew):
        """
        Sets the auto-renew state. If True, automatic renewal of the login token
        is enabled.

        :param auto_renew: set to True to enable auto-renew.
        :type auto_renew: bool
        :return: self.
        :raises IllegalArgumentException: raises the exception if auto_renew is
            not True or False.
        """
        CheckValue.check_boolean(auto_renew, 'auto_renew')
        self.__auto_renew = auto_renew
        return self

    def is_auto_renew(self):
        """
        Returns whether the login token is to be automatically renewed.

        :return: True if auto-renew is set, otherwise False.
        :rtype: bool
        """
        return self.__auto_renew

    def set_logger(self, logger):
        CheckValue.check_logger(logger, 'logger')
        self.__logger = logger
        self.__logutils = LogUtils(logger)
        self.__request_utils = borneo.http.RequestUtils(
            self.__sess, self.__logutils)
        return self

    def get_logger(self):
        return self.__logger

    def set_url_for_test(self):
        self.__url = urlparse(self.__url.geturl().replace('https', 'http'))
        return self

    def validate_auth_string(self, auth_string):
        if self.__is_secure and auth_string is None:
            raise IllegalArgumentException(
                'Secured StoreAccessProvider requires a non-none string.')

    def __get_expiration_time_from_token(self):
        # Retrieve login token from authentication string.
        if self.__auth_string is None:
            return -1
        token = self.__auth_string[
            len(StoreAccessTokenProvider._BEARER_PREFIX):]
        buf = bytearray.fromhex(token)
        bis = ByteInputStream(buf)
        # Read serial version first.
        bis.read_short_int()
        expire_at = bis.read_long()
        return expire_at

    def __refresh_task(self):
        """
        This task sends a request to the server for login session extension.
        Depending on the server policy, a new login token with new expiration
        time may or may not be granted.
        """
        if not self.__is_secure or not self.__auto_renew or self.__is_closed:
            return
        try:
            old_auth = self.__auth_string
            response = self.__send_request(
                old_auth, StoreAccessTokenProvider._RENEW_SERVICE)
            content = response.get_content()
            if response.get_status_code() != codes.ok:
                raise InvalidAuthorizationException(content)
            if self.__is_closed:
                return
            with self.__lock:
                if self.__auth_string == old_auth:
                    self.__auth_string = (
                        StoreAccessTokenProvider._BEARER_PREFIX + content)
            self.__schedule_refresh()
        except Exception as e:
            self.__logutils.log_info('Failed to renew login token: ' + str(e))
            if self.__timer is not None:
                self.__timer.cancel()
                self.__timer = None

    def __schedule_refresh(self):
        # Schedule a login token renew when half of the token life time is
        # reached.
        if not self.__is_secure or not self.__auto_renew:
            return
        # Clean up any existing timer
        if self.__timer is not None:
            self.__timer.cancel()
            self.__timer = None
        acquire_time = int(round(time() * 1000))
        expire_time = self.__get_expiration_time_from_token()
        if expire_time < 0:
            return
        # If it is 10 seconds before expiration, don't do further renew to avoid
        # to many renew request in the last few seconds.
        if expire_time > acquire_time + 10000:
            renew_time = acquire_time + (expire_time - acquire_time) // 2
            self.__timer = Timer(
                (renew_time - acquire_time) // 1000, self.__refresh_task)
            self.__timer.start()

    def __send_request(self, auth_header, service_name):
        # Send HTTPS request to login/renew/logout service location with proper
        # authentication information.
        headers = {'Authorization': auth_header}
        return self.__request_utils.do_get_request(
            self.__url.geturl() + self.__base_path + service_name, headers,
            StoreAccessTokenProvider._HTTP_TIMEOUT_MS)
