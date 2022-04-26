#
# Copyright (c) 2018, 2022 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#

from base64 import b64encode
from json import loads
from requests import ConnectionError, Session, codes
from threading import Lock, Timer
from time import time
from traceback import format_exc
try:
    # noinspection PyCompatibility
    from urlparse import urlparse
except ImportError:
    # noinspection PyUnresolvedReferences,PyCompatibility
    from urllib.parse import urlparse

import borneo.http
import borneo.config
import borneo.operations
from borneo.auth import AuthorizationProvider
from borneo.common import (
    CheckValue, HttpConstants, LogUtils, SSLAdapter, synchronized)
from borneo.exception import (
    IllegalArgumentException, InvalidAuthorizationException, NoSQLException)


class StoreAccessTokenProvider(AuthorizationProvider):
    """
    On-premise only.

    StoreAccessTokenProvider is an :py:class:`borneo.AuthorizationProvider` that
    performs the following functions:

        Initial (bootstrap) login to store, using credentials provided.\n
        Storage of bootstrap login token for re-use.\n
        Optionally renews the login token before it expires.\n
        Logs out of the store when closed.

    If accessing an insecure instance of Oracle NoSQL Database the default
    constructor is used, with no arguments.

    If accessing a secure instance of Oracle NoSQL Database a user name and
    password must be provided. That user must already exist in the NoSQL
    Database and have sufficient permission to perform table operations. That
    user's identity is used to authorize all database operations.

    To access to a store without security enabled, no parameter need to be set
    to the constructor.

    To access to a secure store, the constructor requires a valid user name and
    password to access the target store. The user must exist and have sufficient
    permission to perform table operations required by the application. The user
    identity is used to authorize all operations performed by the application.

    :param user_name: the user name to use for the store. This user must exist
        in the NoSQL Database and is the identity that is used for authorizing
        all database operations.
    :type user_name: str
    :param password: the password for the user.
    :type password: str
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

    def __init__(self, user_name=None, password=None):
        """
        Creates a StoreAccessTokenProvider

        :param user_name: the user name to use for the store. This user must
         exist in the NoSQL Database and is the identity that is used for
         authorizing all database operations.
        :type user_name: str
        :param password: the password for the user.
        :type password: str
        :raises IllegalArgumentException: raises the exception if one or more
         of the parameters is malformed or a required parameter is missing.
        """

        self._endpoint = None
        self._url = None
        self._auth_string = None
        self._auto_renew = True
        self._is_closed = False
        # The base path for security related services.
        self._base_path = HttpConstants.KV_SECURITY_PATH
        # The login token expiration time.
        self._expiration_time = 0
        self._logger = None
        self._logutils = LogUtils(self._logger)
        self._sess = Session()
        self._request_utils = borneo.http.RequestUtils(
            self._sess, self._logutils)
        self._lock = Lock()
        self._timer = None
        self.lock = Lock()

        if user_name is None and password is None:
            # Used to access to a store without security enabled.
            self._is_secure = False
        else:
            if user_name is None or password is None:
                raise IllegalArgumentException('Invalid input arguments.')
            CheckValue.check_str(user_name, 'user_name')
            CheckValue.check_str(password, 'password')
            self._is_secure = True
            self._user_name = user_name
            self._password = password

    @synchronized
    def bootstrap_login(self):
        # Bootstrap login using the provided credentials.
        if not self._is_secure or self._is_closed:
            return
        # Convert the username:password pair in base 64 format.
        pair = self._user_name + ':' + self._password
        try:
            encoded_pair = b64encode(pair)
        except TypeError:
            encoded_pair = b64encode(pair.encode()).decode()
        try:
            # Send request to server for login token.
            response = self._send_request(
                StoreAccessTokenProvider._BASIC_PREFIX + encoded_pair,
                StoreAccessTokenProvider._LOGIN_SERVICE)
            content = response.get_content()
            # Login fail
            if response.get_status_code() != codes.ok:
                raise InvalidAuthorizationException(
                    'Fail to login to service: ' + content)
            if self._is_closed:
                return
            # Generate the authentication string using login token.
            self._auth_string = (StoreAccessTokenProvider._BEARER_PREFIX +
                                 self._parse_json_result(content))
            # Schedule login token renew thread.
            self._schedule_refresh()
        except (ConnectionError, InvalidAuthorizationException) as e:
            self._logutils.log_debug(format_exc())
            raise e
        except Exception as e:
            self._logutils.log_debug(format_exc())
            raise NoSQLException('Bootstrap login fail.', e)

    @synchronized
    def close(self):
        """
        Close the provider, releasing resources such as a stored login token.
        """
        # Don't do anything for non-secure case.
        if not self._is_secure or self._is_closed:
            return
        # Send request for logout.
        try:
            response = self._send_request(
                self._auth_string, StoreAccessTokenProvider._LOGOUT_SERVICE)
            if response.get_status_code() != codes.ok:
                self._logutils.log_error(
                    'Failed to logout user ' + self._user_name + ': ' +
                    response.get_content())
        except Exception as e:
            self._logutils.log_error(
                'Failed to logout user ' + self._user_name + ': ' + str(e))

        # Clean up.
        self._is_closed = True
        self._auth_string = None
        self._expiration_time = 0
        self._password = None
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        if self._sess is not None:
            self._sess.close()

    def get_authorization_string(self, request=None):
        if (request is not None and
                not isinstance(request, borneo.operations.Request)):
            raise IllegalArgumentException(
                'get_authorization_string requires an instance of Request or ' +
                'None as parameter.')
        if not self._is_secure or self._is_closed:
            return None
        # If there is no cached auth string, re-authentication to retrieve the
        # login token and generate the auth string.
        if self._auth_string is None:
            self.bootstrap_login()
        return self._auth_string

    def is_secure(self):
        """
        Returns whether the provider is accessing a secured store.

        :returns: True if accessing a secure store, otherwise False.
        :rtype: bool
        """
        return self._is_secure

    def set_auto_renew(self, auto_renew):
        """
        Sets the auto-renew state. If True, automatic renewal of the login token
        is enabled.

        :param auto_renew: set to True to enable auto-renew.
        :type auto_renew: bool
        :returns: self.
        :raises IllegalArgumentException: raises the exception if auto_renew is
            not True or False.
        """
        CheckValue.check_boolean(auto_renew, 'auto_renew')
        self._auto_renew = auto_renew
        return self

    def is_auto_renew(self):
        """
        Returns whether the login token is to be automatically renewed.

        :returns: True if auto-renew is set, otherwise False.
        :rtype: bool
        """
        return self._auto_renew

    def set_endpoint(self, endpoint):
        """
        Sets the endpoint of the on-prem proxy.

        :param endpoint: the endpoint.
        :type endpoint: str
        :returns: self.
        :raises IllegalArgumentException: raises the exception if endpoint is
            not a string.
        """
        CheckValue.check_str(endpoint, 'endpoint')
        self._endpoint = endpoint
        self._url = borneo.config.NoSQLHandleConfig.create_url(endpoint, '')
        if self._is_secure and self._url.scheme.lower() != 'https':
            raise IllegalArgumentException(
                'StoreAccessTokenProvider requires use of https.')
        return self

    def get_endpoint(self):
        """
        Returns the endpoint of the on-prem proxy.

        :returns: the endpoint.
        :rtype: str
        """
        return self._endpoint

    def set_logger(self, logger):
        CheckValue.check_logger(logger, 'logger')
        self._logger = logger
        self._logutils = LogUtils(logger)
        return self

    def get_logger(self):
        return self._logger

    def set_ssl_context(self, ssl_ctx):
        # Internal use only
        adapter = SSLAdapter(ssl_ctx)
        self._sess.mount(self._url.scheme + '://', adapter)

    def set_url_for_test(self):
        self._url = urlparse(self._url.geturl().replace('https', 'http'))
        return self

    def validate_auth_string(self, auth_string):
        if self._is_secure and auth_string is None:
            raise IllegalArgumentException(
                'Secured StoreAccessProvider requires a non-none string.')

    def _parse_json_result(self, json_result):
        # Retrieve login token from JSON string.
        result = loads(json_result)
        # Extract expiration time from JSON result.
        self._expiration_time = result['expireAt']
        # Extract login token from JSON result.
        return result['token']

    def _refresh_task(self):
        """
        This task sends a request to the server for login session extension.
        Depending on the server policy, a new login token with new expiration
        time may or may not be granted.
        """
        if not self._is_secure or not self._auto_renew or self._is_closed:
            return
        try:
            old_auth = self._auth_string
            response = self._send_request(
                old_auth, StoreAccessTokenProvider._RENEW_SERVICE)
            token = self._parse_json_result(response.get_content())
            if response.get_status_code() != codes.ok:
                raise InvalidAuthorizationException(token)
            if self._is_closed:
                return
            with self._lock:
                if self._auth_string == old_auth:
                    self._auth_string = (
                        StoreAccessTokenProvider._BEARER_PREFIX + token)
            self._schedule_refresh()
        except Exception as e:
            self._logutils.log_error('Failed to renew login token: ' + str(e))
            if self._timer is not None:
                self._timer.cancel()
                self._timer = None

    def _schedule_refresh(self):
        # Schedule a login token renew when half of the token life time is
        # reached.
        if not self._is_secure or not self._auto_renew:
            return
        # Clean up any existing timer
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        acquire_time = int(round(time() * 1000))
        if self._expiration_time <= 0:
            return
        # If it is 10 seconds before expiration, don't do further renew to avoid
        # to many renew request in the last few seconds.
        if self._expiration_time > acquire_time + 10000:
            renew_time = (
                acquire_time + (self._expiration_time - acquire_time) // 2)
            self._timer = Timer(
                float(renew_time - acquire_time) / 1000, self._refresh_task)
            self._timer.start()

    def _send_request(self, auth_header, service_name):
        # Send HTTPS request to login/renew/logout service location with proper
        # authentication information.
        headers = {'Host': self._url.hostname, 'Authorization': auth_header}
        return self._request_utils.do_get_request(
            self._url.geturl() + self._base_path + service_name, headers,
            StoreAccessTokenProvider._HTTP_TIMEOUT_MS)
