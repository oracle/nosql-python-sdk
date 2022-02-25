#
# Copyright (c) 2018, 2022 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#

from abc import ABCMeta, abstractmethod

from .common import CheckValue, HttpConstants
from .exception import IllegalArgumentException


class AuthorizationProvider(object):
    """
    AuthorizationProvider is a callback interface used by the driver to obtain
    an authorization string for a request. It is called when an authorization
    string is required. In general applications need not implement this
    interface, instead using the default mechanisms.

    Instances of this interface must be reentrant and thread-safe.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def close(self):
        """
        Closes the authorization provider and releases any resources it
        may be using.
        """
        pass

    @abstractmethod
    def get_authorization_string(self, request=None):
        """
        Returns an authorization string for the specified request. The string is
        sent to the server in the request and is used for authorization.
        Authorization information can be request-dependent.

        :param request: the request to be issued. This is an
            instance of :py:meth:`Request`.
        :type request: Request
        :returns: a string indicating that the application is authorized to
            perform the request.
        :rtype: str
        """
        pass

    def set_logger(self, logger):
        """
        Sets a logger instance for this provider. If not set, the logger
        associated with the driver is used.

        :param logger: the logger to use.
        :type logger: Logger
        :returns: self.
        :raises IllegalArgumentException: raises the exception if logger is not
            an instance of Logger.
        """
        return self

    def get_logger(self):
        """
        Returns the logger of this provider if set, None if not.

        :returns: the logger.
        :rtype: Logger or None
        """
        pass

    def validate_auth_string(self, auth_string):
        """
        Validates the authentication string. This method is optional and by
        default it just allows a non-none string.

        :param auth_string: the auth string to be validated.
        :type auth_string: str
        :raises IllegalArgumentException: raises the exception if input is not
            a string or none.
        """
        if not CheckValue.is_str(auth_string):
            raise IllegalArgumentException(
                'Configured AuthorizationProvider requires a non-none string.')

    def set_required_headers(self, request, auth_string, headers):
        """
        Internal use only.

        Set HTTP headers required by the provider.

        :param request: the request being processed.
        :type request: Request
        :param auth_string: the auth string.
        :type auth_string: str
        :param headers: the HTTP headers.
        :type headers: dict
        """
        if auth_string is not None:
            headers[HttpConstants.AUTHORIZATION] = auth_string
