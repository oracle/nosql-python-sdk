#
# Copyright (C) 2018, 2019 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
#
# Please see LICENSE.txt file included in the top-level directory of the
# appropriate download for a copy of the license and additional information.
#

from abc import ABCMeta, abstractmethod


class AuthorizationProvider(object):
    """
    AuthorizationProvider is a callback interface used by the driver to
    obtain an authorization string for a request. It is called when an
    authorization string is required. In general applications need not
    implement this interface, instead using the default mechanisms.

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
    def get_authorization_string(self, request):
        """
        Returns an authorization string for the specified request. The string is
        sent to the server in the request and is used for authorization.
        Authorization information can be request-dependent.

        :param request: the request to be issued. This is an
            instance of :py:meth:`Request`.
        :returns:  a string indicating that the application is authorized to
            perform the request.
        """
        pass
