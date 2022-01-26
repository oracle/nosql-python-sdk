#
# Copyright (c) 2018, 2022 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#

from borneo.exception import NoSQLException


class AuthenticationException(NoSQLException):
    """
    On-premise only.

    This exception is thrown when use StoreAccessTokenProvider in following
    cases:

        Authentication information was not provided in the request header.\n
        The authentication session has expired. By default
        :py:class:`StoreAccessTokenProvider` will automatically retry
        authentication operation based on its authentication information.
    """

    def __init__(self, message, cause=None):
        super(AuthenticationException, self).__init__(message, cause)
