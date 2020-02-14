#
# Copyright (C) 2018, 2020 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
#
# Please see LICENSE.txt file included in the top-level directory of the
# appropriate download for a copy of the license and additional information.
#

from abc import ABCMeta, abstractmethod
from copy import deepcopy
from random import random
from time import sleep
try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

from .auth import AuthorizationProvider
from .common import CheckValue, Consistency
from .exception import (
    IllegalArgumentException, OperationThrottlingException, RetryableException,
    SecurityInfoNotReadyException)
from .operations import Request


class RetryHandler(object):
    """
    RetryHandler is called by the request handling system when a
    :py:class:`RetryableException` is thrown. It controls the number of retries
    as well as frequency of retries using a delaying algorithm. A default
    RetryHandler is always configured on a :py:class:`NoSQLHandle` instance and
    can be controlled or overridden using
    :py:meth:`NoSQLHandleConfig.set_retry_handler` and
    :py:meth:`NoSQLHandleConfig.configure_default_retry_handler`.

    It is not recommended that applications rely on a RetryHandler for
    regulating provisioned throughput. It is best to add rate-limiting to the
    application based on a table's capacity and access patterns to avoid
    throttling exceptions.

    Instances of this class must be immutable so they can be shared among
    threads.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_num_retries(self):
        """
        Returns the number of retries that this handler instance will allow
        before the exception is thrown to the application.

        :returns: the number of retries.
        :rtype: int
        """
        pass

    @abstractmethod
    def do_retry(self, request, num_retried, re):
        """
        This method is called when a :py:class:`RetryableException` is thrown
        and the handler determines whether to perform a retry or not based on
        the parameters.

        Default behavior is to *not* retry OperationThrottlingException because
        the retry time is likely much longer than normal because they are DDL
        operations. In addition, *not* retry any requests that should not be
        retired: TableRequest, ListTablesRequest, GetTableRequest,
        TableUsageRequest, GetIndexesRequest.

        Always retry SecurityInfoNotReadyException until exceed the request
        timeout. It's not restrained by the maximum retries configured for this
        handler, the driver with retry handler with 0 retry setting would still
        retry this exception.

        :param request: the request that has triggered the exception.
        :type request: Request
        :param num_retried: the number of retries that have occurred for the
            operation.
        :type num_retried: int
        :param re: the exception that was thrown.
        :type re: RetryableException
        :returns: True if the operation should be retried, False if not, causing
            the exception to be thrown to the application.
        :rtype: bool
        :raises IllegalArgumentException: raises the exception if num_retried is
            not a positive number.
        """
        pass

    @abstractmethod
    def delay(self, num_retried, re):
        """
        This method is called when a :py:class:`RetryableException` is thrown
        and it is determined that the request will be retried based on the
        return value if :py:meth:`do_retry`. It provides a delay between
        retries. Most implementations will sleep for some period of time. The
        method should not return until the desired delay period has passed.
        Implementations should not busy-wait in a tight loop.

        If delayMS is non-zero, use it. Otherwise, use a exponential backoff
        algorithm to compute the time of delay.

        If retry-able exception is SecurityInfoNotReadyException, delay for
        SEC_RETRY_DELAY_MS when number of retries is smaller than 10. Otherwise,
        use the exponential backoff algorithm to compute the time of delay.

        :param num_retried: the number of retries that have occurred for the
            operation.
        :type num_retried: int
        :param re: the exception that was thrown.
        :type re: RetryableException
        :raises IllegalArgumentException: raises the exception if num_retried is
            not a positive number.
        """
        pass


class DefaultRetryHandler(RetryHandler):
    """
    A default instance of :py:class:`RetryHandler`
    """
    # Base time of delay between retries for security info unavailable.
    _SEC_ERROR_DELAY_MS = 100

    def __init__(self, num_retries=10, delay_s=1):
        CheckValue.check_int_ge_zero(num_retries, 'num_retries')
        CheckValue.check_int_ge_zero(delay_s, 'delay_s')
        self._num_retries = num_retries
        self._delay_ms = delay_s * 1000

    def get_num_retries(self):
        return self._num_retries

    def do_retry(self, request, num_retried, re):
        self._check_request(request)
        CheckValue.check_int_gt_zero(num_retried, 'num_retried')
        self._check_retryable_exception(re)
        if isinstance(re, OperationThrottlingException):
            return False
        elif isinstance(re, SecurityInfoNotReadyException):
            # always retry if security info is not read.
            return True
        elif not request.should_retry():
            return False
        return num_retried < self._num_retries

    def delay(self, num_retried, re):
        CheckValue.check_int_gt_zero(num_retried, 'num_retried')
        self._check_retryable_exception(re)
        msec = self._delay_ms
        if msec == 0:
            msec = self._compute_backoff_delay(num_retried, 1000)
        if isinstance(re, SecurityInfoNotReadyException):
            msec = self._sec_info_not_ready_delay(num_retried)
        sleep(float(msec) / 1000)

    @staticmethod
    def _check_request(request):
        if not isinstance(request, Request):
            raise IllegalArgumentException(
                'The parameter request should be an instance of Request.')

    @staticmethod
    def _check_retryable_exception(re):
        if not isinstance(re, RetryableException):
            raise IllegalArgumentException(
                're must be an instance of RetryableException.')

    @staticmethod
    def _compute_backoff_delay(num_retried, base_delay):
        """
        Use an exponential backoff algorithm to compute time of delay.

        Assumption: numRetries starts with 1
        sec = (2^(num_retried-1) + random MS (0-1000))
        """
        msec = (1 << (num_retried - 1)) * base_delay
        msec += random() * base_delay
        return msec

    @staticmethod
    def _sec_info_not_ready_delay(num_retried):
        """
        Handle security information not ready retries. If number of retries is
        smaller than 10, delay for DefaultRetryHandler._SEC_ERROR_DELAY_MS.
        Otherwise, use the backoff algorithm to compute the time of delay.
        """
        msec = DefaultRetryHandler._SEC_ERROR_DELAY_MS
        if num_retried > 10:
            msec = DefaultRetryHandler._compute_backoff_delay(
                num_retried - 10, DefaultRetryHandler._SEC_ERROR_DELAY_MS)
        return msec


class Region(object):
    """
    Cloud service only.

    The class represents a region of Oracle NoSQL Database Cloud.
    """

    OC1_EP_BASE = 'https://nosql.{0}.oci.oraclecloud.com'
    GOV_EP_BASE = 'https://nosql.{0}.oci.oraclegovcloud.com'
    OC4_EP_BASE = 'https://nosql.{0}.oci.oraclegovcloud.uk'

    def __init__(self, region_id):
        self._region_id = region_id

    def endpoint(self):
        """
        Returns the NoSQL Database Cloud Service endpoint string for this
        region.

        :returns: NoSQL Database Cloud Service endpoint string.
        :rtype: str
        :raises IllegalArgumentException: raises the exception if region_id is
            unknown.
        """
        if self._is_oc1_region():
            return str.format(Region.OC1_EP_BASE, self._region_id)
        if self._is_gov_region():
            return str.format(Region.GOV_EP_BASE, self._region_id)
        if self._is_oc4_region():
            return str.format(Region.OC4_EP_BASE, self._region_id)
        raise IllegalArgumentException(
            'Unable to find endpoint for unknown region ' + self._region_id)

    def get_region_id(self):
        """
        Internal use only.

        Returns the region id of this region.

        :returns: the region id.
        :rtype: str
        """
        return self._region_id

    def _is_gov_region(self):
        # Internal use only
        return Regions.GOV_REGIONS.get(self._region_id) is not None

    def _is_oc1_region(self):
        # Internal use only
        return Regions.OC1_REGIONS.get(self._region_id) is not None

    def _is_oc4_region(self):
        # Internal use only
        return Regions.OC4_REGIONS.get(self._region_id) is not None


class Regions(object):
    """
    Cloud service only.

    The class contains the regions in the Oracle Cloud Infrastructure at the
    time of this release. The Oracle NoSQL Database Cloud Service is not
    available in all of these regions. For a definitive list of regions in which
    the Oracle NoSQL Database Cloud Service is available see `Data Regions for
    Platform and Infrastructure Services <https://www.oracle.com/cloud/
    data-regions.html>`_.

    A Region may be provided to :py:class:`NoSQLHandleConfig` to configure a
    handle to communicate in a specific Region.

    The string-based endpoints associated with regions for the Oracle NoSQL
    Database Cloud Service are of the format::

        https://nosql.{region}.oci.{secondLevelDomain}

    Examples of known second level domains include

     * oraclecloud.com
     * oraclegovcloud.com
     * oraclegovcloud.uk

    For example, this is a valid endpoint for the Oracle NoSQL Database Cloud
    Service in the U.S. East region::

        https://nosql.us-ashburn-1.oci.oraclecloud.com

    If the Oracle NoSQL Database Cloud Service becomes available in a region
    not listed here it is possible to connect to that region using the endpoint
    string rather than a Region.

    For more information about Oracle Cloud Infrastructure regions see `Regions
    and Availability Domains <https://docs.cloud.oracle.com/en-us/iaas/Content/
    General/Concepts/regions.htm>`_.
    """
    # OC1
    AP_SEOUL_1 = Region('ap-seoul-1')
    """Region Location: Seoul, South Korea"""
    AP_TOKYO_1 = Region('ap-tokyo-1')
    """Region Location: Tokyo, Japan"""
    AP_MUMBAI_1 = Region('ap-mumbai-1')
    """Region Location: Mumbai, India"""
    AP_SYDNEY_1 = Region('ap-sydney-1')
    """Region Location: Sydney, Australia"""

    UK_LONDON_1 = Region('uk-london-1')
    """Region Location: London, United Kingdom"""
    EU_FRANKFURT_1 = Region('eu-frankfurt-1')
    """Region Location: Frankfurt, Germany"""
    EU_ZURICH_1 = Region('eu-zurich-1')
    """Region Location: Zurich, Switzerland"""

    US_ASHBURN_1 = Region('us-ashburn-1')
    """Region Location: Ashburn, VA"""
    US_PHOENIX_1 = Region('us-phoenix-1')
    """Region Location: Phoenix, AZ"""
    CA_TORONTO_1 = Region('ca-toronto-1')
    """Region Location: Toronto, Canada"""

    SA_SAOPAULO_1 = Region('sa-saopaulo-1')
    """Region Location: Sao Paulo, Brazil"""

    # OC2
    US_LANGLEY_1 = Region('us-langley-1')
    """Region Location: Ashburn, VA"""
    US_LUKE_1 = Region('us-luke-1')
    """Region Location: Phoenix, AZ"""

    # OC3
    US_GOV_ASHBURN_1 = Region('us-gov-ashburn-1')
    """Region Location: Ashburn, VA"""
    US_GOV_CHICAGO_1 = Region('us-gov-chicago-1')
    """Region Location: Chicago, IL"""
    US_GOV_PHOENIX_1 = Region('us-gov-phoenix-1')
    """Region Location: Phoenix, AZ"""

    # OC4
    UK_GOV_LONDON_1 = Region('uk-gov-london-1')
    """Region Location: London, United Kingdom"""

    # OC1
    OC1_REGIONS = dict()
    """A dict that save all the OC1 regions."""
    # APAC
    OC1_REGIONS[AP_SEOUL_1.get_region_id()] = AP_SEOUL_1
    OC1_REGIONS[AP_TOKYO_1.get_region_id()] = AP_TOKYO_1
    OC1_REGIONS[AP_MUMBAI_1.get_region_id()] = AP_MUMBAI_1
    OC1_REGIONS[AP_SYDNEY_1.get_region_id()] = AP_SYDNEY_1

    # EMEA
    OC1_REGIONS[UK_LONDON_1.get_region_id()] = UK_LONDON_1
    OC1_REGIONS[EU_FRANKFURT_1.get_region_id()] = EU_FRANKFURT_1
    OC1_REGIONS[EU_ZURICH_1.get_region_id()] = EU_ZURICH_1

    # LAD
    OC1_REGIONS[SA_SAOPAULO_1.get_region_id()] = SA_SAOPAULO_1

    # North America
    OC1_REGIONS[US_ASHBURN_1.get_region_id()] = US_ASHBURN_1
    OC1_REGIONS[US_PHOENIX_1.get_region_id()] = US_PHOENIX_1
    OC1_REGIONS[CA_TORONTO_1.get_region_id()] = CA_TORONTO_1

    GOV_REGIONS = dict()
    """A dict that save all the government regions."""
    # OC2
    GOV_REGIONS[US_LANGLEY_1.get_region_id()] = US_LANGLEY_1
    GOV_REGIONS[US_LUKE_1.get_region_id()] = US_LUKE_1

    # OC3
    GOV_REGIONS[US_GOV_ASHBURN_1.get_region_id()] = US_GOV_ASHBURN_1
    GOV_REGIONS[US_GOV_CHICAGO_1.get_region_id()] = US_GOV_CHICAGO_1
    GOV_REGIONS[US_GOV_PHOENIX_1.get_region_id()] = US_GOV_PHOENIX_1

    # OC4
    OC4_REGIONS = dict()
    """A dict that save all the OC4 regions."""
    OC4_REGIONS[UK_GOV_LONDON_1.get_region_id()] = UK_GOV_LONDON_1

    @staticmethod
    def get_gov_regions():
        # Internal use only
        return Regions.GOV_REGIONS.values()

    @staticmethod
    def get_oc1_regions():
        # Internal use only
        return Regions.OC1_REGIONS.values()

    @staticmethod
    def get_oc4_regions():
        # Internal use only
        return Regions.OC4_REGIONS.values()

    @staticmethod
    def from_region_id(region_id):
        """
        Returns the Region associated with the string value supplied, or None if
        the string does not represent a known region.

        :param region_id: the string value of the region.
        :type region_id: str
        :returns: the Region or None if the string does not represent a Region.
        :rtype: Region
        """
        if region_id is None:
            raise IllegalArgumentException(
                'Invalid region id ' + str(region_id))
        region_id = region_id.lower()
        region = Regions.OC1_REGIONS.get(region_id)
        if region is None:
            region = Regions.OC4_REGIONS.get(region_id)
        if region is None:
            region = Regions.GOV_REGIONS.get(region_id)
        return region


class NoSQLHandleConfig(object):
    """
    An instance of this class is required by :py:class:`NoSQLHandle`.

    NoSQLHandleConfig groups parameters used to configure a
    :py:class:`NoSQLHandle`. It also provides a way to default common parameters
    for use by :py:class:`NoSQLHandle` methods. When creating a
    :py:class:`NoSQLHandle`, the NoSQLHandleConfig instance is copied
    so modification operations on the instance have no effect on existing
    handles which are immutable. NoSQLHandle state with default values can be
    overridden in individual operations.

    Some of the configuration parameters are optional and have default values if
    not specified.

    The service endpoint is required by constructor used to connect to the
    Oracle NoSQL Database Cloud Service or, if on-premise, the Oracle NoSQL
    Database proxy server. It should be a string or a :py:class:`Region`.

    If a string is provided to endpoint argument, there is flexibility in how
    endpoints are specified. A fully specified endpoint is of the format:

     * http[s]://host:port

    It also accepts portions of a fully specified endpoint, including a region
    id (see :py:class:`Region`) string if using the Cloud service. A valid
    endpoint is one of these:

     * region id string (cloud service only)
     * a string with the syntax [http[s]://]host[:port]

    For example, these are valid endpoint arguments:

     * us-ashburn-1 (equivalent to using Region Regions.US_ASHBURN_1 as the
       endpoint argument)
     * nosql.us-ashburn-1.oci.oraclecloud.com (equivalent to using Region
       Regions.US_ASHBURN_1 as the endpoint argument)
     * https\://nosql.us-ashburn-1.oci.oraclecloud.com:443
     * localhost:8080 - used for connecting to a Cloud Simulator instance
       running locally on port 8080
     * https\://machine-hosting-proxy:443

    When using the endpoint (vs region id) syntax, if the port is omitted, the
    endpoint uses 8080 if protocol is http, and 443 in all other cases. If the
    protocol is omitted, the endpoint uses https if the port is 443, and http in
    all other cases.

    When using the Oracle NoSQL Database Cloud Service, it is recommended that a
    :py:class:`Region` object is provided rather than a Region's id string.

    If a :py:class:`Region` object is provided to endpoint argument, See
    :py:class:`Regions` for information on available regions. For example:

     * Regions.US_ASHBURN_1

    :param endpoint: identifies a server, region id or :py:class:`Region` for
        use by the NoSQLHandle. This is a required parameter.
    :type endpoint: str or Region
    :param provider: :py:class:`AuthorizationProvider` to use for the handle.
    :type provider: AuthorizationProvider
    :raises IllegalArgumentException: raises the exception if the endpoint is
        None or malformed.
    """

    # The default value for request, and table request timeouts in milliseconds,
    # if not configured.
    _DEFAULT_TIMEOUT = 5000
    _DEFAULT_TABLE_REQ_TIMEOUT = 10000
    # The default value for timeouts in milliseconds while waiting for security
    # information is available if it is not configure.
    _DEFAULT_SEC_INFO_TIMEOUT = 10000
    _DEFAULT_CONSISTENCY = Consistency.EVENTUAL

    def __init__(self, endpoint=None, provider=None):
        # Inits a NoSQLHandleConfig object.
        if not isinstance(endpoint, (str, Region)):
            raise IllegalArgumentException(
                'endpoint should be a string or instance of Region.')
        if (provider is not None and
                not isinstance(provider, AuthorizationProvider)):
            raise IllegalArgumentException(
                'provider must be an instance of AuthorizationProvider.')
        if isinstance(endpoint, str):
            self._region = Regions.from_region_id(endpoint)
        else:
            self._region = endpoint
        self._service_url = NoSQLHandleConfig.create_url(
            endpoint if self._region is None else self._region.endpoint(), '/')
        self._auth_provider = provider
        self._compartment = None
        self._timeout = 0
        self._table_request_timeout = 0
        self._sec_info_timeout = NoSQLHandleConfig._DEFAULT_SEC_INFO_TIMEOUT
        self._consistency = None
        self._pool_connections = 2
        self._pool_maxsize = 10
        self._max_content_length = 1024 * 1024
        self._retry_handler = None
        self._proxy_host = None
        self._proxy_port = 0
        self._proxy_username = None
        self._proxy_password = None
        self._logger = None

    def get_service_url(self):
        """
        Returns the url to use for the :py:class:`NoSQLHandle` connection.

        :returns: the url.
        :rtype: ParseResult
        """
        return self._service_url

    def get_region(self):
        """
        Cloud service only.

        Returns the region will be accessed by the NoSQLHandle.

        :returns: the region.
        :rtype: Region
        """
        return self._region

    def set_authorization_provider(self, provider):
        """
        Sets the :py:class:`AuthorizationProvider` to use for the handle. The
        provider must be safely usable by multiple threads.

        :param provider: the AuthorizationProvider.
        :type provider: AuthorizationProvider
        :returns: self.
        :raises IllegalArgumentException: raises the exception if provider is
            not an instance of :py:class:`AuthorizationProvider`.
        """
        if not isinstance(provider, AuthorizationProvider):
            raise IllegalArgumentException(
                'provider must be an instance of AuthorizationProvider.')
        self._auth_provider = provider
        return self

    def get_authorization_provider(self):
        """
        Returns the :py:class:`AuthorizationProvider` configured for the handle,
        or None.

        :returns: the AuthorizationProvider.
        :rtype: AuthorizationProvider
        """
        return self._auth_provider

    def set_default_compartment(self, compartment):
        """
        Cloud service only.

        Sets the default compartment to use for requests sent using the handle.
        Setting the default is optional and if set it is overridden by any
        compartment specified in a request or table name. If no compartment is
        set for a request, either using this default or by specification in a
        request, the behavior varies with how the application is authenticated:

        * If authenticated with a user identity the default is the root
          compartment of the tenancy
        * If authenticated as an instance principal (see
          :py:meth:`borneo.iam.SignatureProvider.create_with_instance_principal`
          ) the compartment id (OCID) must be specified by either using this
          method or in each Request object. If not an exception is thrown.

        :param compartment: may be either the name of a compartment or the id
            (OCID) of a compartment.
        :type compartment: str
        :returns: self.
        :raises IllegalArgumentException: raises the exception if compartment
            is not a string.
        """
        CheckValue.check_str(compartment, 'compartment')
        self._compartment = compartment
        return self

    def get_default_compartment(self):
        """
        Cloud service only.

        Returns the default compartment to use for requests or None if not set.
        The value may be a compartment name or id, as set by
        :py:meth:`set_default_compartment`.

        :returns: the compartment, or None.
        :rtype: str or None
        """
        return self._compartment

    def get_default_timeout(self):
        """
        Returns the default value for request timeout in milliseconds. If there
        is no configured timeout or it is configured as 0, a "default" value of
        5000 milliseconds is used.

        :returns: the default timeout, in milliseconds.
        :rtype: int
        """
        return (NoSQLHandleConfig._DEFAULT_TIMEOUT if self._timeout == 0 else
                self._timeout)

    def get_default_table_request_timeout(self):
        """
        Returns the default value for a table request timeout. If there is no
        configured timeout or it is configured as 0, a "default" default value
        of 10000 milliseconds is used.

        :returns: the default timeout, in milliseconds.
        :rtype: int
        """
        return (NoSQLHandleConfig._DEFAULT_TABLE_REQ_TIMEOUT if
                self._table_request_timeout == 0 else
                self._table_request_timeout)

    def get_default_consistency(self):
        """
        Returns the default consistency value that will be used by the system.
        If consistency has been set using :py:meth:`set_consistency`, that will
        be returned. If not a default value of Consistency.EVENTUAL is returned.

        :returns: the default consistency.
        :rtype: Consistency
        """
        return (NoSQLHandleConfig._DEFAULT_CONSISTENCY if
                self._consistency is None else self._consistency)

    def set_timeout(self, timeout):
        """
        Sets the default request timeout in milliseconds, the default timeout is
        5 seconds.

        :param timeout: the timeout value, in milliseconds.
        :type timeout: int
        :returns: self.
        :raises IllegalArgumentException: raises the exception if timeout is a
            negative number.
        """
        CheckValue.check_int_gt_zero(timeout, 'timeout')
        self._timeout = timeout
        return self

    def get_timeout(self):
        """
        Returns the configured request timeout value, in milliseconds, 0 if it
        has not been set.

        :returns: the timeout, in milliseconds, or 0 if it has not been set.
        :rtype: int
        """
        return self._timeout

    def set_table_request_timeout(self, table_request_timeout):
        """
        Sets the default table request timeout. The default timeout is 5
        seconds. The table request timeout can be specified independently of
        that specified by :py:meth:`set_request_timeout` because table requests
        can take longer and justify longer timeouts. The default timeout is 10
        seconds (10000 milliseconds).

        :param table_request_timeout: the timeout value, in milliseconds.
        :type table_request_timeout: int
        :returns: self.
        :raises IllegalArgumentException: raises the exception if
            table_request_timeout is a negative number.
        """
        CheckValue.check_int_gt_zero(table_request_timeout,
                                     'table_request_timeout')
        self._table_request_timeout = table_request_timeout
        return self

    def get_table_request_timeout(self):
        """
        Returns the configured table request timeout value, in milliseconds.
        The table request timeout default can be specified independently to
        allow it to be larger than a typical data request. If it is not
        specified the default table request timeout of 10000 is used.

        :returns: the timeout, in milliseconds, or 0 if it has not been set.
        :rtype: int
        """
        return self._table_request_timeout

    def set_sec_info_timeout(self, sec_info_timeout):
        """
        Sets the timeout of waiting security information to be available. The
        default timeout is 10 seconds.

        :param sec_info_timeout: the timeout value, in milliseconds.
        :type sec_info_timeout: int
        :returns: self.
        :raises IllegalArgumentException: raises the exception if
            sec_info_timeout is a negative number.
        """
        CheckValue.check_int_gt_zero(sec_info_timeout, 'sec_info_timeout')
        self._sec_info_timeout = sec_info_timeout
        return self

    def get_sec_info_timeout(self):
        """
        Returns the configured timeout value for waiting security information
        to be available, in milliseconds.

        :returns: the timeout, in milliseconds, or 0 if it has not been set.
        :rtype: int
        """
        return self._sec_info_timeout

    def set_consistency(self, consistency):
        """
        Sets the default request :py:class:`Consistency`. If not set in this
        object or by a specific request, the default consistency used is
        Consistency.EVENTUAL.

        :param consistency: the consistency.
        :type consistency: Consistency
        :returns: self.
        :raises IllegalArgumentException: raises the exception if consistency
            is not Consistency.ABSOLUTE or Consistency.EVENTUAL.
        """
        if (consistency != Consistency.ABSOLUTE and
                consistency != Consistency.EVENTUAL):
            raise IllegalArgumentException(
                'Consistency must be Consistency.ABSOLUTE or ' +
                'Consistency.EVENTUAL')
        self._consistency = consistency
        return self

    def get_consistency(self):
        """
        Returns the configured default :py:class:`Consistency`, None if it has
        not been configured.

        :returns: the consistency, or None if it has not been configured.
        :rtype: Consistency
        """
        return self._consistency

    def set_pool_connections(self, pool_connections):
        """
        Sets the number of connection pools to cache.

        :param pool_connections: the number of connection pools.
        :type pool_connections: int
        :returns: self.
        :raises IllegalArgumentException: raises the exception if
            pool_connections is not a positive number.
        """
        CheckValue.check_int_gt_zero(pool_connections, 'pool_connections')
        self._pool_connections = pool_connections
        return self

    def get_pool_connections(self):
        """
        Returns the number of connection pools to cache.

        :returns: the number of connection pools.
        :rtype: int
        """
        return self._pool_connections

    def set_pool_maxsize(self, pool_maxsize):
        """
        Sets the maximum number of individual connections to use to connect to
        to the service. Each request/response pair uses a connection. The pool
        exists to allow concurrent requests and will bound the number of
        concurrent requests. Additional requests will wait for a connection to
        become available.

        :param pool_maxsize: the pool size.
        :type pool_maxsize: int
        :returns: self.
        :raises IllegalArgumentException: raises the exception if pool_maxsize
            is not a positive number.
        """
        CheckValue.check_int_gt_zero(pool_maxsize, 'pool_maxsize')
        self._pool_maxsize = pool_maxsize
        return self

    def get_pool_maxsize(self):
        """
        Returns the maximum number of individual connections to use to connect
        to the service. Each request/response pair uses a connection. The pool
        exists to allow concurrent requests and will bound the number of
        concurrent requests. Additional requests will wait for a connection to
        become available.

        :returns: the pool size.
        :rtype: int
        """
        return self._pool_maxsize

    def get_max_content_length(self):
        """
        Returns the maximum size, in bytes, of a request operation payload. Not
        currently user-configurable.

        :returns: the size.
        :rtype: int
        """
        return self._max_content_length

    def set_retry_handler(self, retry_handler):
        """
        Sets the :py:class:`RetryHandler` to use for the handle. If no handler
        is configured a default is used. The handler must be safely usable by
        multiple threads.

        :param retry_handler: the handler.
        :type retry_handler: RetryHandler
        :returns: self.
        :raises IllegalArgumentException: raises the exception if retry_handler
            is not an instance of :py:class:`RetryHandler`.
        """
        if not isinstance(retry_handler, RetryHandler):
            raise IllegalArgumentException(
                'retry_handler must be an instance of RetryHandler.')
        self._retry_handler = retry_handler
        return self

    def configure_default_retry_handler(self, num_retries, delay_s):
        """
        Sets the :py:class:`RetryHandler` using a default retry handler
        configured with the specified number of retries and a static delay, in
        seconds. 0 retries means no retries. A delay of 0 means "use the
        default delay algorithm" which is an random delay time. A non-zero delay
        will work but is not recommended for production systems as it is not
        flexible.

        The default retry handler will not retry exceptions of type
        :py:class:`OperationThrottlingException`. The reason is that these
        operations are long-running operations, and while technically they can
        be retried, an immediate retry is unlikely to succeed because of the low
        rates allowed for these operations.

        :param num_retries: the number of retries to perform automatically.
            This parameter may be 0 for no retries.
        :type num_retries: int
        :param delay_s: the delay, in seconds. Use 0 to use the default delay
            algorithm.
        :type delay_s: int
        :returns: self.
        :raises IllegalArgumentException: raises the exception if num_retries or
            delay_s is a negative number.
        """
        self._retry_handler = DefaultRetryHandler(num_retries, delay_s)
        return self

    def get_retry_handler(self):
        """
        Returns the :py:class:`RetryHandler` configured for the handle, or None
        if None is set.

        :returns: the handler.
        :rtype: RetryHandler
        """
        return self._retry_handler

    def set_proxy_host(self, proxy_host):
        """
        Sets an HTTP proxy host to be used for the session. If a proxy host is
        specified a proxy port must also be specified, using
        :py:meth:`set_proxy_port`.

        :param proxy_host: the proxy host.
        :type proxy_host: str
        :returns: self.
        :raises IllegalArgumentException: raises the exception if proxy_host is
            not a string.
        """
        CheckValue.check_str(proxy_host, 'proxy_host')
        self._proxy_host = proxy_host
        return self

    def get_proxy_host(self):
        """
        Returns a proxy host, or None if not configured.

        :returns: the host, or None.
        :rtype: str or None
        """
        return self._proxy_host

    def set_proxy_port(self, proxy_port):
        """
        Sets an HTTP proxy port to be used for the session. If a proxy port is
        specified a proxy host must also be specified, using
        :py:meth:`set_proxy_host`.

        :param proxy_port: the proxy port.
        :type proxy_port: int
        :returns: self.
        :raises IllegalArgumentException: raises the exception if proxy_port is
            a negative number.
        """
        CheckValue.check_int_ge_zero(proxy_port, 'proxy_port')
        self._proxy_port = proxy_port
        return self

    def get_proxy_port(self):
        """
        Returns a proxy port, or 0 if not configured.

        :returns: the proxy port.
        :rtype: int
        """
        return self._proxy_port

    def set_proxy_username(self, proxy_username):
        """
        Sets an HTTP proxy user name if the configured proxy host requires
        authentication. If a proxy host is not configured this configuration is
        ignored.

        :param proxy_username: the user name.
        :type proxy_username: str
        :returns: self.
        :raises IllegalArgumentException: raises the exception if proxy_username
            is not a string.
        """
        CheckValue.check_str(proxy_username, 'proxy_username')
        self._proxy_username = proxy_username
        return self

    def get_proxy_username(self):
        """
        Returns a proxy user name, or None if not configured.

        :returns: the user name, or None.
        :rtype: str or None
        """
        return self._proxy_username

    def set_proxy_password(self, proxy_password):
        """
        Sets an HTTP proxy password if the configured proxy host requires
        authentication. If a proxy user name is not configured this
        configuration is ignored.

        :param proxy_password: the password.
        :type proxy_password: str
        :returns: self.
        :raises IllegalArgumentException: raises the exception if proxy_password
            is not a string.
        """
        CheckValue.check_str(proxy_password, 'proxy_password')
        self._proxy_password = proxy_password
        return self

    def get_proxy_password(self):
        """
        Returns a proxy password, or None if not configured.

        :returns: the password, or None.
        :rtype: str or None
        """
        return self._proxy_password

    def set_logger(self, logger):
        """
        Sets the logger used for the driver.

        :param logger: the logger.
        :type logger: Logger
        :returns: self.
        :raises IllegalArgumentException: raises the exception if logger is not
            an instance of Logger.
        """
        CheckValue.check_logger(logger, 'logger')
        self._logger = logger
        return self

    def get_logger(self):
        """
        Returns the logger, or None if not configured by user.

        :returns: the logger.
        :rtype: Logger
        """
        return self._logger

    def clone(self):
        """
        All the configurations will be copied.

        :returns: the copy of the instance.
        :rtype: NoSQLHandleConfig
        """
        auth_provider = self._auth_provider
        logger = self._logger
        self._auth_provider = None
        self._logger = None
        clone_config = deepcopy(self)
        clone_config.set_authorization_provider(
            auth_provider).set_logger(logger)
        self._logger = logger
        self._auth_provider = auth_provider
        return clone_config

    #
    # Return a url from an endpoint string that has the format
    # [protocol:][//]host[:port]
    #
    @staticmethod
    def create_url(endpoint, path):
        # The defaults for protocol and port.
        protocol = 'https'
        port = 443
        #
        # Possible formats are:
        #     host
        #     protocol:[//]host
        #     host:port
        #     protocol:[//]host:port
        #
        parts = endpoint.split(':')

        if len(parts) == 1:
            # 1 part means endpoint is host only.
            host = endpoint
        elif len(parts) == 2:
            # 2 parts:
            #  protocol:[//]host (default port based on protocol)
            #  host:port (default protocol based on port)
            if parts[0].lower().startswith('http'):
                # protocol:[//]host
                protocol = parts[0].lower()
                # May have slashes to strip out.
                host = parts[1]
                if protocol == 'http':
                    # Override the default of 443.
                    port = 8080
            else:
                # host:port
                host = parts[0]
                port = NoSQLHandleConfig.validate_port(endpoint, parts[1])
                if port != 443:
                    # Override the default of https.
                    protocol = 'http'
        elif len(parts) == 3:
            # 3 parts: protocol:[//]host:port
            protocol = parts[0].lower()
            host = parts[1]
            port = NoSQLHandleConfig.validate_port(endpoint, parts[2])
        else:
            raise IllegalArgumentException('Invalid endpoint: ' + endpoint)

        # Strip out any slashes if the format was protocol://host[:port]
        if host.startswith('//'):
            host = host[2:]

        if protocol != 'http' and protocol != 'https':
            raise IllegalArgumentException(
                'Invalid endpoint, protocol must be http or https: ' + endpoint)
        return urlparse(protocol + '://' + host + ':' + str(port) + path)

    @staticmethod
    def validate_port(endpoint, portstring):
        # Check that a port is a valid, non negative integer.
        try:
            port = int(portstring)
            CheckValue.check_int_ge_zero(port, 'port')
            return port
        except ValueError:
            raise IllegalArgumentException(
                'Invalid port value for : ' + endpoint)
