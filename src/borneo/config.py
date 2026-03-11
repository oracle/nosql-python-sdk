#
# Copyright (c) 2018, 2026 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#

from abc import ABCMeta, abstractmethod
from copy import deepcopy
from os import getenv
from random import random
from ssl import SSLContext
from time import sleep, time

from enum import Enum
from typing import Callable

try:
    # noinspection PyCompatibility
    from urlparse import urlparse
except ImportError:
    # noinspection PyUnresolvedReferences,PyCompatibility
    from urllib.parse import urlparse

from .auth import AuthorizationProvider
from .common import CheckValue, Consistency
from .exception import (
    IllegalArgumentException, OperationThrottlingException, RetryableException)
from .operations import Request
from .serdeutil import SerdeUtil

try:
    from . import iam
except ImportError:
    import iam


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
    regulating provisioned throughput. It is best to add rate limiting to the
    application based on a table's capacity and access patterns to avoid
    throttling exceptions:
    see :py:meth:`NoSQLHandleConfig.set_rate_limiting_enabled`.

    Instances of this class must be immutable so they can be shared among
    threads.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_num_retries(self):
        """
        Returns the number of retries that this handler instance will allow
        before the exception is thrown to the application.

        :returns: the max number of retries.
        :rtype: int
        """
        pass

    @abstractmethod
    def do_retry(self, request, num_retried, re):
        """
        This method is called when a :py:class:`RetryableException` is thrown
        and determines whether to perform a retry or not based on the
        parameters.

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
    def delay(self, request, num_retried, re):
        """
        This method is called when a :py:class:`RetryableException` is thrown
        and it is determined that the request will be retried based on the
        return value of :py:meth:`do_retry`. It provides a delay between
        retries. Most implementations will sleep for some period of time. The
        method should not return until the desired delay period has passed.
        Implementations should not busy-wait in a tight loop.

        If delayMS is non-zero, use it. Otherwise, use a exponential backoff
        algorithm to compute the time of delay.

        If retry-able exception is SecurityInfoNotReadyException, delay for
        SEC_RETRY_DELAY_MS when number of retries is smaller than 10. Otherwise,
        use the exponential backoff algorithm to compute the time of delay.

        :param request: request to execute.
        :type request: Request
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
    Default retry handler. It's a default instance of :py:class:`RetryHandler`
    This may be extended by clients for specific use cases.

    The default retry handler decides when and for how long retries will be
    attempted. See :py:class:`RetryHandler` for more information on retry
    handlers.
    """

    def __init__(self, retries=10, delay_s=0):
        CheckValue.check_int_ge_zero(retries, 'retries')
        CheckValue.check_int_ge_zero(delay_s, 'delay_s')
        self._max_retries = retries
        self._fixed_delay_ms = delay_s * 1000

    def get_num_retries(self):
        return self._max_retries

    def do_retry(self, request, num_retried, re):
        """
        Decide whether to retry or not. Default behavior is to *not* retry
        OperationThrottlingException because the retry time is likely much
        longer than normal because they are DDL operations. In addition, *not*
        retry any requests that should not be retried: TableRequest,
        ListTablesRequest, GetTableRequest, TableUsageRequest,
        GetIndexesRequest.
        """
        self._check_request(request)
        CheckValue.check_int_ge_zero(num_retried, 'num_retried')
        self._check_retryable_exception(re)
        if isinstance(re, OperationThrottlingException):
            return False
        elif not request.should_retry():
            return False
        return num_retried < self._max_retries

    def delay(self, request, num_retried, re):
        """
        Delay (sleep) during retry cycle. If delay_ms is non-zero, use it.
        Otherwise, use an incremental backoff algorithm to compute the time of
        delay.
        """
        self._check_request(request)
        CheckValue.check_int_ge_zero(num_retried, 'num_retried')
        self._check_retryable_exception(re)
        delay_ms = self.compute_backoff_delay(request, self._fixed_delay_ms)
        if delay_ms <= 0:
            return
        sleep(float(delay_ms) / 1000)
        request.add_retry_delay_ms(delay_ms)

    @staticmethod
    def compute_backoff_delay(request, fixed_delay_ms):
        """
        Compute an incremental backoff delay in milliseconds.
        This method also checks the request's timeout and ensures the delay will
        not exceed the specified timeout.

        :param request: the request object being executed.
        :type request: Request
        :param fixed_delay_ms: a specific delay to use and check for timeout.
            Pass zero to use the default backoff logic.
        :type fixed_delay_ms: int
        :returns: The number of milliseconds to delay. If zero, do not delay at
            all.
        """
        timeout_ms = request.get_timeout()
        start_time_ms = request.get_start_time_ms()
        delay_ms = fixed_delay_ms
        if delay_ms == 0:
            # Add 200ms plus a small random amount.
            m_sec_to_add = 200 + int(random() * 50)
            delay_ms = request.get_retry_delay_ms()
            delay_ms += m_sec_to_add
        # If the delay would put us over the timeout, reduce it to just before
        # the timeout would occur.
        now_ms = int(round(time() * 1000))
        ms_left = start_time_ms + timeout_ms - now_ms
        if ms_left < delay_ms:
            delay_ms = ms_left
            if delay_ms < 1:
                return 0
        return delay_ms

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

# === Begin autogenerated regions ===
class Region(object):
    """
    Cloud service only.

    The class represents a region of Oracle NoSQL Database Cloud.
    """

    OC1_EP_BASE = 'https://nosql.{0}.oci.oraclecloud.com'
    OC2_EP_BASE = 'https://nosql.{0}.oci.oraclegovcloud.com'
    OC3_EP_BASE = 'https://nosql.{0}.oci.oraclegovcloud.com'
    OC4_EP_BASE = 'https://nosql.{0}.oci.oraclegovcloud.uk'
    OC5_EP_BASE = 'https://nosql.{0}.oci.oraclecloud5.com'
    OC8_EP_BASE = 'https://nosql.{0}.oci.oraclecloud8.com'
    OC9_EP_BASE = 'https://nosql.{0}.oci.oraclecloud9.com'
    OC10_EP_BASE = 'https://nosql.{0}.oci.oraclecloud10.com'
    OC14_EP_BASE = 'https://nosql.{0}.oci.oraclecloud14.com'
    OC15_EP_BASE = 'https://nosql.{0}.oci.oraclecloud15.com'
    OC16_EP_BASE = 'https://nosql.{0}.oci.oraclecloud16.com'
    OC17_EP_BASE = 'https://nosql.{0}.oci.oraclecloud17.com'
    OC19_EP_BASE = 'https://nosql.{0}.oci.oraclecloud.eu'
    OC20_EP_BASE = 'https://nosql.{0}.oci.oraclecloud20.com'
    OC21_EP_BASE = 'https://nosql.{0}.oci.oraclecloud21.com'
    OC22_EP_BASE = 'https://nosql.{0}.oci.psn-pco.it'
    OC23_EP_BASE = 'https://nosql.{0}.oci.oraclecloud23.com'
    OC24_EP_BASE = 'https://nosql.{0}.oci.oraclecloud24.com'
    OC25_EP_BASE = 'https://nosql.{0}.oci.nricloud.jp'
    OC26_EP_BASE = 'https://nosql.{0}.oci.oraclecloud26.com'
    OC27_EP_BASE = 'https://nosql.{0}.oci.oraclecloud27.com'
    OC28_EP_BASE = 'https://nosql.{0}.oci.oraclecloud28.com'
    OC29_EP_BASE = 'https://nosql.{0}.oci.oraclecloud29.com'
    OC31_EP_BASE = 'https://nosql.{0}.oci.sovereigncloud.nz'
    OC35_EP_BASE = 'https://nosql.{0}.oci.oraclecloud35.com'
    OC36_EP_BASE = 'https://nosql.{0}.oci.stccloud.com'
    OC39_EP_BASE = 'https://nosql.{0}.oci.oraclecloud39.com'
    OC40_EP_BASE = 'https://nosql.{0}.oci.jpsovereigncloud.jp'
    OC41_EP_BASE = 'https://nosql.{0}.oci.dutechcloud.ae'
    OC42_EP_BASE = 'https://nosql.{0}.oci.oraclecloud42.com'
    OC43_EP_BASE = 'https://nosql.{0}.oci.thaiaiscloud.com'
    OC46_EP_BASE = 'https://nosql.{0}.oci.oraclecloud46.com'
    OC47_EP_BASE = 'https://nosql.{0}.oci.oraclecloud47.com'
    OC51_EP_BASE = 'https://nosql.{0}.oci.oraclecloud51.com'
    OC52_EP_BASE = 'https://nosql.{0}.oci.oraclecloud52.com'
    OC55_EP_BASE = 'https://nosql.{0}.oci.svcl.jp'
    OC57_EP_BASE = 'https://nosql.{0}.oci.eonecloud.com'


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
        if self._is_oc2_region():
            return str.format(Region.OC2_EP_BASE, self._region_id)
        if self._is_oc3_region():
            return str.format(Region.OC3_EP_BASE, self._region_id)
        if self._is_oc4_region():
            return str.format(Region.OC4_EP_BASE, self._region_id)
        if self._is_oc5_region():
            return str.format(Region.OC5_EP_BASE, self._region_id)
        if self._is_oc8_region():
            return str.format(Region.OC8_EP_BASE, self._region_id)
        if self._is_oc9_region():
            return str.format(Region.OC9_EP_BASE, self._region_id)
        if self._is_oc10_region():
            return str.format(Region.OC10_EP_BASE, self._region_id)
        if self._is_oc14_region():
            return str.format(Region.OC14_EP_BASE, self._region_id)
        if self._is_oc15_region():
            return str.format(Region.OC15_EP_BASE, self._region_id)
        if self._is_oc16_region():
            return str.format(Region.OC16_EP_BASE, self._region_id)
        if self._is_oc17_region():
            return str.format(Region.OC17_EP_BASE, self._region_id)
        if self._is_oc19_region():
            return str.format(Region.OC19_EP_BASE, self._region_id)
        if self._is_oc20_region():
            return str.format(Region.OC20_EP_BASE, self._region_id)
        if self._is_oc21_region():
            return str.format(Region.OC21_EP_BASE, self._region_id)
        if self._is_oc22_region():
            return str.format(Region.OC22_EP_BASE, self._region_id)
        if self._is_oc23_region():
            return str.format(Region.OC23_EP_BASE, self._region_id)
        if self._is_oc24_region():
            return str.format(Region.OC24_EP_BASE, self._region_id)
        if self._is_oc25_region():
            return str.format(Region.OC25_EP_BASE, self._region_id)
        if self._is_oc26_region():
            return str.format(Region.OC26_EP_BASE, self._region_id)
        if self._is_oc27_region():
            return str.format(Region.OC27_EP_BASE, self._region_id)
        if self._is_oc28_region():
            return str.format(Region.OC28_EP_BASE, self._region_id)
        if self._is_oc29_region():
            return str.format(Region.OC29_EP_BASE, self._region_id)
        if self._is_oc31_region():
            return str.format(Region.OC31_EP_BASE, self._region_id)
        if self._is_oc35_region():
            return str.format(Region.OC35_EP_BASE, self._region_id)
        if self._is_oc36_region():
            return str.format(Region.OC36_EP_BASE, self._region_id)
        if self._is_oc39_region():
            return str.format(Region.OC39_EP_BASE, self._region_id)
        if self._is_oc40_region():
            return str.format(Region.OC40_EP_BASE, self._region_id)
        if self._is_oc41_region():
            return str.format(Region.OC41_EP_BASE, self._region_id)
        if self._is_oc42_region():
            return str.format(Region.OC42_EP_BASE, self._region_id)
        if self._is_oc43_region():
            return str.format(Region.OC43_EP_BASE, self._region_id)
        if self._is_oc46_region():
            return str.format(Region.OC46_EP_BASE, self._region_id)
        if self._is_oc47_region():
            return str.format(Region.OC47_EP_BASE, self._region_id)
        if self._is_oc51_region():
            return str.format(Region.OC51_EP_BASE, self._region_id)
        if self._is_oc52_region():
            return str.format(Region.OC52_EP_BASE, self._region_id)
        if self._is_oc55_region():
            return str.format(Region.OC55_EP_BASE, self._region_id)
        if self._is_oc57_region():
            return str.format(Region.OC57_EP_BASE, self._region_id)
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

    def _is_oc1_region(self):
        # Internal use only
        return Regions.OC1_REGIONS.get(self._region_id) is not None

    def _is_oc2_region(self):
        # Internal use only
        return Regions.OC2_REGIONS.get(self._region_id) is not None

    def _is_oc3_region(self):
        # Internal use only
        return Regions.OC3_REGIONS.get(self._region_id) is not None

    def _is_oc4_region(self):
        # Internal use only
        return Regions.OC4_REGIONS.get(self._region_id) is not None

    def _is_oc5_region(self):
        # Internal use only
        return Regions.OC5_REGIONS.get(self._region_id) is not None

    def _is_oc8_region(self):
        # Internal use only
        return Regions.OC8_REGIONS.get(self._region_id) is not None

    def _is_oc9_region(self):
        # Internal use only
        return Regions.OC9_REGIONS.get(self._region_id) is not None

    def _is_oc10_region(self):
        # Internal use only
        return Regions.OC10_REGIONS.get(self._region_id) is not None

    def _is_oc14_region(self):
        # Internal use only
        return Regions.OC14_REGIONS.get(self._region_id) is not None

    def _is_oc15_region(self):
        # Internal use only
        return Regions.OC15_REGIONS.get(self._region_id) is not None

    def _is_oc16_region(self):
        # Internal use only
        return Regions.OC16_REGIONS.get(self._region_id) is not None

    def _is_oc17_region(self):
        # Internal use only
        return Regions.OC17_REGIONS.get(self._region_id) is not None

    def _is_oc19_region(self):
        # Internal use only
        return Regions.OC19_REGIONS.get(self._region_id) is not None

    def _is_oc20_region(self):
        # Internal use only
        return Regions.OC20_REGIONS.get(self._region_id) is not None

    def _is_oc21_region(self):
        # Internal use only
        return Regions.OC21_REGIONS.get(self._region_id) is not None

    def _is_oc22_region(self):
        # Internal use only
        return Regions.OC22_REGIONS.get(self._region_id) is not None

    def _is_oc23_region(self):
        # Internal use only
        return Regions.OC23_REGIONS.get(self._region_id) is not None

    def _is_oc24_region(self):
        # Internal use only
        return Regions.OC24_REGIONS.get(self._region_id) is not None

    def _is_oc25_region(self):
        # Internal use only
        return Regions.OC25_REGIONS.get(self._region_id) is not None

    def _is_oc26_region(self):
        # Internal use only
        return Regions.OC26_REGIONS.get(self._region_id) is not None

    def _is_oc27_region(self):
        # Internal use only
        return Regions.OC27_REGIONS.get(self._region_id) is not None

    def _is_oc28_region(self):
        # Internal use only
        return Regions.OC28_REGIONS.get(self._region_id) is not None

    def _is_oc29_region(self):
        # Internal use only
        return Regions.OC29_REGIONS.get(self._region_id) is not None

    def _is_oc31_region(self):
        # Internal use only
        return Regions.OC31_REGIONS.get(self._region_id) is not None

    def _is_oc35_region(self):
        # Internal use only
        return Regions.OC35_REGIONS.get(self._region_id) is not None

    def _is_oc36_region(self):
        # Internal use only
        return Regions.OC36_REGIONS.get(self._region_id) is not None

    def _is_oc39_region(self):
        # Internal use only
        return Regions.OC39_REGIONS.get(self._region_id) is not None

    def _is_oc40_region(self):
        # Internal use only
        return Regions.OC40_REGIONS.get(self._region_id) is not None

    def _is_oc41_region(self):
        # Internal use only
        return Regions.OC41_REGIONS.get(self._region_id) is not None

    def _is_oc42_region(self):
        # Internal use only
        return Regions.OC42_REGIONS.get(self._region_id) is not None

    def _is_oc43_region(self):
        # Internal use only
        return Regions.OC43_REGIONS.get(self._region_id) is not None

    def _is_oc46_region(self):
        # Internal use only
        return Regions.OC46_REGIONS.get(self._region_id) is not None

    def _is_oc47_region(self):
        # Internal use only
        return Regions.OC47_REGIONS.get(self._region_id) is not None

    def _is_oc51_region(self):
        # Internal use only
        return Regions.OC51_REGIONS.get(self._region_id) is not None

    def _is_oc52_region(self):
        # Internal use only
        return Regions.OC52_REGIONS.get(self._region_id) is not None

    def _is_oc55_region(self):
        # Internal use only
        return Regions.OC55_REGIONS.get(self._region_id) is not None

    def _is_oc57_region(self):
        # Internal use only
        return Regions.OC57_REGIONS.get(self._region_id) is not None

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
    AF_JOHANNESBURG_1 = Region('af-johannesburg-1')
    """Realm: OC1, South Africa (Johannesburg)"""

    AP_BATAM_1 = Region('ap-batam-1')
    """Realm: OC1, Indonesia North (Batam)"""

    AP_CHENNAI_1 = Region('ap-chennai-1')
    """India South (Chennai)"""

    AP_CHUNCHEON_1 = Region('ap-chuncheon-1')
    """Realm: OC1, South Korea North (Chuncheon)"""

    AP_HYDERABAD_1 = Region('ap-hyderabad-1')
    """Realm: OC1, India South (Hyderabad)"""

    AP_KULAI_1 = Region('ap-kulai-1')
    """Malaysia West (Kulai)"""

    AP_MELBOURNE_1 = Region('ap-melbourne-1')
    """Realm: OC1, Australia Southeast (Melbourne)"""

    AP_MUMBAI_1 = Region('ap-mumbai-1')
    """Realm: OC1, India West (Mumbai)"""

    AP_OSAKA_1 = Region('ap-osaka-1')
    """Realm: OC1, Japan Central (Osaka)"""

    AP_SEOUL_1 = Region('ap-seoul-1')
    """Realm: OC1, South Korea Central (Seoul)"""

    AP_SINGAPORE_1 = Region('ap-singapore-1')
    """Realm: OC1, Singapore (Singapore)"""

    AP_SINGAPORE_2 = Region('ap-singapore-2')
    """Realm: OC1, Singapore2 (Singapore)"""

    AP_SYDNEY_1 = Region('ap-sydney-1')
    """Realm: OC1, Australia East (Sydney)"""

    AP_TOKYO_1 = Region('ap-tokyo-1')
    """Realm: OC1, Japan East (Tokyo)"""

    CA_MONTREAL_1 = Region('ca-montreal-1')
    """Realm: OC1, Canada Southeast (Montreal)"""

    CA_TORONTO_1 = Region('ca-toronto-1')
    """Realm: OC1, Canada Southeast (Toronto)"""

    EU_AMSTERDAM_1 = Region('eu-amsterdam-1')
    """Realm: OC1, Netherlands Northwest (Amsterdam)"""

    EU_DUBLIN_3 = Region('eu-dublin-3')
    """Ireland East (Dublin)"""

    EU_FRANKFURT_1 = Region('eu-frankfurt-1')
    """Realm: OC1, Germany Central (Frankfurt)"""

    EU_MADRID_1 = Region('eu-madrid-1')
    """Realm: OC1, Spain (Madrid)"""

    EU_MADRID_3 = Region('eu-madrid-3')
    """Spain Central (Madrid 3)"""

    EU_MARSEILLE_1 = Region('eu-marseille-1')
    """Realm: OC1, France (Marseille)"""

    EU_MILAN_1 = Region('eu-milan-1')
    """Realm: OC1, Italy (Milan)"""

    EU_PARIS_1 = Region('eu-paris-1')
    """Realm: OC1, France (Paris)"""

    EU_STOCKHOLM_1 = Region('eu-stockholm-1')
    """Realm: OC1, Sweden (Stockholm)"""

    EU_TURIN_1 = Region('eu-turin-1')
    """Italy North (Turin)"""

    EU_ZURICH_1 = Region('eu-zurich-1')
    """Realm: OC1, Switzerland North (Zurich)"""

    IL_JERUSALEM_1 = Region('il-jerusalem-1')
    """Realm: OC1, Israel (Jerusalem)"""

    ME_ABUDHABI_1 = Region('me-abudhabi-1')
    """Realm: OC1, UAE (Abu Dhabi)"""

    ME_DUBAI_1 = Region('me-dubai-1')
    """Realm: OC1, UAE East (Dubai)"""

    ME_JEDDAH_1 = Region('me-jeddah-1')
    """Realm: OC1, Saudi Arabia West (Jeddah)"""

    ME_RIYADH_1 = Region('me-riyadh-1')

    MX_MONTERREY_1 = Region('mx-monterrey-1')
    """Realm: OC1, Mexico (Monterrey)"""

    MX_QUERETARO_1 = Region('mx-queretaro-1')
    """Realm: OC1, Mexico (Queretaro)"""

    SA_BOGOTA_1 = Region('sa-bogota-1')
    """Realm: OC1, Colombia (Bogota)"""

    SA_SANTIAGO_1 = Region('sa-santiago-1')
    """Realm: OC1, Chile (Santiago)"""

    SA_SAOPAULO_1 = Region('sa-saopaulo-1')
    """Realm: OC1, Brazil East (Sao Paulo)"""

    SA_VALPARAISO_1 = Region('sa-valparaiso-1')
    """Realm: OC1, Chile (Valparaiso)"""

    SA_VINHEDO_1 = Region('sa-vinhedo-1')
    """Realm: OC1, Brazil (Vinhedo)"""

    UK_LONDON_1 = Region('uk-london-1')
    """Realm: OC1, UK South (London)"""

    UK_CARDIFF_1 = Region('uk-cardiff-1')
    """Realm: OC1, UK West (Newport)"""

    US_ABILENE_1 = Region('us-abilene-1')
    """Realm: OC1, US South Central (Abilene)"""

    US_COLUMBUS_1 = Region('us-columbus-1')
    """US Central (Columbus)"""

    US_QUINCY_1 = Region('us-quincy-1')
    """US West (Quincy)"""

    US_BOARDMAN_1 = Region('us-boardman-1')
    """US West (Boardman)"""

    US_DALLAS_1 = Region('us-dallas-1')
    """Realm: OC1, US South (Dallas)"""

    US_DESMOINES_1 = Region('us-desmoines-1')
    """US Midwest (Des Moines)"""

    US_PHOENIX_1 = Region('us-phoenix-1')
    """Realm: OC1, US West (Phoenix)"""

    US_ASHBURN_1 = Region('us-ashburn-1')
    """Realm: OC1, US East (Ashburn)"""

    US_SALTLAKE_2 = Region('us-saltlake-2')
    """Realm: OC1, US West (Salt Lake)"""

    US_SANJOSE_1 = Region('us-sanjose-1')
    """Realm: OC1, US West (San Jose)"""

    US_SHAWNEE_1 = Region('us-shawnee-1')
    """US Mid West (Shawnee)"""

    US_CHICAGO_1 = Region('us-chicago-1')
    """Realm: OC1, US Central (Chicago)"""

    AP_KULAI_2 = Region('ap-kulai-2')
    """Realm: OC1, Kulai 2"""

    AP_DELHI_1 = Region('ap-delhi-1')
    """Realm: OC1, Delhi 1"""

    US_LANGLEY_1 = Region('us-langley-1')
    """Realm: OC2, US Gov East (Ashburn)"""

    US_LUKE_1 = Region('us-luke-1')
    """Realm: OC2, US Gov West (Phoenix)"""

    US_GOV_ASHBURN_1 = Region('us-gov-ashburn-1')
    """Realm: OC3, US DoD East (Ashburn)"""

    US_GOV_CHICAGO_1 = Region('us-gov-chicago-1')
    """Realm: OC3, US DoD North (Chicago)"""

    US_GOV_PHOENIX_1 = Region('us-gov-phoenix-1')
    """Realm: OC3, US DoD West (Phoenix)"""

    UK_GOV_LONDON_1 = Region('uk-gov-london-1')
    """Realm: OC4, UK Gov South (London)"""

    UK_GOV_CARDIFF_1 = Region('uk-gov-cardiff-1')
    """Realm: OC4, UK Gov West (Cardiff)"""

    US_TACOMA_1 = Region('us-tacoma-1')
    """Realm: OC5, US West (Tacoma)"""

    AP_CHIYODA_1 = Region('ap-chiyoda-1')
    """Realm: OC8, Japan East (Chiyoda)"""

    AP_IBARAKI_1 = Region('ap-ibaraki-1')
    """Realm: OC8, Japan East (Ibaraki) (Note: OCI uses 'ukb' instead of 'ibr')"""

    ME_DCC_MUSCAT_1 = Region('me-dcc-muscat-1')
    """Realm: OC9, Muscat (Dedicated DataCenter)"""

    ME_IBRI_1 = Region('me-ibri-1')
    """Realm: OC9, Ibri 1"""

    AP_DCC_CANBERRA_1 = Region('ap-dcc-canberra-1')
    """Realm: OC10, Canberra (Dedicated DataCenter)"""

    EU_DCC_DUBLIN_1 = Region('eu-dcc-dublin-1')
    """Realm: OC14, Dublin 1 (Dedicated DataCenter)"""

    EU_DCC_DUBLIN_2 = Region('eu-dcc-dublin-2')
    """Realm: OC14, Dublin 2 (Dedicated DataCenter)"""

    EU_DCC_MILAN_1 = Region('eu-dcc-milan-1')
    """Realm: OC14, Milan 1 (Dedicated DataCenter)"""

    EU_DCC_MILAN_2 = Region('eu-dcc-milan-2')
    """Realm: OC14, Milan 2 (Dedicated DataCenter)"""

    EU_DCC_RATING_1 = Region('eu-dcc-rating-1')
    """Realm: OC14, Rating 1 (Dedicated DataCenter)"""

    EU_DCC_RATING_2 = Region('eu-dcc-rating-2')
    """Realm: OC14, Rating 2 (Dedicated DataCenter)"""

    AP_DCC_GAZIPUR_1 = Region('ap-dcc-gazipur-1')
    """Realm: OC15, Bangladesh"""

    US_WESTJORDAN_1 = Region('us-westjordan-1')
    """Realm: OC16, US West (Utah)"""

    US_DCC_PHOENIX_1 = Region('us-dcc-phoenix-1')
    """Realm: OC17, Phoenix 1 (Dedicated DataCenter)"""

    US_DCC_PHOENIX_2 = Region('us-dcc-phoenix-2')
    """Realm: OC17, Phoenix 2 (Dedicated DataCenter)"""

    US_DCC_PHOENIX_4 = Region('us-dcc-phoenix-4')
    """Realm: OC17, Phoenix 4 (Dedicated DataCenter)"""

    EU_FRANKFURT_2 = Region('eu-frankfurt-2')
    """Realm: OC19, Frankfurt (Germany)"""

    EU_MADRID_2 = Region('eu-madrid-2')
    """Realm: OC19, Madrid (Spain)"""

    EU_JOVANOVAC_1 = Region('eu-jovanovac-1')
    """Realm: OC20, Jovanovac (Serbia)"""

    ME_DCC_DOHA_1 = Region('me-dcc-doha-1')
    """Realm: OC21, Doha (Qatar)"""

    EU_DCC_ROME_1 = Region('eu-dcc-rome-1')
    """Realm: OC22, PSN Region Rome"""

    EU_MILAN_2 = Region('eu-milan-2')
    """Realm: OC22, PSN Region Milan"""

    US_SOMERSET_1 = Region('us-somerset-1')

    US_THAMES_1 = Region('us-thames-1')

    EU_DCC_ZURICH_1 = Region('eu-dcc-zurich-1')
    """Realm: OC24, Switzerland dedicated (Zurich)"""

    EU_CRISSIER_1 = Region('eu-crissier-1')

    AP_DCC_OSAKA_1 = Region('ap-dcc-osaka-1')
    """Realm: OC25, Japan dedicated (Osaka)"""

    AP_DCC_TOKYO_1 = Region('ap-dcc-tokyo-1')
    """Realm: OC25, Japan dedicated (Tokyo)"""

    ME_ABUDHABI_3 = Region('me-abudhabi-3')
    """Realm: OC26, Abudhabi"""

    ME_ALAIN_1 = Region('me-alain-1')

    US_DCC_SWJORDAN_1 = Region('us-dcc-swjordan-1')
    """Realm: OC27, SWJordan dedicated (Utah USA)"""

    US_DCC_SWJORDAN_2 = Region('us-dcc-swjordan-2')
    """Realm: OC28, SWJordan dedicated (Utah USA)"""

    ME_ABUDHABI_2 = Region('me-abudhabi-2')
    """Realm: OC29, Abudhabi 2"""

    ME_ABUDHABI_4 = Region('me-abudhabi-4')
    """Realm: OC29, Abudhabi 4"""

    AP_HOBSONVILLE_1 = Region('ap-hobsonville-1')
    """Realm: OC31, Hobsonville dedicated (New Zealand)"""

    AP_SILVERDALE_1 = Region('ap-silverdale-1')
    """Realm: OC31, Silverdale dedicated (New Zealand)"""

    AP_SUWON_1 = Region('ap-suwon-1')
    """Realm: OC35, South Korea North (Suwon)"""

    AP_SEOUL_2 = Region('ap-seoul-2')
    """Realm: OC35, South Korea North (Seoul)"""

    AP_CHUNCHEON_2 = Region('ap-chuncheon-2')
    """Realm: OC35, South Korea North (Chuncheon)"""

    ME_RIYADH_2 = Region('me-riyadh-2')
    """Riyadh 2 Alloy"""

    US_TUKWILA_3 = Region('us-tukwila-3')
    """Realm: OC39, Butterfly Stable"""

    AP_OSAKA_2 = Region('ap-osaka-2')
    """Realm: OC40, Sovereign Cloud (Japan West)"""

    AP_TATEBAYASHI_1 = Region('ap-tatebayashi-1')
    """Realm: OC40, Sovereign Cloud"""

    ME_DUBAI_3 = Region('me-dubai-3')
    """Realm: OC41, UAE North 1"""

    US_NEWARK_1 = Region('us-newark-1')
    """Newark 1 DRCC"""

    AP_PATHUMTHANI_1 = Region('ap-pathumthani-1')
    """Realm: OC43, Thailand 1 (AIS)"""

    AP_SAMUTPRAKAN_1 = Region('ap-samutprakan-1')
    """Thailand 2 (AIS)"""

    UK_LONDON_2 = Region('uk-london-2')
    """Realm: OC46, Butterfly Lab 1"""

    UK_LONDON_3 = Region('uk-london-3')
    """Realm: OC47, Butterfly Lab 2"""

    UK_LONDON_4 = Region('uk-london-4')
    """Realm: OC47, London 4"""

    EU_BUDAPEST_1 = Region('eu-budapest-1')
    """Realm: OC51, Budapest"""

    SA_RIODEJANEIRO_1 = Region('sa-riodejaneiro-1')
    """Realm: OC52, Rio De Janeiro"""

    AP_WESTTOKYO_1 = Region('ap-westtokyo-1')
    """WestTokyo 1 Alloy"""

    ME_ABUDHABI_5 = Region('me-abudhabi-5')
    """Realm: OC57, Abu Dhabi 5"""

    ME_DUBAI_4 = Region('me-dubai-4')
    """Realm: OC57, Dubai 4"""


    # OC1
    OC1_REGIONS = dict()
    OC1_REGIONS[AF_JOHANNESBURG_1.get_region_id()] = AF_JOHANNESBURG_1
    OC1_REGIONS[AP_BATAM_1.get_region_id()] = AP_BATAM_1
    OC1_REGIONS[AP_CHENNAI_1.get_region_id()] = AP_CHENNAI_1
    OC1_REGIONS[AP_CHUNCHEON_1.get_region_id()] = AP_CHUNCHEON_1
    OC1_REGIONS[AP_HYDERABAD_1.get_region_id()] = AP_HYDERABAD_1
    OC1_REGIONS[AP_KULAI_1.get_region_id()] = AP_KULAI_1
    OC1_REGIONS[AP_MELBOURNE_1.get_region_id()] = AP_MELBOURNE_1
    OC1_REGIONS[AP_MUMBAI_1.get_region_id()] = AP_MUMBAI_1
    OC1_REGIONS[AP_OSAKA_1.get_region_id()] = AP_OSAKA_1
    OC1_REGIONS[AP_SEOUL_1.get_region_id()] = AP_SEOUL_1
    OC1_REGIONS[AP_SINGAPORE_1.get_region_id()] = AP_SINGAPORE_1
    OC1_REGIONS[AP_SINGAPORE_2.get_region_id()] = AP_SINGAPORE_2
    OC1_REGIONS[AP_SYDNEY_1.get_region_id()] = AP_SYDNEY_1
    OC1_REGIONS[AP_TOKYO_1.get_region_id()] = AP_TOKYO_1
    OC1_REGIONS[CA_MONTREAL_1.get_region_id()] = CA_MONTREAL_1
    OC1_REGIONS[CA_TORONTO_1.get_region_id()] = CA_TORONTO_1
    OC1_REGIONS[EU_AMSTERDAM_1.get_region_id()] = EU_AMSTERDAM_1
    OC1_REGIONS[EU_DUBLIN_3.get_region_id()] = EU_DUBLIN_3
    OC1_REGIONS[EU_FRANKFURT_1.get_region_id()] = EU_FRANKFURT_1
    OC1_REGIONS[EU_MADRID_1.get_region_id()] = EU_MADRID_1
    OC1_REGIONS[EU_MADRID_3.get_region_id()] = EU_MADRID_3
    OC1_REGIONS[EU_MARSEILLE_1.get_region_id()] = EU_MARSEILLE_1
    OC1_REGIONS[EU_MILAN_1.get_region_id()] = EU_MILAN_1
    OC1_REGIONS[EU_PARIS_1.get_region_id()] = EU_PARIS_1
    OC1_REGIONS[EU_STOCKHOLM_1.get_region_id()] = EU_STOCKHOLM_1
    OC1_REGIONS[EU_TURIN_1.get_region_id()] = EU_TURIN_1
    OC1_REGIONS[EU_ZURICH_1.get_region_id()] = EU_ZURICH_1
    OC1_REGIONS[IL_JERUSALEM_1.get_region_id()] = IL_JERUSALEM_1
    OC1_REGIONS[ME_ABUDHABI_1.get_region_id()] = ME_ABUDHABI_1
    OC1_REGIONS[ME_DUBAI_1.get_region_id()] = ME_DUBAI_1
    OC1_REGIONS[ME_JEDDAH_1.get_region_id()] = ME_JEDDAH_1
    OC1_REGIONS[ME_RIYADH_1.get_region_id()] = ME_RIYADH_1
    OC1_REGIONS[MX_MONTERREY_1.get_region_id()] = MX_MONTERREY_1
    OC1_REGIONS[MX_QUERETARO_1.get_region_id()] = MX_QUERETARO_1
    OC1_REGIONS[SA_BOGOTA_1.get_region_id()] = SA_BOGOTA_1
    OC1_REGIONS[SA_SANTIAGO_1.get_region_id()] = SA_SANTIAGO_1
    OC1_REGIONS[SA_SAOPAULO_1.get_region_id()] = SA_SAOPAULO_1
    OC1_REGIONS[SA_VALPARAISO_1.get_region_id()] = SA_VALPARAISO_1
    OC1_REGIONS[SA_VINHEDO_1.get_region_id()] = SA_VINHEDO_1
    OC1_REGIONS[UK_LONDON_1.get_region_id()] = UK_LONDON_1
    OC1_REGIONS[UK_CARDIFF_1.get_region_id()] = UK_CARDIFF_1
    OC1_REGIONS[US_ABILENE_1.get_region_id()] = US_ABILENE_1
    OC1_REGIONS[US_COLUMBUS_1.get_region_id()] = US_COLUMBUS_1
    OC1_REGIONS[US_QUINCY_1.get_region_id()] = US_QUINCY_1
    OC1_REGIONS[US_BOARDMAN_1.get_region_id()] = US_BOARDMAN_1
    OC1_REGIONS[US_DALLAS_1.get_region_id()] = US_DALLAS_1
    OC1_REGIONS[US_DESMOINES_1.get_region_id()] = US_DESMOINES_1
    OC1_REGIONS[US_PHOENIX_1.get_region_id()] = US_PHOENIX_1
    OC1_REGIONS[US_ASHBURN_1.get_region_id()] = US_ASHBURN_1
    OC1_REGIONS[US_SALTLAKE_2.get_region_id()] = US_SALTLAKE_2
    OC1_REGIONS[US_SANJOSE_1.get_region_id()] = US_SANJOSE_1
    OC1_REGIONS[US_SHAWNEE_1.get_region_id()] = US_SHAWNEE_1
    OC1_REGIONS[US_CHICAGO_1.get_region_id()] = US_CHICAGO_1
    OC1_REGIONS[AP_KULAI_2.get_region_id()] = AP_KULAI_2
    OC1_REGIONS[AP_DELHI_1.get_region_id()] = AP_DELHI_1

    # OC2
    OC2_REGIONS = dict()
    OC2_REGIONS[US_LANGLEY_1.get_region_id()] = US_LANGLEY_1
    OC2_REGIONS[US_LUKE_1.get_region_id()] = US_LUKE_1

    # OC3
    OC3_REGIONS = dict()
    OC3_REGIONS[US_GOV_ASHBURN_1.get_region_id()] = US_GOV_ASHBURN_1
    OC3_REGIONS[US_GOV_CHICAGO_1.get_region_id()] = US_GOV_CHICAGO_1
    OC3_REGIONS[US_GOV_PHOENIX_1.get_region_id()] = US_GOV_PHOENIX_1

    # OC4
    OC4_REGIONS = dict()
    OC4_REGIONS[UK_GOV_LONDON_1.get_region_id()] = UK_GOV_LONDON_1
    OC4_REGIONS[UK_GOV_CARDIFF_1.get_region_id()] = UK_GOV_CARDIFF_1

    # OC5
    OC5_REGIONS = dict()
    OC5_REGIONS[US_TACOMA_1.get_region_id()] = US_TACOMA_1

    # OC8
    OC8_REGIONS = dict()
    OC8_REGIONS[AP_CHIYODA_1.get_region_id()] = AP_CHIYODA_1
    OC8_REGIONS[AP_IBARAKI_1.get_region_id()] = AP_IBARAKI_1

    # OC9
    OC9_REGIONS = dict()
    OC9_REGIONS[ME_DCC_MUSCAT_1.get_region_id()] = ME_DCC_MUSCAT_1
    OC9_REGIONS[ME_IBRI_1.get_region_id()] = ME_IBRI_1

    # OC10
    OC10_REGIONS = dict()
    OC10_REGIONS[AP_DCC_CANBERRA_1.get_region_id()] = AP_DCC_CANBERRA_1

    # OC14
    OC14_REGIONS = dict()
    OC14_REGIONS[EU_DCC_DUBLIN_1.get_region_id()] = EU_DCC_DUBLIN_1
    OC14_REGIONS[EU_DCC_DUBLIN_2.get_region_id()] = EU_DCC_DUBLIN_2
    OC14_REGIONS[EU_DCC_MILAN_1.get_region_id()] = EU_DCC_MILAN_1
    OC14_REGIONS[EU_DCC_MILAN_2.get_region_id()] = EU_DCC_MILAN_2
    OC14_REGIONS[EU_DCC_RATING_1.get_region_id()] = EU_DCC_RATING_1
    OC14_REGIONS[EU_DCC_RATING_2.get_region_id()] = EU_DCC_RATING_2

    # OC15
    OC15_REGIONS = dict()
    OC15_REGIONS[AP_DCC_GAZIPUR_1.get_region_id()] = AP_DCC_GAZIPUR_1

    # OC16
    OC16_REGIONS = dict()
    OC16_REGIONS[US_WESTJORDAN_1.get_region_id()] = US_WESTJORDAN_1

    # OC17
    OC17_REGIONS = dict()
    OC17_REGIONS[US_DCC_PHOENIX_1.get_region_id()] = US_DCC_PHOENIX_1
    OC17_REGIONS[US_DCC_PHOENIX_2.get_region_id()] = US_DCC_PHOENIX_2
    OC17_REGIONS[US_DCC_PHOENIX_4.get_region_id()] = US_DCC_PHOENIX_4

    # OC19
    OC19_REGIONS = dict()
    OC19_REGIONS[EU_FRANKFURT_2.get_region_id()] = EU_FRANKFURT_2
    OC19_REGIONS[EU_MADRID_2.get_region_id()] = EU_MADRID_2

    # OC20
    OC20_REGIONS = dict()
    OC20_REGIONS[EU_JOVANOVAC_1.get_region_id()] = EU_JOVANOVAC_1

    # OC21
    OC21_REGIONS = dict()
    OC21_REGIONS[ME_DCC_DOHA_1.get_region_id()] = ME_DCC_DOHA_1

    # OC22
    OC22_REGIONS = dict()
    OC22_REGIONS[EU_DCC_ROME_1.get_region_id()] = EU_DCC_ROME_1
    OC22_REGIONS[EU_MILAN_2.get_region_id()] = EU_MILAN_2

    # OC23
    OC23_REGIONS = dict()
    OC23_REGIONS[US_SOMERSET_1.get_region_id()] = US_SOMERSET_1
    OC23_REGIONS[US_THAMES_1.get_region_id()] = US_THAMES_1

    # OC24
    OC24_REGIONS = dict()
    OC24_REGIONS[EU_DCC_ZURICH_1.get_region_id()] = EU_DCC_ZURICH_1
    OC24_REGIONS[EU_CRISSIER_1.get_region_id()] = EU_CRISSIER_1

    # OC25
    OC25_REGIONS = dict()
    OC25_REGIONS[AP_DCC_OSAKA_1.get_region_id()] = AP_DCC_OSAKA_1
    OC25_REGIONS[AP_DCC_TOKYO_1.get_region_id()] = AP_DCC_TOKYO_1

    # OC26
    OC26_REGIONS = dict()
    OC26_REGIONS[ME_ABUDHABI_3.get_region_id()] = ME_ABUDHABI_3
    OC26_REGIONS[ME_ALAIN_1.get_region_id()] = ME_ALAIN_1

    # OC27
    OC27_REGIONS = dict()
    OC27_REGIONS[US_DCC_SWJORDAN_1.get_region_id()] = US_DCC_SWJORDAN_1

    # OC28
    OC28_REGIONS = dict()
    OC28_REGIONS[US_DCC_SWJORDAN_2.get_region_id()] = US_DCC_SWJORDAN_2

    # OC29
    OC29_REGIONS = dict()
    OC29_REGIONS[ME_ABUDHABI_2.get_region_id()] = ME_ABUDHABI_2
    OC29_REGIONS[ME_ABUDHABI_4.get_region_id()] = ME_ABUDHABI_4

    # OC31
    OC31_REGIONS = dict()
    OC31_REGIONS[AP_HOBSONVILLE_1.get_region_id()] = AP_HOBSONVILLE_1
    OC31_REGIONS[AP_SILVERDALE_1.get_region_id()] = AP_SILVERDALE_1

    # OC35
    OC35_REGIONS = dict()
    OC35_REGIONS[AP_SUWON_1.get_region_id()] = AP_SUWON_1
    OC35_REGIONS[AP_SEOUL_2.get_region_id()] = AP_SEOUL_2
    OC35_REGIONS[AP_CHUNCHEON_2.get_region_id()] = AP_CHUNCHEON_2

    # OC36
    OC36_REGIONS = dict()
    OC36_REGIONS[ME_RIYADH_2.get_region_id()] = ME_RIYADH_2

    # OC39
    OC39_REGIONS = dict()
    OC39_REGIONS[US_TUKWILA_3.get_region_id()] = US_TUKWILA_3

    # OC40
    OC40_REGIONS = dict()
    OC40_REGIONS[AP_OSAKA_2.get_region_id()] = AP_OSAKA_2
    OC40_REGIONS[AP_TATEBAYASHI_1.get_region_id()] = AP_TATEBAYASHI_1

    # OC41
    OC41_REGIONS = dict()
    OC41_REGIONS[ME_DUBAI_3.get_region_id()] = ME_DUBAI_3

    # OC42
    OC42_REGIONS = dict()
    OC42_REGIONS[US_NEWARK_1.get_region_id()] = US_NEWARK_1

    # OC43
    OC43_REGIONS = dict()
    OC43_REGIONS[AP_PATHUMTHANI_1.get_region_id()] = AP_PATHUMTHANI_1
    OC43_REGIONS[AP_SAMUTPRAKAN_1.get_region_id()] = AP_SAMUTPRAKAN_1

    # OC46
    OC46_REGIONS = dict()
    OC46_REGIONS[UK_LONDON_2.get_region_id()] = UK_LONDON_2

    # OC47
    OC47_REGIONS = dict()
    OC47_REGIONS[UK_LONDON_3.get_region_id()] = UK_LONDON_3
    OC47_REGIONS[UK_LONDON_4.get_region_id()] = UK_LONDON_4

    # OC51
    OC51_REGIONS = dict()
    OC51_REGIONS[EU_BUDAPEST_1.get_region_id()] = EU_BUDAPEST_1

    # OC52
    OC52_REGIONS = dict()
    OC52_REGIONS[SA_RIODEJANEIRO_1.get_region_id()] = SA_RIODEJANEIRO_1

    # OC55
    OC55_REGIONS = dict()
    OC55_REGIONS[AP_WESTTOKYO_1.get_region_id()] = AP_WESTTOKYO_1

    # OC57
    OC57_REGIONS = dict()
    OC57_REGIONS[ME_ABUDHABI_5.get_region_id()] = ME_ABUDHABI_5
    OC57_REGIONS[ME_DUBAI_4.get_region_id()] = ME_DUBAI_4

    @staticmethod
    def get_oc1_regions():
        # Internal use only
        return Regions.OC1_REGIONS.values()

    @staticmethod
    def get_oc2_regions():
        # Internal use only
        return Regions.OC2_REGIONS.values()

    @staticmethod
    def get_oc3_regions():
        # Internal use only
        return Regions.OC3_REGIONS.values()

    @staticmethod
    def get_oc4_regions():
        # Internal use only
        return Regions.OC4_REGIONS.values()

    @staticmethod
    def get_oc5_regions():
        # Internal use only
        return Regions.OC5_REGIONS.values()

    @staticmethod
    def get_oc8_regions():
        # Internal use only
        return Regions.OC8_REGIONS.values()

    @staticmethod
    def get_oc9_regions():
        # Internal use only
        return Regions.OC9_REGIONS.values()

    @staticmethod
    def get_oc10_regions():
        # Internal use only
        return Regions.OC10_REGIONS.values()

    @staticmethod
    def get_oc14_regions():
        # Internal use only
        return Regions.OC14_REGIONS.values()

    @staticmethod
    def get_oc15_regions():
        # Internal use only
        return Regions.OC15_REGIONS.values()

    @staticmethod
    def get_oc16_regions():
        # Internal use only
        return Regions.OC16_REGIONS.values()

    @staticmethod
    def get_oc17_regions():
        # Internal use only
        return Regions.OC17_REGIONS.values()

    @staticmethod
    def get_oc19_regions():
        # Internal use only
        return Regions.OC19_REGIONS.values()

    @staticmethod
    def get_oc20_regions():
        # Internal use only
        return Regions.OC20_REGIONS.values()

    @staticmethod
    def get_oc21_regions():
        # Internal use only
        return Regions.OC21_REGIONS.values()

    @staticmethod
    def get_oc22_regions():
        # Internal use only
        return Regions.OC22_REGIONS.values()

    @staticmethod
    def get_oc23_regions():
        # Internal use only
        return Regions.OC23_REGIONS.values()

    @staticmethod
    def get_oc24_regions():
        # Internal use only
        return Regions.OC24_REGIONS.values()

    @staticmethod
    def get_oc25_regions():
        # Internal use only
        return Regions.OC25_REGIONS.values()

    @staticmethod
    def get_oc26_regions():
        # Internal use only
        return Regions.OC26_REGIONS.values()

    @staticmethod
    def get_oc27_regions():
        # Internal use only
        return Regions.OC27_REGIONS.values()

    @staticmethod
    def get_oc28_regions():
        # Internal use only
        return Regions.OC28_REGIONS.values()

    @staticmethod
    def get_oc29_regions():
        # Internal use only
        return Regions.OC29_REGIONS.values()

    @staticmethod
    def get_oc31_regions():
        # Internal use only
        return Regions.OC31_REGIONS.values()

    @staticmethod
    def get_oc35_regions():
        # Internal use only
        return Regions.OC35_REGIONS.values()

    @staticmethod
    def get_oc36_regions():
        # Internal use only
        return Regions.OC36_REGIONS.values()

    @staticmethod
    def get_oc39_regions():
        # Internal use only
        return Regions.OC39_REGIONS.values()

    @staticmethod
    def get_oc40_regions():
        # Internal use only
        return Regions.OC40_REGIONS.values()

    @staticmethod
    def get_oc41_regions():
        # Internal use only
        return Regions.OC41_REGIONS.values()

    @staticmethod
    def get_oc42_regions():
        # Internal use only
        return Regions.OC42_REGIONS.values()

    @staticmethod
    def get_oc43_regions():
        # Internal use only
        return Regions.OC43_REGIONS.values()

    @staticmethod
    def get_oc46_regions():
        # Internal use only
        return Regions.OC46_REGIONS.values()

    @staticmethod
    def get_oc47_regions():
        # Internal use only
        return Regions.OC47_REGIONS.values()

    @staticmethod
    def get_oc51_regions():
        # Internal use only
        return Regions.OC51_REGIONS.values()

    @staticmethod
    def get_oc52_regions():
        # Internal use only
        return Regions.OC52_REGIONS.values()

    @staticmethod
    def get_oc55_regions():
        # Internal use only
        return Regions.OC55_REGIONS.values()

    @staticmethod
    def get_oc57_regions():
        # Internal use only
        return Regions.OC57_REGIONS.values()

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
            region = Regions.OC2_REGIONS.get(region_id)
        if region is None:
            region = Regions.OC3_REGIONS.get(region_id)
        if region is None:
            region = Regions.OC4_REGIONS.get(region_id)
        if region is None:
            region = Regions.OC5_REGIONS.get(region_id)
        if region is None:
            region = Regions.OC8_REGIONS.get(region_id)
        if region is None:
            region = Regions.OC9_REGIONS.get(region_id)
        if region is None:
            region = Regions.OC10_REGIONS.get(region_id)
        if region is None:
            region = Regions.OC14_REGIONS.get(region_id)
        if region is None:
            region = Regions.OC15_REGIONS.get(region_id)
        if region is None:
            region = Regions.OC16_REGIONS.get(region_id)
        if region is None:
            region = Regions.OC17_REGIONS.get(region_id)
        if region is None:
            region = Regions.OC19_REGIONS.get(region_id)
        if region is None:
            region = Regions.OC20_REGIONS.get(region_id)
        if region is None:
            region = Regions.OC21_REGIONS.get(region_id)
        if region is None:
            region = Regions.OC22_REGIONS.get(region_id)
        if region is None:
            region = Regions.OC23_REGIONS.get(region_id)
        if region is None:
            region = Regions.OC24_REGIONS.get(region_id)
        if region is None:
            region = Regions.OC25_REGIONS.get(region_id)
        if region is None:
            region = Regions.OC26_REGIONS.get(region_id)
        if region is None:
            region = Regions.OC27_REGIONS.get(region_id)
        if region is None:
            region = Regions.OC28_REGIONS.get(region_id)
        if region is None:
            region = Regions.OC29_REGIONS.get(region_id)
        if region is None:
            region = Regions.OC31_REGIONS.get(region_id)
        if region is None:
            region = Regions.OC35_REGIONS.get(region_id)
        if region is None:
            region = Regions.OC36_REGIONS.get(region_id)
        if region is None:
            region = Regions.OC39_REGIONS.get(region_id)
        if region is None:
            region = Regions.OC40_REGIONS.get(region_id)
        if region is None:
            region = Regions.OC41_REGIONS.get(region_id)
        if region is None:
            region = Regions.OC42_REGIONS.get(region_id)
        if region is None:
            region = Regions.OC43_REGIONS.get(region_id)
        if region is None:
            region = Regions.OC46_REGIONS.get(region_id)
        if region is None:
            region = Regions.OC47_REGIONS.get(region_id)
        if region is None:
            region = Regions.OC51_REGIONS.get(region_id)
        if region is None:
            region = Regions.OC52_REGIONS.get(region_id)
        if region is None:
            region = Regions.OC55_REGIONS.get(region_id)
        if region is None:
            region = Regions.OC57_REGIONS.get(region_id)
        return region

# === End autogenerated regions ===

class StatsProfile(Enum):
    """
    The following semantics are attached to the StatsProfile values:
       - NONE: no stats are logged.
       - REGULAR: per request: counters, errors, latencies, delays, retries
       - MORE: stats above plus 95th and 99th percentile latencies.
       - ALL: stats above plus per query information
    """
    NONE = 1
    REGULAR = 2
    MORE = 3
    ALL = 4


# noinspection PyPep8
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

    The service endpoint is used to connect to the Oracle NoSQL Database Cloud
    Service or if on-premises, the Oracle NoSQL Database proxy server. It should
    be a string or a :py:class:`Region`.

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
     * https://nosql.us-ashburn-1.oci.oraclecloud.com:443
     * localhost:8080 - used for connecting to a Cloud Simulator instance
       running locally on port 8080
     * https://machine-hosting-proxy:443

    When using the endpoint (vs region id) syntax, if the port is omitted, the
    endpoint uses 8080 if protocol is http, and 443 in all other cases. If the
    protocol is omitted, the endpoint uses https if the port is 443, and http in
    all other cases.

    When using the Oracle NoSQL Database Cloud Service, it is recommended that a
    :py:class:`Region` object is provided rather than a Region's id string.

    If a :py:class:`Region` object is provided to endpoint argument, See
    :py:class:`Regions` for information on available regions. For example:

     * Regions.US_ASHBURN_1

    For cloud service, one or both of endpoint and provider must be set. For
    other scenarios, endpoint is required while provider is optional.

    :param endpoint: identifies a server, region id or :py:class:`Region` for
        use by the NoSQLHandle.
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
    _DEFAULT_CONSISTENCY = Consistency.EVENTUAL
    _STATS_PROFILE_PROPERTY = "NOSQL_STATS_PROFILE"
    _STATS_INTERVAL_PROPERTY = "NOSQL_STATS_INTERVAL"
    _STATS_PRETTY_PRINT_PROPERTY = "NOSQL_STATS_PRETTY_PRINT"
    _DEFAULT_STATS_PROFILE = StatsProfile.NONE
    _DEFAULT_STATS_INTERVAL = 10 * 60
    _DEFAULT_STATS_PRETTY_PRINT = False

    def __init__(self, endpoint=None, provider=None):
        # Init a NoSQLHandleConfig object.
        endpoint_str = endpoint
        if endpoint is not None:
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
                endpoint_str = endpoint.get_region_id()
            if self._region is None:
                ep = endpoint
            else:
                if isinstance(provider, iam.SignatureProvider):
                    region_in_provider = provider.get_region()
                    if (region_in_provider is not None and
                            region_in_provider != self._region):
                        raise IllegalArgumentException(
                            'Specified region, ' + endpoint_str +
                            ', doesn\'t ' +
                            'match the region in SignatureProvider.')
                ep = self._region.endpoint()
        elif provider is not None:
            if not isinstance(provider, iam.SignatureProvider):
                raise IllegalArgumentException(
                    'provider must be an instance of SignatureProvider.')
            self._region = provider.get_region()
            if self._region is None:
                raise IllegalArgumentException(
                    'Unable to find region from given SignatureProvider.')
            else:
                ep = self._region.endpoint()
        else:
            raise IllegalArgumentException(
                'One or both of endpoint and provider must be set.')
        self._service_url = NoSQLHandleConfig.create_url(ep, '/')
        self._auth_provider = provider
        self._compartment = None
        self._timeout = 0
        self._table_request_timeout = 0
        self._consistency = None
        self._pool_connections = 2
        self._pool_maxsize = 10
        self._max_content_length = 0
        self._retry_handler = None
        self._rate_limiting_enabled = False
        self._default_rate_limiter_percentage = 0.0
        self._proxy_host = None
        self._proxy_port = 0
        self._proxy_username = None
        self._proxy_password = None
        self._ssl_ca_certs = None
        self._ssl_ciphers = None
        self._ssl_ctx = None
        self._ssl_protocol = None
        self._logger = None
        self._is_default_logger = True
        self._serial_version = SerdeUtil.DEFAULT_SERIAL_VERSION
        self._default_namespace = None

        profile_property = getenv(self._STATS_PROFILE_PROPERTY,
                                  self._DEFAULT_STATS_PROFILE.name.lower())
        try:
            self._stats_profile = StatsProfile[profile_property.upper()]
        except KeyError:
            self._stats_profile = StatsProfile.NONE

        self._stats_interval = getenv(self._STATS_INTERVAL_PROPERTY,
                                      self._DEFAULT_STATS_INTERVAL)
        self._stats_interval = int(self._stats_interval)

        self._stats_pretty_print = getenv(self._STATS_PRETTY_PRINT_PROPERTY,
                                          self._DEFAULT_STATS_PRETTY_PRINT)
        self._stats_pretty_print = bool(self._stats_pretty_print)
        # noinspection PyTypeChecker
        self._stats_handler = None  # type: Callable

    def get_service_url(self):
        """
        Returns the url to use for the :py:class:`NoSQLHandle` connection.

        :returns: the url.
        :rtype: ParseResult
        """
        return self._service_url

    def get_region(self):
        """
        Returns the region will be accessed by the NoSQLHandle.

        Cloud service only.

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

        Cloud service only.

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
        Returns the default compartment to use for requests or None if not set.
        The value may be a compartment name or id, as set by
        :py:meth:`set_default_compartment`.

        Cloud service only.

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

    def set_max_content_length(self, max_content_length):
        """
        Sets the maximum size in bytes of request payloads.
        This setting is ignored for cloud operations. If not set, or set to
        zero, the default value of 32MB is used.

        On-premises only.

        :param max_content_length: the maximum bytes allowed in requests. Pass
            zero to use the default.
        :type max_content_length: int
        :returns: self.
        :raises IllegalArgumentException: raises the exception if
            max_content_length is a negative number.
        """
        CheckValue.check_int_ge_zero(max_content_length, 'max_content_length')
        self._max_content_length = max_content_length
        return self

    def get_max_content_length(self):
        """
        Returns the maximum size, in bytes, of a request operation payload.
        This value is ignored for cloud operations.

        On-premises only.

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
        configured with the specified number of retries and a static delay. A
        delay of 0 means "use the default delay algorithm" which is an
        incremental backoff algorithm. A non-zero delay will work but is not
        recommended for production systems as it is not flexible.

        The default retry handler will not retry exceptions of type
        :py:class:`OperationThrottlingException`. The reason is that these
        operations are long-running, and while technically they can be retried,
        an immediate retry is unlikely to succeed because of the low rates
        allowed for these operations.

        :param num_retries: the number of retries to perform automatically.
            This parameter may be 0 for no retries.
        :type num_retries: int
        :param delay_s: the delay, in seconds. Pass 0 to use the default delay
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

    def set_rate_limiting_enabled(self, enable):
        """
        Enables internal rate limiting.

        Cloud service only.

        :param enable: If True, enable internal rate limiting, otherwise disable
            internal rate limiting.
        :type enable: bool
        :returns: self.
        :raises IllegalArgumentException: raises the exception if enable is
            not a boolean.
        """
        CheckValue.check_boolean(enable, 'enable')
        self._rate_limiting_enabled = enable
        return self

    def get_rate_limiting_enabled(self):
        """
        Returns whether the rate limiting is enabled.

        Internal use only.

        :returns: True if rate limiting is enabled, otherwise False.
        :rtype: bool
        """
        return self._rate_limiting_enabled

    def set_default_rate_limiting_percentage(self, percent):
        """
        Sets a default percentage of table limits to use. This may be useful for
        cases where a client should only use a portion of full table limits.
        This only applies if rate limiting is enabled using
        :py:meth:`set_rate_limiting_enabled`.

        The default for this value is 100.0 (full table limits).

        Cloud service only.

        :param percent: the percentage of table limits to use. This value must
            be positive.
        :type percent: int or float or Decimal
        :returns: self.
        :raises IllegalArgumentException: raises the exception if percent is
            not a positive digital number.
        """
        CheckValue.check_float_gt_zero(percent, 'percent')
        self._default_rate_limiter_percentage = float(percent)
        return self

    def get_default_rate_limiting_percentage(self):
        """
        Returns the default percentage.

        Internal use only.

        :returns: the default percentage.
        :rtype: float
        """
        if self._default_rate_limiter_percentage == 0.0:
            return 100.0
        return self._default_rate_limiter_percentage

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

    def set_ssl_ca_certs(self, ssl_ca_certs):
        """
        When running against on-premises Oracle NoSQL Database with security
        enabled, certificates should be specified using this method. Otherwise
        environment variable REQUESTS_CA_BUNDLE should be configured. See `the
        installation guide <https://nosql-python-sdk.readthedocs.io/en/stable/
        installation.html>`_ for the configuration of REQUESTS_CA_BUNDLE.

        On-premises only.

        :param ssl_ca_certs: ssl ca certificates.
        :type ssl_ca_certs: str
        :returns: self.
        :raises IllegalArgumentException: raises the exception if ssl_ca_certs
            is not a string.
        """
        CheckValue.check_str(ssl_ca_certs, 'ssl_ca_certs')
        self._ssl_ca_certs = ssl_ca_certs
        return self

    def get_ssl_ca_certs(self):
        """
        Returns the SSL CA certificates.

        :returns: ssl ca certificates.
        :rtype: str
        """
        return self._ssl_ca_certs

    def set_ssl_cipher_suites(self, ssl_ciphers):
        """
        Set SSL cipher suites to enable.

        :param ssl_ciphers: ssl ciphers in a string in the OpenSSL cipher list
            format.
        :type ssl_ciphers: str
        :returns: self.
        :raises IllegalArgumentException: raises the exception if ssl_ciphers is
            not a string.
        """
        CheckValue.check_str(ssl_ciphers, 'ssl_ciphers')
        if self._ssl_ciphers is None:
            self._ssl_ciphers = ssl_ciphers
        else:
            self._ssl_ciphers = ':'.join([self._ssl_ciphers, ssl_ciphers])
        return self

    def get_ssl_cipher_suites(self):
        """
        Returns the SSL cipher suites to enable.

        :returns: ssl ciphers in a string in the OpenSSL cipher list format.
        :rtype: str
        """
        return self._ssl_ciphers

    def set_ssl_context(self, ssl_ctx):
        # Internal use only
        if not isinstance(ssl_ctx, SSLContext):
            raise IllegalArgumentException(
                'set_ssl_context requires an instance of SSLContext as ' +
                'parameter.')
        self._ssl_ctx = ssl_ctx
        return self

    def get_ssl_context(self):
        # Internal use only
        return self._ssl_ctx

    def set_ssl_protocol(self, ssl_protocol):
        """
        Set SSL protocol to enable.

        :param ssl_protocol: ssl protocol version.
        :type ssl_protocol: int
        :returns: self.
        :raises IllegalArgumentException: raises the exception if ssl_protocol
            is a negative integer.
        """
        CheckValue.check_int_ge_zero(ssl_protocol, 'ssl_protocol')
        self._ssl_protocol = ssl_protocol
        return self

    def get_ssl_protocol(self):
        """
        Returns the SSL protocols to enable.

        :returns: ssl protocols.
        :rtype: int
        """
        return self._ssl_protocol

    def set_logger(self, logger):
        """
        Sets the logger used for the driver.

        :param logger: the logger or None, None means disable logging.
        :type logger: Logger
        :returns: self.
        :raises IllegalArgumentException: raises the exception if logger is not
            an instance of Logger.
        """
        CheckValue.check_logger(logger, 'logger')
        self._logger = logger
        self._is_default_logger = False
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

    def is_default_logger(self):
        # Internal use only
        return self._is_default_logger

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
            host = host.lstrip('/')

        if protocol != 'http' and protocol != 'https':
            raise IllegalArgumentException(
                'Invalid endpoint, protocol must be http or https: ' + endpoint)
        return urlparse(protocol + '://' + host + ':' + str(port) + path)

    @staticmethod
    def validate_port(endpoint, portstring):
        # Check that a port is a valid, non negative integer.
        try:
            # Strip out any slashes after the port string.
            portstring = portstring.rstrip('/')
            port = int(portstring)
            CheckValue.check_int_ge_zero(port, 'port')
            return port
        except ValueError:
            raise IllegalArgumentException(
                'Invalid port value for : ' + endpoint)

    def set_stats_handler(self, stats_handler):
        # type: (Callable) -> NoSQLHandleConfig
        """
        Registers a user defined stats handler. The handler is called at the end
        of the interval with a structure containing the logged stat values.

        Note: setting a stats handler will not affect the stats log entries.
        """
        if not isinstance(stats_handler, Callable):
            raise IllegalArgumentException(
                'stats_handler must be of Callable type')
        self._stats_handler = stats_handler
        return self

    def get_stats_handler(self):
        # type: (...) -> Callable
        """
        Returns the registered stats handler.
        """
        return self._stats_handler

    def get_stats_profile(self):
        # type: () -> StatsProfile
        """
        Returns the stats collection stats_profile. Default stats stats_profile
        is NONE.
        """
        return self._stats_profile

    def set_stats_profile(self, stats_profile):
        # type: (StatsProfile) -> NoSQLHandleConfig
        """
        Set the stats collection stats_profile. Default stats stats_profile is
        NONE.
        """
        if stats_profile is not None and not isinstance(stats_profile,
                                                        StatsProfile):
            raise IllegalArgumentException('profile must be a StatsProfile.')
        self._stats_profile = stats_profile
        return self

    def get_stats_interval(self):
        # type: () -> int
        """
        Returns the current collection interval.
        Default interval is 600 seconds, i.e. 10 min.
        """
        return self._stats_interval

    def set_stats_interval(self, interval):
        # type: (int) -> NoSQLHandleConfig
        """
        Sets interval size in seconds.
        Default interval is 600 seconds, i.e. 10 min.
        """
        CheckValue.check_int_gt_zero(interval, "interval")
        self._stats_interval = interval
        return self

    def get_stats_pretty_print(self):
        """
        Returns the current JSON pretty print flag.
        Default is disabled.
        """
        return self._stats_pretty_print

    def set_stats_pretty_print(self, pretty_print):
        # type: (bool) -> NoSQLHandleConfig
        """
        Enable JSON pretty print for easier human reading.
        Default is disabled.
        """
        CheckValue.check_boolean(pretty_print, "pretty_print")
        self._stats_pretty_print = pretty_print
        return self

    def get_serial_version(self):
        return self._serial_version

    def set_serial_version(self, version):
        self._serial_version = version

    def set_default_namespace(self, namespace):
        """
        Sets the default namespace to use for requests that use a table
        name

        On-premises only.

        :param namespace: the default namespace.
        :type namespace: str
        :returns: self
        :versionadded:: 5.4.0
        """
        self._default_namespace = namespace
        return self

    def get_default_namespace(self):
        """
        Returns the default namespace or None if not set.

        On-premises only.

        :returns: the default namespace or None.
        :rtype: str
        :versionadded:: 5.4.0
        """
        return self._default_namespace
