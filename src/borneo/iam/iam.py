#
# Copyright (C) 2018, 2020 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
#
# Please see LICENSE.txt file included in the top-level directory of the
# appropriate download for a copy of the license and additional information.
#

from base64 import b64encode
from cryptography.hazmat.primitives.asymmetric.padding import PKCS1v15
from cryptography.hazmat.primitives.hashes import SHA256
from datetime import datetime
from locale import LC_ALL, setlocale
from re import match
from requests import Session
from threading import Timer
try:
    import oci
except ImportError:
    oci = None

from borneo.auth import AuthorizationProvider
from borneo.common import CheckValue, HttpConstants, LogUtils, Memoize
from borneo.exception import IllegalArgumentException
from borneo.http import RequestUtils
from borneo.operations import Request


class SignatureProvider(AuthorizationProvider):
    """
    Cloud service only.

    An instance of :py:class:`borneo.AuthorizationProvider` that generates and
    caches signature for each request as authorization string.

    :param provider: the oci config or InstancePrincipalsSecurityTokenSigner.
    :type provider: dict or InstancePrincipalsSecurityTokenSigner
    :param profile_name: user profile name.
    :type profile_name: str
    :param config_file: path of configuration file.
    :type config_file: str
    :param duration_seconds: the cache duration in seconds.
    :type duration_seconds: int
    :param refresh_ahead: the refresh time before AT expiry in seconds.
    :type refresh_ahead: int
    :raises IllegalArgumentException: raises the exception if the parameters
        are not valid.
    """

    SIGNING_HEADERS = '(request-target) host date'
    CACHE_KEY = 'signature'
    """Cache key name."""
    MAX_ENTRY_LIFE_TIME = 300
    """Maximum lifetime of signature 300 seconds."""
    DEFAULT_REFRESH_AHEAD = 10
    """Default refresh time before signature expiry, 10 seconds."""
    SIGNATURE_HEADER_FORMAT = (
        'Signature headers="{0}",keyId="{1}",algorithm="{2}",signature="{3}",' +
        'version="{4}"')
    SIGNATURE_VERSION = 1
    OCID_PATTERN = (
        '^([0-9a-zA-Z-_]+[.:])([0-9a-zA-Z-_]*[.:]){3,}([0-9a-zA-Z-_]+)$')

    def __init__(self, provider=None, config_file=None, profile_name=None,
                 duration_seconds=MAX_ENTRY_LIFE_TIME,
                 refresh_ahead=DEFAULT_REFRESH_AHEAD):
        """
        The SignatureProvider that generates and caches request signature.
        """
        CheckValue.check_int_gt_zero(duration_seconds, 'duration_seconds')
        CheckValue.check_int_gt_zero(refresh_ahead, 'refresh_ahead')
        if duration_seconds > SignatureProvider.MAX_ENTRY_LIFE_TIME:
            raise IllegalArgumentException(
                'Access token cannot be cached longer than ' +
                str(SignatureProvider.MAX_ENTRY_LIFE_TIME) + ' seconds.')
        try:
            if provider is not None:
                if config_file is not None or profile_name is not None:
                    raise IllegalArgumentException(
                        'config_file and profile_name are not allowed to be ' +
                        'set if provider is set.')
                if not isinstance(
                    provider,
                    (dict,
                     oci.auth.signers.InstancePrincipalsSecurityTokenSigner)):
                    raise IllegalArgumentException(
                        'provider should be a dict or an instance of ' +
                        'InstancePrincipalsSecurityTokenSigner.')
                self._provider = provider
            else:
                CheckValue.check_str(config_file, 'config_file', True)
                CheckValue.check_str(profile_name, 'profile_name', True)
                if config_file is None and profile_name is None:
                    # Use default user profile and private key from default path
                    # of configuration file ~/.oci/config.
                    self._provider = oci.config.from_file()
                elif config_file is None and profile_name is not None:
                    # Use user profile with given profile name and private key
                    # from default path of configuration file ~/.oci/config.
                    self._provider = oci.config.from_file(
                        profile_name=profile_name)
                elif config_file is not None and profile_name is None:
                    # Use user profile with default profile name and private key
                    # from specified configuration file.
                    self._provider = oci.config.from_file(
                        file_location=config_file)
                elif profile_name is not None and config_file is not None:
                    # Use user profile with given profile name and private key
                    # from specified configuration file.
                    self._provider = oci.config.from_file(
                        file_location=config_file, profile_name=profile_name)
        except AttributeError:
            raise ImportError('No module named oci')

        if isinstance(self._provider, dict):
            signer = oci.signer.Signer(
                tenancy=self._provider['tenancy'], user=self._provider['user'],
                fingerprint=self._provider['fingerprint'],
                private_key_file_location=self._provider['key_file'])
            self._private_key = signer.private_key
        self._signature_cache = Memoize(duration_seconds)
        self._refresh_interval_s = (duration_seconds - refresh_ahead if
                                    duration_seconds > refresh_ahead else 0)

        # Refresh timer.
        self._timer = None
        self._service_host = None
        self._logger = None
        self._logutils = LogUtils()
        self._sess = Session()
        self._request_utils = RequestUtils(self._sess, self._logutils)

    def close(self):
        """
        Closes the signature provider.
        """
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

    def get_authorization_string(self, request=None):
        if not isinstance(request, Request):
            raise IllegalArgumentException(
                'get_authorization_string requires an instance of Request as ' +
                'parameter.')
        if self._service_host is None:
            raise IllegalArgumentException(
                'Unable to find service host, use set_service_host to load ' +
                'from NoSQLHandleConfig')
        sig_details = self._get_signature_details()
        if sig_details is not None:
            return sig_details.get_signature_header()

    def get_logger(self):
        return self._logger

    def set_logger(self, logger):
        CheckValue.check_logger(logger, 'logger')
        self._logger = logger
        self._logutils = LogUtils(logger)
        self._request_utils = RequestUtils(self._sess, self._logutils)
        return self

    def set_required_headers(self, request, auth_string, headers):
        sig_details = self._get_signature_details()
        if sig_details is None:
            return
        headers[HttpConstants.AUTHORIZATION] = (
            sig_details.get_signature_header())
        headers[HttpConstants.DATE] = sig_details.get_date()
        compartment_id = request.get_compartment_id()
        if compartment_id is None:
            # If request doesn't has compartment id, set the tenant id as the
            # default compartment, which is the root compartment in IAM if using
            # user principal.
            compartment_id = self._get_tenant_ocid()
        if compartment_id is not None:
            headers[HttpConstants.REQUEST_COMPARTMENT_ID] = compartment_id

    def set_service_host(self, config):
        service_url = config.get_service_url()
        if service_url is None:
            raise IllegalArgumentException('Must set service URL first.')
        self._service_host = service_url.hostname
        return self

    @staticmethod
    def create_with_instance_principal():
        """
        Create the SignatureProvider that generates and caches request signature
        using instance principal.

        :returns: a SignatureProvider.
        :rtype: SignatureProvider
        """
        return SignatureProvider(
            oci.auth.signers.InstancePrincipalsSecurityTokenSigner())

    def _is_valid_ocid(self, ocid):
        return match(SignatureProvider.OCID_PATTERN, ocid)

    def _get_key_id(self):
        tenant_id = self._provider['tenancy']
        user_id = self._provider['user']
        fingerprint = self._provider['fingerprint']
        CheckValue.check_str(tenant_id, 'tenant_id')
        CheckValue.check_str(user_id, 'user_id')
        CheckValue.check_str(fingerprint, 'fingerprint')
        if not self._is_valid_ocid(tenant_id):
            raise IllegalArgumentException(
                'Tenant Id ' + tenant_id + ' does not match OCID pattern')
        if not self._is_valid_ocid(user_id):
            raise IllegalArgumentException(
                'User Id ' + user_id + ' does not match OCID pattern')
        return str.format('{0}/{1}/{2}', tenant_id, user_id, fingerprint)

    def _get_signature_details(self):
        sig_details = self._signature_cache.get(SignatureProvider.CACHE_KEY)
        if sig_details is not None:
            return sig_details
        sig_details = self._get_signature_details_internal()
        self._signature_cache.set(SignatureProvider.CACHE_KEY, sig_details)
        self._schedule_refresh()
        return sig_details

    def _get_signature_details_internal(self):
        setlocale(LC_ALL, 'en_US')
        date_str = datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
        if isinstance(self._provider, dict):
            key_id = self._get_key_id()
        else:
            self._provider.refresh_security_token()
            self._private_key = self._provider.private_key
            key_id = self._provider.api_key
        try:
            signature = self._private_key.sign(
                self._signing_content(date_str), PKCS1v15(), SHA256())
            signature = b64encode(signature)
        except TypeError:
            signature = self._private_key.sign(
                self._signing_content(date_str).encode(), PKCS1v15(), SHA256())
            signature = b64encode(signature).decode()
        sig_header = str.format(
            SignatureProvider.SIGNATURE_HEADER_FORMAT,
            SignatureProvider.SIGNING_HEADERS, key_id, 'rsa-sha256', signature,
            SignatureProvider.SIGNATURE_VERSION)
        return SignatureProvider.SignatureDetails(sig_header, date_str)

    def _get_tenant_ocid(self):
        """
        Get tenant OCID if using user principal.

        :returns: tenant OCID of user.
        :rtype: str
        """
        if isinstance(self._provider, dict):
            return self._provider['tenancy']

    def _refresh_task(self):
        try:
            sig_details = self._get_signature_details_internal()
            if sig_details is not None:
                self._signature_cache.set(SignatureProvider.CACHE_KEY,
                                          sig_details)
                self._schedule_refresh()
        except Exception as e:
            # Ignore the failure of refresh. The driver would try to generate
            # signature in the next request if signature is not available, the
            # failure would be reported at that moment.
            self._logutils.log_warning(
                'Unable to refresh cached request signature, ' + str(e))
            self._timer.cancel()
            self._timer = None

    def _schedule_refresh(self):
        # If refresh interval is 0, don't schedule a refresh.
        if self._refresh_interval_s == 0:
            return
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        self._timer = Timer(self._refresh_interval_s, self._refresh_task)
        self._timer.start()

    def _signing_content(self, date_str):
        return ('(request-target): post /' + HttpConstants.NOSQL_DATA_PATH +
                '\nhost: ' + self._service_host + '\ndate: ' + date_str)

    class SignatureDetails(object):
        def __init__(self, signature_header, date_str):
            # Signing date, keep it and pass along with each request, so
            # requests can reuse the signature within the 5-mins time window.
            self._date = date_str
            # Signature header string.
            self._signature_header = signature_header

        def get_date(self):
            return self._date

        def get_signature_header(self):
            return self._signature_header

        def is_valid(self, header):
            if header is None:
                return False
            return header == self._signature_header
