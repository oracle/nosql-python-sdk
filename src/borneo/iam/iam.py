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
from os import path
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


class SignatureProvider(AuthorizationProvider):
    """
    Cloud service only.

    An instance of :py:class:`borneo.AuthorizationProvider` that generates and
    caches signature for each request as authorization string. A number of
    pieces of information are required for configuration. See `Required Keys and
    OCIDs <https://docs.cloud.oracle.com/iaas/Content/API/Concepts/
    apisigningkey.htm>`_ for information and instructions on how to create the
    required keys and OCIDs for configuration. The required information
    includes:

        * A signing key, used to sign requests.
        * A pass phrase for the key, if it is encrypted.
        * The fingerprint of the key pair used for signing.
        * The OCID of the tenancy.
        * The OCID of a user in the tenancy.

    All of this information is required to authenticate and authorize access to
    the service. See :ref:`creds-label` for information on how to acquire this
    information.

    There are two different ways to authorize an application:

    1. Using a specific user's identity.
    2. Using an Instance Principal, which can be done when running on a compute
       instance in the Oracle Cloud Infrastructure (OCI). See
       :py:meth:`create_with_instance_principal` and `Calling Services from
       Instances <https://docs.cloud.oracle.com/iaas/Content/Identity/Tasks/
       callingservicesfrominstances.htm>`_.

    The latter can be simpler to use when running on an OCI compute instance,
    but limits the ability to use a compartment name vs OCID when naming
    compartments and tables in :py:class:`Request` classes and when naming
    tables in queries. A specific user identity is best for naming flexibility,
    allowing both compartment names and OCIDs.

    When using a specific user's identity there are 3 options for providing the
    required information:

    1. Using a instance of oci.signer.Signer or
       oci.auth.signers.InstancePrincipalsSecurityTokenSigner
    2. Directly providing the credentials via parameters
    3. Using a configuration file

    Only one method of providing credentials can be used, and if they are mixed
    the priority from high to low is:

    * Signer or InstancePrincipalsSecurityTokenSigner(provider is used)
    * Credentials as arguments (tenant_id, etc used)
    * Configuration file (config_file is used)

    :param provider: an instance of oci.signer.Signer or
        oci.auth.signers.InstancePrincipalsSecurityTokenSigner.
    :type provider: Signer or InstancePrincipalsSecurityTokenSigner
    :param config_file: path of configuration file.
    :type config_file: str
    :param profile_name: user profile name. Only valid with config_file.
    :type profile_name: str
    :param tenant_id: id of the tenancy
    :type tenant_id: str
    :param user_id: id of a specific user
    :type user_id: str
    :param private_key: path to private key or private key content
    :type private_key: str
    :param fingerprint: fingerprint for the private key
    :type fingerprint: str
    :param pass_phrase: pass_phrase for the private key if created
    :type pass_phrase: str
    :param duration_seconds: the signature cache duration in seconds.
    :type duration_seconds: int
    :param refresh_ahead: the refresh time before signature cache expiry
       in seconds.
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

    def __init__(self, provider=None, config_file=None, profile_name=None,
                 tenant_id=None, user_id=None, fingerprint=None,
                 private_key=None, pass_phrase=None,
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

        #
        # This class depends on the oci package
        #
        if oci is None:
            raise ImportError('Package \'oci\' is required; please install')

        try:
            if provider is not None:
                if not isinstance(
                    provider,
                    (oci.signer.Signer,
                     oci.auth.signers.InstancePrincipalsSecurityTokenSigner)):
                    raise IllegalArgumentException(
                        'provider should be an instance of oci.signer.Signer' +
                        'or oci.auth.signers.' +
                        'InstancePrincipalsSecurityTokenSigner.')
                self._provider = provider
            elif (tenant_id is None or user_id is None or fingerprint is None or
                  private_key is None):
                CheckValue.check_str(config_file, 'config_file', True)
                CheckValue.check_str(profile_name, 'profile_name', True)
                if config_file is None and profile_name is None:
                    # Use default user profile and private key from default path
                    # of configuration file ~/.oci/config.
                    config = oci.config.from_file()
                elif config_file is None and profile_name is not None:
                    # Use user profile with given profile name and private key
                    # from default path of configuration file ~/.oci/config.
                    config = oci.config.from_file(profile_name=profile_name)
                elif config_file is not None and profile_name is None:
                    # Use user profile with default profile name and private key
                    # from specified configuration file.
                    config = oci.config.from_file(file_location=config_file)
                else:  # config_file is not None and profile_name is not None
                    # Use user profile with given profile name and private key
                    # from specified configuration file.
                    config = oci.config.from_file(
                        file_location=config_file, profile_name=profile_name)
                self._provider = oci.signer.Signer(
                    config['tenancy'], config['user'], config['fingerprint'],
                    config['key_file'], config.get('pass_phrase'),
                    config.get('key_content'))
            else:
                CheckValue.check_str(tenant_id, 'tenant_id')
                CheckValue.check_str(user_id, 'user_id')
                CheckValue.check_str(fingerprint, 'fingerprint')
                CheckValue.check_str(private_key, 'private_key')
                CheckValue.check_str(pass_phrase, 'pass_phrase', True)
                if path.isfile(private_key):
                    key_file = private_key
                    key_content = None
                else:
                    key_file = None
                    key_content = private_key
                self._provider = oci.signer.Signer(
                    tenant_id, user_id, fingerprint, key_file, pass_phrase,
                    key_content)
        except AttributeError:
            raise ImportError('Package \'oci\' is required; please install')
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
        compartment = request.get_compartment()
        if compartment is None:
            # If request doesn't has compartment, set the tenant id as the
            # default compartment, which is the root compartment in IAM if using
            # user principal. If using an instance principal this value is
            # None.
            compartment = self._get_tenant_ocid()
        if compartment is not None:
            headers[HttpConstants.REQUEST_COMPARTMENT_ID] = compartment
        else:
            raise IllegalArgumentException(
                'Compartment is None. When authenticating using an Instance ' +
                'Principal the compartment for the operation must be specified.'
            )

    def set_service_host(self, config):
        service_url = config.get_service_url()
        if service_url is None:
            raise IllegalArgumentException('Must set service URL first.')
        self._service_host = service_url.hostname
        return self

    @staticmethod
    def create_with_instance_principal(iam_auth_uri=None):
        """
        Creates a SignatureProvider using an instance principal. This method may
        be used when calling the Oracle NoSQL Database Cloud Service from an
        Oracle Cloud compute instance. It authenticates with the instance
        principal and uses a security token issued by IAM to do the actual
        request signing.

        When using an instance principal the compartment (OCID) must be
        specified on each request or defaulted by using
        :py:meth:`borneo.NoSQLHandleConfig.set_default_compartment`. If the
        compartment is not specified for an operation an exception will be
        thrown.

        See `Calling Services from Instances <https://docs.cloud.oracle.com/
        iaas/Content/Identity/Tasks/callingservicesfrominstances.htm>`_

        :param iam_auth_uri: the URI is usually detected automatically, specify
            the URI if you need to overwrite the default, or encounter the
            *Invalid IAM URI* error, it is optional.
        :type iam_auth_uri: str
        :returns: a SignatureProvider.
        :rtype: SignatureProvider
        """
        if iam_auth_uri is None:
            return SignatureProvider(
                oci.auth.signers.InstancePrincipalsSecurityTokenSigner())
        else:
            return SignatureProvider(
                oci.auth.signers.InstancePrincipalsSecurityTokenSigner(
                    federation_endpoint=iam_auth_uri))

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
        if isinstance(self._provider,
                      oci.auth.signers.InstancePrincipalsSecurityTokenSigner):
            self._provider.refresh_security_token()
        private_key = self._provider.private_key
        key_id = self._provider.api_key

        try:
            signature = private_key.sign(
                self._signing_content(date_str), PKCS1v15(), SHA256())
            signature = b64encode(signature)
        except TypeError:
            signature = private_key.sign(
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
        if isinstance(self._provider, oci.signer.Signer):
            return self._provider.api_key.split('/')[0]

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
        return (HttpConstants.REQUEST_TARGET + ': post /' +
                HttpConstants.NOSQL_DATA_PATH + '\nhost: ' +
                self._service_host + '\ndate: ' + date_str)

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
