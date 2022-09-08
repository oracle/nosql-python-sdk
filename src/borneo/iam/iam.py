#
# Copyright (c) 2018, 2022 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#

OCI_PYTHON_SDK_NO_SERVICE_IMPORTS=True

from os import path
from requests import Request
from threading import Lock, Timer
from time import sleep, time
try:
    from oci.signer import Signer
    from oci.auth.signers import SecurityTokenSigner
    from oci.auth.signers import EphemeralResourcePrincipalSigner
    from oci.auth.signers import InstancePrincipalsSecurityTokenSigner
    from oci.auth.signers import get_resource_principals_signer
    from oci.config import from_file
    oci = 'yes'
except ImportError:
    oci = None

from borneo.auth import AuthorizationProvider
from borneo.common import (
    CheckValue, HttpConstants, LogUtils, Memoize, synchronized)
from borneo.config import Region, Regions
from borneo.exception import IllegalArgumentException


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

    There are three different ways to authorize an application:

    1. Using a specific user's identity.
    2. Using an Instance Principal, which can be done when running on a compute
       instance in the Oracle Cloud Infrastructure (OCI). See
       :py:meth:`create_with_instance_principal` and `Calling Services from
       Instances <https://docs.cloud.oracle.com/iaas/Content/Identity/Tasks/
       callingservicesfrominstances.htm>`_.
    3. Using a Resource Principal, which can be done when running within a
       Function within the Oracle Cloud Infrastructure (OCI). See
       :py:meth:`create_with_resource_principal` and `Accessing Other Oracle
       Cloud Infrastructure Resources from Running Functions <https://docs.
       cloud.oracle.com/en-us/iaas/Content/Functions/Tasks/
       functionsaccessingociresources.htm>`_.

    The latter 2 limit the ability to use a compartment name vs OCID when naming
    compartments and tables in :py:class:`Request` classes and when naming
    tables in queries. A specific user identity is best for naming flexibility,
    allowing both compartment names and OCIDs.

    When using a specific user's identity there are 3 options for providing the
    required information:

    1. Using a instance of oci.signer.Signer or
       oci.auth.signers.SecurityTokenSigner
    2. Directly providing the credentials via parameters
    3. Using a configuration file

    Only one method of providing credentials can be used, and if they are mixed
    the priority from high to low is:

    * Signer or SecurityTokenSigner(provider is used)
    * Credentials as arguments (tenant_id, etc used)
    * Configuration file (config_file is used)

    :param provider: an instance of oci.signer.Signer or
        oci.auth.signers.SecurityTokenSigner.
    :type provider: Signer or SecurityTokenSigner
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
    :param region: identifies the region will be accessed by the NoSQLHandle
    :type region: Region
    :param duration_seconds: the signature cache duration in seconds.
    :type duration_seconds: int
    :param refresh_ahead: the refresh time before signature cache expiry
       in seconds.
    :type refresh_ahead: int
    :raises IllegalArgumentException: raises the exception if the parameters
        are not valid.
    """

    CACHE_KEY = 'signature'
    """Cache key name."""
    # Use 240 so that it expires well before the 300s token lifetime
    MAX_ENTRY_LIFE_TIME = 240
    """Maximum lifetime of signature 240 seconds."""
    DEFAULT_REFRESH_AHEAD = 10
    """Default refresh time before signature expiry, 10 seconds."""

    def __init__(self, provider=None, config_file=None, profile_name=None,
                 tenant_id=None, user_id=None, fingerprint=None,
                 private_key=None, pass_phrase=None, region=None,
                 duration_seconds=MAX_ENTRY_LIFE_TIME,
                 refresh_ahead=DEFAULT_REFRESH_AHEAD):
        """
        The SignatureProvider that generates and caches request signature.
        """
        #
        # This class depends on the oci package
        #
        SignatureProvider._check_oci()
        CheckValue.check_int_gt_zero(duration_seconds, 'duration_seconds')
        CheckValue.check_int_gt_zero(refresh_ahead, 'refresh_ahead')
        if duration_seconds > SignatureProvider.MAX_ENTRY_LIFE_TIME:
            raise IllegalArgumentException(
                'Access token cannot be cached longer than ' +
                str(SignatureProvider.MAX_ENTRY_LIFE_TIME) + ' seconds.')

        self._region = None
        if provider is not None:
            if not isinstance(
                provider,
                (Signer, SecurityTokenSigner)):
                raise IllegalArgumentException(
                    'provider should be an instance of oci.signer.Signer or ' +
                    'oci.auth.signers.SecurityTokenSigner.')
            self._provider = provider
            if region is not None:
                region_id = region
            else:
                try:
                    region_id = provider.region
                except AttributeError:
                    region_id = None
            if region_id is not None:
                self._region = Regions.from_region_id(region_id)
        elif (tenant_id is None or user_id is None or fingerprint is None or
              private_key is None):
            CheckValue.check_str(config_file, 'config_file', True)
            CheckValue.check_str(profile_name, 'profile_name', True)
            if config_file is None and profile_name is None:
                # Use default user profile and private key from default path of
                # configuration file ~/.oci/config.
                config = from_file()
            elif config_file is None and profile_name is not None:
                # Use user profile with given profile name and private key from
                # default path of configuration file ~/.oci/config.
                config = from_file(profile_name=profile_name)
            elif config_file is not None and profile_name is None:
                # Use user profile with default profile name and private key
                # from specified configuration file.
                config = from_file(file_location=config_file)
            else:  # config_file is not None and profile_name is not None
                # Use user profile with given profile name and private key from
                # specified configuration file.
                config = from_file(
                    file_location=config_file, profile_name=profile_name)
            self._provider = Signer(
                config['tenancy'], config['user'], config['fingerprint'],
                config['key_file'], config.get('pass_phrase'),
                config.get('key_content'))
            region_id = config.get('region')
            if region_id is not None:
                self._provider.region = region_id
                self._region = Regions.from_region_id(region_id)
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
            self._provider = Signer(
                tenant_id, user_id, fingerprint, key_file, pass_phrase,
                key_content)
            if region is not None:
                if not isinstance(region, Region):
                    raise IllegalArgumentException(
                        'region must be an instance of an instance of Region.')
                self._provider.region = region.get_region_id()
                self._region = region

        self._signature_cache = Memoize(duration_seconds)
        self._refresh_ahead = refresh_ahead
        self._refresh_interval_s = (duration_seconds - refresh_ahead if
                                    duration_seconds > refresh_ahead else 0)

        # Refresh timer.
        self._timer = None
        self._service_url = None
        self._logger = None
        self._logutils = LogUtils()
        self.lock = Lock()

    def close(self):
        """
        Closes the signature provider.
        """
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

    def get_authorization_string(self, request=None):
        if self._service_url is None:
            raise IllegalArgumentException(
                'Unable to find service url, use set_service_url to load ' +
                'from NoSQLHandleConfig')
        sig_details = self._get_signature_details()
        if sig_details is not None:
            return sig_details['authorization']

    def get_logger(self):
        return self._logger

    def get_region(self):
        # Internal use only.
        return self._region

    def get_resource_principal_claim(self, key):
        """
        Resource principal session tokens carry JWT claims. Permit the retrieval
        of the value from the token by given key.
        See :py:class:`borneo.ResourcePrincipalClaimKeys`.

        :param key: the name of a claim in the session token.
        :type key: str
        :returns: the claim value.
        :rtype: str
        """
        if not isinstance(self._provider,
                          EphemeralResourcePrincipalSigner):
            raise IllegalArgumentException(
                'Only ephemeral resource principal support.')
        return self._provider.get_claim(key)

    def set_logger(self, logger):
        CheckValue.check_logger(logger, 'logger')
        self._logger = logger
        self._logutils = LogUtils(logger)
        return self

    def set_required_headers(self, request, auth_string, headers):
        sig_details = self._get_signature_details()
        if sig_details is None:
            return
        headers[HttpConstants.AUTHORIZATION] = sig_details['authorization']
        headers[HttpConstants.DATE] = sig_details['date']
        if sig_details.get(HttpConstants.OPC_OBO_TOKEN) is not None:
            headers[HttpConstants.OPC_OBO_TOKEN] = sig_details['opc-obo-token']
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

    def set_service_url(self, config):
        service_url = config.get_service_url()
        if service_url is None:
            raise IllegalArgumentException('Must set service URL first.')
        self._service_url = (service_url.scheme + '://' + service_url.hostname +
                             '/' + HttpConstants.NOSQL_DATA_PATH)
        return self

    @staticmethod
    def create_with_instance_principal(iam_auth_uri=None, region=None,
                                       logger=None):
        """
        Creates a SignatureProvider using an instance principal. This method may
        be used when calling the Oracle NoSQL Database Cloud Service from an
        Oracle Cloud compute instance. It authenticates with the instance
        principal and uses a security token issued by IAM to do the actual
        request signing.

        When using an instance principal the compartment id (OCID) must be
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
        :param region: identifies the region will be accessed by the
            NoSQLHandle, it is optional.
        :type region: Region
        :param logger: the logger used by the SignatureProvider, it is optional.
        :type logger: Logger
        :returns: a SignatureProvider.
        :rtype: SignatureProvider
        """
        SignatureProvider._check_oci()
        if iam_auth_uri is None:
            provider = InstancePrincipalsSecurityTokenSigner()
        else:
            provider = InstancePrincipalsSecurityTokenSigner(
                federation_endpoint=iam_auth_uri)
        if region is not None:
            provider.region = region.get_region_id()
        signature_provider = SignatureProvider(provider)
        return (signature_provider if logger is None else
                signature_provider.set_logger(logger))

    @staticmethod
    def create_with_resource_principal(logger=None):
        """
        Creates a SignatureProvider using a resource principal. This method may
        be used when calling the Oracle NoSQL Database Cloud Service from other
        Oracle Cloud service resource such as Functions. It uses a resource
        provider session token (RPST) that enables the resource such as function
        to authenticate itself.

        When using an resource principal the compartment id (OCID) must be
        specified on each request or defaulted by using
        :py:meth:`borneo.NoSQLHandleConfig.set_default_compartment`. If the
        compartment id is not specified for an operation an exception will be
        thrown.

        See `Accessing Other Oracle Cloud Infrastructure Resources from Running
        Functions <https://docs.cloud.oracle.com/en-us/iaas/Content/Functions/
        Tasks/functionsaccessingociresources.htm>`_.

        :param logger: the logger used by the SignatureProvider, it is optional.
        :type logger: Logger
        :returns: a SignatureProvider.
        :rtype: SignatureProvider
        """
        SignatureProvider._check_oci()
        signature_provider = SignatureProvider(
            get_resource_principals_signer())
        return (signature_provider if logger is None else
                signature_provider.set_logger(logger))

    @synchronized
    def get_signature_details_internal(self):
        # Visible for testing.
        request = Request(method='post', url=self._service_url)
        request = self._provider.without_content_headers(request.prepare())
        sig_details = request.headers
        self._signature_cache.set(SignatureProvider.CACHE_KEY, sig_details)
        self._schedule_refresh()
        return sig_details

    @staticmethod
    def _check_oci():
        if oci is None:
            raise ImportError('Package "oci" is required; please install.')

    def _get_signature_details(self):
        sig_details = self._signature_cache.get(SignatureProvider.CACHE_KEY)
        if sig_details is not None:
            return sig_details
        return self.get_signature_details_internal()

    def _get_tenant_ocid(self):
        """
        Get tenant OCID if using user principal.

        :returns: tenant OCID of user.
        :rtype: str
        """
        if isinstance(self._provider, Signer):
            return self._provider.api_key.split('/')[0]

    def _refresh_task(self):
        timeout =  self._refresh_ahead
        start_ms = int(round(time() * 1000))
        error_logged = False
        while True:
            try:
                # refresh security token before create new signature
                if (isinstance(
                    self._provider,
                    InstancePrincipalsSecurityTokenSigner) or
                    isinstance(
                    self._provider,
                    EphemeralResourcePrincipalSigner)):
                    self._provider.refresh_security_token()

                self.get_signature_details_internal()
                return
            except Exception as e:
                # Ignore the refresh failure, then sleep and try again until
                # the timeout. Log the failure the first time only. If the
                # refresh failure continues until the task times out the
                # driver will attempt to generate a signature in the next
                # request. If that operation fails, it will be reported to
                # the user as an exception
                if not error_logged:
                    self._logutils.log_error(
                        'Unable to refresh cached request signature, ' + str(e))
                    error_logged = True

            # check for timeout in the loop
            if (int(round(time() * 1000)) - start_ms >= timeout):
                self._logutils.log_error(
                    'Request signature refresh timed out after ' + str(timeout))
                break
            sleep(0.1)

        # if we get here the refresh failed and timed out. Cancel the timer.
        # It will get re-created when the next in-line call to get signature
        # details is called
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
