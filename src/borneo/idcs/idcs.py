#
# Copyright (C) 2018, 2019 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
#
# Please see LICENSE.txt file included in the top-level directory of the
# appropriate download for a copy of the license and additional information.
#

from abc import ABCMeta, abstractmethod
from base64 import b64encode
from json import loads
from os import environ, path, sep
from requests import Session, codes
from threading import Lock, Timer
try:
    from urlparse import urlparse
    from urllib import quote
except ImportError:
    from urllib.parse import urlparse, quote

from borneo.auth import AuthorizationProvider
from borneo.common import CheckValue, LogUtils, Memoize
from borneo.exception import (
    IllegalArgumentException, IllegalStateException,
    InvalidAuthorizationException, RequestTimeoutException,
    UnauthorizedException)
from borneo.http import RequestUtils
from borneo.operations import ListTablesRequest, TableRequest


class Utils:
    # PSM scope
    PSM_SCOPE = 'urn:opc:resource:consumer::all'
    # IDCS Apps endpoint used to find App info
    APP_ENDPOINT = '/admin/v1/Apps'
    # IDCS AppRoles endpoint used to find role
    ROLE_ENDPOINT = '/admin/v1/AppRoles'
    # IDCS Grants endpoint used to grant role
    GRANT_ENDPOINT = '/admin/v1/Grants'
    # IDCS AppStatusChanger endpoint used to deactivate client
    STATUS_ENDPOINT = '/admin/v1/AppStatusChanger'
    # IDCS OAuth2 access token endpoint
    TOKEN_ENDPOINT = '/oauth2/v1/token'
    # The default value for request timeouts in milliseconds
    DEFAULT_TIMEOUT_MS = 12000

    # Default cache control
    _NO_STORE = 'no-store'
    # SCIM content type
    _SCIM_CONTENT = 'application/scim+json'
    # Content type of acquiring access token request
    _TOKEN_REQUEST_CONTENT_TYPE = (
        'application/x-www-form-urlencoded; charset=UTF-8')
    _KEEP_ALIVE = 'keep-alive'

    @staticmethod
    def get_field(content, field, sub_field=None, allow_none=True):
        content_str = content
        content = loads(content)
        value = content.get(field)
        values = None
        if value is None:
            value = Utils.__get_field_recursively(content, field)
            if not allow_none and value is None:
                raise IllegalStateException(
                    field + ' doesn\'t exist in ' + content_str)
        if isinstance(value, list) and sub_field is not None:
            values = list()
            for item in value:
                if isinstance(item, dict):
                    values.append(item.get(sub_field))
        return value if sub_field is None else values

    @staticmethod
    def handle_idcs_errors(response, action, unauthorized_msg):
        """
        Possible error codes returned from IDCS for SCIM endpoints.

        Unexpected case:\n
        307, 308 - redirect-related errors\n
        400 - bad request, indicates code error\n
        403 - request operation is not allowed\n
        404 - this PSMApp doesn't exist\n
        405 - method not allowed\n
        409 - version mismatch\n
        412 - precondition failed for update op\n
        413 - request too long\n
        415 - not acceptable\n
        501 - this method not implemented

        Security failure case:\n
        401 - no permission

        Service unavailable:\n
        500 - internal server error\n
        503 - service unavailable\n
        """
        response_content = response.get_content()
        response_code = response.get_status_code()
        if response_code == codes.unauthorized:
            raise UnauthorizedException(
                action + ' is unauthorized. ' + unauthorized_msg +
                '. Error response: ' + response_content)
        elif (response_code == codes.server_error or
              response_code == codes.unavailable):
            raise RequestTimeoutException(
                action + ' error, expect to retry, error response: ' +
                response_content + ', status code: ' + str(response_code))
        else:
            raise IllegalStateException(
                action + ' error, IDCS error response: ' + response_content)

    @staticmethod
    def scim_headers(host, auth):
        # Default HTTP headers with SCIM content type
        return Utils.__headers(host, auth, Utils._SCIM_CONTENT)

    @staticmethod
    def token_headers(host, auth):
        # Default HTTP headers with URL-encoded content type
        return Utils.__headers(host, auth, Utils._TOKEN_REQUEST_CONTENT_TYPE)

    @staticmethod
    def __get_field_recursively(content, field):
        field_value = None
        try:
            items = content.iteritems()
        except AttributeError:
            items = content.items()
        for key, value in items:
            if key == field:
                field_value = value
                break
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        field_value = Utils.__get_field_recursively(
                            item, field)
        return field_value

    @staticmethod
    def __headers(host, auth, content_type):
        headers = {'Host': host,
                   'Content-Type': content_type,
                   'Authorization': auth,
                   'Connection': Utils._KEEP_ALIVE,
                   'cache-control': Utils._NO_STORE}
        return headers


class AccessTokenProvider(AuthorizationProvider):
    """
    AccessTokenProvider is an instance of
    :py:meth:`borneo.AuthorizationProvider` that uses access tokens (ATs)
    obtained from the Oracle
    Identity Cloud Service (IDCS) as authorization information.

    An instance of this class is supplied to the driver via
    :py:meth:`borneo.NoSQLHandleConfig.set_authorization_provider`.

    This class caches access tokens acquired to minimize external calls. The
    callback provider is invoked if the AT is evicted from cache.

    It should not be necessary for applications to implement this interface.
    The mechanisms for dealing with IDCS are relatively complex and the
    :py:class:`DefaultAccessTokenProvider` is sufficient for most needs.

    :param duration_seconds: the cache duration in seconds.
    :param refresh_ahead: the refresh time before AT expiry in seconds.
    """

    ANDC_AUD_PREFIX = 'urn:opc:andc:entitlementid='
    """
    This string is used as prefix of the audience of an OAuth resource. The full
    audience is urn\:opc\:andc\:entitlementid={ENTITLEMENT_ID}, the
    ENTITLEMENT_ID is dynamically generated by Oracle Cloud.
    """

    SCOPE = 'urn:opc:andc:resource:consumer::all'
    """
    There is currently has only one fixed OAuth scope, which can be used in the
    AT acquisition to build the fully qualified scope along with audience.
    """

    ACCOUNT_AT_KEY = 'ACCOUNT'
    """Static string as key of account access token."""

    SERVICE_AT_KEY = 'SERVICE'
    """Static string as key of service access token."""

    DROP_TABLE_KEYWORD = 'DROP TABLE'
    """Keyword used to identify drop table request."""

    MAX_ENTRY_LIFE_TIME = 85400
    """Maximum lifetime of PSM and ANDC access token."""

    DEFAULT_REFRESH_AHEAD = 10
    """Default refresh time before AT expiry, 10 seconds."""

    TOKEN_PREFIX = 'Bearer '
    """The Access Token prefix in authorization header"""

    def __init__(self, duration_seconds=MAX_ENTRY_LIFE_TIME,
                 refresh_ahead=DEFAULT_REFRESH_AHEAD):
        """
        Creates an AccessTokenProvider with the specified cache duration. The
        duration specifies the lifetime of an AT in the cache. Setting the
        duration too large runs the risk that client requests may stall because
        a current token is not available or expired. Setting the duration too
        small means that there may be unnecessary overhead due to token renewal.
        """
        self.__timer = None
        # Refresh time before AT expired from cache.
        self.__refresh_ahead_s = refresh_ahead
        CheckValue.check_int_gt_zero(duration_seconds, 'duration_seconds')
        if duration_seconds > AccessTokenProvider.MAX_ENTRY_LIFE_TIME:
            raise IllegalArgumentException(
                'Access token cannot be cached longer than ' +
                str(AccessTokenProvider.MAX_ENTRY_LIFE_TIME) + ' seconds')
        self.__duration_seconds = duration_seconds
        self.__at_cache = Memoize(self.__duration_seconds)
        # AT refresh interval, if zero, no refresh will be scheduled.
        self.__refresh_interval_s = (
            self.__duration_seconds - self.__refresh_ahead_s if
            self.__duration_seconds > self.__refresh_ahead_s else 0)

    def get_authorization_string(self, request):
        """
        Returns IDCS ATs appropriate for the operation.

        :param request: the request that client attempts to issue.
        :returns: authorization string that can be present by client, which
            indicates this client is authorized to issue the specified request.
        """
        need_account_at = self.__need_account_at(request)
        key = (AccessTokenProvider.ACCOUNT_AT_KEY if need_account_at else
               AccessTokenProvider.SERVICE_AT_KEY)
        auth_string = self.__at_cache.get(key)
        if auth_string is None:
            auth_string = self.__get_at(key)
        return auth_string

    @abstractmethod
    def get_account_access_token(self):
        """
        Returns an account AT to be used for one of the following operations:

            Create table\n
            Drop table\n
            Alter table limits\n
            List table

        Oracle Cloud creates an OAuth resource named "PSMApp-cacct-{id}" when
        the cloud account is created. Note that id will be dynamically generated
        for each account by Oracle Cloud. This method must return an AT with
        audience in the account OAuth resource. Note that currently this OAuth
        resource only allows Resource Owner Grant type to acquire the access
        token. In other words, the client id and secret pair along with user
        name and password need to be present in the token acquisition payload.

        :returns: an account access token.
        :raises RequestTimeoutException: raises the exception if token
            acquisition does not complete within the configured timeout
            interval.
        """
        pass

    @abstractmethod
    def get_service_access_token(self):
        """
        Returns an AT to be used for service operations which is all data
        operations and generally any operation that is not an account
        operations.

        :returns: a service access token.
        :raises RequestTimeoutException: raises the exception if token
            acquisition does not complete within the configured timeout
            interval.
        """
        pass

    def close(self):
        """
        Closes the authorization provider.
        """
        if self.__timer is not None:
            self.__timer.cancel()
            self.__timer = None

    def __get_at(self, key):
        # Check if at is already present.
        at = self.__at_cache.get(key)
        if at is not None:
            return at
        if key == AccessTokenProvider.ACCOUNT_AT_KEY:
            at = self.get_account_access_token()
        else:
            at = self.get_service_access_token()
        value = None
        if at is not None:
            value = AccessTokenProvider.TOKEN_PREFIX + at
            self.__at_cache.set(key, value)
            # Only schedule refresh ANDC AT used for DML requests.
            if key == AccessTokenProvider.SERVICE_AT_KEY:
                self.__schedule_refresh()
        return value

    def __need_account_at(self, request):
        """
        Requests need account access token.

        - ListTables
        - CreateTable
        - AlterTable, alter limits only
        - DropTable
        """
        if isinstance(request, ListTablesRequest):
            return True
        if not isinstance(request, TableRequest):
            return False
        # Find if it's CreateTable and AlterTable limits, only these two
        # requests have TableLimits present.
        if request.get_table_limits() is not None:
            return True
        # Check if request is DropTable by looking up the keyword.
        ddl = request.get_statement()
        if ddl is not None:
            formatted_ddl = ' '.join(ddl.split()).upper()
            return formatted_ddl.startswith(
                AccessTokenProvider.DROP_TABLE_KEYWORD)
        return False

    def __refresh_task(self):
        try:
            at = self.get_service_access_token()
            if at is not None:
                value = AccessTokenProvider.TOKEN_PREFIX + at
                self.__at_cache.set(AccessTokenProvider.SERVICE_AT_KEY, value)
                self.__schedule_refresh()
        except Exception:
            # Ignore the failure of refresh. The driver would try to
            # acquire/refresh the AT in the next request if AT is not available,
            # the failure would be reported at that moment.
            self.__timer.cancel()
            self.__timer = None

    def __schedule_refresh(self):
        # If refresh interval is 0, don't schedule a refresh.
        if self.__refresh_interval_s == 0:
            return
        if self.__timer is not None:
            self.__timer.cancel()
            self.__timer = None
        self.__timer = Timer(self.__refresh_interval_s, self.__refresh_task)
        self.__timer.start()


class DefaultAccessTokenProvider(AccessTokenProvider):
    """
    An instance of :py:class:`AccessTokenProvider` that acquires access tokens
    from Oracle Identity Cloud Service (IDCS) using information provided by
    :py:class:`CredentialsProvider`. By default the
    :py:class:`CredentialsProvider` is used.

    This class requires tenant-specific information in order to properly
    communicate with IDCS using the credential information:

        A tenant-specific URL used to communicate with IDCS.\n
        An entitlement id. This is generated by Oracle during account creation.

    The tenant-specific IDCS URL is the IDCS host assigned to the tenant. After
    logging into the IDCS admin console, copy the host of the IDCS admin console
    URL. For example, the format of the admin console URL is
    "https\://{tenantId}.identity.oraclecloud.com/ui/v1/adminconsole". The
    "https\://{tenantId}.identity.oraclecloud.com" portion is the required
    parameter.

    The entitlement id can be found using the IDCS admin console. After logging
    into the IDCS admin console, choose *Applications* from the button on the
    top left. Find the Application named ANDC, enter the Resources tab in the
    Configuration. There is a field called primary audience, the entitlement id
    parameter is the value of "urn\:opc\:andc\:entitlementid", which is treated
    as a string. For example if your primary audience is
    "urn\:opc\:andc\:entitlementid=123456789" then the parameter is "123456789"

    NOTE: above is simple python doc. This information is on the implementation.

    ATs are acquiring using OAuth client ID and secret pair along with user name
    and password using `Resource Owner Grant Type <https://docs.oracle.com/en/\
    cloud/paas/identity-cloud/rest-api/ROPCGT.html>`_

    This provider uses :py:class:`PropertiesCredentialsProvider` by default to
    obtain credentials. These credentials are used to build IDCS access token
    payloads to acquire the required access tokens. The expiry window is a
    constant 120,000 milliseconds.

    :param idcs_props_file: the path of an IDCS properties file.
    :param idcs_url: an IDCS URL, provided by IDCS.
    :param entitlement_id: service entitlement ID, which can be found from the
        primary audience of the application named ANDC from IDCS.
    :param creds_provider: a credentials provider.
    :param timeout_ms: the access token acquisition request timeout in
        milliseconds.
    :param cache_duration_seconds: the amount of time the access tokens are
        cached in the provider, in seconds.
    :param refresh_ahead: the refresh time before AT expired from cache.
    :raises IllegalArgumentException: raises the exception if parameters are not
        expected type.
    """
    # Payload used to acquire access token with resource owner grant flow.
    _RO_GRANT_FORMAT = 'grant_type=password&username={0}&scope={1}&password='

    # Payload used to acquire IDCS access token with client grant flow.
    _CLIENT_GRANT_PAYLOAD = (
        'grant_type=client_credentials&scope=urn:opc:idm:__myscopes__')

    # IDCS fully qualified scope to acquire IDCS AT.
    _IDCS_SCOPE = 'urn:opc:idm:__myscopes__'

    # Filter using service type URN to get PSMApp metadata from IDCS when
    # provider attempts to acquire PSM AT using PSMApp client id and secret. The
    # PSMApp metadata contains PSMApp client id, secret and primary audience of
    # PSM for this cloud account.
    _PSM_APP_FILTER = '?filter=serviceTypeURN+eq+%22PSMResourceTenatApp%22'

    # Filter using OAuth client id to find client metadata from IDCS.
    _CLIENT_FILTER = '?filter=name+eq+'

    # Field name in the IDCS access token response.
    _AT_FIELD = 'access_token'

    # Properties in the IDCS properties file.
    _IDCS_URL_PROP = 'idcs_url'
    _CREDS_FILE_PROP = 'creds_file'
    _ENTITLEMENT_ID_PROP = 'entitlement_id'

    # Default properties file at ~/.andc/idcs.props
    _DEFAULT_PROPS_FILE = environ['HOME'] + sep + '.andc' + sep + 'idcs.props'

    def __init__(self, idcs_props_file=_DEFAULT_PROPS_FILE, idcs_url=None,
                 entitlement_id=None, creds_provider=None,
                 timeout_ms=Utils.DEFAULT_TIMEOUT_MS,
                 cache_duration_seconds=AccessTokenProvider.MAX_ENTRY_LIFE_TIME,
                 refresh_ahead=AccessTokenProvider.DEFAULT_REFRESH_AHEAD):
        # Constructs a default access token provider.
        if idcs_url is None:
            CheckValue.check_str(idcs_props_file, 'idcs_props_file')
            super(DefaultAccessTokenProvider, self).__init__(
                DefaultAccessTokenProvider.MAX_ENTRY_LIFE_TIME,
                DefaultAccessTokenProvider.DEFAULT_REFRESH_AHEAD)
            self.__idcs_url = self.__get_idcs_url(idcs_props_file)
            entitlement = self.__get_entitlement_id(idcs_props_file)
            self.__creds_provider = (
                PropertiesCredentialsProvider().set_properties_file(
                    self.__get_credential_file(idcs_props_file)))
            self.__timeout_ms = Utils.DEFAULT_TIMEOUT_MS
        else:
            CheckValue.check_str(idcs_url, 'idcs_url')
            self.__is_credentials_provider(creds_provider)
            CheckValue.check_int_gt_zero(timeout_ms, 'timeout_ms')
            CheckValue.check_int_gt_zero(cache_duration_seconds,
                                         'cache_duration_seconds')
            CheckValue.check_int_gt_zero(refresh_ahead, 'refresh_ahead')
            super(DefaultAccessTokenProvider, self).__init__(
                cache_duration_seconds, refresh_ahead)
            self.__idcs_url = idcs_url
            entitlement = entitlement_id
            self.__creds_provider = (PropertiesCredentialsProvider() if
                                     creds_provider is None else creds_provider)
            self.__timeout_ms = timeout_ms
        url = urlparse(self.__idcs_url)
        self.__host = url.hostname
        self.__andc_fqs = None
        if entitlement is not None:
            CheckValue.check_str(entitlement, 'entitlement_id')
            self.__andc_fqs = (
                AccessTokenProvider.ANDC_AUD_PREFIX + entitlement +
                AccessTokenProvider.SCOPE)
        self.__psm_fqs = None
        self.__logger = None
        self.__logutils = LogUtils()
        self.__sess = Session()
        self.__request_utils = RequestUtils(self.__sess, self.__logutils)

    def set_credentials_provider(self, provider):
        """
        Sets :py:class:`CredentialsProvider`.

        :param provider: the credentials provider.
        :returns: self.
        :raises IllegalArgumentException: raises the exception if provider is
            not an instance of CredentialsProvider.
        """
        self.__is_credentials_provider(provider)
        self.__creds_provider = provider
        return self

    def set_logger(self, logger):
        """
        Sets a logger instance for this provider. If not set, the logger
        associated with the driver is used.

        :param logger: the logger.
        :returns: self.
        :raises IllegalArgumentException: raises the exception if logger is not
            an instance of Logger.
        """
        CheckValue.check_logger(logger, 'logger')
        self.__logger = logger
        self.__logutils = LogUtils(logger)
        self.__request_utils = RequestUtils(self.__sess, self.__logutils)
        return self

    def get_logger(self):
        """
        Returns the logger of this provider if set, None if not.

        :returns: the logger.
        """
        return self.__logger

    def get_account_access_token(self):
        self.__ensure_creds_provider()
        self.__find_oauth_scopes()
        if self.__psm_fqs is not None:
            return self.__get_at_by_password(self.__psm_fqs)
        return self.__get_at_by_psm_app()

    def get_service_access_token(self):
        self.__ensure_creds_provider()
        if self.__andc_fqs is None:
            self.__find_oauth_scopes()
        if self.__andc_fqs is None:
            raise IllegalStateException(
                'Unable to find service scope, OAuth client isn\'t ' +
                'configured properly, run OAuthClient utility to verify and ' +
                'recreate.')
        return self.__get_at_by_password(self.__andc_fqs)

    def close(self):
        super(DefaultAccessTokenProvider, self).close()
        if self.__sess is not None:
            self.__sess.close()

    def __ensure_creds_provider(self):
        if self.__creds_provider is None:
            raise IllegalArgumentException(
                'CredentialsProvider unavailable.')

    def __find_oauth_scopes(self):
        # Find PSM and ANDC FQS from allowed scopes of OAuth client.
        if self.__andc_fqs is not None and self.__psm_fqs is not None:
            return
        creds = self.__get_client_creds()
        oauth_id = creds.get_credential_alias()
        auth = self.__get_auth_header(oauth_id, creds.get_secret())
        try:
            auth = 'Bearer ' + self.__get_access_token(
                auth, DefaultAccessTokenProvider._CLIENT_GRANT_PAYLOAD,
                DefaultAccessTokenProvider._IDCS_SCOPE)
        except InvalidAuthorizationException:
            self.__logutils.log_debug(
                'Cannot get access token with IDCS scope using client grant.')
            return
        response = self.__request_utils.do_get_request(
            self.__idcs_url + Utils.APP_ENDPOINT +
            DefaultAccessTokenProvider._CLIENT_FILTER + '%22' + oauth_id + '%22',
            Utils.scim_headers(self.__host, auth), self.__timeout_ms)
        if response is None:
            raise IllegalStateException(
                'Error getting client metadata from Identity Cloud Service, ' +
                'no response.')
        if response.get_status_code() >= codes.multiple_choices:
            Utils.handle_idcs_errors(
                response, 'Getting client metadata',
                'Please verify if the OAuth client is configured properly.')
        fqs_list = Utils.get_field(
            response.get_content(), 'allowedScopes', 'fqs')
        if fqs_list is None:
            return
        for fqs in fqs_list:
            if fqs.startswith(AccessTokenProvider.ANDC_AUD_PREFIX):
                self.__andc_fqs = fqs
            elif fqs.endswith(Utils.PSM_SCOPE):
                self.__psm_fqs = fqs

    def __get_access_token(self, auth_header, payload, fqs):
        response = self.__request_utils.do_post_request(
            self.__idcs_url + Utils.TOKEN_ENDPOINT, Utils.token_headers(
                self.__host, auth_header), payload, self.__timeout_ms)
        if response is None:
            raise IllegalStateException('Error acquiring access token with '
                                        'scope ' + fqs + ', no response')
        response_code = response.get_status_code()
        content = response.get_content()
        if response_code >= codes.multiple_choices:
            self.__handle_token_error_response(response_code, content)
        return self.__parse_access_token_response(content)

    def __get_at_by_password(self, fqs):
        user_creds = self.__get_user_creds()
        client_creds = self.__get_client_creds()
        # URL encode fqs.
        encoded_fqs = quote(fqs.encode())
        auth_header = self.__get_auth_header(
            client_creds.get_credential_alias(), client_creds.get_secret())
        replaced = str.format(DefaultAccessTokenProvider._RO_GRANT_FORMAT,
                              user_creds.get_credential_alias(), encoded_fqs)
        # Build the actual payload to acquire access token.
        payload = replaced + user_creds.get_secret()
        return self.__get_access_token(auth_header, payload, fqs)

    def __get_at_by_psm_app(self):
        """
        Acquiring account access token using PSMApp provisioned by Oracle for
        each tenant. Keeping this path to remain the backward compatibility if
        users are using the client id and secret from Application ANDC. This
        path shouldn't work after IDCS hides client secret of Oracle-created
        Applications. This will be deprecated eventually.
        """
        # 1. acquire IDCS AT
        result = self.__get_at_by_password(
            DefaultAccessTokenProvider._IDCS_SCOPE)
        if result is None:
            raise IllegalStateException(
                'Error acquiring Identity Cloud Service access token, unable ' +
                'to get metadata to proceed acquiring account access token.')
        # 2. look up audience, client id and secret of PSMApp
        auth_header = 'Bearer ' + result
        psm_info = self.__get_psm_app(auth_header)
        if psm_info is None:
            raise IllegalStateException(
                'Error finding required metadata from Identity Cloud Service,' +
                ' unable to proceed acquiring account access token.')
        # 3. acquire PSM AT
        auth_header = self.__get_auth_header(psm_info.client_id,
                                             psm_info.client_secret)
        psm_fqs = psm_info.audience + Utils.PSM_SCOPE
        user_creds = self.__get_user_creds()
        replaced = str.format(DefaultAccessTokenProvider._RO_GRANT_FORMAT,
                              user_creds.get_credential_alias(), psm_fqs)
        payload = replaced + user_creds.get_secret()
        return self.__get_access_token(auth_header, payload, psm_fqs)

    def __get_auth_header(self, client_id, secret):
        # Return authorization header in form of 'Basic <clientId:secret>'.
        pair = client_id + ':' + secret
        try:
            return 'Basic ' + b64encode(pair)
        except TypeError:
            return 'Basic ' + b64encode(pair.encode()).decode()

    def __get_client_creds(self):
        creds = self.__creds_provider.get_oauth_client_credentials()
        if creds is None:
            raise IllegalArgumentException(
                'OAuth client credentials unavailable.')
        return creds

    def __get_credential_file(self, properties_file):
        creds_file = PropertiesCredentialsProvider.get_property_from_file(
            properties_file, DefaultAccessTokenProvider._CREDS_FILE_PROP)
        if creds_file is None:
            return PropertiesCredentialsProvider._DEFAULT_CREDS_FILE
        return creds_file

    def __get_entitlement_id(self, properties_file):
        return PropertiesCredentialsProvider.get_property_from_file(
            properties_file, DefaultAccessTokenProvider._ENTITLEMENT_ID_PROP)

    def __get_idcs_url(self, properties_file):
        # Methods used to fetch IDCS-related properties from given file.
        idcs_url = PropertiesCredentialsProvider.get_property_from_file(
            properties_file, DefaultAccessTokenProvider._IDCS_URL_PROP)
        if idcs_url is None:
            raise IllegalArgumentException(
                'Must specify IDCS URL in IDCS properties file.')
        return idcs_url

    def __get_psm_app(self, auth_header):
        """
        Get PSMApp metadata from IDCS. The secret of PSMApp will be hidden by
        IDCS, if no secret, return an error to ask users create custom client.
        This will be deprecated eventually.
        """
        # Get PSMApp metadata from IDCS.
        response = self.__request_utils.do_get_request(
            self.__idcs_url + Utils.APP_ENDPOINT +
            DefaultAccessTokenProvider._PSM_APP_FILTER,
            Utils.token_headers(self.__host, auth_header), self.__timeout_ms)
        if response is None:
            raise IllegalStateException(
                'Error getting required metadata from Identity Cloud Service,' +
                ' unable to acquire account access token, no response')
        response_code = response.get_status_code()
        content = response.get_content()
        if response_code >= codes.multiple_choices:
            Utils.handle_idcs_errors(
                response, 'Getting account metadata',
                'Please grant user Identity Domain Administrator or ' +
                'Application Administrator role')
        oauth_id = 'name'
        audience = 'audience'
        secret = 'clientSecret'
        try:
            oauth_id_value = Utils.get_field(content, oauth_id)
            audience_value = Utils.get_field(content, audience)
            secret_value = Utils.get_field(content, secret)
        except IllegalStateException as ise:
            raise UnauthorizedException(
                'Please grant user Identity Domain Administrator or ' +
                'Application Administrator role. ' + str(ise))
        if oauth_id_value is None or audience_value is None:
            raise IllegalStateException(
                'Account metadata response contains invalid value: ' + content)
        if secret_value is None:
            raise IllegalStateException(
                'Account metadata doesn\'t have a secret, unable to acquire ' +
                'account access token. Must create the custom OAuth Client ' +
                'first. Account metadata: ' + content)
        return DefaultAccessTokenProvider.PSMAppInfo(
            oauth_id_value, secret_value, audience_value)

    def __get_user_creds(self):
        user_creds = self.__creds_provider.get_user_credentials()
        if user_creds is None:
            raise IllegalArgumentException('User credentials unavailable.')
        return user_creds

    def __handle_token_error_response(self, response_code, content):
        if response_code >= codes.server_error:
            self.__logutils.log_info(
                'Error acquiring access token, expected to retry, error ' +
                'response: ' + content + ', status code: ' + str(response_code))
            raise RequestTimeoutException(
                'Error acquiring access token, expected to retry, error ' +
                'response: ' + content + ', status code: ' + str(response_code))
        elif response_code == codes.bad and content is None:
            # IDCS doesn't return error message in case of credentials has
            # invalid URL encoded characters.
            raise IllegalArgumentException(
                'Error acquiring access token, status code: ' +
                str(response_code) +
                ', CredentialsProvider supplies invalid credentials')
        else:
            raise InvalidAuthorizationException(
                'Error acquiring access token from Identity Cloud Service. ' +
                'IDCS error response: ' + content + ', status code: ' +
                str(response_code))

    def __is_credentials_provider(self, provider):
        if (provider is not None and
                not isinstance(provider, CredentialsProvider)):
            raise IllegalArgumentException('provider must be an instance of ' +
                                           'CredentialsProvider.')

    def __parse_access_token_response(self, response):
        """
        A valid response from IDCS is in JSON format and must contains the field
        "access_token" and "expires_in".
        """
        response = loads(response)
        access_token = response.get(DefaultAccessTokenProvider._AT_FIELD)
        if access_token is None:
            raise IllegalStateException(
                'Access token response contains invalid value, response: ' +
                str(response))
        self.__logutils.log_debug('Acquired access token ' + access_token)
        return access_token

    class PSMAppInfo:
        def __init__(self, client_id, client_secret, audience):
            self.client_id = client_id
            self.client_secret = client_secret
            self.audience = audience


class IDCSCredentials:
    """
    A credentials object that bundles a string alias and char array secret used
    to authenticate with IDCS for token acquisition.

    :param alias: an alias or user name in string format.
    :param secret: a password, secret or key that is associated with the alias.
        It is copied on construction.
    """

    def __init__(self, alias, secret):
        # Construct a new instance
        CheckValue.check_str(alias, 'alias')
        CheckValue.check_str(secret, 'secret')
        self.__alias = alias
        self.__secret = secret

    def get_credential_alias(self):
        """
        Identifies the alias of the credentials.

        :returns: the name of the alias associated with the credentials.
        """
        return self.__alias

    def get_secret(self):
        """
        Gets the secret. This returns a copy of the secret.

        :returns: the secret for the credentials object.
        """
        return self.__secret


class CredentialsProvider(object):
    """
    CredentialsProvider returns 2 types of :py:class:`IDCSCredentials`
    Credentials encapsulating user name and password
    Credentials encapsulating client id and secret

    These credentials are used by an instance of :py:class:`AccessTokenProvider`
    to acquire access tokens (ATs) for operations.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_oauth_client_credentials(self):
        """
        Returns a :py:class:`IDCSCredentials` instance encapsulating an OAuth
        Client id and secret.

        :returns: IDCS credentials or None if not available.
        """
        pass

    @abstractmethod
    def get_user_credentials(self):
        """
        Returns a :py:class:`IDCSCredentials` instance encapsulating user name
        and password. Note that the user password returned must be URL encoded.

        :returns: IDCS credentials or None if not available.
        """
        pass


class PropertiesCredentialsProvider(CredentialsProvider):
    """
    A credentials provider that fetches credentials stored in a properties file.
    The property name and value must be written in the form of key=value. This
    provider parses each line of the specified file, extracting properties.
    Leading and trailing whitespace in key and value are ignored.

    These property names are recognized and are case-sensitive:

    User Credentials

        andc_username\n
        andc_user_pwd

    OAuth client credentials

        andc_client_id\n
        andc_client_secret

    The default file is ~/.andc/credentials and can be modified using
    :py:meth:`set_properties_file`.
    """
    # Properties in the properties file.
    USER_NAME_PROP = 'andc_username'
    PWD_PROP = 'andc_user_pwd'
    CLIENT_ID_PROP = 'andc_client_id'
    CLIENT_SECRET_PROP = 'andc_client_secret'

    # Default credentials file at ~/.andc/credentials.
    _DEFAULT_CREDS_FILE = environ['HOME'] + sep + '.andc' + sep + 'credentials'

    def __init__(self):
        self.__properties_file = (
            PropertiesCredentialsProvider._DEFAULT_CREDS_FILE)
        self.__lock = Lock()

    def set_properties_file(self, file_path):
        """
        Sets the properties file to use to a non-default path.

        :param file_path: a path to the file to use.
        :returns: self.
        """
        CheckValue.check_str(file_path, 'file_path')
        if not path.exists(file_path):
            raise IllegalArgumentException(
                'Path: \'' + file_path + '\' not found.')
        self.__properties_file = file_path
        return self

    def get_oauth_client_credentials(self):
        client_id = self.__get_property_from_file(
            PropertiesCredentialsProvider.CLIENT_ID_PROP)
        client_secret = self.__get_property_from_file(
            PropertiesCredentialsProvider.CLIENT_SECRET_PROP)
        if client_id is None or client_secret is None:
            return None
        return IDCSCredentials(client_id, client_secret)

    def get_user_credentials(self):
        user_name = self.__get_property_from_file(
            PropertiesCredentialsProvider.USER_NAME_PROP)
        password = self.__get_property_from_file(
            PropertiesCredentialsProvider.PWD_PROP)
        if user_name is None or password is None:
            return None
        password = quote(password.encode())
        return IDCSCredentials(user_name, password)

    @staticmethod
    def get_property_from_file(properties_file, property_name):
        """
        Read each line of file, which represents a single key value pair
        delimited by "=". The white spaces before and after the key and value
        are ignored and trimmed.

        :param properties_file: the path to the property file.
        :param property_name: the property name to get.
        """
        with open(properties_file) as prop_file:
            for line in prop_file:
                delimiter = line.find('=')
                if delimiter == -1:
                    continue
                key = line[0: delimiter].strip()
                value = line[delimiter + 1: len(line)].strip()
                if property_name.lower() == key.lower() and value is not None:
                    return value
            return None

    def __get_property_from_file(self, property_name):
        return PropertiesCredentialsProvider.get_property_from_file(
            self.__properties_file, property_name)

    def __get_plain_user_credentials(self):
        # Get user credentials without URL encoding.
        user_name = self.__get_property_from_file(
            PropertiesCredentialsProvider.USER_NAME_PROP)
        password = self.__get_property_from_file(
            PropertiesCredentialsProvider.PWD_PROP)
        if user_name is None or password is None:
            return None
        return IDCSCredentials(user_name, password)
