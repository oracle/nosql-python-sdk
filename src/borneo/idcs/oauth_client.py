#
# Copyright (C) 2018, 2019 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
#
# Please see LICENSE.txt file included in the top-level directory of the
# appropriate download for a copy of the license and additional information.
#

from __future__ import print_function
from argparse import ArgumentParser
from json import loads
from logging import FileHandler, INFO, WARNING, getLogger
from os import mkdir, path, remove, sep
from requests import Session, codes
from sys import argv
from traceback import format_exc
try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

from borneo.common import LogUtils
from borneo.exception import IllegalStateException
from borneo.http import RequestUtils
try:
    from .idcs import AccessTokenProvider, PropertiesCredentialsProvider, Utils
except (ImportError, ValueError):
    from idcs import AccessTokenProvider, PropertiesCredentialsProvider, Utils


class OAuthClient:
    """
    Utility to create a custom OAuth client.

    To connect and authenticate to the Oracle NoSQL Database Cloud Service, a
    client needs to acquire an access token from Oracle Identity Cloud Service
    (IDCS). As a prerequisite, a custom OAuth client named *NoSQLClient* must
    be created first using this utility. This custom client needs to be created
    only once for a tenant.

    This utility needs a valid access token in a token file that can be
    downloaded from the IDCS admin console. After logging into the IDCS admin
    console, choose *Applications* from the button on the top left. Find the
    Application named *ANDC*, click the button *Generate Access Token*, in the
    pop-up window, pick *Invoke Identity Cloud Service APIs* under
    *Customized Scopes*. Click on *Download Token* and a token file will be
    generated and downloaded. Note that this token has a lifetime of one hour.

    After the token file has been downloaded, run this utility to complete the
    OAuth Client creation:

    .. code-block:: shell

      python oauth_client.py -create -idcs_url <tenant-specific IDCS URL> \
-token <token file>

    The tenant-specific IDCS URL is the IDCS host assigned to the tenant. After
    logging into the IDCS admin console, copy the host of the IDCS admin console
    URL. For example, the format of the admin console URL is
    "https\://{tenantId}.identity.oraclecloud.com/ui/v1/adminconsole". The
    "https\://{tenantId}.identity.oraclecloud.com" portion is the required
    parameter.

    After creation, the utility will print out *NoSQLClient is created*.
    The OAuth client id and secret will also be printed out. A credentials file
    template *credentials.tmp* with client id an secret will be generated at the
    working directory by default. Use *-credsdir* to specify different directory.

    This utility also can be used to delete this custom OAuth client in case the
    creation process failed unexpectedly.

    .. code-block:: shell

      python oauth_client.py -delete -idcs_url <tenant-specific IDCS URL> \
-token <token file>

    In addition, this utility can be used to verify if OAuth client is
    configured properly, for example

    .. code-block:: shell

      python oauth_client.py -verify -idcs_url <tenant-specific IDCS URL> \
-token <token file>
    """
    #
    # NOTE: above is simple doc. This information is on the implementation.
    # This custom OAuth client must be created with a specified name. The client
    # must:
    # - enable password, client_credentials as allowed grants
    # - have PSM and NDCS fully-qualified scopes (FQS) as allowed scopes
    # - have ANDC_FullAccessRole
    #
    # The OAuth client creation steps are:
    # 1. Find PSM and NDCS primary audiences from IDCS
    # 2. Build PSM and NDCS FQS with primary audiences, put in the OAuth \
    # client JSON payload
    # 3. POST <idcs_url>/admin/v1/Apps with OAuth client JSON payload
    # 4. Find role ID of ANDC_FullAccessRole
    # 5. Grant ANDC_FullAccessRole to created custom OAuth client
    #

    # Default OAuth client name
    _DEFAULT_NAME = 'NoSQLClient'
    # Default credentials template file name
    _CREDS_TMP = 'credentials.temp'
    # Endpoint with filter used to get PSM App
    _PSM_APP_EP = (Utils.APP_ENDPOINT +
                   '?filter=serviceTypeURN+eq+%22PSMResourceTenatApp%22')
    # Endpoint with filter used to get ANDC App
    _ANDC_APP_EP = (Utils.APP_ENDPOINT + '?filter=serviceTypeURN+eq+%22' +
                    'ANDC_ServiceEntitlement%22+and+isOAuthResource+eq+true')
    # Endpoint with filter used to get role ID of ANDC_FullAccessRole
    _ANDC_ROLE_EP = (Utils.ROLE_ENDPOINT +
                     '?filter=displayName+eq+%22ANDC_FullAccessRole%22')
    # Endpoint with filter used to get oauth client
    _CLIENT_EP = Utils.APP_ENDPOINT + '?filter=displayName+eq+%22'
    # JSON used to create custom OAuth client
    _CLIENT = (
        '{{"displayName": "{0}","isOAuthClient": true,' +
        '"isOAuthResource": false,"isUnmanagedApp": true,"active": true,' +
        '"description": "Custom OAuth Client for application access to ' +
        'NoSQL Database Cloud Service","clientType": "confidential",' +
        '"allowedGrants": ["password", "client_credentials"]' +
        ',"trustScope": "Explicit","allowedScopes": [' +
        '{{"fqs": "{1}"}},{{"fqs": "{2}"}}],' +
        '"schemas": ["urn:ietf:params:scim:schemas:oracle:idcs:App"],' +
        '"basedOnTemplate": {{"value": "CustomWebAppTemplateId"}}}}')
    # JSON used to grant role to client
    _GRANT = (
        '{{"app": {{"value": "{0}"}},"entitlement": {{' +
        '"attributeName": "appRoles","attributeValue": "{1}"}},' +
        '"grantMechanism": "ADMINISTRATOR_TO_APP",' +
        '"grantee": {{"value": "{2}","type": "App"}},' +
        '"schemas": ["urn:ietf:params:scim:schemas:oracle:idcs:Grant"]}}')
    _DEACTIVATE = (
        '{"active": false,"schemas": [' +
        '"urn:ietf:params:scim:schemas:oracle:idcs:AppStatusChanger"]}')

    # Main argument flags
    _IDCS_URL_FLAG = '-idcs_url'
    _TOKEN_FILE_FLAG = '-token'
    _CREATE_FLAG = '-create'
    _DELETE_FLAG = '-delete'
    _VERIFY_FLAG = '-verify'
    _NAME_FLAG = '-name'
    _DIR_FLAG = '-credsdir'
    _TIMEOUT_FLAG = '-timeout'
    _VERBOSE_FLAG = '-verbose'

    def __init__(self):
        self.__parse_args()
        url = urlparse(self.__idcs_url)
        self.__host = url.hostname
        # logger used for HTTP request logging
        self.__logger = self.__get_logger()
        self.__logutils = LogUtils(self.__logger)
        self.__sess = Session()
        self.__request_utils = RequestUtils(self.__sess, self.__logutils)

    def execute_commands(self):
        try:
            if self.__delete:
                self.__do_delete()
            elif self.__create:
                self.__do_create()
            else:
                errors = list()
                self.__do_verify(errors)
                if len(errors) != 0:
                    print('Verification failed: ')
                    for err in errors:
                        print(err)
        except Exception:
            print(format_exc())
        finally:
            if self.__sess is not None:
                self.__sess.close()

    def __add_app(self, auth, payload):
        # Add the custom OAuth client
        response = self.__request_utils.do_post_request(
            self.__idcs_url + Utils.APP_ENDPOINT,
            Utils.scim_headers(self.__host, auth), payload, self.__timeout_ms)
        self.__check_not_none(response, 'response of adding OAuth client')
        response_code = response.get_status_code()
        content = response.get_content()
        if response_code == codes.conflict:
            raise IllegalStateException(
                'OAuth Client ' + self.__name + ' already exists. To ' +
                'recreate, run with ' + OAuthClient._DELETE_FLAG + '. To ' +
                'verify if existing client is configured correctly, run with ' +
                OAuthClient._VERIFY_FLAG)
        elif response_code >= codes.multiple_choices:
            OAuthClient.__idcs_errors(response, 'Adding custom client')
        app_id = 'id'
        oauth_id = 'name'
        secret = 'clientSecret'
        app_id_value = Utils.get_field(content, app_id)
        oauth_id_value = Utils.get_field(content, oauth_id)
        secret_value = Utils.get_field(content, secret)
        if (app_id_value is None or oauth_id_value is None or
                secret_value is None):
            raise IllegalStateException(
                str.format('Unable to find {0} or {1} or {2} in ,' + content,
                           app_id, oauth_id, secret))
        return OAuthClient.Client(app_id_value, oauth_id_value, secret_value)

    def __check_not_none(self, response, action):
        if response is None:
            raise IllegalStateException(
                'Error ' + action + ' from Oracle Identity Cloud Service, ' +
                'no response')

    def __creds_template(self, client_id, secret):
        file_dir = ((path.abspath(path.dirname(argv[0])) if
                     self.__temp_file_dir is None else
                     self.__temp_file_dir) + sep + OAuthClient._CREDS_TMP)
        if path.exists(file_dir):
            remove(file_dir)
        with open(file_dir, 'w') as f:
            if client_id is not None:
                f.write(PropertiesCredentialsProvider.CLIENT_ID_PROP + '=' +
                        client_id + '\n')
                f.write(PropertiesCredentialsProvider.CLIENT_SECRET_PROP + '=' +
                        secret + '\n')
            f.write(PropertiesCredentialsProvider.USER_NAME_PROP + '=\n')
            f.write(PropertiesCredentialsProvider.PWD_PROP + '=\n')
        return file_dir

    def __deactivate_app(self, auth, app_id):
        # Deactivate OAuth client
        response = self.__request_utils.do_put_request(
            self.__idcs_url + Utils.STATUS_ENDPOINT + sep + app_id,
            Utils.scim_headers(self.__host, auth), OAuthClient._DEACTIVATE,
            self.__timeout_ms)
        self.__check_not_none(
            response, 'response of deactivating OAuth client')
        if codes.ok <= response.get_status_code() < codes.multiple_choices:
            return
        OAuthClient.__idcs_errors(
            response, 'deactivating OAuth client ' + self.__name)

    def __do_create(self):
        self.__output('Creating OAuth Client ' + self.__name)
        try:
            # Find PSM and ANDC fqs
            auth = 'Bearer ' + self.__get_bootstrap_token()
            psm_fqs = self.__get_psm_audience(auth) + Utils.PSM_SCOPE
            andc = self.__get_andc_info(auth)
            andc_fqs = andc.audience + AccessTokenProvider.SCOPE
            self.__log_verbose('Found scopes ' + psm_fqs + ', ' + andc_fqs)
            # Add custom client
            add_app = OAuthClient._CLIENT.format(
                self.__name, psm_fqs, andc_fqs)
            client_info = self.__add_app(auth, add_app)
            self.__log_verbose('Added OAuth client ' + self.__name)
            # Find ANDC role id
            role_id = self.__get_id(
                auth, self.__idcs_url + OAuthClient._ANDC_ROLE_EP, 'role')
            self.__log_verbose('Found role id ' + role_id)
            # Grant ANDC_FullAccessRole to custom client
            grant = OAuthClient._GRANT.format(
                andc.app_id, role_id, client_info.app_id)
            self.__grant_role(auth, grant)
            self.__log_verbose('Granted role to OAuth client')
            self.__output(self.__name + ' is created\nClient ID: ' +
                          client_info.oauth_id + '\nClient secret: ' +
                          client_info.secret)
            creds_path = self.__creds_template(client_info.oauth_id,
                                               client_info.secret)
            self.__output('Credential template file ' + creds_path)
        except Exception as e:
            self.__output('Failed to create OAuth client ' + self.__name)
            raise e

    def __do_delete(self):
        self.__output('Deleting OAuth Client ' + self.__name)
        try:
            auth = 'Bearer ' + self.__get_bootstrap_token()
            # Find OAuth client AppId
            app_id = self.__get_id(
                auth, self.__idcs_url + OAuthClient._CLIENT_EP + self.__name +
                '%22', 'client')
            self.__log_verbose('Found OAuth client AppId: ' + app_id)
            # Deactivate the OAuth client
            self.__deactivate_app(auth, app_id)
            self.__log_verbose('OAuth client deactivated')
            # Remove the OAuth client
            self.__remove_client(auth, app_id)
            self.__output(self.__name + ' is deleted')
        except Exception as e:
            self.__output('Failed to remove OAuth client ' + self.__name)
            raise e

    def __do_verify(self, errors):
        self.__output('Verifying OAuth Client ' + self.__name)
        try:
            auth = 'Bearer ' + self.__get_bootstrap_token()
            response = self.__request_utils.do_get_request(
                self.__idcs_url + OAuthClient._CLIENT_EP + self.__name + '%22',
                Utils.scim_headers(self.__host, auth), self.__timeout_ms)
            self.__check_not_none(response, 'client metadata')
            response_code = response.get_status_code()
            content = response.get_content()
            if response_code >= codes.multiple_choices:
                OAuthClient.__idcs_errors(
                    response, 'Getting client ' + self.__name)
            grants = Utils.get_field(content, 'allowedGrants')
            if grants is None:
                # No results in response
                raise IllegalStateException(
                    'OAuth Client ' + self.__name + ' doesn\'t exist, or the ' +
                    'token file is invalid, user who downloads the token ' +
                    'must have Identity Domain Administrator role')
            # Verify if client has required grants
            self.__verify_grants(grants, errors)
            # Verify if client has PSM and ANDC FQS
            self.__verify_scopes(
                Utils.get_field(content, 'allowedScopes', 'fqs'), errors)
            # Verify if client has ANDC role
            self.__verify_role(
                Utils.get_field(content, 'grantedAppRoles', 'display'), errors)
            if len(errors) > 0:
                return
            self.__output('Verification succeed')
        except Exception as e:
            self.__output('Verification failed of OAuth client ' + self.__name)
            raise e

    def __get_andc_info(self, auth):
        # Get App ANDC metadata from IDCS
        response = self.__request_utils.do_get_request(
            self.__idcs_url + OAuthClient._ANDC_APP_EP,
            Utils.scim_headers(self.__host, auth), self.__timeout_ms)
        self.__check_not_none(response, 'getting service metadata')
        content = response.get_content()
        if response.get_status_code() >= codes.multiple_choices:
            OAuthClient.__idcs_errors(response, 'Getting service metadata')
        audience = 'audience'
        app_id = 'id'
        audience_value = Utils.get_field(content, audience)
        app_id_value = Utils.get_field(content, app_id)
        if audience_value is None or app_id_value is None:
            raise IllegalStateException(
                str.format('Unable to find {0} or {1} in ,' + content,
                           audience, app_id))
        return OAuthClient.ANDC(app_id_value, audience_value)

    def __get_bootstrap_token(self):
        # Read access token from given file
        with open(self.__at_file, 'r') as at_file:
            content = at_file.read()
        bootstrap_token = loads(content)
        field = 'app_access_token'
        app_access_token = bootstrap_token.get(field)
        if app_access_token is None:
            raise IllegalStateException(
                'Access token file contains invalid value: ' + content)
        return app_access_token

    def __get_id(self, auth, url, resource):
        response = self.__request_utils.do_get_request(
            url, Utils.scim_headers(self.__host, auth), self.__timeout_ms)
        self.__check_not_none(response, 'getting ' + resource + ' id')
        if response.get_status_code() >= codes.multiple_choices:
            OAuthClient.__idcs_errors(response, 'Getting id of ' + resource)
        return str(Utils.get_field(
            response.get_content(), 'id', allow_none=False))

    def __get_logger(self):
        """
        Returns the logger used for OAuthClient.
        """
        logger = getLogger(self.__class__.__name__)
        if self.__verbose:
            logger.setLevel(WARNING)
        else:
            logger.setLevel(INFO)
        log_dir = (path.abspath(path.dirname(argv[0])) + sep + 'logs')
        if not path.exists(log_dir):
            mkdir(log_dir)
        logger.addHandler(FileHandler(log_dir + sep + 'oauth.log'))
        return logger

    def __get_psm_audience(self, auth):
        response = self.__request_utils.do_get_request(
            self.__idcs_url + OAuthClient._PSM_APP_EP,
            Utils.scim_headers(self.__host, auth), self.__timeout_ms)
        self.__check_not_none(response, 'getting account metadata')
        if response.get_status_code() >= codes.multiple_choices:
            OAuthClient.__idcs_errors(response, 'Getting account metadata')
        return str(Utils.get_field(
            response.get_content(), 'audience', allow_none=False))

    def __grant_role(self, auth, payload):
        # Grant ANDC_FullAccessRole to OAuth client
        response = self.__request_utils.do_post_request(
            self.__idcs_url + Utils.GRANT_ENDPOINT,
            Utils.scim_headers(self.__host, auth), payload,
            self.__timeout_ms)
        self.__check_not_none(response, ' response of granting role')
        if codes.ok <= response.get_status_code() < codes.multiple_choices:
            return
        OAuthClient.__idcs_errors(response, 'Granting required role to client')

    def __log_verbose(self, msg):
        if self.__verbose:
            print(msg)

    def __output(self, msg):
        print(msg)

    def __parse_args(self):
        parser = ArgumentParser(prog='OAuthClient')
        parser.add_argument(
            OAuthClient._IDCS_URL_FLAG, required=True, help='The idcs_url.',
            metavar='<tenant-base IDCS URL>')
        parser.add_argument(
            OAuthClient._TOKEN_FILE_FLAG, required=True,
            help='The path of the token get from IDCS admin console.',
            metavar='<access token file path>')
        parser.add_argument(
            OAuthClient._NAME_FLAG, default=OAuthClient._DEFAULT_NAME,
            help='The OAuth Client name.',
            metavar='<client name> default: NoSQLClient')
        parser.add_argument(
            OAuthClient._DIR_FLAG,
            help='The directory for generating the credentials file template.',
            metavar=('<credentials template directory path> ' +
                     'default: current dir'))
        parser.add_argument(
            OAuthClient._TIMEOUT_FLAG, type=int,
            default=Utils.DEFAULT_TIMEOUT_MS, help='The timeout.',
            metavar='<request timeout> default: 12000 ms')
        parser.add_argument(
            OAuthClient._CREATE_FLAG, action='store_true',
            help='To create the OAuth Client.')
        parser.add_argument(
            OAuthClient._DELETE_FLAG, action='store_true',
            help='To delete the OAuth Client.')
        parser.add_argument(
            OAuthClient._VERIFY_FLAG, action='store_true',
            help='To verify the OAuth Client.')
        parser.add_argument(
            OAuthClient._VERBOSE_FLAG, action='store_true',
            help='To log verbose information.')

        args = parser.parse_args()
        self.__idcs_url = args.idcs_url
        self.__at_file = args.token
        self.__name = args.name
        self.__temp_file_dir = args.credsdir
        self.__timeout_ms = args.timeout
        self.__create = args.create
        self.__delete = args.delete
        self.__verify = args.verify
        self.__verbose = args.verbose

        if not (self.__create or self.__delete or self.__verify):
            parser.error(
                'Missing required argument ' + OAuthClient._CREATE_FLAG +
                ' | ' + OAuthClient._DELETE_FLAG + ' | ' +
                OAuthClient._VERIFY_FLAG)

    def __remove_client(self, auth, app_id):
        response = self.__request_utils.do_delete_request(
            self.__idcs_url + Utils.APP_ENDPOINT + sep + app_id,
            Utils.scim_headers(self.__host, auth), self.__timeout_ms)
        self.__check_not_none(response, 'response of deleting OAuth client')
        if codes.ok <= response.get_status_code() < codes.multiple_choices:
            return
        OAuthClient.__idcs_errors(
            response, 'removing OAuth client ' + self.__name)

    def __verify_grants(self, grants, errors):
        self.__log_verbose('OAuth client allowed grants: ' + str(grants))
        match = 0
        for grant in grants:
            if (grant.lower() == 'password' or
                    grant.lower() == 'client_credentials'):
                match += 1
        if match != 2:
            errors.append('Missing required allowed grants, require Resource ' +
                          'Owner and Client Credentials')
        self.__log_verbose('Grants verification succeed')

    def __verify_role(self, roles, errors):
        if roles is None:
            raise IllegalStateException(
                'OAuth client ' + self.__name + ' doesn\'t have roles')
        self.__log_verbose('OAuth client allowed roles: ' + str(roles))
        match = 0
        for role in roles:
            if role == 'ANDC_FullAccessRole':
                match += 1
        if match != 1:
            errors.append('Missing required role ANDC_FullAccessRole')
        self.__log_verbose('Role verification succeed')

    def __verify_scopes(self, fqs_list, errors):
        self.__log_verbose('OAuth client allowed scopes: ' + str(fqs_list))
        match = 0
        for fqs in fqs_list:
            if Utils.PSM_SCOPE in fqs or AccessTokenProvider.SCOPE in fqs:
                match += 1
        if match != 2:
            errors.append('Missing required OAuth scopes, client only have ' +
                          str(fqs_list))
        self.__log_verbose('Scope verification succeed')

    @staticmethod
    def __idcs_errors(response, action):
        Utils.handle_idcs_errors(
            response, action, ' Access token in the token file expired,' +
            ' or the token file is generated with incorrect scopes,' +
            ' requires Identity Domain Administrator')

    class ANDC:
        def __init__(self, app_id, audience):
            self.app_id = app_id
            self.audience = audience

    class Client:
        def __init__(self, app_id, oauth_id, secret):
            self.app_id = app_id
            self.oauth_id = oauth_id
            self.secret = secret


if __name__ == '__main__':
    auth_client = OAuthClient()
    auth_client.execute_commands()
