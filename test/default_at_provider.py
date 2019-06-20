#
# Copyright (C) 2018, 2019 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
#
# Please see LICENSE.txt file included in the top-level directory of the
# appropriate download for a copy of the license and additional information.
#

import unittest
from os import remove
from requests import ConnectionError, codes
from socket import error
from sys import version_info
from threading import Thread
try:
    from SimpleHTTPServer import SimpleHTTPRequestHandler
    from SocketServer import TCPServer
    from urllib import unquote
except ImportError:
    from http.server import SimpleHTTPRequestHandler
    from socketserver import TCPServer
    from urllib.parse import unquote

from borneo import (
    IllegalArgumentException, IllegalStateException,
    InvalidAuthorizationException, ListTablesRequest, TableRequest)
from borneo.idcs import (
    DefaultAccessTokenProvider, PropertiesCredentialsProvider)
from parameters import idcs_url
from testutils import (
    credentials_file, fake_credentials_file, generate_credentials_file,
    generate_properties_file, properties_file)


class TestDefaultAccessTokenProvider(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        global CLIENT_INFO, EMPTY_INFO, NEW_PSM_INFO, PSM_INFO, TOKEN_RESULT
        global APP_ENDPOINT, TOKEN_ENDPOINT, ACCOUNT_AT, CLIENT_AT, SERVICE_AT
        global GET_INFO, POST_INFO
        CLIENT_INFO = (
            '{"schemas": [' +
            '"urn:ietf:params:scim:api:messages:2.0:ListResponse"],' +
            '"totalResults": 1,"Resources": [' +
            '{"name": "NoSQLClient","allowedScopes": [' +
            '{"fqs": "http://psmurn:opc:resource:consumer::all",' +
            '"idOfDefiningApp": "deabd3635565402ebd4848286ae5a3a4"},' +
            '{"fqs": "urn:opc:andc:entitlementid=zzz' +
            'urn:opc:andc:resource:consumer::all",\n' +
            '"idOfDefiningApp": "897fe6f66712491497c20a9fa9cddaf0"}]}]}')
        EMPTY_INFO = (
            '{"schemas": [' +
            '"urn:ietf:params:scim:api:messages:2.0:ListResponse"],' +
            '"totalResults": 0,"Resources": []}')
        NEW_PSM_INFO = (
            '{"schemas": [' +
            '"urn:ietf:params:scim:api:messages:2.0:ListResponse"],' +
            '"totalResults": 1,"Resources": [' +
            '{"audience": "http://psm","name": "PSMApp-cacct12345"}]}')
        PSM_INFO = (
            '{"schemas": [' +
            '"urn:ietf:params:scim:api:messages:2.0:ListResponse"],' +
            '"totalResults": 1,"Resources": [' +
            '{"audience": "http://psm","name": "PSMApp-cacct12345",' +
            '"clientSecret": "adfjeelkc"}]}')
        TOKEN_RESULT = ('{{"access_token": "{0}","expires_in": "100",\
"token_type": "Bearer"}}')

        APP_ENDPOINT = '/admin/v1/Apps'
        TOKEN_ENDPOINT = '/oauth2/v1/token'

        ACCOUNT_AT = 'account-at'
        CLIENT_AT = 'client-at'
        SERVICE_AT = 'service-at'

        GET_INFO = CLIENT_INFO
        POST_INFO = None

    def setUp(self):
        generate_credentials_file()
        self.creds_provider = PropertiesCredentialsProvider(
        ).set_properties_file(fake_credentials_file)
        self.base = 'http://localhost:' + str(8080)
        generate_properties_file(self.base, fake_credentials_file)
        self.token_provider = None

    def tearDown(self):
        remove(fake_credentials_file)
        remove(properties_file)
        if self.token_provider is not None:
            self.token_provider.close()
            self.token_provider = None

    def testAccessTokenProviderIllegalInit(self):
        # illegal idcs props file
        self.assertRaises(IllegalArgumentException, DefaultAccessTokenProvider,
                          {'properties_file': properties_file})
        # illegal idcs url
        self.assertRaises(IllegalArgumentException, DefaultAccessTokenProvider,
                          idcs_url={'idcs_url': idcs_url})
        # illegal properties credentials provider
        self.assertRaises(IllegalArgumentException, DefaultAccessTokenProvider,
                          idcs_url=self.base,
                          creds_provider='IllegalPropertiesCredentialsProvider')
        # illegal timeout
        self.assertRaises(IllegalArgumentException, DefaultAccessTokenProvider,
                          idcs_url=self.base, timeout_ms='IllegalTimeout')
        self.assertRaises(IllegalArgumentException, DefaultAccessTokenProvider,
                          idcs_url=self.base, timeout_ms=0)
        self.assertRaises(IllegalArgumentException, DefaultAccessTokenProvider,
                          idcs_url=self.base, timeout_ms=-1)
        # illegal cache duration seconds
        self.assertRaises(IllegalArgumentException, DefaultAccessTokenProvider,
                          idcs_url=self.base,
                          cache_duration_seconds='IllegalCacheSeconds')
        self.assertRaises(IllegalArgumentException, DefaultAccessTokenProvider,
                          idcs_url=self.base, cache_duration_seconds=0)
        self.assertRaises(IllegalArgumentException, DefaultAccessTokenProvider,
                          idcs_url=self.base, cache_duration_seconds=-1)
        self.assertRaises(IllegalArgumentException, DefaultAccessTokenProvider,
                          idcs_url=self.base, cache_duration_seconds=85401)
        # illegal refresh ahead
        self.assertRaises(IllegalArgumentException, DefaultAccessTokenProvider,
                          idcs_url=self.base,
                          refresh_ahead='IllegalRefreshAhead')
        self.assertRaises(IllegalArgumentException, DefaultAccessTokenProvider,
                          idcs_url=self.base, refresh_ahead=0)
        self.assertRaises(IllegalArgumentException, DefaultAccessTokenProvider,
                          idcs_url=self.base, refresh_ahead=-1)

    def testAccessTokenProviderSetIllegalCredentialsProvider(self):
        self.token_provider = DefaultAccessTokenProvider(properties_file)
        self.assertRaises(IllegalArgumentException,
                          self.token_provider.set_credentials_provider,
                          'IllegalCredentialsProvider')

    def testAccessTokenProviderSetIllegalLogger(self):
        self.token_provider = DefaultAccessTokenProvider(idcs_url=self.base)
        self.assertRaises(IllegalArgumentException,
                          self.token_provider.set_logger, 'IllegalLogger')

    def testAccessTokenProviderGetAuthorizationStringWithIllegalRequest(self):
        self.token_provider = DefaultAccessTokenProvider(idcs_url=self.base)
        self.assertRaises(IllegalArgumentException,
                          self.token_provider.get_authorization_string,
                          'IllegalRequest')

    def testAccessTokenProviderGets(self):
        self.token_provider = DefaultAccessTokenProvider(idcs_url=self.base)
        self.assertIsNone(self.token_provider.get_logger())

    def testAccessTokenProviderGetAuthorizationString(self):
        global GET_INFO, POST_INFO
        GET_INFO = CLIENT_INFO
        POST_INFO = None
        httpd, port = self.__find_port_start_server(TokenHandler)

        self.base = 'http://localhost:' + str(port)
        self.token_provider = DefaultAccessTokenProvider(
            idcs_url=self.base, creds_provider=self.creds_provider)
        # get authorization string for ListTablesRequest
        result = self.token_provider.get_authorization_string(
            ListTablesRequest())
        self.assertIsNotNone(result)
        self.assertEqual(result, 'Bearer ' + ACCOUNT_AT)
        # get authorization string for TableRequest
        result = self.token_provider.get_authorization_string(TableRequest())
        self.assertIsNotNone(result)
        self.assertEqual(result, 'Bearer ' + SERVICE_AT)
        self.__stop_server(httpd)

    def testAccessTokenProviderGetAccountAccessToken(self):
        global GET_INFO, POST_INFO
        GET_INFO = CLIENT_INFO
        POST_INFO = None
        httpd, port = self.__find_port_start_server(TokenHandler)

        # connect to illegal idcs url
        self.base = 'http://localhost:80'
        generate_properties_file(self.base, fake_credentials_file)
        self.token_provider = DefaultAccessTokenProvider(properties_file)
        self.assertRaises(ConnectionError,
                          self.token_provider.get_account_access_token)
        self.token_provider.close()

        self.base = 'http://localhost:' + str(port)
        # connect to legal idcs url
        generate_properties_file(self.base, fake_credentials_file)
        self.token_provider = DefaultAccessTokenProvider(properties_file)
        result = self.token_provider.get_account_access_token()
        self.assertIsNotNone(result)
        self.assertEqual(result, ACCOUNT_AT)
        self.__stop_server(httpd)

    def testAccessTokenProviderGetServiceAccessToken(self):
        global GET_INFO, POST_INFO
        GET_INFO = CLIENT_INFO
        POST_INFO = None
        httpd, port = self.__find_port_start_server(TokenHandler)

        # connect to illegal idcs url
        self.base = 'http://localhost:80'
        self.token_provider = DefaultAccessTokenProvider(
            idcs_url=self.base, creds_provider=self.creds_provider)
        self.assertRaises(ConnectionError,
                          self.token_provider.get_service_access_token)
        self.token_provider.close()

        self.base = 'http://localhost:' + str(port)
        # connect to legal idcs url
        self.token_provider = DefaultAccessTokenProvider(
            idcs_url=self.base, creds_provider=self.creds_provider)
        result = self.token_provider.get_service_access_token()
        self.assertIsNotNone(result)
        self.assertEqual(result, SERVICE_AT)
        self.__stop_server(httpd)

    def testAccessTokenProviderNoClientInfo(self):
        global GET_INFO, POST_INFO
        GET_INFO = PSM_INFO
        POST_INFO = None
        httpd, port = self.__find_port_start_server(TokenHandler)

        self.base = 'http://localhost:' + str(port)
        self.token_provider = DefaultAccessTokenProvider(
            idcs_url=self.base, creds_provider=self.creds_provider)
        if version_info.major == 2:
            # get service access token
            self.assertRaisesRegexp(
                IllegalStateException,
                'Unable to find service scope from OAuth Client.*$',
                self.token_provider.get_service_access_token)
        else:
            # get service access token
            self.assertRaisesRegex(
                IllegalStateException,
                'Unable to find service scope from OAuth Client.*$',
                self.token_provider.get_service_access_token)
        # get account access toke
        self.assertEqual(
            self.token_provider.get_account_access_token(), ACCOUNT_AT)
        self.__stop_server(httpd)

    def testAccessTokenProviderNoSecret(self):
        global GET_INFO, POST_INFO
        GET_INFO = NEW_PSM_INFO
        POST_INFO = None
        httpd, port = self.__find_port_start_server(TokenHandler)

        self.base = 'http://localhost:' + str(port)
        self.token_provider = DefaultAccessTokenProvider(
            idcs_url=self.base, creds_provider=self.creds_provider)
        if version_info.major == 2:
            self.assertRaisesRegexp(
                IllegalStateException,
                'Account metadata doesn\'t have a secret,.*$',
                self.token_provider.get_account_access_token)
        else:
            self.assertRaisesRegex(
                IllegalStateException,
                'Account metadata doesn\'t have a secret,.*$',
                self.token_provider.get_account_access_token)
        self.__stop_server(httpd)

    def testAccessTokenProviderWithServerError(self):
        global GET_INFO, POST_INFO
        GET_INFO = CLIENT_INFO
        POST_INFO = '{"server_error"}'
        httpd, port = self.__find_port_start_server(TokenHandler)

        self.base = 'http://localhost:' + str(port)
        self.token_provider = DefaultAccessTokenProvider(
            idcs_url=self.base, entitlement_id='123456789',
            creds_provider=self.creds_provider)
        if version_info.major == 2:
            self.assertRaisesRegexp(
                InvalidAuthorizationException,
                '^.*IDCS error response: \{"server_error"\}.*$',
                self.token_provider.get_service_access_token)
        else:
            self.assertRaisesRegex(
                InvalidAuthorizationException,
                '^.*IDCS error response: \{"server_error"\}.*$',
                self.token_provider.get_service_access_token)
        self.__stop_server(httpd)

    def testAccessTokenProviderNoPSMInfo(self):
        global GET_INFO, POST_INFO
        GET_INFO = EMPTY_INFO
        POST_INFO = None
        httpd, port = self.__find_port_start_server(TokenHandler)

        self.base = 'http://localhost:' + str(port)
        self.token_provider = DefaultAccessTokenProvider(
            idcs_url=self.base, entitlement_id='123456789',
            creds_provider=self.creds_provider)
        if version_info.major == 2:
            self.assertRaisesRegexp(
                IllegalStateException,
                'Account metadata response contains invalid value: .*$',
                self.token_provider.get_account_access_token)
        else:
            self.assertRaisesRegex(
                IllegalStateException,
                'Account metadata response contains invalid value: .*$',
                self.token_provider.get_account_access_token)
        self.__stop_server(httpd)

    def testAccessTokenProviderOldPath(self):
        global GET_INFO, POST_INFO
        GET_INFO = PSM_INFO
        POST_INFO = None
        httpd, port = self.__find_port_start_server(TokenHandler)

        self.base = 'http://localhost:' + str(port)
        self.token_provider = DefaultAccessTokenProvider(
            idcs_url=self.base, entitlement_id='123456789',
            creds_provider=self.creds_provider)
        # get authorization string for ListTablesRequest
        result = self.token_provider.get_authorization_string(
            ListTablesRequest())
        self.assertIsNotNone(result)
        self.assertEqual(result, 'Bearer ' + ACCOUNT_AT)
        # get authorization string for TableRequest
        result = self.token_provider.get_authorization_string(TableRequest())
        self.assertIsNotNone(result)
        self.assertEqual(result, 'Bearer ' + SERVICE_AT)
        self.__stop_server(httpd)

    if idcs_url() is not None:
        def testRealCloudGetAuthorizationStringAndToken(self):
            generate_properties_file(idcs_url(), credentials_file)
            self.token_provider = DefaultAccessTokenProvider(
                idcs_props_file=properties_file)
            # get authorization string for ListTablesRequest
            result = self.token_provider.get_authorization_string(
                ListTablesRequest())
            self.assertIsNotNone(result)
            # get authorization string for TableRequest
            result = self.token_provider.get_authorization_string(
                TableRequest())
            self.assertIsNotNone(result)
            # get account access token
            result = self.token_provider.get_account_access_token()
            self.assertIsNotNone(result)
            # get service access token
            result = self.token_provider.get_service_access_token()
            self.assertIsNotNone(result)

    def __find_port_start_server(self, token_handler):
        port = 8000
        while True:
            try:
                httpd = TCPServer(('', port), token_handler)
            except error:
                port += 1
            else:
                break
        thread = Thread(target=httpd.serve_forever)
        thread.setDaemon(True)
        thread.start()
        return httpd, port

    def __stop_server(self, httpd):
        httpd.shutdown()
        httpd.server_close()


class TokenHandler(SimpleHTTPRequestHandler, object):
    def do_GET(self):
        rawpath = self.path.split('?')[0]
        if rawpath == APP_ENDPOINT:
            self.send_response(codes.ok)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(GET_INFO)))
            self.end_headers()
            self.wfile.write(GET_INFO.encode())

    def do_POST(self):
        rawpath = self.path.split('?')[0]
        content = self.rfile.read(int(self.headers['Content-Length']))
        content = unquote(content.decode())
        if rawpath == TOKEN_ENDPOINT:
            if POST_INFO is None:
                if 'andc' in content:
                    res = str.format(TOKEN_RESULT, SERVICE_AT)
                elif 'opc:idm' in content:
                    res = str.format(TOKEN_RESULT, CLIENT_AT)
                else:
                    res = str.format(TOKEN_RESULT, ACCOUNT_AT)
                self.send_response(codes.ok)
            else:
                res = POST_INFO
                self.send_response(codes.bad)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(res)))
            self.end_headers()
            self.wfile.write(res.encode())

    def log_request(self, code='-', size='-'):
        pass


if __name__ == '__main__':
    unittest.main()
