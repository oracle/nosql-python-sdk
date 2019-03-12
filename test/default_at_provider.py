#
# Copyright (C) 2018, 2019 Oracle and/or its affiliates. All rights reserved.
#
# This software is licensed with the Universal Permissive License (UPL) version 1.0
#
# Please see LICENSE.txt file included in the top-level directory of the
# appropriate download for a copy of the license and additional information.
#

import unittest
from os import remove
from requests import ConnectionError
from socket import error
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
from parameters import (
    credentials_file, entitlement_id, idcs_url, properties_file)
from testutils import generate_credentials_file, generate_properties_file


class TestDefaultAccessTokenProvider(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        global TOKEN_RESULT, PSM_INFO

        TOKEN_RESULT = ('{{"access_token": "{0}", "refresh_token": "{1}", \
"expires_in": "100", "token_type": "Bearer"}}')
        PSM_INFO = ('{\n' +
                    ' "schemas": [\n' +
                    ' "urn:ietf:params:scim:api:messages:2.0:ListResponse"\n' +
                    ' ],\n' +
                    ' "totalResults": 1,\n' +
                    ' "Resources": [\n' +
                    ' {\n' +
                    '  "audience": "http://psm",\n' +
                    '  "name": "PSMApp-cacct12345",\n' +
                    '  "clientSecret": "abcdefghi"\n' +
                    ' }\n' +
                    ' ]\n' +
                    '}')

    def setUp(self):
        generate_credentials_file()
        self.base = 'http://localhost:' + str(8000)
        generate_properties_file(self.base)
        self.creds_provider = PropertiesCredentialsProvider(
        ).set_properties_file(credentials_file)
        self.token_provider = None

    def tearDown(self):
        remove(credentials_file)
        remove(properties_file)
        if self.token_provider is not None:
            self.token_provider.close()
            self.token_provider = None

    def testAccessTokenProviderIllegalInit(self):
        # illegal idcs props file
        self.assertRaises(IllegalArgumentException, DefaultAccessTokenProvider,
                          {'properties_file': properties_file})
        # illegal entitlement id
        self.assertRaises(IllegalArgumentException, DefaultAccessTokenProvider,
                          entitlement_id=0, idcs_url=self.base)

        # illegal idcs url
        self.assertRaises(IllegalArgumentException, DefaultAccessTokenProvider,
                          entitlement_id=entitlement_id, idcs_url=None)
        # illegal use refresh token flag
        self.assertRaises(IllegalArgumentException, DefaultAccessTokenProvider,
                          entitlement_id=entitlement_id, idcs_url=self.base,
                          use_refresh_token='IllegalUseRefreshToken')
        # illegal properties credentials provider
        self.assertRaises(IllegalArgumentException, DefaultAccessTokenProvider,
                          entitlement_id=entitlement_id, idcs_url=self.base,
                          creds_provider='IllegalPropertiesCredentialsProvider')
        # illegal timeout
        self.assertRaises(IllegalArgumentException, DefaultAccessTokenProvider,
                          entitlement_id=entitlement_id, idcs_url=self.base,
                          timeout_ms='IllegalTimeout')
        self.assertRaises(IllegalArgumentException, DefaultAccessTokenProvider,
                          entitlement_id=entitlement_id, idcs_url=self.base,
                          timeout_ms=0)
        self.assertRaises(IllegalArgumentException, DefaultAccessTokenProvider,
                          entitlement_id=entitlement_id, idcs_url=self.base,
                          timeout_ms=-1)
        # illegal cache duration seconds
        self.assertRaises(IllegalArgumentException, DefaultAccessTokenProvider,
                          entitlement_id=entitlement_id, idcs_url=self.base,
                          cache_duration_seconds='IllegalCacheSeconds')
        self.assertRaises(IllegalArgumentException, DefaultAccessTokenProvider,
                          entitlement_id=entitlement_id, idcs_url=self.base,
                          cache_duration_seconds=0)
        self.assertRaises(IllegalArgumentException, DefaultAccessTokenProvider,
                          entitlement_id=entitlement_id, idcs_url=self.base,
                          cache_duration_seconds=-1)
        self.assertRaises(IllegalArgumentException, DefaultAccessTokenProvider,
                          entitlement_id=entitlement_id, idcs_url=self.base,
                          cache_duration_seconds=85401)
        # illegal refresh ahead
        self.assertRaises(IllegalArgumentException, DefaultAccessTokenProvider,
                          entitlement_id=entitlement_id, idcs_url=self.base,
                          refresh_ahead='IllegalRefreshAhead')
        self.assertRaises(IllegalArgumentException, DefaultAccessTokenProvider,
                          entitlement_id=entitlement_id, idcs_url=self.base,
                          refresh_ahead=0)
        self.assertRaises(IllegalArgumentException, DefaultAccessTokenProvider,
                          entitlement_id=entitlement_id, idcs_url=self.base,
                          refresh_ahead=-1)

    def testAccessTokenProviderSetIllegalCredentialsProvider(self):
        self.token_provider = DefaultAccessTokenProvider(properties_file)
        self.assertRaises(IllegalArgumentException,
                          self.token_provider.set_credentials_provider,
                          'IllegalCredentialsProvider')

    def testAccessTokenProviderSetIllegalLogger(self):
        self.token_provider = DefaultAccessTokenProvider(
            entitlement_id=entitlement_id, idcs_url=self.base)
        self.assertRaises(IllegalArgumentException,
                          self.token_provider.set_logger, 'IllegalLogger')

    def testAccessTokenProviderGetAuthorizationString(self):
        account_at = 'account-at'
        service_at = 'service-at'
        refresh_token = 'refresh_token'

        class TokenHandler(SimpleHTTPRequestHandler):
            def do_GET(self):
                rawpath = self.path.split('?')[0]
                if rawpath == '/admin/v1/Apps':
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                    self.send_header('Content-Length', str(len(PSM_INFO)))
                    self.end_headers()
                    self.wfile.write(PSM_INFO.encode())

            def do_POST(self):
                rawpath = self.path.split('?')[0]
                if rawpath == '/oauth2/v1/token':
                    content = self.rfile.read(
                        int(self.headers['content-length']))
                    content = unquote(content.decode())
                    if 'andc' in content or 'refresh_token' in content:
                        res = str.format(TOKEN_RESULT, service_at,
                                         refresh_token)
                    else:
                        res = str.format(TOKEN_RESULT, account_at,
                                         refresh_token)
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                    self.send_header('Content-Length', str(len(res)))
                    self.end_headers()
                    self.wfile.write(res.encode())
        httpd, port = self.__find_port_start_server(TokenHandler)

        self.base = 'http://localhost:' + str(port)
        # get authorization string for ListTablesRequest
        self.token_provider = DefaultAccessTokenProvider(
            entitlement_id=entitlement_id, idcs_url=self.base,
            use_refresh_token=True, creds_provider=self.creds_provider)
        result = self.token_provider.get_authorization_string(
            ListTablesRequest())
        self.assertIsNotNone(result)
        self.assertEqual(result, 'Bearer ' + account_at)
        # get authorization string for TableRequest
        result = self.token_provider.get_authorization_string(TableRequest())
        self.assertIsNotNone(result)
        self.assertEqual(result, 'Bearer ' + service_at)
        self.assertEqual(self.creds_provider.get_service_refresh_token(),
                         refresh_token)
        self.__stop_server(httpd)

    def testAccessTokenProviderGetAccountAccessToken(self):
        account_at = 'account-at'
        refresh_token = 'refresh_token'

        class TokenHandler(SimpleHTTPRequestHandler):
            def do_GET(self):
                rawpath = self.path.split('?')[0]
                if rawpath == '/admin/v1/Apps':
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                    self.send_header('Content-Length', str(len(PSM_INFO)))
                    self.end_headers()
                    self.wfile.write(PSM_INFO.encode())

            def do_POST(self):
                rawpath = self.path.split('?')[0]
                if rawpath == '/oauth2/v1/token':
                    res = str.format(TOKEN_RESULT, account_at, refresh_token)
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                    self.send_header('Content-Length', str(len(res)))
                    self.end_headers()
                    self.wfile.write(res.encode())
        httpd, port = self.__find_port_start_server(TokenHandler)

        # connect to illegal idcs url
        self.base = 'http://localhost:80'
        generate_properties_file(self.base)
        self.token_provider = DefaultAccessTokenProvider(properties_file)
        self.assertRaises(ConnectionError,
                          self.token_provider.get_account_access_token)

        self.base = 'http://localhost:' + str(port)
        # connect to legal idcs url, use_refresh_token = False
        generate_properties_file(self.base)
        self.token_provider = DefaultAccessTokenProvider(properties_file)
        result = self.token_provider.get_account_access_token()
        self.assertIsNotNone(result)
        self.assertEqual(result, account_at)
        self.assertIsNone(self.creds_provider.get_service_refresh_token())
        # connect to legal idcs url, use_refresh_token = True
        self.token_provider = DefaultAccessTokenProvider(
            entitlement_id=entitlement_id, idcs_url=self.base,
            use_refresh_token=True, creds_provider=self.creds_provider)
        result = self.token_provider.get_account_access_token()
        self.assertIsNotNone(result)
        self.assertEqual(result, account_at)
        self.assertEqual(self.creds_provider.get_service_refresh_token(),
                         refresh_token)
        self.__stop_server(httpd)

    def testAccessTokenProviderGetServiceAccessToken(self):
        service_at = 'service-at'
        refresh_token = 'refresh_token'

        class TokenHandler(SimpleHTTPRequestHandler):
            def do_POST(self):
                rawpath = self.path.split('?')[0]
                if rawpath == '/oauth2/v1/token':
                    res = str.format(TOKEN_RESULT, service_at, refresh_token)
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                    self.send_header('Content-Length', str(len(res)))
                    self.end_headers()
                    self.wfile.write(res.encode())
        httpd, port = self.__find_port_start_server(TokenHandler)

        # connect to illegal idcs url
        self.base = 'http://localhost:80'
        self.token_provider = DefaultAccessTokenProvider(
            entitlement_id=entitlement_id, idcs_url=self.base,
            use_refresh_token=True, creds_provider=self.creds_provider)
        self.assertRaises(ConnectionError,
                          self.token_provider.get_service_access_token)

        self.base = 'http://localhost:' + str(port)
        # connect to legal idcs url, use_refresh_token = False
        generate_properties_file(self.base)
        self.token_provider = DefaultAccessTokenProvider(properties_file)
        result = self.token_provider.get_service_access_token()
        self.assertIsNotNone(result)
        self.assertEqual(result, service_at)
        self.assertIsNone(self.creds_provider.get_service_refresh_token())
        # connect to legal idcs url, use_refresh_token = True
        self.token_provider = DefaultAccessTokenProvider(
            entitlement_id=entitlement_id, idcs_url=self.base,
            use_refresh_token=True, creds_provider=self.creds_provider)
        result = self.token_provider.get_service_access_token()
        self.assertIsNotNone(result)
        self.assertEqual(result, service_at)
        self.assertEqual(self.creds_provider.get_service_refresh_token(),
                         refresh_token)
        self.__stop_server(httpd)

    def testAccessTokenProviderWithServerError(self):
        error_msg = '{"server_error"}'

        class TokenHandler(SimpleHTTPRequestHandler):
            def do_POST(self):
                rawpath = self.path.split('?')[0]
                if rawpath == '/oauth2/v1/token':
                    self.send_response(400)
                    self.send_header('Content-Type', 'application/json')
                    self.send_header('Content-Length', str(len(error_msg)))
                    self.end_headers()
                    self.wfile.write(error_msg.encode())
        httpd, port = self.__find_port_start_server(TokenHandler)

        self.base = 'http://localhost:' + str(port)
        self.token_provider = DefaultAccessTokenProvider(
            entitlement_id=entitlement_id, idcs_url=self.base,
            use_refresh_token=True, creds_provider=self.creds_provider)
        self.assertRaisesRegexp(InvalidAuthorizationException,
                                '^.*IDCS error response: \{"server_error"\}.*$',
                                self.token_provider.get_service_access_token)
        self.__stop_server(httpd)

    def testAccessTokenProviderWithNoPSMInfo(self):
        account_at = 'account-at'
        service_at = 'service-at'
        refresh_token = 'refresh_token'

        class TokenHandler(SimpleHTTPRequestHandler):
            def do_POST(self):
                rawpath = self.path.split('?')[0]
                if rawpath == '/oauth2/v1/token':
                    content = self.rfile.read(
                        int(self.headers['content-length']))
                    content = unquote(content.decode())
                    if 'andc' in content or 'refresh_token' in content:
                        res = str.format(TOKEN_RESULT, service_at,
                                         refresh_token)
                    else:
                        res = str.format(TOKEN_RESULT, account_at,
                                         refresh_token)
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/json')
                    self.send_header('Content-Length', str(len(res)))
                    self.end_headers()
                    self.wfile.write(res.encode())
        httpd, port = self.__find_port_start_server(TokenHandler)

        self.base = 'http://localhost:' + str(port)
        self.token_provider = DefaultAccessTokenProvider(
            entitlement_id=entitlement_id, idcs_url=self.base,
            use_refresh_token=True, creds_provider=self.creds_provider)
        # get service access token
        result = self.token_provider.get_service_access_token()
        self.assertIsNotNone(result)
        self.assertEqual(result, service_at)
        self.assertEqual(self.creds_provider.get_service_refresh_token(),
                         refresh_token)
        # get account access token
        self.assertRaisesRegexp(
            IllegalStateException, '^[\s\S]*Error code[\s\S]*<p>Message: ' +
            'File not found.[\s\S]*Error code explanation:[\s\S]*$',
            self.token_provider.get_account_access_token)
        self.__stop_server(httpd)

    if idcs_url is not None:
        def testRealCloudGetAuthorizationStringAndToken(self):
            # illegal entitlement_id
            self.token_provider = DefaultAccessTokenProvider(
                entitlement_id='123456789', idcs_url=idcs_url,
                creds_provider=self.creds_provider)
            self.assertRaisesRegexp(
                InvalidAuthorizationException,
                '^.*\{"error":"invalid_scope",' +
                '"error_description":"Invalid scope."\}.*$',
                self.token_provider.get_authorization_string, TableRequest())
            # legal entitlement_id
            generate_properties_file(idcs_url)
            self.token_provider = DefaultAccessTokenProvider(
                idcs_props_file=properties_file, use_refresh_token=True)
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
            # Check service refresh token
            self.assertIsNone(self.creds_provider.get_service_refresh_token())

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


if __name__ == '__main__':
    unittest.main()
