#
# Copyright (c) 2018, 2022 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#
import sys
import unittest
from requests import codes
from socket import error
from threading import Thread
from time import sleep, time
try:
    # noinspection PyCompatibility
    from SimpleHTTPServer import SimpleHTTPRequestHandler
    # noinspection PyCompatibility
    from SocketServer import TCPServer
except ImportError:
    # noinspection PyCompatibility
    from http.server import SimpleHTTPRequestHandler
    # noinspection PyCompatibility
    from socketserver import TCPServer

from borneo import IllegalArgumentException
from borneo.kv import StoreAccessTokenProvider


class TestStoreAccessTokenProvider(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        global LOGIN_PATH, LOGOUT_PATH, RENEW_PATH
        LOGIN_PATH = '/V2/nosql/security/login'
        LOGOUT_PATH = '/V2/nosql/security/logout'
        RENEW_PATH = '/V2/nosql/security/renew'
        # basicAuthString matching user name test and password NoSql00__123456
        global USER_NAME, PASSWORD, BASIC_AUTH_STRING
        USER_NAME = 'test'
        PASSWORD = 'NoSql00__123456'
        BASIC_AUTH_STRING = 'Basic dGVzdDpOb1NxbDAwX18xMjM0NTY='

        global AUTH_TOKEN_PREFIX, LOGIN_TOKEN, RENEW_TOKEN
        AUTH_TOKEN_PREFIX = 'Bearer '
        LOGIN_TOKEN = 'LOGIN_TOKEN'
        RENEW_TOKEN = 'RENEW_TOKEN'

        global PORT
        PORT = cls._find_port_start_server(TokenHandler)

    @classmethod
    def tearDownClass(cls):
        if cls.httpd is not None:
            cls.httpd.shutdown()
            cls.httpd.server_close()
            cls.httpd = None

    def setUp(self):
        self.base = 'https://localhost:' + str(PORT)
        self.token_provider = None

    def tearDown(self):
        if self.token_provider is not None:
            self.token_provider.close()
            self.token_provider = None

    def testAccessTokenProviderIllegalInit(self):
        # illegal user name
        self.assertRaises(IllegalArgumentException, StoreAccessTokenProvider,
                          {'user_name': USER_NAME}, PASSWORD)
        self.assertRaises(IllegalArgumentException, StoreAccessTokenProvider,
                          '', PASSWORD)
        # illegal password
        self.assertRaises(IllegalArgumentException, StoreAccessTokenProvider,
                          USER_NAME, {'password': PASSWORD})
        self.assertRaises(IllegalArgumentException, StoreAccessTokenProvider,
                          USER_NAME, '')
        # one of the required parameters is None
        self.assertRaises(IllegalArgumentException, StoreAccessTokenProvider,
                          None, PASSWORD)

    def testAccessTokenProviderSetIllegalAutoRenew(self):
        self.token_provider = StoreAccessTokenProvider()
        self.assertRaises(IllegalArgumentException,
                          self.token_provider.set_auto_renew, 'IllegalRenew')

    def testAccessTokenProviderSetIllegalEndpoint(self):
        self.token_provider = StoreAccessTokenProvider(USER_NAME, PASSWORD)
        self.assertRaises(IllegalArgumentException,
                          self.token_provider.set_endpoint, None)
        self.assertRaises(IllegalArgumentException,
                          self.token_provider.set_endpoint,
                          {'endpoint': self.base})
        self.assertRaises(IllegalArgumentException,
                          self.token_provider.set_endpoint,
                          'localhost:notanint')
        self.assertRaises(IllegalArgumentException,
                          self.token_provider.set_endpoint, 'localhost:-1')
        self.assertRaises(IllegalArgumentException,
                          self.token_provider.set_endpoint, 'localhost:8080')
        self.assertRaises(IllegalArgumentException,
                          self.token_provider.set_endpoint, 'ttp://localhost')
        self.assertRaises(IllegalArgumentException,
                          self.token_provider.set_endpoint, 'http://localhost')
        self.assertRaises(IllegalArgumentException,
                          self.token_provider.set_endpoint,
                          'localhost:8080:foo')
        self.assertRaises(IllegalArgumentException,
                          self.token_provider.set_endpoint,
                          'https://localhost:-1:x')

    def testAccessTokenProviderSetIllegalLogger(self):
        self.token_provider = StoreAccessTokenProvider()
        self.assertRaises(IllegalArgumentException,
                          self.token_provider.set_logger, 'IllegalLogger')

    def testAccessTokenProviderGetAuthorizationStringWithIllegalRequest(self):
        self.token_provider = StoreAccessTokenProvider()
        self.assertRaises(IllegalArgumentException,
                          self.token_provider.get_authorization_string,
                          'IllegalRequest')

    def testAccessTokenProviderGets(self):
        base = 'https://localhost:80'
        self.token_provider = StoreAccessTokenProvider(
            USER_NAME, PASSWORD).set_auto_renew(False).set_endpoint(base)
        self.assertTrue(self.token_provider.is_secure())
        self.assertFalse(self.token_provider.is_auto_renew())
        self.assertEqual(self.token_provider.get_endpoint(), base)
        self.assertIsNone(self.token_provider.get_logger())

    def testAccessTokenProviderGetAuthorizationString(self):
        self.token_provider = StoreAccessTokenProvider(USER_NAME, PASSWORD)
        self.token_provider.set_endpoint(self.base)
        self.token_provider.set_url_for_test()
        # get authorization string.
        result = self.token_provider.get_authorization_string()
        self.assertIsNotNone(result)
        self.assertTrue(result.startswith(AUTH_TOKEN_PREFIX))
        self.assertEqual(result[len(AUTH_TOKEN_PREFIX):], LOGIN_TOKEN)
        # Wait for the refresh to complete
        sleep(10)
        result = self.token_provider.get_authorization_string()
        self.assertEqual(result[len(AUTH_TOKEN_PREFIX):], RENEW_TOKEN)
        self.token_provider.close()
        self.assertIsNone(self.token_provider.get_authorization_string())

    def testAccessTokenProviderMultiThreads(self):
        self.token_provider = StoreAccessTokenProvider(USER_NAME, PASSWORD)
        self.token_provider.set_endpoint(self.base)
        self.token_provider.set_url_for_test()
        threads = list()
        for i in range(5):
            t = Thread(target=self._run)
            t.start()
            threads.append(t)
        for t in threads:
            t.join()

    @classmethod
    def _find_port_start_server(cls, token_handler):
        port = 9000
        while True:
            try:
                cls.httpd = TCPServer(('', port), token_handler)
            except error:
                port += 1
            else:
                break
        thread = Thread(target=cls.httpd.serve_forever)
        if sys.version_info[0] < 3 or (sys.version_info[0] == 3 and
                                       sys.version_info[1] < 10):
            thread.setDaemon(True)
        else:
            thread.daemon = True
        thread.start()
        return port

    def _run(self):
        try:
            for i in range(5):
                self.token_provider.bootstrap_login()
        finally:
            self.token_provider.close()


class TokenHandler(SimpleHTTPRequestHandler, object):

    def do_GET(self):
        rawpath = self.path.split('?')[0]
        auth_string = self.headers['Authorization']
        if rawpath == LOGIN_PATH:
            assert auth_string == BASIC_AUTH_STRING
            self._generate_login_token(LOGIN_TOKEN)
        elif rawpath == RENEW_PATH:
            assert auth_string.startswith(AUTH_TOKEN_PREFIX)
            self._generate_login_token(RENEW_TOKEN)
        elif rawpath == LOGOUT_PATH:
            assert auth_string.startswith(AUTH_TOKEN_PREFIX)
            self.send_response(codes.ok)

    def log_request(self, code='-', size='-'):
        pass

    def _generate_login_token(self, token_text):
        expire_time = int(round(time() * 1000)) + 15000
        content = ('{"token": "' + token_text + '", "expireAt": ' +
                   str(expire_time) + '}')
        self.send_response(codes.ok)
        self.send_header('Content-Length', str(len(content)))
        self.end_headers()
        self.wfile.write(content.encode())


if __name__ == '__main__':
    unittest.main()
