#
# Copyright (C) 2018, 2019 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
#
# Please see LICENSE.txt file included in the top-level directory of the
# appropriate download for a copy of the license and additional information.
#

import unittest
from requests import codes
from socket import error
from struct import pack, unpack
from threading import Thread
from time import sleep, time
try:
    from SimpleHTTPServer import SimpleHTTPRequestHandler
    from SocketServer import TCPServer
except ImportError:
    from http.server import SimpleHTTPRequestHandler
    from socketserver import TCPServer

from borneo import IllegalArgumentException
from borneo.idcs import StoreAccessTokenProvider


class TestDefaultAccessTokenProvider(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        global LOGIN_PATH, LOGOUT_PATH, RENEW_PATH
        LOGIN_PATH = '/V0/nosql/security/login'
        LOGOUT_PATH = '/V0/nosql/security/logout'
        RENEW_PATH = '/V0/nosql/security/renew'
        # basicAuthString matching user name test and password NoSql00__123456
        global USER_NAME, PASSWORD, BASIC_AUTH_STRING
        USER_NAME = 'test'
        PASSWORD = 'NoSql00__123456'
        BASIC_AUTH_STRING = 'Basic dGVzdDpOb1NxbDAwX18xMjM0NTY='

        global AUTH_TOKEN_PREFIX, LOGIN_TOKEN, RENEW_TOKEN
        AUTH_TOKEN_PREFIX = 'Bearer '
        LOGIN_TOKEN = 'LOGIN_TOKEN'
        RENEW_TOKEN = 'RENEW_TOKEN'

    def setUp(self):
        self.base = 'http://localhost:' + str(8080)
        self.token_provider = None

    def tearDown(self):
        if self.token_provider is not None:
            self.token_provider.close()
            self.token_provider = None

    def testAccessTokenProviderIllegalInit(self):
        # illegal user name
        self.assertRaises(IllegalArgumentException, StoreAccessTokenProvider, 0,
                          PASSWORD, self.base)
        # illegal password
        self.assertRaises(IllegalArgumentException, StoreAccessTokenProvider,
                          USER_NAME, 0, self.base)
        # illegal login url
        self.assertRaises(IllegalArgumentException, StoreAccessTokenProvider,
                          USER_NAME, PASSWORD, 0)
        # illegal security base url
        self.assertRaises(IllegalArgumentException, StoreAccessTokenProvider,
                          USER_NAME, PASSWORD, self.base, 0)
        # illegal logger
        self.assertRaises(IllegalArgumentException, StoreAccessTokenProvider,
                          USER_NAME, PASSWORD, self.base, 'SecurityBaseUrl',
                          'IllegalLogger')
        # one of the required parameters is None
        self.assertRaises(IllegalArgumentException, StoreAccessTokenProvider,
                          None, PASSWORD, self.base)

    def testAccessTokenProviderSetIllegalAutoRenew(self):
        self.token_provider = StoreAccessTokenProvider()
        self.assertRaises(IllegalArgumentException,
                          self.token_provider.set_auto_renew, 'IllegalRenew')

    def testAccessTokenProviderSetIllegalLogger(self):
        self.token_provider = StoreAccessTokenProvider()
        self.assertRaises(IllegalArgumentException,
                          self.token_provider.set_logger, 'IllegalLogger')

    def testAccessTokenProviderGetAuthorizationStringWithIllegalRequest(self):
        self.token_provider = StoreAccessTokenProvider()
        self.assertRaises(IllegalArgumentException,
                          self.token_provider.get_authorization_string,
                          'IllegalRequest')

    def testAccessTokenProviderGetAuthorizationString(self):
        class TokenHandler(SimpleHTTPRequestHandler):
            def do_GET(self):
                rawpath = self.path.split('?')[0]
                if rawpath == LOGIN_PATH:
                    self.__generate_login_token(LOGIN_TOKEN)
                elif rawpath == RENEW_PATH:
                    self.__generate_login_token(RENEW_TOKEN)
                elif rawpath == LOGOUT_PATH:
                    self.send_response(codes.ok)

            def __generate_login_token(self, token_text):
                content = bytearray()
                bos = ByteOutputStream(content)
                bos.write_short_int(1)
                bos.write_long(int(round(time() * 1000)) + 15000)
                try:
                    buf = bytearray(token_text.encode())
                except UnicodeDecodeError:
                    buf = bytearray(token_text)
                bos.write_bytearray(buf)
                try:
                    hex_str = content.hex()
                except AttributeError:
                    hex_str = str(content).encode('hex')
                self.send_response(codes.ok)
                self.send_header('Content-Length', str(len(hex_str)))
                self.end_headers()
                self.wfile.write(hex_str.encode())

        httpd, port = self.__find_port_start_server(TokenHandler)

        self.base = 'http://localhost:' + str(port)
        self.token_provider = StoreAccessTokenProvider(
            USER_NAME, PASSWORD, self.base)
        # get authorization string.
        result = self.token_provider.get_authorization_string()
        self.assertIsNotNone(result)
        self.assertTrue(result.startswith(AUTH_TOKEN_PREFIX))
        self.assertEqual(self.__read_token_from_auth(result), LOGIN_TOKEN)
        # Wait for the refresh to complete
        sleep(10)
        result = self.token_provider.get_authorization_string()
        self.assertEqual(self.__read_token_from_auth(result), RENEW_TOKEN)
        self.token_provider.close()
        self.assertIsNone(self.token_provider.get_authorization_string())
        self.__stop_server(httpd)

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

    def __read_token_from_auth(self, auth_string):
        token = auth_string[len(AUTH_TOKEN_PREFIX):]
        buf = bytearray.fromhex(token)
        bis = ByteInputStream(buf)
        bis.read_short_int()
        bis.read_long()
        return bis.read_all().decode()

    def __stop_server(self, httpd):
        httpd.shutdown()
        httpd.server_close()


class ByteInputStream:
    """
    The ByteInputStream provides methods to get data with different type from
    a bytearray.
    """

    def __init__(self, content):
        self.__content = content

    def read_all(self):
        return self.__content

    def read_fully(self, buf):
        for index in range(len(buf)):
            buf[index] = self.__content.pop(0)

    def read_long(self):
        buf = bytearray(8)
        self.read_fully(buf)
        res, = unpack('>q', buf)
        return res

    def read_short_int(self):
        buf = bytearray(2)
        self.read_fully(buf)
        res, = unpack('>h', buf)
        return res


class ByteOutputStream:
    """
    The ByteOutputStream provides methods to write data with different type into
    a bytearray.
    """

    def __init__(self, content):
        self.__content = content

    def write_bytearray(self, value):
        for index in range(len(value)):
            self.__content.append(value[index])

    def write_long(self, value):
        val_s = pack('>q', value)
        self.write_value(val_s)

    def write_short_int(self, value):
        val_s = pack('>h', value)
        self.write_value(val_s)

    def write_value(self, value):
        val_b = bytearray(value)
        self.write_bytearray(val_b)


if __name__ == '__main__':
    unittest.main()
