#
# Copyright (C) 2018, 2019 Oracle and/or its affiliates. All rights reserved.
#
# This software is licensed with the Universal Permissive License (UPL) version 1.0
#
# Please see LICENSE.txt file included in the top-level directory of the
# appropriate download for a copy of the license and additional information.
#

import unittest
from os import path, remove
try:
    from urllib import quote
except ImportError:
    from urllib.parse import quote

from borneo import IllegalArgumentException, idcs
from parameters import credentials_file
from testutils import (
    andc_client_id, andc_client_secret, andc_username, andc_user_pwd,
    generate_credentials_file)


class TestPropertiesCredentialsProvider(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._credentials_file = credentials_file + '_test'
        generate_credentials_file(cls._credentials_file)

    @classmethod
    def tearDownClass(cls):
        remove(cls._credentials_file)

    def setUp(self):
        self.credentials_file = credentials_file + '_test'
        self.provider = idcs.PropertiesCredentialsProvider()

    def tearDown(self):
        self.provider = None

    def testCredentialsProviderSetIllegalPropertiesFile(self):
        self.assertRaises(IllegalArgumentException,
                          self.provider.set_properties_file, 0)
        self.assertRaises(IllegalArgumentException,
                          self.provider.set_properties_file, 'abc')

    def testCredentialsProviderStoreIllegalServiceRefreshToken(self):
        self.assertRaises(IllegalArgumentException,
                          self.provider.store_service_refresh_token, 0)

    def testCredentialsProviderGetOAuthClientCredentials(self):
        self.provider.set_properties_file(self.credentials_file)
        creds = self.provider.get_oauth_client_credentials()
        self.assertEqual(creds.get_credential_alias(), andc_client_id)
        self.assertEqual(creds.get_secret(), andc_client_secret)

    def testCredentialsProviderGetUserCredentials(self):
        self.provider.set_properties_file(self.credentials_file)
        creds = self.provider.get_user_credentials()
        self.assertEqual(creds.get_credential_alias(), andc_username)
        self.assertEqual(creds.get_secret(), quote(andc_user_pwd.encode()))

    def testCredentialsProviderGetServiceRefreshToken(self):
        self.provider.set_properties_file(self.credentials_file)
        self.assertIsNone(self.provider.get_service_refresh_token())

    def testCredentialsProviderStoreServiceRefreshToken(self):
        test_refresh_token = 'test-refresh-token'
        self.provider.set_properties_file(self.credentials_file)
        self.provider.store_service_refresh_token(test_refresh_token)
        creds = self.provider.get_oauth_client_credentials()
        self.assertEqual(creds.get_credential_alias(), andc_client_id)
        self.assertEqual(creds.get_secret(), andc_client_secret)
        creds = self.provider.get_user_credentials()
        self.assertEqual(creds.get_credential_alias(), andc_username)
        self.assertEqual(creds.get_secret(), quote(andc_user_pwd.encode()))
        self.assertEqual(self.provider.get_service_refresh_token(),
                         test_refresh_token)

    def testCredentialsProviderFormatCredentialsFile(self):
        # generate a credentials file with spaces in each line.
        tmp = 'tmp'
        if path.exists(tmp):
            remove(tmp)
        with open(tmp, 'w') as f:
            f.write('andc_client_id    =    ' + andc_client_id + '\n')
            f.write('andc_client_secret    =' + andc_client_secret + '\n')
            f.write('andc_username=    ' + andc_username + '\n')
            f.write('andc_user_pwd' + andc_user_pwd + '\n')
        # check the get results.
        self.provider.set_properties_file(tmp)
        creds = self.provider.get_oauth_client_credentials()
        self.assertEqual(creds.get_credential_alias(), andc_client_id)
        self.assertEqual(creds.get_secret(), andc_client_secret)
        self.assertIsNone(self.provider.get_user_credentials())
        self.assertIsNone(self.provider.get_service_refresh_token())
        remove(tmp)


if __name__ == '__main__':
    unittest.main()
