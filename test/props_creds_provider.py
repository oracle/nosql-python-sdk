#
# Copyright (C) 2018, 2019 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
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

from borneo import IllegalArgumentException
from borneo.idcs import PropertiesCredentialsProvider
from testutils import (
    andc_client_id, andc_client_secret, andc_username, andc_user_pwd,
    fake_credentials_file, generate_credentials_file)


class TestPropertiesCredentialsProvider(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        generate_credentials_file()

    @classmethod
    def tearDownClass(cls):
        remove(fake_credentials_file)

    def setUp(self):
        self.provider = PropertiesCredentialsProvider()

    def tearDown(self):
        self.provider = None

    def testCredentialsProviderSetIllegalPropertiesFile(self):
        self.assertRaises(IllegalArgumentException,
                          self.provider.set_properties_file, 0)
        self.assertRaises(IllegalArgumentException,
                          self.provider.set_properties_file, 'abc')

    def testCredentialsProviderGetOAuthClientCredentials(self):
        self.provider.set_properties_file(fake_credentials_file)
        creds = self.provider.get_oauth_client_credentials()
        self._check_client_credentials(creds)

    def testCredentialsProviderGetUserCredentials(self):
        self.provider.set_properties_file(fake_credentials_file)
        creds = self.provider.get_user_credentials()
        self.assertEqual(creds.get_credential_alias(), andc_username)
        self.assertEqual(creds.get_secret(), quote(andc_user_pwd.encode()))

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
        self._check_client_credentials(creds)
        self.assertIsNone(self.provider.get_user_credentials())
        remove(tmp)

    def _check_client_credentials(self, creds):
        self.assertEqual(creds.get_credential_alias(), andc_client_id)
        self.assertEqual(creds.get_secret(), andc_client_secret)


if __name__ == '__main__':
    unittest.main()
