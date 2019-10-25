#
# Copyright (C) 2018, 2019 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
#
# Please see LICENSE.txt file included in the top-level directory of the
# appropriate download for a copy of the license and additional information.
#

import unittest
from oci.auth.signers import InstancePrincipalsSecurityTokenSigner
from oci.config import from_file
from os import path, remove
from time import sleep

from borneo import IllegalArgumentException, NoSQLHandleConfig, TableRequest
from borneo.iam import SignatureProvider

from parameters import iam_principal, tenant_id
from testutils import fake_credentials_file, fake_key_file


class TestSignatureProvider(unittest.TestCase):
    def setUp(self):
        self.base = 'http://localhost:' + str(8000)
        self._generate_credentials_file()
        self.token_provider = None
        # Not matter which request.
        self.request = TableRequest().set_compartment_id(tenant_id)
        self.handle_config = NoSQLHandleConfig(self.base)

    def tearDown(self):
        remove(fake_credentials_file)
        if self.token_provider is not None:
            self.token_provider.close()
            self.token_provider = None

    def testAccessTokenProviderIllegalInit(self):
        # illegal provider
        self.assertRaises(IllegalArgumentException, SignatureProvider,
                          'IllegalProvider')
        # illegal config_file
        self.assertRaises(IllegalArgumentException, SignatureProvider,
                          config_file={'config_file': fake_credentials_file})
        # illegal profile_name
        self.assertRaises(IllegalArgumentException, SignatureProvider,
                          profile_name={'profile_name': 'DEFAULT'})
        # illegal cache duration seconds
        self.assertRaises(IllegalArgumentException, SignatureProvider,
                          duration_seconds='IllegalDurationSeconds')
        self.assertRaises(IllegalArgumentException, SignatureProvider,
                          duration_seconds=0)
        self.assertRaises(IllegalArgumentException, SignatureProvider,
                          duration_seconds=-1)
        self.assertRaises(IllegalArgumentException, SignatureProvider,
                          duration_seconds=301)
        # illegal refresh ahead
        self.assertRaises(IllegalArgumentException, SignatureProvider,
                          refresh_ahead='IllegalRefreshAhead')
        self.assertRaises(IllegalArgumentException, SignatureProvider,
                          refresh_ahead=0)
        self.assertRaises(IllegalArgumentException, SignatureProvider,
                          refresh_ahead=-1)
        # both provider and config_file
        self.assertRaises(IllegalArgumentException, SignatureProvider,
                          {}, fake_credentials_file)

    def testAccessTokenProviderSetIllegalLogger(self):
        self.token_provider = SignatureProvider(
            config_file=fake_credentials_file)
        self.assertRaises(IllegalArgumentException,
                          self.token_provider.set_logger, 'IllegalLogger')

    def testAccessTokenProviderGetAuthorizationStringWithIllegalRequest(self):
        provider = from_file(file_location=fake_credentials_file)
        self.token_provider = SignatureProvider(provider)
        self.assertRaises(IllegalArgumentException,
                          self.token_provider.get_authorization_string,
                          'IllegalRequest')

    def testAccessTokenProviderGets(self):
        self.token_provider = SignatureProvider(
            config_file=fake_credentials_file)
        self.assertIsNone(self.token_provider.get_logger())

    def testAccessTokenProviderGetAuthorizationString(self):
        self.token_provider = SignatureProvider(
            config_file=fake_credentials_file, duration_seconds=5,
            refresh_ahead=1)
        self.assertRaises(
            IllegalArgumentException,
            self.token_provider.get_authorization_string, self.request)
        self.token_provider.set_service_host(self.handle_config)
        auth_string = self.token_provider.get_authorization_string(
            self.request)
        # Cache duration is about 5s, string should be the same.
        self.assertEqual(
            auth_string,
            self.token_provider.get_authorization_string(self.request))
        # Wait for the refresh to complete.
        sleep(5)
        # The new signature string should be cached.
        self.assertNotEqual(
            auth_string,
            self.token_provider.get_authorization_string(self.request))

    if iam_principal() == 'instance principal':
        def testInstancePrincipalGetAuthorizationString(self):
            signer = InstancePrincipalsSecurityTokenSigner()
            self.token_provider = SignatureProvider(
                signer, duration_seconds=5, refresh_ahead=1)
            self.assertRaises(
                IllegalArgumentException,
                self.token_provider.get_authorization_string, self.request)
            self.token_provider.set_service_host(self.handle_config)
            auth_string = self.token_provider.get_authorization_string(
                self.request)
            # Cache duration is about 5s, string should be the same.
            self.assertEqual(
                auth_string,
                self.token_provider.get_authorization_string(self.request))
            # Wait for the refresh to complete.
            sleep(5)
            # The new signature string should be cached.
            self.assertNotEqual(
                auth_string,
                self.token_provider.get_authorization_string(self.request))

    def _generate_credentials_file(self):
        # Generate credentials file
        if path.exists(fake_credentials_file):
            remove(fake_credentials_file)

        with open(fake_credentials_file, 'w') as cred_file:
            cred_file.write('[DEFAULT]\n')
            cred_file.write('tenancy=ocid1.tenancy.oc1..tenancy\n')
            cred_file.write('user=ocid1.user.oc1..user\n')
            cred_file.write('fingerprint=fingerprint\n')
            cred_file.write('key_file=' + fake_key_file + '\n')


if __name__ == '__main__':
    unittest.main()
