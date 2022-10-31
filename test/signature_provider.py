#
# Copyright (c) 2018, 2022 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#
import sys
import unittest
from os import path, remove
from threading import Condition, Thread
from time import sleep
try:
    import oci
    found = True
except ImportError:
    oci = None
    found = False

from borneo import (
    IllegalArgumentException, NoSQLHandleConfig, Regions, TableRequest)
from borneo.iam import SignatureProvider
from parameters import iam_principal
from testutils import fake_credentials_file, fake_key_file


class TestSignatureProvider(unittest.TestCase):
    if found:

        def setUp(self):
            self.base = 'http://localhost:' + str(8000)
            self._generate_credentials_file()
            self.token_provider = None
            # Not matter which request.
            self.request = TableRequest()
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
            self.assertRaises(
                IllegalArgumentException, SignatureProvider,
                config_file={'config_file': fake_credentials_file})
            # illegal profile_name
            self.assertRaises(IllegalArgumentException, SignatureProvider,
                              profile_name={'profile_name': 'DEFAULT'})
            # illegal tenant_id
            self.assertRaises(
                IllegalArgumentException, SignatureProvider, tenant_id={},
                user_id='user', fingerprint='fingerprint', private_key='key')
            self.assertRaises(
                IllegalArgumentException, SignatureProvider, tenant_id='',
                user_id='user', fingerprint='fingerprint', private_key='key')
            # illegal user_id
            self.assertRaises(
                IllegalArgumentException, SignatureProvider, tenant_id='tenant',
                user_id={}, fingerprint='fingerprint', private_key='key')
            self.assertRaises(
                IllegalArgumentException, SignatureProvider, tenant_id='tenant',
                user_id='', fingerprint='fingerprint', private_key='key')
            # illegal fingerprint
            self.assertRaises(
                IllegalArgumentException, SignatureProvider, tenant_id='tenant',
                user_id='user', fingerprint={}, private_key='key')
            self.assertRaises(
                IllegalArgumentException, SignatureProvider, tenant_id='tenant',
                user_id='user', fingerprint='', private_key='key')
            # illegal private_key
            self.assertRaises(
                IllegalArgumentException, SignatureProvider, tenant_id='tenant',
                user_id='user', fingerprint='fingerprint', private_key={})
            self.assertRaises(
                IllegalArgumentException, SignatureProvider, tenant_id='tenant',
                user_id='user', fingerprint='fingerprint', private_key='')
            # illegal pass phrase
            self.assertRaises(
                IllegalArgumentException, SignatureProvider, tenant_id='tenant',
                user_id='user', fingerprint='fingerprint', private_key='key',
                pass_phrase={})
            self.assertRaises(
                IllegalArgumentException, SignatureProvider, tenant_id='tenant',
                user_id='user', fingerprint='fingerprint', private_key='key',
                pass_phrase='')
            # illegal region
            self.assertRaises(
                IllegalArgumentException, SignatureProvider, tenant_id='tenant',
                user_id='user', fingerprint='fingerprint', private_key='key',
                pass_phrase={}, region='IllegalRegion')

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

        def testAccessTokenProviderSetIllegalLogger(self):
            self.token_provider = SignatureProvider(
                config_file=fake_credentials_file)
            self.assertRaises(IllegalArgumentException,
                              self.token_provider.set_logger, 'IllegalLogger')

        def testAccessTokenProviderGetAuthStringWithIllegalRequest(self):
            config = oci.config.from_file(file_location=fake_credentials_file)
            provider = oci.signer.Signer(
                config['tenancy'], config['user'], config['fingerprint'],
                config['key_file'], config.get('pass_phrase'),
                config.get('key_content'))
            self.token_provider = SignatureProvider(provider)
            self.assertRaises(IllegalArgumentException,
                              self.token_provider.get_authorization_string,
                              'IllegalRequest')

        def testAccessTokenProviderGets(self):
            self.token_provider = SignatureProvider(
                config_file=fake_credentials_file)
            self.assertIsNone(self.token_provider.get_logger())

        def testAccessTokenProviderGetAuthStringWithConfigFile(self):
            self.token_provider = SignatureProvider(
                config_file=fake_credentials_file, duration_seconds=5,
                refresh_ahead=1)
            self.assertRaises(
                IllegalArgumentException,
                self.token_provider.get_authorization_string, self.request)
            self.token_provider.set_service_url(self.handle_config)
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
            #
            # Exercise concurrent refresh schedule. The refresh might be
            # scheduled by NoSQLHandle.get_authorization_string or the refresh
            # task itself. Start two threads to call the common
            # SignatureProvider.get_signature_details_internal simultaneously
            # that would schedule a refresh to simulate this case.
            #
            latch = TestSignatureProvider.CountDownLatch(1)
            threads = list()
            for i in range(2):
                t = TestSignatureProvider.TestThread(self, latch)
                t.start()
                threads.append(t)
            latch.count_down()
            for t in threads:
                t.join()

        def testAccessTokenProviderGetAuthStringWithoutConfigFile(self):
            self.token_provider = SignatureProvider(
                tenant_id='ocid1.tenancy.oc1..tenancy',
                user_id='ocid1.user.oc1..user', fingerprint='fingerprint',
                private_key=fake_key_file, duration_seconds=5, refresh_ahead=1)
            self.assertRaises(
                IllegalArgumentException,
                self.token_provider.get_authorization_string, self.request)
            self.token_provider.set_service_url(self.handle_config)
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

        def testAccessTokenProviderGetRegion(self):
            # no region
            config = oci.config.from_file(file_location=fake_credentials_file)
            provider = oci.signer.Signer(
                config['tenancy'], config['user'], config['fingerprint'],
                config['key_file'], config.get('pass_phrase'),
                config.get('key_content'))
            self.token_provider = SignatureProvider(provider)
            self.assertIsNone(self.token_provider.get_region())
            self.token_provider.close()
            # region get from provider parameter of constructor
            provider.region = config['region']
            self.token_provider = SignatureProvider(provider)
            self.assertEqual(self.token_provider.get_region(),
                             Regions.US_ASHBURN_1)
            self.token_provider.close()
            # region get from config_file parameter of constructor
            self.token_provider = SignatureProvider(
                config_file=fake_credentials_file)
            self.assertEqual(self.token_provider.get_region(),
                             Regions.US_ASHBURN_1)
            self.token_provider.close()
            # region from region parameter of constructor
            self.token_provider = SignatureProvider(
                tenant_id='ocid1.tenancy.oc1..tenancy',
                user_id='ocid1.user.oc1..user', fingerprint='fingerprint',
                private_key=fake_key_file, region=Regions.US_ASHBURN_1,
                duration_seconds=5, refresh_ahead=1)
            self.assertEqual(self.token_provider.get_region(),
                             Regions.US_ASHBURN_1)

        if iam_principal() == 'instance principal':

            def testInstancePrincipalGetAuthString(self):
                signer = (
                    oci.auth.signers.InstancePrincipalsSecurityTokenSigner())
                self.token_provider = SignatureProvider(
                    signer, duration_seconds=5, refresh_ahead=1)
                self.assertRaises(
                    IllegalArgumentException,
                    self.token_provider.get_authorization_string, self.request)
                self.token_provider.set_service_url(self.handle_config)
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

            def testInstancePrincipalGetRegion(self):
                self.token_provider = (
                    SignatureProvider.create_with_instance_principal(
                        region=Regions.US_ASHBURN_1))
                self.assertEqual(self.token_provider.get_region(),
                                 Regions.US_ASHBURN_1)

        @staticmethod
        def _generate_credentials_file():
            # Generate credentials file
            if path.exists(fake_credentials_file):
                remove(fake_credentials_file)

            with open(fake_credentials_file, 'w') as cred_file:
                cred_file.write('[DEFAULT]\n')
                cred_file.write('tenancy=ocid1.tenancy.oc1..tenancy\n')
                cred_file.write('user=ocid1.user.oc1..user\n')
                cred_file.write('fingerprint=fingerprint\n')
                cred_file.write('key_file=' + fake_key_file + '\n')
                cred_file.write('region=us-ashburn-1\n')

        class CountDownLatch(object):

            def __init__(self, count):
                self._count = count
                self._lock = Condition()

            def count_down(self):
                self._lock.acquire()
                self._count -= 1
                if self._count <= 0:
                    if sys.version_info[0] < 3 or (sys.version_info[0] == 3 and
                                                   sys.version_info[1] < 10):
                        self._lock.notifyAll()
                    else:
                        self._lock.notify_all()
                self._lock.release()

            def wait(self, wait_secs=None):
                self._lock.acquire()
                if wait_secs is not None:
                    self._lock.wait(wait_secs)
                else:
                    while self._count > 0:
                        self._lock.wait()
                self._lock.release()

            def get_count(self):
                self._lock.acquire()
                count = self._count
                self._lock.release()
                return count

        class TestThread(Thread):

            def __init__(self, outer, latch):
                super(TestSignatureProvider.TestThread, self).__init__()
                self._outer = outer
                self._latch = latch

            def run(self):
                self._latch.wait()
                self._outer.assertIsNotNone(
                    self._outer.token_provider.get_signature_details_internal())


if __name__ == '__main__':
    unittest.main()
