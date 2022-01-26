#
# Copyright (c) 2018, 2022 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#

import unittest
from time import time

from borneo import IllegalArgumentException, TimeToLive, TimeUnit


class TestTimeToLive(unittest.TestCase):
    def testTimeToLiveIllegalInit(self):
        self.assertRaises(IllegalArgumentException, TimeToLive, 'IllegalValue',
                          TimeUnit.DAYS)
        self.assertRaises(IllegalArgumentException, TimeToLive, 1, None)
        self.assertRaises(IllegalArgumentException, TimeToLive, 1,
                          'IllegalTimeUnit')

    def testTimeToLiveSetIllegalHours(self):
        self.assertRaises(IllegalArgumentException, TimeToLive.of_hours,
                          'IllegalHours')
        self.assertRaises(IllegalArgumentException, TimeToLive.of_hours, -1)

    def testTimeToLiveSetIllegalDays(self):
        self.assertRaises(IllegalArgumentException, TimeToLive.of_days,
                          'IllegalDays')
        self.assertRaises(IllegalArgumentException, TimeToLive.of_days, -1)

    def testTimeToLiveToDays(self):
        ttl = TimeToLive.of_hours(26)
        self.assertEqual(ttl.to_days(), 1)

    def testTimeToLiveToHours(self):
        ttl = TimeToLive.of_days(2)
        self.assertEqual(ttl.to_hours(), 48)

    def testTimeToLiveToExpirationTime(self):
        ttl = TimeToLive.of_days(0)
        self.assertRaises(IllegalArgumentException, ttl.to_expiration_time,
                          'IllegalReferenceTime')
        self.assertRaises(IllegalArgumentException, ttl.to_expiration_time, 0)
        self.assertRaises(IllegalArgumentException, ttl.to_expiration_time, -1)
        self.assertEqual(ttl.to_expiration_time(int(round(time() * 1000))), 0)
        ttl = TimeToLive.of_hours(16)
        reference_time = int(round(time() * 1000))
        self.assertEqual(ttl.to_expiration_time(reference_time),
                         reference_time + 16 * 60 * 60 * 1000)
        ttl = TimeToLive.of_days(16)
        self.assertEqual(ttl.to_expiration_time(reference_time),
                         reference_time + 16 * 24 * 60 * 60 * 1000)

    def testTimeToLiveUnitIsDaysOrHours(self):
        ttl = TimeToLive.of_days(6)
        self.assertTrue(ttl.unit_is_days())
        self.assertFalse(ttl.unit_is_hours())
        ttl = TimeToLive.of_hours(6)
        self.assertFalse(ttl.unit_is_days())
        self.assertTrue(ttl.unit_is_hours())

    def testTimeToLiveGets(self):
        ttl = TimeToLive.of_days(8)
        self.assertEqual(ttl.get_value(), 8)
        self.assertEqual(ttl.get_unit(), TimeUnit.DAYS)
        ttl = TimeToLive.of_hours(10)
        self.assertEqual(ttl.get_value(), 10)
        self.assertEqual(ttl.get_unit(), TimeUnit.HOURS)


if __name__ == '__main__':
    unittest.main()
