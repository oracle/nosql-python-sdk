#
# Copyright (c) 2018, 2022 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#

import unittest

from borneo import FieldRange, IllegalArgumentException, MultiDeleteRequest
from parameters import table_name, tenant_id
from testutils import get_handle


class TestFieldRange(unittest.TestCase):

    def setUp(self):
        self.handle = get_handle(tenant_id)
        self.multi_delete_request = MultiDeleteRequest().set_table_name(
            table_name).set_key({'fld_sid': 1})

    def tearDown(self):
        self.handle.close()

    def testFieldRangeIllegalInit(self):
        self.assertRaises(IllegalArgumentException, FieldRange, 0)

    def testFieldRangeSetIllegalStart(self):
        fr = FieldRange('fld_id')
        self.assertRaises(IllegalArgumentException, fr.set_start, None, True)
        self.assertRaises(IllegalArgumentException, fr.set_start, 0,
                          'IllegalIsInclusive')

    def testFieldRangeSetIllegalEnd(self):
        fr = FieldRange('fld_id')
        self.assertRaises(IllegalArgumentException, fr.set_end, None, False)
        self.assertRaises(IllegalArgumentException, fr.set_start, -1,
                          'IllegalIsInclusive')

    def testFieldRangeGets(self):
        fr = FieldRange('fld_id').set_start('abc', False).set_end('def', True)
        self.assertEqual(fr.get_field_path(), 'fld_id')
        self.assertEqual(fr.get_start(), 'abc')
        self.assertFalse(fr.get_start_inclusive())
        self.assertEqual(fr.get_end(), 'def')
        self.assertTrue(fr.get_end_inclusive())

    def testFieldRangeNoStartAndEnd(self):
        fr = FieldRange('fld_id')
        self.multi_delete_request.set_range(fr)
        self.assertRaises(IllegalArgumentException, self.handle.multi_delete,
                          self.multi_delete_request)

    def testFieldRangeDifferentStartEndType(self):
        fr = FieldRange('fld_id').set_start('abc', False).set_end(3, True)
        self.multi_delete_request.set_range(fr)
        self.assertRaises(IllegalArgumentException, self.handle.multi_delete,
                          self.multi_delete_request)


if __name__ == '__main__':
    unittest.main()
