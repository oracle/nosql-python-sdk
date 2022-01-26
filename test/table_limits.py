#
# Copyright (c) 2018, 2022 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#

import unittest

from borneo import IllegalArgumentException, TableLimits, TableRequest
from parameters import table_name, tenant_id
from testutils import get_handle


class TestTableLimits(unittest.TestCase):

    def setUp(self):
        self.handle = get_handle(tenant_id)
        self.limits = TableLimits(30, 10, 1)
        self.table_request = TableRequest().set_statement(
            'CREATE TABLE ' + table_name + '(fld_id INTEGER, fld_long LONG, \
PRIMARY KEY(fld_id))')

    def tearDown(self):
        self.limits = None
        self.handle.close()

    def testTableLimitsIllegalInit(self):
        self.assertRaises(IllegalArgumentException, TableLimits,
                          'IllegalReadUnits', 1, 0)
        self.assertRaises(IllegalArgumentException, TableLimits,
                          -1, 'IllegalWriteUnits', 0)
        self.assertRaises(IllegalArgumentException, TableLimits,
                          -1, 0, 'IllegalStorageGb')

    def testTableLimitsSetIllegalReadUnits(self):
        self.assertRaises(IllegalArgumentException, self.limits.set_read_units,
                          'IllegalReadUnits')

    def testTableLimitsSetIllegalWriteUnits(self):
        self.assertRaises(IllegalArgumentException, self.limits.set_write_units,
                          'IllegalWriteUnits')

    def testTableLimitsSetIllegalStorageGb(self):
        self.assertRaises(IllegalArgumentException, self.limits.set_storage_gb,
                          'IllegalStorageGb')

    def testTableLimitsNegativeValues(self):
        # negative read units
        self.table_request.set_table_limits(TableLimits(-1, 10, 1))
        self.assertRaises(IllegalArgumentException, self.handle.table_request,
                          self.table_request)
        # read_units = 0
        self.table_request.set_table_limits(TableLimits(0, 10, 1))
        self.assertRaises(IllegalArgumentException, self.handle.table_request,
                          self.table_request)
        # negative write units
        self.table_request.set_table_limits(TableLimits(30, -1, 1))
        self.assertRaises(IllegalArgumentException, self.handle.table_request,
                          self.table_request)
        # write_units = 0
        self.table_request.set_table_limits(TableLimits(30, 0, 1))
        self.assertRaises(IllegalArgumentException, self.handle.table_request,
                          self.table_request)
        # negative storage gb
        self.table_request.set_table_limits(TableLimits(30, 10, -1))
        self.assertRaises(IllegalArgumentException, self.handle.table_request,
                          self.table_request)
        # storage_gb = 0
        self.table_request.set_table_limits(TableLimits(30, 10, 0))
        self.assertRaises(IllegalArgumentException, self.handle.table_request,
                          self.table_request)


if __name__ == '__main__':
    unittest.main()
