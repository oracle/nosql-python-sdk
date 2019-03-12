#
# Copyright (C) 2018, 2019 Oracle and/or its affiliates. All rights reserved.
#
# This software is licensed with the Universal Permissive License (UPL) version 1.0
#
# Please see LICENSE.txt file included in the top-level directory of the
# appropriate download for a copy of the license and additional information.
#

from os import system
from sys import version_info

from parameters import num_procs

for proc_id in range(num_procs):
    system('nohup python' + str(version_info.major) +
           ' stresstest.py --process ' + str(proc_id) + ' >> out.log 2>&1 &')
