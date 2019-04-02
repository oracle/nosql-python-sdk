#
# Copyright (C) 2018, 2019 Oracle and/or its affiliates. All rights reserved.
#
# This software is licensed with the Universal Permissive License (UPL) version 1.0
#
# Please see LICENSE.txt file included in the top-level directory of the
# appropriate download for a copy of the license and additional information.
#

from logging import INFO

from borneo import Consistency

#
# Parameters used by test code -- Cloud Simulator Configuration
#
# This file is configured for the unit tests to be run against a Cloud
# Simulator instance. The simulator is used so that limits that exist in
# the cloud service are not involved and there is no cost involved in running
# the unit tests.
#
# The default settings below are sufficient if the Cloud Simulator has been
# started on the endpoint, localhost:8080, which is its default. If not, the
# parameters in this file should be changed as needed.
#

# A test tenant_id, only used for the Cloud Simulator
tenant_id = 'test_tenant'
# A prefix for table names
table_prefix = 'pytest'
# The table name to use.
table_name = table_prefix + 'Users'
# The index name to use.
index_name = 'idx'
# The endpoint to use to connect to the service. This endpoint is for a
# Cloud Simulator running on its default port (8080) on the local machine.
# Unit tests are generally run against the Cloud Simulator.
endpoint = 'localhost:8080'
# The timeout of the http request and the operations.
timeout = 30000
# The table request timeout, used when a table operation doesn't set its own.
table_request_timeout = 60000
# The consistency for read operations.
consistency = Consistency.ABSOLUTE
# The number of connection pools to cache.
pool_connections = 10
# The maximum number of individual connections to use to connect to server.
pool_maxsize = 10
# The logger level.
logger_level = INFO
# The wait timeout for table request.
wait_timeout = 120000

#
# Internal testing use
#


def idcs_url():
    return None


def is_cloudsim():
    return True


def is_dev_pod():
    return False


def is_prod_pod():
    return idcs_url() is not None


def is_pod():
    return is_dev_pod() or is_prod_pod()


def is_minicloud():
    return False


def not_cloudsim():
    return not is_cloudsim()
