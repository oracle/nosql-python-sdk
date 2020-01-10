#
# Copyright (C) 2018, 2020 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
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
# To run against the on-prem proxy, you need to start the kvstore and proxy
# first. Then change the following parameters if you use non-security store.
#
#              endpoint = 'your_on_prem_proxy_endpoint'
#              is_cloudsim = False
#              is_onprem = True
#
# Change additional parameters below if your on-prem proxy is running against a
# security store.
#
#              user_name = 'your_store_user_name'
#              password = 'your_store_user_password'
#              security = True
#

# A test tenant_id, only used for the Cloud Simulator.
tenant_id = 'test_tenant'
# A prefix for table names.
table_prefix = 'pytest'
# The table name to use.
table_name = table_prefix + 'Users'
# The index name to use.
index_name = 'idx'
# The endpoint to use to connect to the service. This endpoint is for a Cloud
# Simulator running on its default port (8080) on the local machine, or a
# on-prem proxy started by the customer. Unit tests can be run against both the
# Cloud Simulator and on-prem proxy.
endpoint = 'localhost:8080'
# User name for on-prem proxy, for non-secure store, use the default None.
user_name = None
# Password for on-prem proxy, for non-secure store, use the default None.
password = None
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


def iam_principal():
    # Use 'user principal' or 'instance principal' for production pod, and use
    # None for minicloud testing.
    return None


def is_cloudsim():
    return True


def is_dev_pod():
    return False


def is_minicloud():
    return False


def is_onprem():
    return False


def is_prod_pod():
    return iam_principal() is not None


def is_pod():
    return is_dev_pod() or is_prod_pod()


def not_cloudsim():
    return not is_cloudsim()


def security():
    # Set to enable security is using on-prem security mode.
    return False
