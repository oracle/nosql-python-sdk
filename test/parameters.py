#
# Copyright (c) 2018, 2022 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#

from logging import INFO

from borneo import Consistency
from config import endpoint, server_type, version
try:
    from config import ca_certs
except ImportError:
    ca_certs = None
try:
    from config import user_name
except ImportError:
    user_name = None
try:
    from config import user_password
except ImportError:
    user_password = None

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
endpoint = endpoint
# Server version.
version = version
# SSL CA certificates for on-prem proxy. Configure it to specify CA certificates
# or set REQUESTS_CA_BUNDLE environment variable when running against a secure
# store. For non-secure store, use the default None.
ca_certs = ca_certs
# User name for on-prem proxy, for non-secure store, use the default None.
user_name = user_name
# Password for on-prem proxy, for non-secure store, use the default None.
password = user_password
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


# Internal use only.
def iam_principal():
    # Use 'user principal', 'instance principal' or 'resource principal' for
    # production pod, and use None for minicloud testing.
    return None


def is_cloudsim():
    return server_type == 'cloudsim'


# Internal use only.
def is_dev_pod():
    return False


# Internal use only.
def is_minicloud():
    return False


def is_onprem():
    return server_type == 'onprem'


# Internal use only.
def is_prod_pod():
    return iam_principal() is not None


# Internal use only.
def is_pod():
    return is_dev_pod() or is_prod_pod()


def not_cloudsim():
    return not is_cloudsim()


def rate_limiter_extended():
    # Set to enable rate limiter extended tests.
    return False


def security():
    # Enable security test cases in on-prem security mode.
    return user_name is not None and password is not None
