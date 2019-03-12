#
# Copyright (C) 2018, 2019 Oracle and/or its affiliates. All rights reserved.
#
# This software is licensed with the Universal Permissive License (UPL) version 1.0
#
# Please see LICENSE.txt file included in the top-level directory of the
# appropriate download for a copy of the license and additional information.
#

from logging import INFO
from os import environ, sep, path
from sys import argv

from borneo import Consistency, DefaultRetryHandler

#
# Parameters used by test code.
#
# This file can be used to run against a Cloud Simulator instance or
# the Oracle NoSQL Database Cloud Service. The latter is not recommended
# as it will result in throughput consumption for the tenancy used for
# testing.
#
# These parameters control the use of the Cloud Simulator and the
# Oracle NoSQL Database Cloud Service.
#
# To run with Cloud Simulator, the default setting is sufficient if the
# server has been started on the endpoint, localhost:8080, which is its
# default. If not, you can change parameters in this file as needed.
#
# To run with the Oracle NoSQL Cloud Service, you need to set the
# following parameters, leaving the others defaulted:
#     protocol = 'https',
#     http_host = 'ans.uscom-east-1.oraclecloud.com'
#     http_port = 443,
#     idcs_url = 'your_idcs_url'
#     entitlement_id = 'your_entitlement_id'
#     andc_client_id = 'your_andc_client_id',
#     andc_client_secret = 'your_andc_client_secret'
#     andc_username = 'your_andc_username'
#     andc_user_pwd = 'your_andc_user_pwd'
#

# A test tenant_id, only used for the Cloud Simulator
tenant_id = 'test_tenant'
# The table name to use.
table_name = 'users'
# The index name to use.
index_name = 'idx'
# The protocol that used to connect to the server:
# protocol = 'http' -- for Cloud Simulator
# protocol = 'https' -- for Service
protocol = 'http'
# The host on which the server is running, for the service it should be
# 'ans.uscom-east-1.oraclecloud.com' or other supported endpoint.
# For the Cloud Simulator, use localhost, assuming the server is local
http_host = 'localhost'
# The port to use. For https and the service, use 443, for the Cloud
# Simulator use 8080, or the part used to start the simulator.
http_port = 8080
# The timeout of the http request and the operations.
timeout = 30000
# The table request timeout, used when a table operation doesn't set its own.
table_request_timeout = 60000
# The timeout for waiting security information is available.
sec_info_timeout = 20000
# The consistency for read operations.
consistency = Consistency.ABSOLUTE
# The number of connection pools to cache.
pool_connections = 10
# The maximum number of individual connections to use to connect to server.
pool_maxsize = 10
# The retry handler.
retry_handler = DefaultRetryHandler(10, 5)
# The logger level.
logger_level = INFO
# The wait timeout for table request.
wait_timeout = 120000

#
# HTTP proxy settings are generally not required. If the server is
# running behind an HTTP proxy server they may be needed.
#

# The proxy host.
proxy_host = None
# The proxy port.
proxy_port = 0
# The proxy username.
proxy_username = None
# The proxy password.
proxy_password = None

#
# These parameters are only used if an actual cloud service is available and
# must be set to valid credentials. Note that use of these will consume
# resources and tests may not operate properly in this environment because of
# resource constraints.
#

# Credentials file path.
credentials_file = environ['HOME'] + sep + '.andc' + sep + 'test_credentials'
# Properties file path
properties_file = environ['HOME'] + sep + '.andc' + sep + 'test_properties'
# Your client id
andc_client_id = 'test-client'
# Your client secret
andc_client_secret = 'test-client-secret'
# Your user name
andc_username = 'test-user'
# Your password
andc_user_pwd = 'test-user-pwd'
# Your entitlement id
entitlement_id = '123456789'
# Your IDCS server url
idcs_url = None

#
# Internal testing configuration
#

# Enable security or not, false for Cloud Sim
security = False
# Test keys directory.
keystore = path.abspath(path.dirname(argv[0])) + sep + 'tenant.pem'
# The tier name, it should be 'test_tier'.
tier_name = None
# The sc port for setting the tier.
sc_port = 13600

def not_cloudsim():
    # Can be used by tests to determine the environment. Returns False if
    # running in cloudsim, otherwise returns True.
    if protocol == 'http' and not security:
        return False
    return True
