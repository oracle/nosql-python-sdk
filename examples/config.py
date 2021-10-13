#
# Copyright (c) 2018, 2021 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/

#
# Parameters used by example code -- Cloud Simulator Configuration
#
# This file is configured for the examples to be run against a Cloud Simulator
# instance.
#
# The default settings below are sufficient if the Cloud Simulator has been
# started on the endpoint, localhost:8080, which is its default. If not, the
# parameters in this file should be changed as needed. Please see sample config
# options in: config_cloudsim.py, config_onprem.py and config_cloud.py, change
# the parameters in those files, then copy the content to this file.
#

# The endpoint to use to connect to the service. This endpoint is for a Cloud
# Simulator running on its default port (8080) on the local machine.
#endpoint = 'localhost:8080'

# The server type.
#server_type = 'cloudsim'

endpoint = 'http://localhost:8080'

# The server type, please don't change it.
server_type = 'onprem'

# Please set the following parameters if running against secure .

# SSL CA certificates. Configure it to specify CA certificates or set
# REQUESTS_CA_BUNDLE environment variable when running against a secure
# database. For non-secure database, use the default None.
ca_certs = None
# User name for secure database, for non-secure database, use the default None.
user_name = None
# Password for secure database, for non-secure database, use the default None.
user_password = None