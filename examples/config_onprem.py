#
# Copyright (c) 2018, 2022 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#

#
# Parameters used by example code -- On-premise Oracle NoSQL database
#
# This file is configured for the example to be run against a On-prem Oracle
# NoSQL database. Please start the database and proxy first.
#
# The default settings below are sufficient if the On-prem proxy has been
# started on the endpoint, localhost:8080, with security disable. If not, the
# parameters in this file should be changed as needed.
#

# The endpoint to use to connect to the service. This endpoint is for a on-prem
# proxy started by the customer. Use 'http' protocol for non-secure database and
# 'https' for secure database.
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
