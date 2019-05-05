#
# Copyright (C) 2018, 2019 Oracle and/or its affiliates. All rights reserved.
#
# This software is licensed with the Universal Permissive License (UPL) version 1.0
#
# Please see LICENSE.txt file included in the top-level directory of the
# appropriate download for a copy of the license and additional information.
#

#
# To run against the Cloud Simulator
#
# Assumes there is a running Cloud Simulator instance listening on port 8080 on
# the local host. If non-default host or port are desired, use flags to
# change them.
#
# Run against Oracle NoSQL Database Cloud Service:
#
# Requires an Oracle Cloud account with a subscription to the Oracle NoSQL
# Database Cloud Service. Obtain client id and secret from Oracle Identity Cloud
# Service (IDCS) admin console, choose Applications from the button on the top
# left. Find the Application named ANDC. The client id and secret are in the
# General Information of Configuration. Create a new file in the specified path
# set by parameter "credentials_file" below, open the file in your text editor,
# add the following information and save the file. This file should be secured
# so that only the application has access to read it.
#
#     andc_client_id=<application_client_id from admin console>
#     andc_client_secret=<application_client_secret admin console>
#     andc_username=<user name of cloud account>
#     andc_user_pwd=<user password of cloud account>
#
# After that is done this information is required to run the example, or any
# application using the service.
#
#     o IDCS URL assigned to the tenancy
#     o entitlement id
#
# The tenant-specific IDCS URL is the IDCS host assigned to the tenant. After
# logging into the IDCS admin console, copy the host of the IDCS admin console
# URL. For example, the format of the admin console URL is
# "https://{tenantId}.identity.oraclecloud.com/ui/v1/adminconsole". The
# "https://{tenantId}.identity.oraclecloud.com" portion is the required.
# Then assign the IDCS URL to the idcs_url variable below.
#
# The entitlement id can be found using the IDCS admin console. After logging
# into the IDCS admin console, choose Applications from the button on the top
# left. Find the Application named ANDC, enter the Resources tab in the
# Configuration. There is a field called primary audience, the entitlement id
# parameter is the value of "urn:opc:andc:entitlementid", which is treated as a
# string. For example if your primary audience is
# "urn:opc:andc:entitlementid=123456789" then the parameter is "123456789".
# Then assign the entitlement id to the entitlement_id variable below.
#
# These variables control whether the program uses the Cloud Simulator or the
# real service. Modify as necessary for your environment. By default the
# variables are set to use the Cloud Simulator.
#
# Cloud Simulator: a tenant id -- simple string
# Service: not used
tenant_id = 'test_tenant'

# Cloud Simulator: not used
# Service: your entitlement id
entitlement_id = None

# The table name created. It must not contain '.' or '_'
table_name = 'pythontable'

# The index name created. It must not contain '.' or '_'
index_name = 'pythonindex'

# Cloud Simulator: True
# Service: False
# using_cloud_sim = False
using_cloud_sim = True

# Cloud Simulator: 'localhost:8080' or the host running Cloud Simulator
# Service: 'ndcs.uscom-east-1.oraclecloud.com' or appropriate region host
endpoint = 'localhost:8080'

# Cloud Simulator: not used
# Service: url for reaching IDCS
idcs_url = 'your_idcs_url'

# Cloud Simulator: not used
# Service: absolute path to credentials file
credentials_file = 'path-to-your-credentials-file'

#
# Set to True to drop the table at the end of the example
#
drop_table = False
