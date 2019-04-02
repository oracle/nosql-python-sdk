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
# Database Cloud Service. You need to generate the credentials file template
# credentials.tmp using the OAuthClient tool first. Then create a new file in
# specified path set by parameter "credentials_file" below, and copy content of
# credentials.tmp to the file. Open the file in your text editor, add only the
# following information and save the file. This file should be secured so that
# only the application has access to read it
#
#     andc_client_id=<application_client_id from credential file>
#     andc_client_secret=<application_client_secret from credential file>
#
# After that is done this information is required to run the example, or any
# application using the service.
#
#     o IDCS URL assigned to the tenancy
#
# The tenant-specific IDCS URL is the IDCS host assigned to the tenant. After
# logging into the IDCS admin console, copy the host of the IDCS admin console
# URL. For example, the format of the admin console URL is
# "https://{tenantId}.identity.oraclecloud.com/ui/v1/adminconsole". The
# "https://{tenantId}.identity.oraclecloud.com" portion is the required.
# Then Assign the IDCS URL to the idcs_url variable below
#
# These variables control whether the program uses the Cloud Simulator or the
# real service. Modify as necessary for your environment. By default the
# variables are set to use the Cloud Simulator
#
# Cloud Simulator: a tenant id -- simple string
# Service: not used
tenant_id = 'test_tenant'

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
