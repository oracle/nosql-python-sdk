#
# Copyright (C) 2018, 2020 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
#
# Please see LICENSE.txt file included in the top-level directory of the
# appropriate download for a copy of the license and additional information.
#

#
# To run against the Cloud Simulator
#
# Assumes there is a running Cloud Simulator instance listening on port 8080 on
# the local host. If non-default host or port are desired, use flags to change
# them.
#
# To run against an on-premise Oracle NoSQL database
#
# First you need to start the database and proxy and if using a secure store
# create a user and password with the required privileges.
#
# Then change the following parameters if you use a not-secure store.
#
#              endpoint = 'your_on_prem_proxy_endpoint'
#              is_cloudsim = False
#              is_onprem = True
#
# Change additional parameters below if your on-prem proxy is running against a
# secure store.
#
#              user_name = 'your_store_user_name'
#              password = 'your_store_user_password'
#              security = True
#
# Run against Oracle NoSQL Database Cloud Service using Oracle Cloud
# Infrastructure(OCI) user principal verification.
#
# This Requires an Oracle Cloud account with a subscription to the Oracle NoSQL
# Database Cloud Service. Login OCI Console
# https://console.us-ashburn-1.oraclecloud.com. Then follow the steps below to
# get the required information.

# Step1: Generate a RSA key pair in PEM format (minimum 2048 bits).
# Step2: Upload the PEM public key and get the key's fingerprint. Click your
#        username in the top-right corner of the console, click User Settings,
#        click Add Public Key, paste the contents of the PEM public key in the
#        dialog box and click Add. Then you can see the key's fingerprint is
#        displayed under the public key.
# Step3: Get the tenancy OCID from the OCI Console on the Tenancy Details page.
#        Open the navigation menu, under Governance and Administration, go to
#        Administration and click Tenancy Details. The tenancy OCID is shown
#        under Tenancy Information.
# Step4: Get the user's OCID from OCI Console on User Settings page. Open the
#        Profile menu (User menu icon) and click User Settings. You can find the
#        user's OCID is shown under User Information.
#
# Create a credential file in the specified path set by parameter
# "credentials_file" below, open the file in your text editor, add the following
# information obtained from the previous steps. This file should be secured so
# that only the application has access to read it.
#
#     [DEFAULT]
#     tenancy=<your-tenancy-id-from-oci-console>
#     user=<your-user-id-from-oci-console>
#     fingerprint=<fingerprint-of-your-public-key>
#     key_file=<path-to-your-private-key-file>
#
# Run against Oracle NoSQL Database Cloud Service using Oracle Cloud
# Infrastructure(OCI) instance principal verification.
#
# This Requires an Oracle Cloud account with a subscription to the Oracle NoSQL
# Database Cloud Service and a OCI machine. You need to run the example on the
# OCI machine without the credentials file.
#
# These variables control whether the program uses the Cloud Simulator or the
# real service. Modify as necessary for your environment. By default the
# variables are set to use the Cloud Simulator.
#
# Cloud Simulator: a tenant id -- simple string
# Service/on-premise: not used
tenant_id = 'pythontenant'

# The table name created. It must not contain '.' or '_'
table_name = 'pythontable'

# The index name created. It must not contain '.' or '_'
index_name = 'pythonindex'

# Cloud Simulator: True
# Otherwise: False
# using_cloud_sim = False
using_cloud_sim = True

# Service: True
# Otherwise: False
# using_service = True
using_service = False

# On-premise only:
# On-premise: True
# Otherwise: False
# using_on_prem = True
using_on_prem = False

# Endpoint is required.
#
# Cloud Simulator: 'http://localhost:8080' or the host running Cloud Simulator
# or a on-prem proxy started by the customer.
# Service: a endpoint string, a region id or a Region, using for example:
#  'nosql.us-ashburn-1.oci.oraclecloud.com', 'us-ashburn-1' or
#  Regions.US_ASHBURN_1
# on-premise: 'http://localhost:80' or 'https://localhost:443' (or use the
# appropriate host:port for the proxy)
endpoint = 'localhost:8080'

# On-premise only:
# Non-secure store: None
# Secure store: your store user name
user_name = None

# On-premise only:
# Non-secure store: None
# Secure store: your store user password
password = None

# Cloud Simulator/on-premise: not used
# Service: 'user principal' or 'instance principal'
principal = None

# Cloud Simulator/on-premise: not used
# Service: absolute path to credentials file
credentials_file = 'path-to-your-credentials-file'

#
# Set to True to drop the table at the end of the example
#
drop_table = False
