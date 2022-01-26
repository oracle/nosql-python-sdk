#
# Copyright (c) 2018, 2022 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/

#
# Parameters used by example code -- Oracle NoSQL Database Cloud Service
#
# This file is configured for the example to be run against Oracle NoSQL
# Database Cloud Service.
#
# To use Oracle Cloud Infrastructure(OCI) user principal verification. The
# default settings below are sufficient if ~/.oci/config is provided. To
# generate ~/.oci/config, it requires an Oracle Cloud account with a
# subscription to the Oracle NoSQL Database Cloud Service. Login OCI Console
# https://console.us-ashburn-1.oraclecloud.com. Then follow the steps below to
# get the required information:
#
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
# Create ~/.oci/config, open the file in your text editor, add the following
# information obtained from the previous steps. This file should be secured so
# that only the application has access to read it.
#
#     [DEFAULT]
#     tenancy=<your-tenancy-id-from-oci-console>
#     user=<<your-user-id-from-oci-console>>
#     fingerprint=<fingerprint-of-your-public-key>
#     key_file=<path-to-your-private-key-file>
#     pass_phrase=<pass-phrase-to-your-private-key-file>
#
# If you want to put ~/.oci/config to somewhere else, set the "credentials_file"
# parameter to point to the config file you just created.
#
# To use Oracle Cloud Infrastructure(OCI) instance principal verification. It
# requires an Oracle Cloud account with a subscription to the Oracle NoSQL
# Database Cloud Service and a OCI machine. You need to run the example on the
# OCI machine without the credentials file. Set the parameter "principal" to
# "instance principal".
#
# To use Oracle Cloud Infrastructure(OCI) resource principal verification. Set
# the parameter "principal" to "resource principal", then execute the example
# as a function using https://github.com/fnproject/fn
#

from os import path

# A endpoint string, a region id or a Region, for example:
#  'nosql.us-ashburn-1.oci.oraclecloud.com', 'us-ashburn-1' or
#  Regions.US_ASHBURN_1
endpoint = 'us-ashburn-1'

# The server type, please don't change it.
server_type = 'cloud'

# Please use one of 'user principal', 'instance principal' or
# 'resource principal'.
principal = 'user principal'

# Absolute path to credentials file, default path is "~/.oci/config"
credentials_file = path.join(path.expanduser('~'), '.oci', 'config')
