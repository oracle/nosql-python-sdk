#
# Copyright (c) 2018, 2022 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#

from config import endpoint, server_type
try:
    from config import ca_certs
except ImportError:
    ca_certs = None
try:
    from config import credentials_file
except ImportError:
    credentials_file = None
try:
    from config import principal
except ImportError:
    principal = None
try:
    from config import user_name
except ImportError:
    user_name = None
try:
    from config import user_password
except ImportError:
    user_password = None

#
# These variables control whether the program uses the Cloud Simulator or the
# real service. Modify as necessary for your environment. By default the
# variables are set to use the Cloud Simulator.
#
# Cloud Simulator/Service: a tenant id -- simple string
# On-premise: not used
if credentials_file is None:
    tenant_id = 'pythontenant'
else:
    assert isinstance(credentials_file, str)
    with open(credentials_file, 'r') as creds:
        for line in creds.readlines():
            curline = line.strip().split("=")
            if curline[0] == 'tenancy':
                tenant_id = curline[1]
                break

# The table name created. It must not contain '.' or '_'
table_name = 'pythontable'

# The index name created. It must not contain '.' or '_'
index_name = 'pythonindex'

# Cloud Simulator: True
# Otherwise: False
# using_cloud_sim = False
using_cloud_sim = server_type == 'cloudsim'

# Service: True
# Otherwise: False
# using_service = True
using_service = server_type == 'cloud'

# On-premise only:
# On-premise: True
# Otherwise: False
# using_on_prem = True
using_on_prem = server_type == 'onprem'

# Endpoint is required.
#
# Cloud Simulator: 'http://localhost:8080' or the host running Cloud Simulator
# or a on-prem proxy started by the customer.
# Service: a endpoint string, a region id or a Region, using for example:
#  'nosql.us-ashburn-1.oci.oraclecloud.com', 'us-ashburn-1' or
#  Regions.US_ASHBURN_1
# on-premise: 'http://localhost:80' or 'https://localhost:443' (or use the
# appropriate host:port for the proxy)
endpoint = endpoint

# On-premise only:
# Non-secure store: None
# Secure store: your CA certificate path
ca_certs = ca_certs

# On-premise only:
# Non-secure store: None
# Secure store: your store user name
user_name = user_name

# On-premise only:
# Non-secure store: None
# Secure store: your store user password
password = user_password

# Cloud Simulator/on-premise: not used
# Service: 'user principal', 'instance principal' or 'resource principal'
principal = principal

# Cloud Simulator/on-premise: not used
# Service: absolute path to credentials file
credentials_file = credentials_file

#
# Set to True to drop the table at the end of the example
#
drop_table = False
