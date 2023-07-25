
README for nosql.py shell for Oracle NoSQL Database

NOTE: this is a preview-only, use at your own risk program. It uses the
Oracle NoSQL SDK for Python (borneo) as well as the Oracle OCI SDK for
Python (if using the cloud service). Feedback on usability and features and
potential enhancements is greatly appreciated.

Report issues on the GitHub project page:
  https://github.com/oracle/nosql-python-sdk

nosql.py is a simple interactive shell for an Oracle NoSQL Database instance.
It supports table management operations as well as basic CRUD operations
on tables, as well as queries.

The instance can be the cloud service, an on-premise instance (via proxy), or
a Cloud Simulator instance.
 Usage: nosql.py -s|--service <service> -e|--endpoint <endpoint> [-v]
    [-c|--creds <credentials_file>] - cloud service credentials file
    [-t|--tenant <tenant_id>] - cloud service tenant ocid
    [-u|--user <user_id>] - cloud service tenancy user ocid
    [-f|--fingerprint <fingerprint>] - cloud service fingerprint for user
    [-k|--key <private-key>] - cloud service private key file path or string
    [-p|--pass <pass_phrase for key or password>] - private key pass or user password

  -v for verbose
  service is one of <cloud | cloudsim | kvstore>
  endpoint is a cloud region or url to a cloudsim or on-prem proxy, e.g.
     -e us-ashburn-1
     -e http://localhost:8080

Examples:
  $ python3 nosql.py -s cloudsim -e localhost:8080
  $ python3 nosql.py -s cloud -e us-ashburn-1

 Requirements:
  1. Python 3.6+
  2. Python dependencies (install using pip or other mechanism):
   o borneo
    $ pip install borneo
   o cmd2
    $ pip install cmd2
   o pyreadline or gnureadline (for cmd2)
    $ pip install gnureadline
  3. If running against the Cloud Simulator, it can be downloaded from
  here:
   http://www.oracle.com/technetwork/topics/cloud/downloads/index.html
  The Cloud Simulator requires Java
  4. If running against the Oracle NoSQL Database Cloud Service an account
  must be used.
