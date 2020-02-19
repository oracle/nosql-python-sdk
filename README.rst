Oracle NoSQL Database Python SDK
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

=====
About
=====

This is the Python SDK for Oracle NoSQL Database. Python 2.7+ and 3.5+ are
supported. The SDK provides interfaces, documentation, and examples to help
develop Python applications that connect to the Oracle NoSQL
Database Cloud Service, Oracle NoSQL Database and to the Oracle NoSQL
Cloud Simulator (which runs on a local machine).

In order to run the Oracle NoSQL Cloud Simulator, a separate download is
necessary from the Oracle NoSQL OTN download page. Throughout the
documentation the Oracle NoSQL Database Cloud Service and Cloud Simulator are
referred to as the "cloud service" while the Oracle NoSQL Database is referred
to as "on-premise." In the `API reference <https://nosql-python-sdk.readthedocs.
io/en/latest/api.html>`_ classes and interfaces are noted if they are only
relevant to a specific environment.

The on-premise configuration requires a running instance of the Oracle NoSQL
database. In addition a running proxy service is required. See `Oracle NoSQL
Database Downloads <https://www.oracle.com/database/technologies/nosql-database-
server-downloads.html>`_ for downloads, and see `Information about the proxy
<https://docs.oracle.com/pls/topic/lookup?ctx=en/database/other-databases/nosql-
database/19.3/admin&id=NSADM-GUID-C110AF57-8B35-4C48-A82E-2621C6A5ED72>`_ for
proxy configuration information.

This project is open source and maintained by Oracle Corp. The home page for
the project is `here <https://nosql-python-sdk.readthedocs.io/en/latest/
index.html>`_

============
Installation
============

The SDK can be installed using pip::

    pip install borneo

See `the installation guide <https://nosql-python-sdk.readthedocs.io/en/latest/
installation.html>`_ for additional requirements and and alternative install
methods.

========
Examples
========

Examples can be found `on GitHub <https://github.com/oracle/nosql-python-sdk/
tree/master/examples>`_.

Examples include simple, standalone programs. They include comments bout how
they can be configured and run in the different supported environments.

=============
Documentation
=============

The `documentation <https://nosql-python-sdk.readthedocs.io/en/latest>`_ has
information on using the SDK as well as an `API reference <https://nosql-python-
sdk.readthedocs.io/en/latest/api.html>`_ describing the classes.

=======
Changes
=======

See the `Changelog <https://github.com/oracle/nosql-python-sdk/blob/master/
CHANGELOG.rst>`_.

============
Contributing
============

The nosql-python-sdk is an open source project. See `Contributing <https://
github.com/oracle/nosql-python-sdk/blob/master/CONTRIBUTING.rst>`_ for
information on how to contribute to the project.

==========
Quickstart
==========

The following is a quick start tutorial to run a simple program in the supported
environments. The same template source code is used for all environments. The
first step is to cut the program below and paste it into an editor for minor
modifications. The instructions assume that is stored as quickstart.py, but you
can use any name you like. The quickstart example supports 3 environments:

1. Oracle NoSQL Database Cloud Service
2. Oracle NoSQL Cloud Simulator
3. Oracle NoSQL Database on-premise, using the proxy server

See `Running Quickstart <#run-quickstart>`_ for instructions on how to edit and
run the quickstart program in different environments. The instructions assume
that the *borneo* package has been installed.

.. code-block:: pycon

    #
    # Copyright (C) 2018, 2020 Oracle and/or its affiliates. All rights reserved.
    #
    # Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
    #
    # Please see LICENSE.txt file included in the top-level directory of the
    # appropriate download for a copy of the license and additional information.
    #

    #
    # This is a simple quickstart to demonstrate use of the Python driver for
    # the Oracle NoSQL Database. It can be used to run against the Oracle NoSQL
    # Database Cloud Service, against the Cloud Simulator, or against an
    # on-premise Oracle NoSQL database.
    #
    # Usage:
    #   python quickstart.py <cloud | cloudsim | kvstore>
    #
    # Use cloud for the Cloud Service
    # Use cloudsim for the Cloud Simulator
    # Use kvstore for the on-premise database
    #
    # This example is not intended to be an exhaustive overview of the API,
    # which has a number of additional operations.
    #
    # Requirements:
    #  1. Python 2.7 or 3.5+
    #  2. Python dependencies (install using pip or other mechanism):
    #   o requests
    #   o oci (only if running against the Cloud Service)
    #  3. If running against the Cloud Simulator, it can be downloaded from
    #  here:
    #   http://www.oracle.com/technetwork/topics/cloud/downloads/index.html
    #  It requires Java
    #  4. If running against the Oracle NoSQL Database Cloud Service an account
    #  must be used.
    #

    import sys

    from borneo import (
        AuthorizationProvider, DeleteRequest, GetRequest,
        IllegalArgumentException, NoSQLHandle, NoSQLHandleConfig, PutRequest,
        QueryRequest, Regions, TableLimits, TableRequest)
    from borneo.iam import SignatureProvider
    from borneo.kv import StoreAccessTokenProvider


    #
    # EDIT: these values based on desired region and/or endpoint for a local
    # server
    #
    cloud_region = Regions.EU_ZURICH_1
    cloudsim_endpoint = 'localhost:8080'
    kvstore_endpoint = 'localhost:80'
    cloudsim_id = 'cloudsim'  # a fake user id/namespace for the Cloud Simulator

    # Cloud Service Only
    #
    # EDIT: set these variables to the credentials to use if you are not using
    # a configuration file in ~/.oci/config
    # Use of these credentials vs a file is determined by a value of tenancy
    # other than None.
    #
    tenancy = None  # tenancy'd OCID (string)
    user = None  # user's OCID (string)
    private_key = 'path-to-private-key-or-private-key-content'
    fingerprint = 'fingerprint for uploaded public key'
    # pass phrase (string) for private key, or None if not set
    pass_phrase = None


    class CloudsimAuthorizationProvider(AuthorizationProvider):
        """
        Cloud Simulator Only.

        This class is used as an AuthorizationProvider when using the Cloud
        Simulator, which has no security configuration. It accepts a string
        tenant_id that is used as a simple namespace for tables.
        """

        def __init__(self, tenant_id):
            super(CloudsimAuthorizationProvider, self).__init__()
            self._tenant_id = tenant_id

        def close(self):
            pass

        def get_authorization_string(self, request=None):
            return 'Bearer ' + self._tenant_id


    def get_handle(nosql_env):
        """
        Returns a NoSQLHandle based on the requested environment. The
        differences among the supported environments are encapsulated in this
        method.
        """
        if nosql_env == 'cloud':
            endpoint = cloud_region
            #
            # Get credentials using SignatureProvider. SignatureProvider has
            # several ways to accept credentials. See the documentation:
            #  https://nosql-python-sdk.readthedocs.io/en/latest/api/borneo.iam.SignatureProvider.html
            #
            if tenancy is not None:
                print('Using directly provided credentials')
                #
                # Credentials are provided directly
                #
                provider = SignatureProvider(tenant_id=tenancy,
                                             user_id=user,
                                             fingerprint=fingerprint,
                                             private_key=private_key,
                                             pass_phrase=pass_phrase)
            else:
                #
                # Credentials will come from a file.
                #
                # By default the file is ~/.oci/config. A config_file = <path>
                # argument can be passed to specify a different file.
                #
                print('Using credentials and DEFAULT profile from ' +
                      '~/.oci/config')
                provider = SignatureProvider()
        elif nosql_env == 'cloudsim':
            print('Using cloud simulator endpoint ' + cloudsim_endpoint)
            endpoint = cloudsim_endpoint
            provider = CloudsimAuthorizationProvider(cloudsim_id)

        elif nosql_env == 'kvstore':
            print('Using on-premise endpoint ' + kvstore_endpoint)
            endpoint = kvstore_endpoint
            provider = StoreAccessTokenProvider()

        else:
            raise IllegalArgumentException('Unknown environment: ' + nosql_env)

        return NoSQLHandle(NoSQLHandleConfig(endpoint, provider))


    def main():

        table_name = 'PythonQuickstart'

        if len(sys.argv) != 2:
            print('Usage: python quickstart.py <cloud | cloudsim | kvstore>')
            raise SystemExit

        nosql_env = sys.argv[1:][0]
        print('Using environment: ' + str(nosql_env))

        handle = None
        try:

            #
            # Create a handle
            #
            handle = get_handle(nosql_env)

            #
            # Create a table
            #
            statement = (
                'Create table if not exists {} (id integer, sid integer, ' +
                'name string, primary key(shard(sid), id))').format(table_name)
            request = TableRequest().set_statement(statement).set_table_limits(
                TableLimits(30, 10, 1))
            handle.do_table_request(request, 50000, 3000)
            print('After create table')

            #
            # Put a few rows
            #
            request = PutRequest().set_table_name(table_name)
            for i in range(10):
                value = {'id': i, 'sid': 0, 'name': 'myname' + str(i)}
                request.set_value(value)
                handle.put(request)
            print('After put of 10 rows')

            #
            # Get the row
            #
            request = GetRequest().set_key({'id': 1, 'sid': 0}).set_table_name(
                table_name)
            result = handle.get(request)
            print('After get: ' + str(result))

            #
            # Query, using a range
            #
            statement = (
                'select * from ' + table_name + ' where id > 2 and id < 8')
            request = QueryRequest().set_statement(statement)
            result = handle.query(request)
            print('Query results for: ' + statement)
            for r in result.get_results():
                print('\t' + str(r))

            #
            # Delete the row
            #
            request = DeleteRequest().set_table_name(table_name).set_key(
                {'id': 1, 'sid': 0})
            result = handle.delete(request)
            print('After delete: ' + str(result))

            #
            # Get again to show deletion
            #
            request = GetRequest().set_key({'id': 1, 'sid': 0}).set_table_name(
                table_name)
            result = handle.get(request)
            print('After get (should be None): ' + str(result))

            #
            # Drop the table
            #
            request = TableRequest().set_statement(
                'drop table if exists {} '.format(table_name))
            result = handle.table_request(request)

            #
            # Table drop can take time, depending on the state of the system.
            # If this wait fails the table will still probably been dropped
            #
            result.wait_for_completion(handle, 40000, 2000)
            print('After drop table')

            print('Quickstart is complete')
        except Exception as e:
            print(e)
        finally:
            # If the handle isn't closed Python will not exit properly
            if handle is not None:
                handle.close()


    if __name__ == '__main__':
        main()

.. _run-quickstart:

Running Quickstart
------------------

Run Against the Oracle NoSQL Database Cloud Service
===================================================

Running against the Cloud Service requires an Oracle Cloud account. See
`Configure for the Cloud Service <https://nosql-python-sdk.readthedocs.io/en/
latest/installation.html#configure-for-the-cloud-service>`_ for information on
getting an account and acquiring required credentials.

1. Collect the following information:

 * Tenancy ID
 * User ID
 * API signing key (private key file in PEM format)
 * Fingerprint for the public key uploaded to the user's account
 * Private key pass phrase, needed only if the private key is encrypted

2. Edit *quickstart.py* and add your information. There are 2 ways to supply
   credentials in the program:

   * Directly provide the credential information. To use this method, modify the
     values of the variables at the top of the program: *tenancy*, *user*,
     *private_key*, *fingerprint*, and *pass_phrase*, setting them to the
     corresponding information you've collected.
   * Using a configuration file. In this case the information you've collected
     goes into a file, ~/.oci/config. `Configure for the Cloud Service <https://
     nosql-python-sdk.readthedocs.io/en/latest/installation.html#configure-for-
     the-cloud-service>`_ describes the contents of the file. It will look like
     this::

      [DEFAULT]
      tenancy=<your-tenancy-id>
      user=<your-user-id>
      fingerprint=<fingerprint-of-your-public-key>
      key_file=<path-to-your-private-key-file>
      pass_phrase=<optional-pass-phrase-for-key-file>

3. Decide which region you want to use and modify the *cloud_region* variable to
   the desired region. See `Regions documentation <https://nosql-python-sdk.
   readthedocs.io/en/latest/api/borneo.Regions.html>`_ for possible regions. Not
   all support the Oracle NoSQL Database Cloud Service.

4. Run the program:

.. code-block:: pycon

    python quickstart.py cloud

Run Against the Oracle NoSQL Cloud Simulator
============================================

Running against the Oracle NoSQL Cloud Simulator requires a running Cloud
Simulator instance. See `Using the Cloud Simulator <https://oracle.github.io/
nosql-node-sdk/tutorial-connect-cloud.html#cloudsim>`_ for information on how to
download and start the Cloud Simulator.

1. Start the Cloud Simulator based on instructions above. Note the HTTP port
   used. By default it is *8080* on *localhost*.

2. The *quickstart.py* program defaults to *localhost:8080* so if the Cloud
   Simulator was started using default values no editing is required.

3. Run the program:

.. code-block:: pycon

    python quickstart.py cloudsim

Run Against Oracle NoSQL on-premise
===================================

Running against the Oracle NoSQL Database on-premise requires a running Oracle
NoSQL Database instance as well as a running NoSQL Proxy server instance. The
program will connect to the proxy server.

See `Connecting to an On-Premise Oracle NoSQL Database <https://oracle.github.io
/nosql-node-sdk/tutorial-connect-on-prem.html>`_ for information on how to
download and start the database instance and proxy server. The database and
proxy should be started without security enabled for this quickstart program to
operate correctly. A secure configuration requires a secure proxy and more
complex configuration.

1. Start the Oracle NoSQL Database and proxy server based on instructions above.
   Note the HTTP port used. By default the endpoint is *localhost:80*.

2. The *quickstart.py* program defaults to *localhost:80*. If the proxy was
   started using a different host or port edit the settings accordingly.

3. Run the program:

.. code-block:: pycon

    python quickstart.py kvstore

=======
License
=======

Copyright (C) 2018, 2020 Oracle and/or its affiliates. All rights reserved.

This SDK is licensed under the Universal Permissive License 1.0. See
`LICENSE <./LICENSE.txt>`_ for details
