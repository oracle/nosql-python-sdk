.. _install:

~~~~~~~~~~~~
Installation
~~~~~~~~~~~~

This topic describes how to install, configure, and use the Oracle NoSQL
Database Python SDK. There are several supported environments:

1. Oracle NoSQL Database Cloud Service
2. Oracle NoSQL Database Cloud Simulator
3. Oracle NoSQL Database on-premise

---------------
 Prerequisites
---------------

The Python SDK requires:

* Python version 2.7.5 or 3.5 or later, running on Mac, Windows, or Linux.
* For the Oracle NoSQL Cloud Service:

  * An Oracle Cloud Infrastructure account
  * A user created in that account, in a group with a policy that grants the
    desired permissions.

* For the Oracle NoSQL Database Cloud Simulator:

  * See `Download the Oracle NoSQL Cloud Simulator <https://docs.oracle.com/pls/
    topic/lookup?ctx=en/cloud/paas/nosql-cloud&id=CSNSD-GUID-3E11C056-B144-4EEA-
    8224-37F4C3CB83F6>`_ to download and start the Cloud Simulator.

* For the on-premise Oracle NoSQL Database:

  * An instance of the database (See `Oracle NoSQL Database Downloads <https://
    www.oracle.com/database/technologies/nosql-database-server-downloads.html>`_
    )
  * A running proxy server, see `Information about the proxy <https://docs.
    oracle.com/pls/topic/lookup?ctx=en/database/other-databases/nosql-database/
    19.3/admin&id=NSADM-GUID-C110AF57-8B35-4C48-A82E-2621C6A5ED72>`_

------------------------------------
 Downloading and Installing the SDK
------------------------------------

You can install the Python SDK through the Python Package Index (PyPI), or
alternatively through GitHub.

====
PyPi
====

To install from `PyPI <https://pypi.python.org/pypi/borneo>`_ use the following
command::

    pip install borneo

======
GitHub
======

To install from GitHub:

1. Download the SDK from `GitHub <https://github.com/oracle/nosql-python-sdk/
   releases>`_. The download is a zip containing a whl file and documentation.
2. Extract the files from the zip.
3. Use the following command to install the SDK::

    pip install borneo-*-py2.py3-none-any.whl

  .. note::

      If you're unable to install the whl file, make sure pip is up to date.
      Use ``pip install -U pip`` and then try to install the whl file again.


---------------------
 Configuring the SDK
---------------------

This section describes configuring the SDK for the 3 environments supported.
Skip to the section or sections of interest. The areas where the environments
and use differ are

1. Authentication and authorization. This is encapsulated in the
   AuthorizationProvider interface. The Cloud Service is secure and requires a
   Cloud Service identity as well as authorization for desired operations. The
   Cloud Simulator is not secure at all and requires no identity. The on-premise
   configuration can be either secure or not and it also requires an instance of
   the proxy service to access the database.
2. API differences. Some classes and methods are specific to an environment. For
   example, the on-premise configuration includes methods to create namespaces
   and users and these concepts don't exist in the cloud service. Similarly, the
   cloud service includes interfaces to specify and acquire throughput
   information on tables that is not relevant on-premise.

===============================
Configure for the Cloud Service
===============================

The SDK requires an Oracle Cloud account and a subscription to the Oracle NoSQL
Database Cloud Service. If you do not already have an Oracle Cloud account you
can start `here <https://www.oracle.com/cloud>`_. Credentials used for
connecting an application are associated with a specific user. If needed, create
a user for the person or system using the api. See `Adding Users <https://docs.
cloud.oracle.com/en-us/iaas/Content/GSG/Tasks/addingusers.htm>`_.

Using the SDK with the Oracle NoSQL Database Cloud Service also requires
installation of the Oracle Cloud Infrastructure (OCI) Python SDK::

    pip install oci

.. _creds-label:

Acquire Credentials for the Oracle NoSQL Database Cloud Service
_______________________________________________________________

These steps only need to be performed one time for a user. If they have already
been done they can be skipped. You need to obtain the following credentials:

 * Tenancy ID
 * User ID
 * API signing key (private key in PEM format)
 * Private key pass phrase, only needed if the private key is encrypted
 * Fingerprint for the public key uploaded to the user's account

See `Required Keys and OCIDs <https://docs.cloud.oracle.com/iaas/Content/API/
Concepts/apisigningkey.htm>`_  for detailed descriptions of the above
credentials and the steps you need to perform to obtain them. Specifically:

 * `How to Generate an API Signing Key <https://docs.cloud.oracle.com/en-us/
   iaas/Content/API/Concepts/apisigningkey.htm#How>`_
 * `How to Get the Key's Fingerprint <https://docs.cloud.oracle.com/en-us/iaas/
   Content/API/Concepts/apisigningkey.htm#How3>`_
 * `How to Upload the Public Key <https://docs.cloud.oracle.com/en-us/iaas/
   Content/API/Concepts/apisigningkey.htm#How2>`_
 * `Where to Get the Tenancy's OCID and User's OCID <https://docs.cloud.oracle.
   com/en-us/iaas/Content/API/Concepts/apisigningkey.htm#Other>`_


Supplying Credentials to an Application
_______________________________________

Credentials are used to establish the initial connection from your application
to the service. There are 2 ways to supply credentials to the application:

 1. Directly, via API
 2. Using a configuration file

Both mechanisms use :class:`borneo.iam.SignatureProvider` to handle credentials.
If using a configuration file it's default location is *$HOME/.oci/config*, but
the location can be changed using the api.

The format of the configuration file is that of a properties file with the
format of *key=value*, with one property per line. The contents and format are::

    [DEFAULT]
    tenancy=<your-tenancy-id>
    user=<your-user-id>
    fingerprint=<fingerprint-of-your-public-key>
    key_file=<path-to-your-private-key-file>
    pass_phrase=<optional-pass-phrase-for-key-file>

The Tenancy ID, User ID and fingerprint should be acquired using the
instructions above. The path to your private key file is the absolute path of
the RSA private key. The order of the properties does not matter. The
*[DEFAULT]* portion is the *profile*. A configuration file may contain multiple
profiles with the target profile specified in the
:class:`borneo.iam.SignatureProvider` parameters.

Provide credentials without a configuration file:

.. code-block:: pycon

                from borneo.iam import SignatureProvider

                #
                # Use SignatureProvider directly via API. Note that the
                # private_key argument can either point to a key file or be the
                # string content of the private key itself.
                #
                at_provider = SignatureProvider(
                    tenant_id='ocid1.tenancy.oc1..tenancy',
                    user_id='ocid1.user.oc1..user',
                    private_key=key_file_or_key,
                    fingerprint='fingerprint',
                    pass_phrase='mypassphrase')

Provide credentials using a configuration file in the default location, using
the default profile:

.. code-block:: pycon

                from borneo.iam import SignatureProvider

                #
                # Use SignatureProvider with a default credentials file and
                # profile $HOME/.oci/config
                #
                at_provider = SignatureProvider()

Provide credentials using a configuration file in a non-default location and
non-default profile:

.. code-block:: pycon

                from borneo.iam import SignatureProvider

                #
                # Use SignatureProvider with a non-default credentials file and
                # profile
                #
                at_provider = SignatureProvider(config_file='myconfigfile',
                    profile_name='myprofile')


Connecting an Application
_________________________

The first step in any Oracle NoSQL Database Cloud Service application is to
create a *handle* used to send requests to the service. The handle is configured
using your credentials and other authentication information as well as the
endpoint to which the application will connect. An example endpoint is to use
the region **Regions.US_ASHBURN_1**. Information on regions can be found in
:class:`borneo.Regions`.

.. code-block:: pycon

                from borneo import NoSQLHandle, NoSQLHandleConfig, Regions
                from borneo.iam import SignatureProvider

                #
                # Required information:
                #

                # the region to which the application will connect
                region = Regions.US_ASHBURN_1

                # if using a specified credentials file
                credentials_file = <path-to-your-credentials-file>

                #
                # Create an AuthorizationProvider
                #
                at_provider = SignatureProvider(config_file=credentials_file)

                #
                # create a configuration object
                #
                config = NoSQLHandleConfig(region, at_provider)

                #
                # create a handle from the configuration object
                #
                handle = NoSQLHandle(config)

See examples and test code for specific details. Both of these use config*.py
files for configuration of required information.

=================================
Configure for the Cloud Simulator
=================================

The Oracle NoSQL Cloud Simulator is a useful way to use this SDK to connect to a
local server that supports the same protocol. The Cloud Simulator requires Java
8 or higher.

See `Download the Oracle NoSQL Cloud Simulator <https://docs.oracle.com/pls/
topic/lookup?ctx=en/cloud/paas/nosql-cloud&id=CSNSD-GUID-3E11C056-B144-4EEA-8224
-37F4C3CB83F6>`_ to download and start the Cloud Simulator.

 1. Download and start the Cloud Simulator
 2. Follow instructions in the examples/config.py file for connecting examples
    to the Cloud Simulator. By default that file is configured to communicate
    with the Cloud Simulator, using default configuration.

The Cloud Simulator does not require the credentials and authentication
information required by the Oracle NoSQL Database Cloud Service. The Cloud
Simulator should not be used for deploying applications or important data.

Before using the Cloud Service it is recommended that users start with the Cloud
Simulator to become familiar with the interfaces supported by the SDK.

==================================================
Configure for the On-Premise Oracle NoSQL Database
==================================================

The on-premise configuration requires a running instance of the Oracle NoSQL
database. In addition a running proxy service is required. See `Oracle NoSQL
Database Downloads <https://www.oracle.com/database/technologies/nosql-database-
server-downloads.html>`_ for downloads, and see `Information about the proxy
<https://docs.oracle.com/pls/topic/lookup?ctx=en/database/other-databases/nosql-
database/19.3/admin&id=NSADM-GUID-C110AF57-8B35-4C48-A82E-2621C6A5ED72>`_ for
proxy configuration information.

If running a secure store, a certificate path should be specified through the
REQUESTS_CA_BUNDLE environment variable:

 $ export REQUESTS_CA_BUNDLE=<path-to-certificate>/certificate.pem:\
 $REQUESTS_CA_BUNDLE

Or :func:`borneo.NoSQLHandleConfig.set_ssl_ca_certs`.

In addition, a user identity must be created in the store (separately) that has
permission to perform the required operations of the application, such as
manipulated tables and data. The identity is used in the
:class:`borneo.kv.StoreAccessTokenProvider`.

If the store is not secure, an empty instance of
:class:`borneo.kv.StoreAccessTokenProvider` is used. For example:

.. code-block:: pycon

  from borneo import NoSQLHandle, NoSQLHandleConfig
  from borneo.kv import StoreAccessTokenProvider

  #
  # Assume the proxy is running on localhost:8080
  #
  endpoint = 'http://localhost:8080'

  #
  # Assume the proxy is secure and running on localhost:443
  #
  endpoint = 'https://localhost:443'

  #
  # Create the AuthorizationProvider for a secure store:
  #
  ap = StoreAccessTokenProvider(user_name, password)

  #
  # Create the AuthorizationProvider for a not secure store:
  #
  ap = StoreAccessTokenProvider()

  #
  # create a configuration object
  #
  config = NoSQLHandleConfig(endpoint).set_authorization_provider(ap)

  #
  # set the certificate path if running a secure store
  #
  config.set_ssl_ca_certs(<ca_certs>)

  #
  # create a handle from the configuration object
  #
  handle = NoSQLHandle(config)
