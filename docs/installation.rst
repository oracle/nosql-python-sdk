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

To install from `PyPI <https://pypi.python.org/pypi/oci>`_ use the following
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
can start `here <https://cloud.oracle.com/home>`_


Acquire Credentials for the Oracle NoSQL Database Cloud Service
_______________________________________________________________

See `Required Keys and OCIDs <https://docs.cloud.oracle.com/iaas/Content/API/
Concepts/apisigningkey.htm>`_ for additional information.

Several pieces of information comprise your credentials used by the Oracle NoSQL
Database Cloud Service:

 * Tenancy ID
 * User ID
 * Fingerprint
 * Private Key File

How to acquire Tenancy ID, and User ID:

 1. Sign into your **Oracle Cloud Infrastructure Console** page.
 2. Open the navigation menu, under Governance and Administration, go to
    **Administration** and click **Tenancy Details**.
 3. The Tenancy ID is shown under **Tenancy Information**. Click **Copy** to
    copy it to your clipboard, then paste it to your credentials file.
 4. Go back to **Oracle Cloud Infrastructure Console** page, open the
    **Profile** menu (User menu icon) and click **User Settings**.
 5. The User ID is shown under **User Information**. Click **Copy** to copy it
    to your clipboard, then paste it to your credentials file.
 
How to generate an API Signing Key, upload the public key and get the
fingerprint:

 1. If you haven't already, create a .oci directory to store the credentials.
    $ mkdir ~/.oci
 2. Generate the private key with one of the following commands.
    $ openssl genrsa -out ~/.oci/key.pem 2048
 3. Ensure that only you can read the private key file.
    $ chmod go-rwx ~/.oci/key.pem
 4. Generate the public key.
    $ openssl rsa -pubout -in ~/.oci/key.pem -out ~/.oci/key_public.pem
 5. Sign into your **Oracle Cloud Infrastructure Console** page.
 6. Click your username in the top-right corner of the Console, and then click
    **User Settings**.
 7. Click **Add Public Key** and paste the contents of the PEM public key in the
    dialog box and click **Add**.
 8. The key's fingerprint is displayed (for example, 12:34:56:78:90:ab:cd:ef:12:
    34:56:78:90:ab:cd:ef).
 9. Copy the key's fingerprint to your clipboard, then paste it to your
    credentials file.
 10. Put the path to your private key to your credentials file.


Supplying Credentials to an Application
_______________________________________

Credentials are used to establish the initial connection from your application
to the service. The way to supply the credentials is to use a credentials file,
:class:`borneo.iam.SignatureProvider` reads credentials from the credentials
file, by default the credentials file is found in *$HOME/.oci/config but the
location can be changed using::

    SignatureProvider(config_file=<path-to-your-credentials-file>)
    
The format of the file is that of a properties file with the format of
*key=value*, with one property per line. The contents and format are::

    [DEFAULT]
    tenancy=<your-tenancy-id>
    user=<your-user-id>
    fingerprint=<fingerprint-of-your-public-key>
    key_file=<path-to-your-private-key-file>

The Tenancy ID, User ID and fingerprint should be acquired using the
instructions above. The path to your private key file is the absolute path of
the RSA private key. The order of the properties does not matter.

.. code-block:: pycon

                from borneo.iam import SignatureProvider

                #
                # Use SignatureProvider with a default credentials file
                # $HOME/.oci/config
                #
                at_provider = SignatureProvider()


Connecting an Application
_________________________

The first step in any Oracle NoSQL Database Cloud Service application is to
create a *handle* used to send requests to the service. The handle is configured
using your credentials and other authentication information as well as the
communication endpoint. The endpoint is specific to the region you use, for
example, **ndcs.uscom-east-1.oraclecloud.com** or, if connecting to the Cloud
Simulator, **localhost:8080**.

.. code-block:: pycon

                from borneo import NoSQLHandle, NoSQLHandleConfig
                from borneo.iam import SignatureProvider

                #
                # Required information:
                #
                endpoint=<communication_endpoint>

                # if using a specified credentials file
                credentials_file=<path-to-your-credentials-file>

                #
                # Create an AuthorizationProvider
                #
                at_provider = SignatureProvider(config_file=credentials_file)

                #
                # create a configuration object
                #
                config = NoSQLHandleConfig(endpoint, provider=at_provider)

                #
                # create a handle from the configuration object
                #
                handle = NoSQLHandle(config)

See examples and test code for specific details. Both of these use
*parameters.py* files for configuration of required information.

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
 2. Follow instructions in the examples/parameters.py file for connecting
    examples to the Cloud Simulator. By default that file is configured to
    communicate with the Cloud Simulator, using default configuration.

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

If running a secure store a user identity must be created in the store
(separately) that has permission to perform the required operations of the
application, such as manipulated tables and data. The identity is used in the
:class:`borneo.kv.StoreAccessTokenProvider`. If the store is not secure an empty
instance of :class:`borneo.kv.StoreAccessTokenProvider` is used. For example.

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
  ap = StoreAccessTokenProvider(userName, password)

  #
  # Create the AuthorizationProvider for a not secure store:
  #
  ap = StoreAccessTokenProvider()

  #
  # create a configuration object
  #
  config = NoSQLHandleConfig(endpoint).set_authorization_provider(ap)

  #
  # create a handle from the configuration object
  #
  handle = NoSQLHandle(config)
