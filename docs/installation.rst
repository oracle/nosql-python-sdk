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

See `Accessing Client Credentials <https://docs.oracle.com/pls/topic/lookup?ctx=
en/cloud/paas/nosql-cloud&id=CSNSD-GUID-86E1E271-92AB-4F35-89FA-955E4359B16E>`_
and `Required Credentials <https://docs.oracle.com/pls/topic/lookup?ctx=en/cloud
/paas/nosql-cloud&id=CSNSD-GUID-EA8C0EC9-1CD8-48FD-9DA7-3FFCFEC975B8>`_ for
additional information.

Several pieces of information comprise your credentials used by the Oracle NoSQL
Database Cloud Service:

 * Client ID
 * Client Secret
 * User Name
 * User Password

In addition, 2 more pieces of information are used by applications to use the
service:

 * Entitlement ID
 * IDCS URL

How to acquire Client ID, Client Secret, Entitlement ID, and IDCS URL:

 1. Sign into your **My Services** page
 2. Click on the menu in the top left corner of the page and navigate into
    **Users**
 3. On **User Management** page click on **Identity Console** (in the upper
    right)
 4. On the **Identity Console** page click on the menu in the upper left and
    navigate to **Applications**
 5. On the **Applications** page, click on **ANDC** (NoSQL Database)
 6. On the **ANDC** page, click the **Configuration** tab and expand **General
    Information**
 7. The *Client ID* is visible to you.  Click on **Show Secret**. A pop-up
    dialog box appears displaying the Client Secret.  Copy and paste the
    *Client ID* and *Client Secret* to a new text file in a text editor.
 8. On the **ANDC** page, expand **Resources**. You'll see the **Entitlement
    ID** displayed in the **Primary Audience** text box. Copy only the 9-digit
    entitlement ID value.
 9. Copy the IDCS URL from the browser's address bar. Copy only up to
    **https://idcs-xxx.identity.oraclecloud.com**. Save the URL with your other
    saved information.

If you have multiple developers using the same tenancy for the service, share
these credentials with them. Each developer will use the Client ID and Client
Secret along with their own user name and password to create usable credentials.
The entitlement ID and IDCS URL are used in the application to connect to the
Oracle NoSQL Database Cloud Service.


Supplying Credentials to an Application
_______________________________________

Credentials are used to establish the initial connection from your application
to the service. There are 2 ways to supply the credentials:

1. Using a file and :class:`borneo.idcs.PropertiesCredentialsProvider`
2. Using an instance of :class:`borneo.idcs.CredentialsProvider` to supply the
   credentials

Using a file is handy and makes it easy to share credentials but it is not
particularly secure as the information is in plain text in the file. If done the
permission settings on the file should limit read access to just the
application. In general it is recommended that secure applications create an
instance of :class:`borneo.idcs.CredentialsProvider`.

Creating Your Own CredentialsProvider
=====================================

You can supply credentials with your own implementation of
:class:`borneo.idcs.CredentialsProvider`.

.. code-block:: pycon

                from borneo.idcs import (
                    CredentialsProvider, DefaultAccessTokenProvider,
                    IDCSCredentials)

                class MyCredentialsProvider(CredentialsProvider):

                    def get_oauth_client_credentials(self):
                        return IDCSCredentials('your_idcs_client_id',
                                               'your_client_secret')

                    def get_user_credentials(self):
                        #
                        # password must be URL-encoded. This can be done using
                        # urllib.parse.quote
                        #
                        return IDCSCredentials('your_oracle_cloud_user_name',
                                               'your_oracle_cloud_password')

                #
                # Use MyCredentialsProvider
                #
                at_provider = DefaultAccessTokenProvider(
                    idcs_url, entitlement_id)
                at_provider.set_credentials_provider(MyCredentialsProvider())


Using a File for Credentials
============================

:class:`borneo.idcs.PropertiesCredentialsProvider` reads credentials from a
file. By default the credentials file is found in *$HOME/.andc/credentials* but
the location can be changed using
:func:`borneo.idcs.PropertiesCredentialsProvider.set_properties_file`. The
format of the file is that of a properties file with the format of *key=value*,
with one property per line. The contents and format are::

   andc_username=<your_cloud_username>
   andc_user_pwd=<your_cloud_password>
   andc_client_id=<application_client_id>
   andc_client_secret=<application_client_secret>

The client ID and client secret should be acquired using the instructions above.
The cloud username and password are for the cloud login. The order of the
properties does not matter.

.. code-block:: pycon

                from borneo.idcs import (
                    DefaultAccessTokenProvider, PropertiesCredentialsProvider)

                #
                # Use PropertiesCredentialsProvider
                #
                at_provider = DefaultAccessTokenProvider(
                    idcs_url, entitlement_id)
                at_provider.set_credentials_provider(
                    PropertiesCredentialsProvider().set_credentials_file(
                    <path-to-file>))


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
                from borneo.idcs import (DefaultAccessTokenProvider,
                    PropertiesCredentialsProvider)

                #
                # Required information:
                #
                idcs_url=<your_idcs_url>
                entitlement_id=<your_entitlement_id>
                endpoint=<communication_endpoint>

                # if using a credentials file
                credentials_file=<path_to_your_credentials_file>

                #
                # Create an AuthorizationProvider
                #  o requires IDCS URL and Entitlement ID
                #
                at_provider = DefaultAccessTokenProvider(
                    idcs_url=idcs_url, entitlement_id=entitlement_id)

                #
                # set the credentials provider. 2 examples:
                # 1. using a properties file
                # 2. using a custom CredentialsProvider
                #

                #
                # (1) set the credentials provider based on your credentials
                # file
                #
                at_provider.set_credentials_provider(
                    PropertiesCredentialsProvider().set_properties_file(
                    credentials_file)

                # OR
                #
                # (2) use your own instance of CredentialsProvider (e.g.
                # MyCredentialsProvider -- see above example)
                #
                # at_provider.set_credentials_provider(MyCredentialsProvider())

                #
                # create a configuration object
                #
                config = NoSQLHandleConfig(endpoint).set_authorization_provider(
                    provider)

                #
                # create a handle from the configuration object
                #
                handle = NoSQLHandle(config)

See examples and test code for specific details. Both of these use
*parameters.py* files for configuration of required information. The examples
can use either style of CredentialsProvider -- using a file or using a custom
class.

=================================
Configure for the Cloud Simulator
=================================

The Oracle NoSQL Cloud Simulator is a useful way to use
this SDK to connect to a local server that supports the same protocol. The Cloud
Simulator requires Java 8 or higher.

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
