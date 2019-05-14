.. _install:


Installation
~~~~~~~~~~~~

This topic describes how to install, configure, and use the Oracle NoSQL Database Cloud Service Python SDK.

---------------
 Prerequisites
---------------

The Python SDK requires:

* An Oracle Cloud Infrastructure account
* A user created in that account, in a group with a policy that grants the desired permissions.
* Python version 2.7.5 or 3.5 or later, running on Mac, Windows, or Linux.

------------------------------------
 Downloading and Installing the SDK
------------------------------------

You can install the Python SDK through the Python Package Index (PyPI), or alternatively through GitHub.

====
PyPi
====

To install from `PyPI <https://pypi.python.org/pypi/oci>`_ use the following command::

    pip install borneo

======
GitHub
======

To install from GitHub:

1. Download the SDK from `GitHub <https://github.com/oracle/nosql-python-sdk/releases>`_.
   The download is a zip containing a whl file and documentation.
2. Extract the files from the zip.
3. Use the following command to install the SDK::

    pip install borneo-*-py2.py3-none-any.whl

  .. note::

      If you're unable to install the whl file, make sure pip is up to date.
      Use ``pip install -U pip`` and then try to install the whl file again.


---------------------
 Configuring the SDK
---------------------

The SDK requires an Oracle Cloud account and a subscription to the Oracle NoSQL
Database Cloud Service. If you do not already have an Oracle Cloud account you can start
`here <https://cloud.oracle.com/home>`_

===============================================================
Acquire Credentials for the Oracle NoSQL Database Cloud Service
===============================================================

See `Accessing Client Credentials <https://docs.oracle.com/pls/topic/lookup?ctx=en/cloud/paas/nosql-cloud&id=CSNSD-GUID-86E1E271-92AB-4F35-89FA-955E4359B16E>`_
and
`Required Credentials <https://docs.oracle.com/pls/topic/lookup?ctx=en/cloud/paas/nosql-cloud&id=CSNSD-GUID-EA8C0EC9-1CD8-48FD-9DA7-3FFCFEC975B8>`_
for additional information.

Several pieces of information comprise your credentials used by the
Oracle NoSQL Database Cloud Service:

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
 2. Click on the menu in the top left corner of the page and navigate into **Users**
 3. On **User Management** page click on **Identity Console** (in the upper
    right)
 4. On the **Identity Console** page click on the menu in the upper left and
    navigate to **Applications**
 5. On the **Applications** page, click on **ANDC** (NoSQL Database)
 6. On the **ANDC** page, click the **Configuration** tab and
    expand **General Information**
 7. The *Client ID* is visible to you.  Click on **Show Secret**. A pop-up
    dialog box appears displaying the Client Secret.  Copy and paste the
    *Client ID* and *Client Secret* to a new text file in a text editor.
 8. On the **ANDC** page, expand **Resources**. You'll see the *Entitlement
    ID** displayed in the **Primary Audience** text box. Copy only the 9-digit
    entitlement ID value.
 9. Copy the IDCS URL from the browser's address bar. Copy only up to
     **https://idcs-xxx.identity.oraclecloud.com**. Save the URL with your other
     saved information.

If you have multiple developers using the same tenancy for the service, share
these credentials with them. Each developer will use the Client ID and Client
Secret along with their own user name and password to create a usable
credentials file. The entitlement ID and IDCS URL are used in the application to
connect to the Oracle NoSQL Database Cloud Service.

=========================
Create a Credentials File
=========================

It is possible to provide credentials to your application programmatically but
it can be easier to use a single credentials file and
:class:`borneo.idcs.PropertiesCredentialsProvider` makes file-based access
simple.

By default the credentials file is found in *~/.andc/credentials* but the
location can be changed. The format is that of a properties file with the
format of *key=value*, with one property per line.
The contents and format are:
::

   andc_username=<your_cloud_username>
   andc_user_pwd=<your_cloud_password>
   andc_client_id=<application_client_id>
   andc_client_secret=<application_client_secret>

The client ID and client secret should be acquired using the instructions above.
The cloud username and password are for the cloud login. The order of the
properties does not matter.

=========================
Connecting an Application
=========================

The first step in any Oracle NoSQL Database Cloud Service application is to
create a *handle* used to send requests to the service. The handle is configured
using your credentials and other authentication information as well as the
communication endpoint. The endpoint is specific to the region you use, for
example, **ndcs.uscom-east-1.oraclecloud.com** or, if connecting to
the Cloud Simulator, **localhost:8080**.

.. code-block:: pycon

                from borneo import(NoSQLHandle, NoSQLHandleConfig)
                from borneo.idcs import (DefaultAccessTokenProvider,
                    PropertiesCredentialsProvider)

                #
                # Required information:
                #
                idcs_url=<your_idcs_url>
                entitlement_id=<your_entitlement_id>
                credentials_file=<path_to_your_credentials_file>
                endpoint=<communication_endpoint>

                #
                # Create an AuthorizationProvider
                #  o requires IDCS URL and Entitlement ID
                #
                at_provider = DefaultAccessTokenProvider(
                    idcs_url=idcs_url, entitlement_id=entitlement_id)

                #
                # set the credentials provider based on your credentials file
                #
                at_provider.set_credentials_provider(
                    PropertiesCredentialsProvider().set_properties_file(credentials_file)

                #
                # create a configuration object
                #
                config = NoSQLHandleConfig().set_authorization_provider(provider)

                #
                # create a handle from the configuration object
                #
                handle = NoSQLHandle(config)

See examples and test code for specific details. Both of these use *parameters.py*
files for configuration of required information. The examples have instructions

-------------------------
Using the Cloud Simulator
-------------------------

The instructions above are focused on connecting to the Oracle NoSQL Database
Cloud Service directly. The Oracle NoSQL Cloud Simulator is a useful way to use
this SDK to connect to a local server that supports the same protocol. The Cloud
Simulator requires Java 8 or higher.

See
`Download the Oracle NoSQL Cloud Simulator <https://docs.oracle.com/pls/topic/lookup?ctx=en/cloud/paas/nosql-cloud&id=CSNSD-GUID-3E11C056-B144-4EEA-8224-37F4C3CB83F6>`_ to download and start the Cloud Simulator.

 1. Download and start the Cloud Simulator
 2. Follow instructions in the examples/parameters.py file for connecting
    examples to the Cloud Simulator. By default that file is configured to
    communicate with the Cloud Simulator, using default configuration.

The Cloud Simulator does not require the credentials and authentication
information required by the Oracle NoSQL Database Cloud Service. The Cloud
Simulator should not be used for deploying applications or important data.



It is recommended that users start with the Cloud Simulator to become familiar
with the interfaces supported by the SDK.
