
Python SDK for Oracle NoSQL Database
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This document is for developers of the Python SDK for the Oracle NoSQL Database.
Developers are those who need to modify source and examples, build and run tests
and examples, and build documentation.

===============
Getting Started
===============
Clone the repository and Install dependencies.

1. Make sure that Python is installed on your system, at least version 2.7.
2. Install pip if it is not installed, follow the `pip installation instructions
   <https://pip.pypa.io/en/stable/installing>`_.
3. Clone the repository and install development dependencies::

     git clone https://github.com/oracle/nosql-python-sdk.git
     cd nosql-python-sdk
     pip install -r requirements.txt

Running Tests and Examples
==========================

During development the unit tests and examples run against either a local Cloud
Simulator server, which can run on the local machine or an instance of the
on-premise Proxy that allows access to a local instance of the Oracle NoSQL
Database. See `Download the Oracle NoSQL Cloud Simulator <https://docs.oracle.
com/pls/topic/lookup?ctx=en/cloud/paas/nosql-cloud&id=CSNSD-GUID-3E11C056-B144-
4EEA-8224-37F4C3CB83F6>`_ to download and start the Cloud Simulator. See `Oracle
NoSQL Downloads <https://www.oracle.com/database/technologies/nosql-database-
server-downloads.html>`_ to download the on-premise product and proxy server.

Tests and examples have settings that can be changed based on environment. Test
settings are in test/config*.py. Refer to the comments in the tests and examples
for details. The default test/config.py and test/config_cloudsim.py will use a
Cloud Simulator instance that is running on its default settings of
localhost:8080, config_cloudsim.py is a backup of config.py, when config.py is
overwritten by other config*.py, the default config.py of Cloud Simulator is
back up in config_cloudsim.py.

All tests require that your PYTHONPATH be set to the development tree:

 $ export PYTHONPATH=<path-to-nosql-python-sdk>/nosql-python-sdk/src:\
 $PYTHONPATH

If using on-premise Oracle NoSQL database with security enabled, the certificate
path can be specified through the REQUESTS_CA_BUNDLE environment variable:

 $ export REQUESTS_CA_BUNDLE=<path-to-certificate>/certificate.pem:\
 $REQUESTS_CA_BUNDLE

Or use the API :func:`borneo.NoSQLHandleConfig.set_ssl_ca_certs` to specify it.

Run Unit Tests
--------------

    1. The <path-to-repo>/test/config.py is used to run the unit test against
       Cloud Simulator, modify it to suit your environment. When config.py is
       overwritten by other config*.py, config_cloudsim.py is used to run the
       unit test against Cloud Simulator, modify it to suit your environment.
       Then copy the content of config_onprem.py to config.py.
    2. The <path-to-repo>/test/config_onprem.py is used to run the unit test
       against on-premise proxy, modify it to suit your environment. Then copy
       the content of config_onprem.py to config.py.

    Notice that the comments in these config files tells you how to modify the
    settings.

    3. With the desired server running, start testing.

       .. code-block:: pycon

          $ cd <path-to-repo>/test
          $ python -m unittest discover -p '*.py' (Run all the tests)
          $ python <testcase>.py (Run individual test)

       You can also run a test case using the following command

       .. code-block:: pycon

          $ python -m unittest <testfile>.<testclass>.<testname>
          e.g.
          $ python -m unittest put.TestPut.testPutNoVersionWithMatchVersion

Run Examples
------------

    1. Set PYTHONPATH to point to the development tree.

       $ export PYTHONPATH=<path-to-nosql-python-sdk>/nosql-python-sdk/src:\
       $PYTHONPATH

    2. The <path-to-repo>/examples/config.py is used to run the example against
       Cloud Simulator, modify it to suit your environment. When config.py is
       overwritten by other config*.py, config_cloudsim.py is used to run the
       unit test against Cloud Simulator, modify it to suit your environment.
       Then copy the content of config_onprem.py to config.py.
    3. The <path-to-repo>/examples/config_onprem.py is used to run the example
       against on-premise proxy, modify it to suit your environment. Then copy
       the content of config_onprem.py to config.py.
    4. The <path-to-repo>/examples/config_cloud.py is used to run the example
       against Cloud Service, modify it to suit your environment. Then copy the
       content of config_onprem.py to config.py.

    Notice that the comments in these config files tells you how to modify the
    settings.

    5. With the desired server running, run an example.

       .. code-block:: pycon

          $ cd <path-to-repo>/examples
          $ python multi_data_ops.py

Building Documentation
======================

Note: new classes and methods must be added to the appropriate files in docs/api/
or they will not be found by this build. That process is manual. The same applies
to methods that have been removed

The documentation build depends on sphinx (http://sphinx-doc.org/install.html),
sphinx-automodapi, and sphinx_rtd_theme. They should have been installed per the
instructions above.

.. code-block:: pycon

   $ cd <path-to-repo>/docs
   $ make html

Documentation is built into <path-to-repo>/docs/_build.
If public api classes are modified it may be necessary to modify, add, or remove
files in <path-to-repo>/docs/api as well as modifying relevant files in the docs
directory.
