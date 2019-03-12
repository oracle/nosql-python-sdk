=======================================================
 Python Driver for Oracle NoSQL Database Cloud Service
=======================================================

This document is for developers of the Python SDK for the Oracle NoSQL Database
Cloud Service. Developers are those who need to modify source and examples,
build and run tests and examples, and build documentation.

Dependencies
============

Dependencies include Python 2.7/3.5 and a few Python modules.

1. Make sure that Python is installed on your system, at least version 2.7.

2. Install pip if it is not installed, follow the instructions to install it here:
    https://pip.pypa.io/en/stable/installing/

3. Install 3rd party dependencies:

   $  pip install -r requirements.txt

These are the dependencies:

 * requests -- used for HTTP communication
 * rsa, numpy -- used for unit testing
 * sphinx, sphinx-automodapi, sphinx_rtd_theme -- used for doc

Running Tests and Examples
==========================

During development the unit tests and examples run against a local CloudSim server,
which can run on the local machine. By default the tests and examples expect it on
the endpoint, localhost:8080.

Tests and examples have settings that can be changed based on environment.
Test settings are in test/parameters.py. Refer to the comments in the tests and
examples for details.

All tests require that your PYTHONPATH be set to the development tree:
 $ export PYTHONPATH=<path-to-sk.python>/sk.python/src:$PYTHONPATH

Run Unit Tests
--------------

    1. Modify <path-to-repo>/test/parameters.py to suit your environment. The
       comments in that file tells you how to modify the settings.
    2. With the CloudSim server running, start testing.

      .. code-block::

        $ cd <path-to-repo>/test
        $ python -m unittest discover -p '*.py' (Run all the tests)
        $ python <testcase>.py (Run individual test)

      You can also run a test case using the following command

      .. code-block::

            $ python -m unittest <testfile>.<testclass>.<testname>
            e.g.
            $ python -m unittest put.TestPut.testPutNoVersionWithMatchVersion

Run Stress Test
---------------

The stress test is a multi-threaded test that exercises concurrent operation in
the driver. It also assumes a running CloudSim instance on the default endpoint,
localhost:8080.

    1. Modify <path-to-repo>/stresstest/parameters.py to suit your environment.
       The comments in that file tells you how to modify the settings.
    2. With CloudSim running, start the test.

      .. code-block::

         $ cd <path-to-repo>/stresstest
         $ python start.py (Start a multiprocess stress test)

Run Examples
------------

    1. Set PYTHONPATH to point to the development tree.
       $ export PYTHONPATH=<path-to-sk.python>/sk.python/src:$PYTHONPATH
    2. Modify <path-to-repo>/examples/parameters.py to suit your environment.
       The comments in that file tells you how to modify the settings.
    3. With the CloudSim running, run a test

      .. code-block::

       $ cd <path-to-repo>/examples
       $ python multi_data_ops.py

Building Documentation
======================

The documentation build depends on sphinx (http://sphinx-doc.org/install.html),
sphinx-automodapi, and sphinx_rtd_theme. They all need to be installed. Once
installed the documentation can be built:

.. code-block::

  $ cd <path-to-repo>/docs
  $ make html

If public api classes are modified it may be necessary to modify, add, or remove
files in <path-to-repo>/docs/api as well as modifying relevant files in the docs
directory.
