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

In order to run with the on-premise Oracle NoSQL Database an instance of the
database is required (See `Oracle NoSQL Database Downloads <https://www.oracle.
com/database/technologies/nosql-database-server-downloads.html>`_ and a running
proxy service is required (see `Information about the proxy <https://docs.
oracle.com/pls/topic/lookup?ctx=en/database/other-databases/nosql-database/19.3/
admin&id=NSADM-GUID-C110AF57-8B35-4C48-A82E-2621C6A5ED72>`_.

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

===========
Development
===========

The `development readme <https://github.com/oracle/nosql-python-sdk/blob/master/
README-DEV.rst>`_ has information about running tests and examples, building the
documentation, and other development activities.

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

=======
License
=======

Copyright (C) 2018, 2019 Oracle and/or its affiliates. All rights reserved.

This SDK is licensed under the Universal Permissive License 1.0. See
`LICENSE <https://github.com/oracle/nosql-python-sdk/blob/master/LICENSE.txt>`_
for details
