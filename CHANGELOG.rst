Change Log
~~~~~~~~~~
All notable changes to this project will be documented in this file.

The format is based on `Keep a Changelog <http://keepachangelog.com/>`_.
====================
 Unreleased
====================

Fixed
-----

* Fixed import package collections.abc for Callable.

Added
_____

* Cloud only: New regions: us-tacoma-1, us-chicago-1, eu-dcc-milan-1,
  eu-dcc-milan-2, eu-dcc-dublin-1, eu-dcc-dublin-2, eu-dcc-rating-1,
  eu-dcc-rating-2, us-westjordan-1, us-dcc-phoenix-1, us-dcc-phoenix-2
* Added new method borneo.NoSQLHandle.query_iterable(query_request) to get an
  iterable that contains all the results of a query.
* Added support for specifying update operations from parent and child tables
  when executing write_multiple operations.

====================
 5.3.5 - 2022-08-09
====================

Fixed
_____

* Cloud only. Fixed references to the OCI "auth" package from iam.py that could lead to the error, "name 'auth' is not defined."

====================
 5.3.4 - 2022-06-13
====================

Fixed
_____

* Cloud only. Recognize the region parameter in the SignatureProvider constructor when an explicit provider is passed

Added
_____

* Added client statistics. Users can enable internal driver statistics by
  using ``NOSQL_STATS_PROFILE=[none|regular|more|all]`` environment variable or
  by using the ``NoSQLHandleConfig.set_stats_profile()`` api.
* Cloud only: New regions: eu-paris-1, eu-madrid-1 and mx-queretaro-1.

====================
 5.3.3 - 2022-04-19
====================

Fixed
_____

* Cloud only: fixed the internal, automatic refresh of the security token when using Instance Principal authentication so that it is done well before the token expires
* Use selective module import for OCI SDK modules, and set the environment variable, OCI_PYTHON_SDK_NO_SERVICE_IMPORTS=1, to improve import speed by suppressing import of unnecessary modules from the OCI SDK
* Added dependencies to setup.py so that "pip install" automatically includes them

Added
_____

* Support for session persistence. If a Set-Cookie HTTP header is present  borneo will now set a Cookie header using the requested session value

====================
 5.3.0 - 2022-02-17
====================

Added
_____

* Cloud only: support for on-demand tables

  * Changes to TableLimits to specify on-demand tables
* Existing row modification is made available in Results when the operation fails
  and the previous is requested
* On-premise only: support for setting Durability in write operations

  * Added Durability class and methods to set Durability

Changed
_______

* Cloud only: updated OCI regions
* The SDK now detects the version of the server it's connected to  and adjusts its capabilities to match. This allows the SDK to communicate with servers that may only support an earlier protocol version, with the corresponding feature restrictions

Fixed
_____

* Fixed handling of request id so that each request now gets a new id

====================
 5.2.4 - 2021-05-19
====================

NOTE: a couple of versions were skipped because of internal versioning issues.
There are no public releases for 5.2.2 and 5.2.3

Added
_____

* Added NoSQLHandleConfig.set_max_content_length to allow on-premise
  configuration of a maximum request content size. It defaults to 32MB.
* If a delegation token is being used for authorization the HTTP header,
  'opc-obo-token' will be sent with the contents of the token.
* Rate Limiting (cloud only):

  * New method NoSQLHandleConfig.set_rate_limiting_enabled to enable automatic
    internal rate limiting based on table read and write throughput limits.
  * If rate limiting is enabled:

    * NoSQLHandleConfig.set_default_rate_limiting_percentage can control how
      much of a table's full limits this client handle can consume
      (default = 100%).
    * Result classes now have a Result.get_rate_limit_delayed_ms method to
      return the amount of time an operation was delayed due to internal rate
      limiting.

  * Add rate limiting example and test.

* RetryStats: New object allows the application to see how much time and for
  what reasons an operation was internally retried.

  * For successful operations, retry stats can be retrieved using
    Result.get_retry_stats.
  * Otherwise, the original Request may have retry stats available via
    Request.get_retry_stats (for example, after an exception was thrown).

* Cloud only: New regions: ap-chiyoda-1, me-dubai-1, sa-santiago-1 and
  uk-cardiff-1.
* Added dependency on dateutil package for flexible timestamp handling


Changed
_______

* DefaultRetryHandler now uses incremental backoff mechanism (instead of fixed
  1-second delay) and may be extended.
* Updated examples to use NoSQLHandle.do_table_request instead of
  NoSQLHandle.table_request followed by TableResult.wait_for_completion.
* Change PreparedStatement.set_variable method to support both name and position
  variables.
* Enhance handling of TIMESTAMP types to better handle a datetime instance with
  an explicit timezone. By default fields of type TIMESTAMP returned by the system
  are represented by a  "naive" (not timezone aware) datetime object in the timezone UTC.
* Timestamp and log level are no longer hard-coded in log messages - rather the
  default logger is configured with a formatter that includes them. An application
  that provides its own logger may choose its own format.
* Adjusted several log messages to use more appropriate levels (ERROR for errors,
  DEBUG for chatter).

Fixed
_____

* Fixed a performance issue that causes results to be returned more slowly as
  they got larger. The Python List pop() method was mistakenly being used
  on large arrays.
* Ensure that TableLimits is always None in TableResult on-premise.
* Fixed synchronization problem in SignatureProvider.
* Fixed a problem where the cloud service might succeed when dropping a table
  that does not exist without using "drop table if exists" when it should throw
  TableNotFoundException

Removed
_______

* NoSQLHandleConfig.set_sec_info_timeout and
  NoSQLHandleConfig.get_sec_info_timeout has been removed.

====================
 5.2.1 - 2020-08-14
====================

Added
_____

* Added NoSQLHandleConfig.set_ssl_cipher_suites to allow the user to configure
  preferred SSL ciphers, and NoSQLHandleConfig.get_ssl_cipher_suites to get the
  ssl cipher setting.
* Added NoSQLHandleConfig.set_ssl_protocol to allow the user to configure
  preferred SSL protocol, and NoSQLHandleConfig.get_ssl_protocol to get the ssl
  protocol setting.
* Added NoSQLHandleConfig.set_ssl_ca_certs to allow the user to configure SSL CA
  certificates, and NoSQLHandleConfig.get_ssl_ca_certs to get the SSL CA
  certificates setting.
* Cloud only. Added new regions: AP_HYDERABAD_1, AP_MELBOURNE_1, AP_OSAKA_1,
  CA_MONTREAL_1, EU_AMSTERDAM_1, ME_JEDDAH_1.
* Cloud only. Added support for authenticating via Resource Principal. This can
  be used in Oracle Cloud Functions to access NoSQL cloud service:

  * Added a new method SignatureProvider.create_with_resource_principal.
  * Added a new method SignatureProvider.get_resource_principal_claim to
    retrieve resource principal metadata with ResourcePrincipalClaimKeys such as
    compartment and tenancy OCID.
* Added generic group by and SELECT DISTINCT. These features will only work with
  servers that also support generic group by.

Changed
_______

* Cloud only. Added the support in SignatureProvider to configure and pass
  region to NoSQLHandleConfig:

  * SignatureProvider built with OCI standard config file is now able to read
    'region' parameter from config file and pass to NoSQLHandleConfig
    implicitly.
  * Change constructor of SignatureProvider to allow passing Region
    programmatically with user profile.
  * Change the method SignatureProvider.create_with_instance_principal to allow
    setting Region with instance principal.
* Deprecated QueryRequest.set_continuation_key and
  QueryRequest.get_continuation_key, use QueryRequest.is_done instead.

Fixed
_____

* On-premise only. Don't validate request sizes.
* TableUsageRequest: added validation check that end time must be greater than
  start time if both of them are specified, throw IAE if end time is smaller
  than start time.
* Changed min/max implementation to make them deterministic.
* On-premise only. Fixed a problem where the HTTP Host header was not being
  adding in all request cases. This prevented use of an intermediate proxy such
  as Nginx, which validates headers.

Removed
_______

* The requirement of third party package "cryptography" has been removed.
* The TableBusyException has been removed.

====================
 5.2.0 - 2020-02-20
====================

Added
_____

* OCI Native support for the cloud service

  * Include support for IAM based security in the cloud service.
  * When using the cloud service, tables are now created in compartments.
    Compartments can be specified for tables in APIs and query statements. By
    default the compartment is the root compartment of the tenancy when
    authenticated as a specific user. The compartment name or id can be
    specified by default in NoSQLHandleConfig or specified in each Request
    object. The compartment name can also be used as a prefix on a table name
    where table names are accepted and in queries, e.g. "mycompartment:mytable".

Removed
_______

* Removed support for IDCS based security in the cloud service.
* TableResult.wait_for_state() has been removed. Use wait_for_completion().

====================
 5.1.0 - 2019-08-30
====================

Added
_____

* Added PutRequest.set_exact_match() to allow the user to control whether an
  exact schema match is required on a put. The default behavior is false.
* Support for complex, multi-shard queries:

  * Sorted/ordered multi-shard queries.
  * Multi-shard aggregation.
  * Geo-spatial queries such as geo_near().

* Support for Identity Columns:

  * Added PutRequest.get/set_identity_cache_size() to allow a user to control
    the number of cached values are used for identity columns. The default value
    is set when the identity column is defined.
  * Added PutResult.get_generated_value() which will return a non-none value if
    an identity column value was generated by the operation. This is only
    relevant for tables with an identity column defined.

* Added a new, simpler TableResult.wait_for_completion() method to wait for the
  completion of a TableRequest vs waiting for a specific state.

* Added NoSQLHandle.do_table_request() to encapsulate a TableRequest and waiting
  for its completion in a single, synchronous call.
* Added OperationNotSupportedException to handle operations that are specific to
  on-premise and cloud service environments.

* Support for both the Oracle NoSQL Database Cloud Service and the on-premise
  Oracle NoSQL Database product.

  * Added StoreAccessTokenProvider for authentication of access to an on-premise
    store
  * Added AuthenticationException to encapsulate authentication problems when
    accessing an on-premise store.
  * Added SystemRequest, SystemStatusRequest, and SystemResult for
    administrative operations that are not table-specific.
  * Added methods on NoSQLHandle for *system* requests, which are those that do
    not involve specific tables:

      * system_request(), system_status(), list_namespaces(), list_users(),
        list_roles()

  * Added NoSQLHandle.do_system_request to encapsulate a SystemRequest and
    waiting for its completion in a single, synchronous call.
  * Now that the driver can access both the cloud service and an on-premise
    store some operations, classes and exceptions are specific to each
    environment. These are noted in updated API documentation.


Changed
_______

* Parameters to TableResult.wait_for_state() changed. It is no longer static and
  acts on *self*, modifying state as required.

Removed
_______

* TableResult.wait_for_state_res() has been removed. Use wait_for_state().

====================
 5.0.0 - 2019-03-31
====================

Added
_____

* Initial Release
* Support for Oracle NoSQL Database Cloud Service
