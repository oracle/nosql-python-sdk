Change Log
~~~~~~~~~~
All notable changes to this project will be documented in this file.

The format is based on `Keep a Changelog <http://keepachangelog.com/>`_.

====================
 Unpublished
====================

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
  

Changed
_______

* DefaultRetryHandler now uses incremental backoff mechanism (instead of fixed
  1-second delay) and may be extended.
* Updated examples to use NoSQLHandle.do_table_request instead of
  NoSQLHandle.table_request followed by TableResult.wait_for_completion.
* Change PreparedStatement.set_variable method to support both name and position
  variables.
  
Fixed
_____

* Ensure that TableLimits is always None in TableResult on-premise.
* Fix synchronization problem of SignatureProvider.

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
