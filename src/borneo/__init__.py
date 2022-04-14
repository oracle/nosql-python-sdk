#
# Copyright (c) 2018, 2022 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#

# This environment variable suppresses the import of all services
# when importing from the OCI SDK. It can greatly speed up
# program startup
import os
os.environ['OCI_PYTHON_SDK_NO_SERVICE_IMPORTS']='1'

from . import iam
from . import kv
from .auth import AuthorizationProvider
from .common import (
    Consistency, Durability, FieldRange, PutOption, ResourcePrincipalClaimKeys, State,
    SystemState, TableLimits, TimeToLive, TimeUnit, UserInfo, Version,
    IndexInfo, PreparedStatement)
from .config import (
    DefaultRetryHandler, NoSQLHandleConfig, Region, Regions, RetryHandler,
    StatsProfile)
from .driver import NoSQLHandle
from .exception import (
    BatchOperationNumberLimitException, IllegalArgumentException,
    IllegalStateException, IndexExistsException, IndexNotFoundException,
    InvalidAuthorizationException, NoSQLException,
    OperationNotSupportedException, OperationThrottlingException,
    ReadThrottlingException, RequestSizeLimitException, RequestTimeoutException,
    ResourceExistsException, ResourceNotFoundException, RetryableException,
    SecurityInfoNotReadyException, SystemException, TableExistsException,
    TableNotFoundException, ThrottlingException, WriteThrottlingException)
from .operations import (
    DeleteRequest, DeleteResult, GetIndexesRequest, GetIndexesResult,
    GetRequest, GetResult, GetTableRequest, ListTablesRequest, ListTablesResult,
    MultiDeleteRequest, MultiDeleteResult, OperationResult, PrepareRequest,
    PrepareResult, PutRequest, PutResult, QueryRequest, QueryResult, Request,
    Result, SystemRequest, SystemResult, SystemStatusRequest, TableRequest,
    TableResult, TableUsageRequest, TableUsageResult, WriteMultipleRequest,
    WriteMultipleResult)
from .stats import (StatsControl)
from .version import __version__

__all__ = ['AuthorizationProvider',
           'BatchOperationNumberLimitException',
           'Consistency',
           'Durability',
           'DefaultRetryHandler',
           'DeleteRequest',
           'DeleteResult',
           'FieldRange',
           'GetIndexesRequest',
           'GetIndexesResult',
           'GetRequest',
           'GetResult',
           'GetTableRequest',
           'IllegalArgumentException',
           'IllegalStateException',
           'IndexExistsException',
           'IndexInfo',
           'IndexNotFoundException',
           'InvalidAuthorizationException',
           'ListTablesRequest',
           'ListTablesResult',
           'MultiDeleteRequest',
           'MultiDeleteResult',
           'NoSQLException',
           'NoSQLHandle',
           'NoSQLHandleConfig',
           'OperationNotSupportedException',
           'OperationResult',
           'OperationThrottlingException',
           'PreparedStatement',
           'PrepareRequest',
           'PrepareResult',
           'PutOption',
           'PutRequest',
           'PutResult',
           'QueryRequest',
           'QueryResult',
           'ReadThrottlingException',
           'Region',
           'Regions',
           'Request',
           'RequestSizeLimitException',
           'RequestTimeoutException',
           'ResourceExistsException',
           'ResourcePrincipalClaimKeys',
           'ResourceNotFoundException',
           'Result',
           'RetryHandler',
           'RetryableException',
           'SecurityInfoNotReadyException',
           'State',
           "StatsControl",
           'StatsProfile',
           'SystemException',
           'SystemRequest',
           'SystemResult',
           'SystemState',
           'SystemStatusRequest',
           'TableExistsException',
           'TableLimits',
           'TableNotFoundException',
           'TableRequest',
           'TableResult',
           'TableUsageRequest',
           'TableUsageResult',
           'ThrottlingException',
           'TimeToLive',
           'TimeUnit',
           'UserInfo',
           'Version',
           'WriteMultipleRequest',
           'WriteMultipleResult',
           'WriteThrottlingException'
           ]
