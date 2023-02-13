#
# Copyright (c) 2018, 2022 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#

from abc import ABCMeta, abstractmethod
from collections import OrderedDict
from datetime import datetime
from dateutil import parser, tz
from decimal import (
    Context, Decimal, ROUND_05UP, ROUND_CEILING, ROUND_DOWN, ROUND_FLOOR,
    ROUND_HALF_DOWN, ROUND_HALF_EVEN, ROUND_HALF_UP, ROUND_UP)
from sys import version_info

from .common import (
    CheckValue, Empty, IndexInfo, JsonNone, PackedInteger, PreparedStatement,
    PutOption, State, SystemState, TableLimits, TableUsage, TimeUnit, Version,
    enum)
from .exception import (
    BatchOperationNumberLimitException, DeploymentException,
    EvolutionLimitException, IllegalArgumentException, IllegalStateException,
    IndexExistsException, IndexLimitException, IndexNotFoundException,
    InvalidAuthorizationException, KeySizeLimitException, NoSQLException,
    OperationNotSupportedException, OperationThrottlingException,
    ReadThrottlingException, RequestSizeLimitException, RequestTimeoutException,
    ResourceExistsException, ResourceNotFoundException, RowSizeLimitException,
    SecurityInfoNotReadyException, SystemException, TableExistsException,
    TableLimitException, TableNotFoundException, TableSizeException,
    UnauthorizedException, UnsupportedProtocolException,
    WriteThrottlingException)
from .kv import AuthenticationException
from .query import PlanIter, QueryDriver, TopologyInfo
from .serdeutil import SerdeUtil

try:
    from . import operations
except ImportError:
    import operations


class BinaryProtocol(object):
    """
    A base class for binary protocol serialization and constant protocol values.
    Constants are used instead of relying on ordering of values in enumerations
    or other derived protocol state.
    """

    @staticmethod
    def deserialize_consumed_capacity(bis, result):
        result.set_read_units(SerdeUtil.read_packed_int(bis))
        result.set_read_kb(SerdeUtil.read_packed_int(bis))
        result.set_write_kb(SerdeUtil.read_packed_int(bis))

    @staticmethod
    def deserialize_system_result(bis):
        result = operations.SystemResult()
        result.set_state(SerdeUtil.get_operation_state(bis.read_byte()))
        result.set_operation_id(SerdeUtil.read_string(bis))
        result.set_statement(SerdeUtil.read_string(bis))
        result.set_result_string(SerdeUtil.read_string(bis))
        return result

    @staticmethod
    def deserialize_generated_value(bis, result):
        has_generated_value = bis.read_boolean()
        if not has_generated_value:
            return
        result.set_generated_value(SerdeUtil.convert_value_to_none(
            BinaryProtocol.read_field_value(bis)))

    @staticmethod
    def deserialize_table_result(bis, result, serial_version):
        has_info = bis.read_boolean()
        if has_info:
            result.set_compartment_id(SerdeUtil.read_string(bis))
            result.set_table_name(SerdeUtil.read_string(bis))
            result.set_state(
                SerdeUtil._get_table_state(bis.read_byte()))
            has_static_state = bis.read_boolean()
            if has_static_state:
                read_kb = SerdeUtil.read_packed_int(bis)
                write_kb = SerdeUtil.read_packed_int(bis)
                storage_gb = SerdeUtil.read_packed_int(bis)
                capacity_mode = SerdeUtil.CAPACITY_MODE.PROVISIONED
                if serial_version > 2:
                    capacity_mode = bis.read_byte()
                # on-prem tables may return all 0 because of protocol
                # limitations that lump the schema with limits. Return None to
                # user for those cases.
                if not (read_kb == 0 and write_kb == 0 and storage_gb == 0):
                    result.set_table_limits(
                        TableLimits(read_kb, write_kb, storage_gb, capacity_mode))
                result.set_schema(SerdeUtil.read_string(bis))
            result.set_operation_id(SerdeUtil.read_string(bis))

    @staticmethod
    def deserialize_write_response(bis, result, serial_version):
        return_info = bis.read_boolean()
        if not return_info:
            return
        # Existing info always includes both value and version.
        result.set_existing_value(SerdeUtil.convert_value_to_none(
            BinaryProtocol.read_field_value(bis)))
        result.set_existing_version(BinaryProtocol.read_version(bis))
        if serial_version > 2:
            result.set_existing_modification_time(SerdeUtil.read_packed_long(bis))
        else:
            result.set_existing_modification_time(0)

    @staticmethod
    def read_dict(bis):
        # Read length.
        bis.read_int()
        size = bis.read_int()
        result = OrderedDict()
        count = 0
        while count < size:
            key = SerdeUtil.read_string(bis)
            value = BinaryProtocol.read_field_value(bis)
            result[key] = value
            count += 1
        return result

    @staticmethod
    def read_field_value(bis):
        # Deserialize a generic field value.
        t = bis.read_byte()
        if t == SerdeUtil.FIELD_VALUE_TYPE.ARRAY:
            return BinaryProtocol.read_list(bis)
        elif t == SerdeUtil.FIELD_VALUE_TYPE.BINARY:
            return SerdeUtil.read_bytearray(bis, False)
        elif t == SerdeUtil.FIELD_VALUE_TYPE.BOOLEAN:
            return bis.read_boolean()
        elif t == SerdeUtil.FIELD_VALUE_TYPE.DOUBLE:
            return bis.read_float()
        elif t == SerdeUtil.FIELD_VALUE_TYPE.EMPTY:
            return Empty()
        elif t == SerdeUtil.FIELD_VALUE_TYPE.INTEGER:
            return SerdeUtil.read_packed_int(bis)
        elif t == SerdeUtil.FIELD_VALUE_TYPE.JSON_NULL:
            return JsonNone()
        elif t == SerdeUtil.FIELD_VALUE_TYPE.LONG:
            return SerdeUtil.read_packed_long(bis)
        elif t == SerdeUtil.FIELD_VALUE_TYPE.MAP:
            return BinaryProtocol.read_dict(bis)
        elif t == SerdeUtil.FIELD_VALUE_TYPE.STRING:
            return SerdeUtil.read_string(bis)
        elif t == SerdeUtil.FIELD_VALUE_TYPE.TIMESTAMP:
            return SerdeUtil.read_datetime(bis)
        elif t == SerdeUtil.FIELD_VALUE_TYPE.NUMBER:
            return SerdeUtil.read_decimal(bis)
        elif t == SerdeUtil.FIELD_VALUE_TYPE.NULL:
            return None
        else:
            raise IllegalStateException('Unknown value type code: ' + str(t))

    @staticmethod
    def read_list(bis):
        # Read length.
        bis.read_int()
        length = bis.read_int()
        result = list()
        count = 0
        while count < length:
            result.append(BinaryProtocol.read_field_value(bis))
            count += 1
        return result

    @staticmethod
    def read_math_context(bis):
        value_to_name = {0: ROUND_UP,
                         1: ROUND_DOWN,
                         2: ROUND_CEILING,
                         3: ROUND_FLOOR,
                         4: ROUND_HALF_UP,
                         5: ROUND_HALF_DOWN,
                         6: ROUND_HALF_EVEN,
                         8: ROUND_05UP}
        code = bis.read_byte()
        if code == 0:
            return None
        elif code == 1:
            return Context(prec=7, rounding=ROUND_HALF_EVEN)
        elif code == 2:
            return Context(prec=16, rounding=ROUND_HALF_EVEN)
        elif code == 3:
            return Context(prec=34, rounding=ROUND_HALF_EVEN)
        elif code == 4:
            return Context(prec=0, rounding=ROUND_HALF_UP)
        elif code == 5:
            precision = bis.read_int()
            rounding_mode = value_to_name.get(bis.read_int())
            return Context(prec=precision, rounding=rounding_mode)
        else:
            raise IOError('Unknown MathContext code.')

    @staticmethod
    def read_topology_info(bis):
        seq_num = SerdeUtil.read_packed_int(bis)
        SerdeUtil.trace(
            'read_topology_info: seq_num = ' + str(seq_num), 4)
        if seq_num < -1:
            raise IOError('Invalid topology sequence number: ' + str(seq_num))
        if seq_num == -1:
            # No topology info sent by proxy.
            return None
        shard_ids = SerdeUtil.read_packed_int_array(bis)
        return TopologyInfo(seq_num, shard_ids)

    @staticmethod
    def read_version(bis):
        return Version.create_version(SerdeUtil.read_bytearray(bis, False))

    # Writes fields from ReadRequest.
    @staticmethod
    def serialize_read_request(request, bos):
        BinaryProtocol.serialize_request(request, bos)
        SerdeUtil.write_string(bos, request.get_table_name())
        bos.write_byte(request.get_consistency())

    # Writes fields from WriteRequest
    @staticmethod
    def serialize_write_request(request, bos, serial_version):
        BinaryProtocol.serialize_request(request, bos)
        SerdeUtil.write_string(bos, request.get_table_name())
        bos.write_boolean(request.get_return_row())
        BinaryProtocol.write_durability(request, bos, serial_version)

    @staticmethod
    def serialize_request(request, bos):
        SerdeUtil.write_packed_int(bos, request.get_timeout())

    @staticmethod
    def write_durability(request, bos, serial_version):
        if serial_version < 3:
            return
        dur = request.get_durability()
        if dur is None:
            bos.write_byte(0)
            return
        val = dur.master_sync
        val |= (dur.replica_sync << 2)
        val |= (dur.replica_ack << 4)
        bos.write_byte(val)

    @staticmethod
    def write_dict(bos, value):
        # Serialize a dict.
        # Leave an integer-sized space for length.
        offset = bos.get_offset()
        bos.write_int(0)
        start = bos.get_offset()
        bos.write_int(len(value))
        for key in value:
            SerdeUtil.write_string(bos, key)
            BinaryProtocol.write_field_value(bos, value[key])
        # Update the length value.
        bos.write_int_at_offset(offset, bos.get_offset() - start)

    @staticmethod
    def write_field_range(bos, field_range):
        if field_range is None:
            bos.write_boolean(False)
            return
        bos.write_boolean(True)
        SerdeUtil.write_string(bos, field_range.get_field_path())
        if field_range.get_start() is not None:
            bos.write_boolean(True)
            BinaryProtocol.write_field_value(bos, field_range.get_start())
            bos.write_boolean(field_range.get_start_inclusive())
        else:
            bos.write_boolean(False)
        if field_range.get_end() is not None:
            bos.write_boolean(True)
            BinaryProtocol.write_field_value(bos, field_range.get_end())
            bos.write_boolean(field_range.get_end_inclusive())
        else:
            bos.write_boolean(False)

    @staticmethod
    def write_field_value(bos, value):
        # Serialize a generic field value.
        bos.write_byte(SerdeUtil._get_type(value))
        if value is not None:
            if isinstance(value, list):
                BinaryProtocol.write_list(bos, value)
            elif isinstance(value, bytearray):
                SerdeUtil.write_bytearray(bos, value)
            elif isinstance(value, bool):
                bos.write_boolean(value)
            elif isinstance(value, float):
                bos.write_float(value)
            elif CheckValue.is_int(value):
                SerdeUtil.write_packed_int(bos, value)
            elif CheckValue.is_long(value):
                SerdeUtil.write_packed_long(bos, value)
            elif isinstance(value, dict):
                BinaryProtocol.write_dict(bos, value)
            elif CheckValue.is_str(value):
                SerdeUtil.write_string(bos, value)
            elif isinstance(value, datetime):
                SerdeUtil.write_datetime(bos, value)
            elif isinstance(value, Decimal) or CheckValue.is_overlong(value):
                SerdeUtil.write_decimal(bos, value)
            else:
                raise IllegalStateException(
                    'Unknown value type ' + str(type(value)))

    @staticmethod
    def write_list(bos, value):
        # Serialize a list.
        # Leave an integer-sized space for length.
        offset = bos.get_offset()
        bos.write_int(0)
        start = bos.get_offset()
        bos.write_int(len(value))
        for item in value:
            BinaryProtocol.write_field_value(bos, item)
        # Update the length value.
        bos.write_int_at_offset(offset, bos.get_offset() - start)

    @staticmethod
    def write_math_context(bos, math_context):
        name_to_value = {ROUND_UP: 0,
                         ROUND_DOWN: 1,
                         ROUND_CEILING: 2,
                         ROUND_FLOOR: 3,
                         ROUND_HALF_UP: 4,
                         ROUND_HALF_DOWN: 5,
                         ROUND_HALF_EVEN: 6,
                         ROUND_05UP: 8}
        if math_context is None:
            bos.write_byte(0)
        else:
            bos.write_byte(5)
            bos.write_int(math_context.prec)
            bos.write_int(name_to_value.get(math_context.rounding))

    @staticmethod
    def write_op_code(bos, op):
        # Writes the opcode for the operation.
        bos.write_byte(op)

    @staticmethod
    def write_record(bos, record):
        """
        Writes a dict.

        This is public to allow a caller to get the size of a value outside of
        the context of serialization.
        """
        BinaryProtocol.write_field_value(bos, record)

    @staticmethod
    def write_ttl(bos, ttl):
        if ttl is None:
            SerdeUtil.write_packed_long(bos, -1)
            return
        SerdeUtil.write_packed_long(bos, ttl.get_value())
        if ttl.unit_is_days():
            bos.write_byte(TimeUnit.DAYS)
        elif ttl.unit_is_hours():
            bos.write_byte(TimeUnit.HOURS)
        else:
            raise IllegalStateException('Invalid TTL unit in ttl ' + str(ttl))

    @staticmethod
    def write_version(bos, version):
        CheckValue.check_not_none(version, 'array')
        SerdeUtil.write_bytearray(bos, version.get_bytes())


class RequestSerializer(object):
    """
    Base class of different kinds of RequestSerializer.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def serialize(self, request, bos, serial_version):
        """
        Method used to serialize the request.
        """
        pass

    @abstractmethod
    def deserialize(self, request, bis, serial_version):
        """
        Method used to deserialize the request.
        """
        pass


class DeleteRequestSerializer(RequestSerializer):
    """
    The flag indicates if the serializer is used for a standalone request or a
    sub operation of WriteMultiple request.

    If it is used to serialize the sub operation, then some information like
    timeout, namespace and table_name will be skipped during serialization.
    """

    def __init__(self, is_sub_request=False):
        self._is_sub_request = is_sub_request

    def serialize(self, request, bos, serial_version):
        match_version = request.get_match_version()
        op_code = (SerdeUtil.OP_CODE.DELETE if match_version is None else
                   SerdeUtil.OP_CODE.DELETE_IF_VERSION)
        BinaryProtocol.write_op_code(bos, op_code)
        if self._is_sub_request:
            bos.write_boolean(request.get_return_row())
        else:
            BinaryProtocol.serialize_write_request(request, bos, serial_version)
        BinaryProtocol.write_field_value(bos, request.get_key())
        if match_version is not None:
            BinaryProtocol.write_version(bos, match_version)

    def deserialize(self, request, bis, serial_version):
        result = operations.DeleteResult()
        BinaryProtocol.deserialize_consumed_capacity(bis, result)
        result.set_success(bis.read_boolean())
        BinaryProtocol.deserialize_write_response(bis, result, serial_version)
        return result

class GetIndexesRequestSerializer(RequestSerializer):

    def serialize(self, request, bos, serial_version):
        BinaryProtocol.write_op_code(bos, SerdeUtil.OP_CODE.GET_INDEXES)
        BinaryProtocol.serialize_request(request, bos)
        SerdeUtil.write_string(bos, request.get_table_name())
        if request.get_index_name() is not None:
            bos.write_boolean(True)
            SerdeUtil.write_string(bos, request.get_index_name())
        else:
            bos.write_boolean(False)

    def deserialize(self, request, bis, serial_version):
        result = operations.GetIndexesResult()
        num_indexes = SerdeUtil.read_packed_int(bis)
        indexes = list()
        count = 0
        while count < num_indexes:
            indexes.append(self._deserialize_index_info(bis))
            count += 1
        result.set_indexes(indexes)
        return result

    @staticmethod
    def _deserialize_index_info(bis):
        index_name = SerdeUtil.read_string(bis)
        num_fields = SerdeUtil.read_packed_int(bis)
        field_names = list()
        count = 0
        while count < num_fields:
            field_names.append(SerdeUtil.read_string(bis))
            count += 1
        return IndexInfo(index_name, field_names)


class GetRequestSerializer(RequestSerializer):

    def serialize(self, request, bos, serial_version):
        BinaryProtocol.write_op_code(bos, SerdeUtil.OP_CODE.GET)
        BinaryProtocol.serialize_read_request(request, bos)
        BinaryProtocol.write_field_value(bos, request.get_key())

    def deserialize(self, request, bis, serial_version):
        result = operations.GetResult()
        BinaryProtocol.deserialize_consumed_capacity(bis, result)
        has_row = bis.read_boolean()
        if has_row:
            result.set_value(SerdeUtil.convert_value_to_none(
                BinaryProtocol.read_field_value(bis)))
            result.set_expiration_time(SerdeUtil.read_packed_long(bis))
            result.set_version(BinaryProtocol.read_version(bis))
            if serial_version > 2:
                result.set_modification_time(SerdeUtil.read_packed_long(bis))
            else:
                result.set_modification_time(0)
        return result


class GetTableRequestSerializer(RequestSerializer):

    def serialize(self, request, bos, serial_version):
        BinaryProtocol.write_op_code(bos, SerdeUtil.OP_CODE.GET_TABLE)
        BinaryProtocol.serialize_request(request, bos)
        SerdeUtil.write_string(bos, request.get_table_name())
        SerdeUtil.write_string(bos, request.get_operation_id())

    def deserialize(self, request, bis, serial_version):
        result = operations.TableResult()
        BinaryProtocol.deserialize_table_result(bis, result, serial_version)
        return result


class ListTablesRequestSerializer(RequestSerializer):

    def serialize(self, request, bos, serial_version):
        BinaryProtocol.write_op_code(bos, SerdeUtil.OP_CODE.LIST_TABLES)
        BinaryProtocol.serialize_request(request, bos)
        bos.write_int(request.get_start_index())
        bos.write_int(request.get_limit())
        # new in V2.
        SerdeUtil.write_string(bos, request.get_namespace())

    def deserialize(self, request, bis, serial_version):
        result = operations.ListTablesResult()
        num_tables = SerdeUtil.read_packed_int(bis)
        tables = list()
        count = 0
        while count < num_tables:
            tables.append(SerdeUtil.read_string(bis))
            count += 1
        result.set_tables(tables)
        result.set_last_index_returned(SerdeUtil.read_packed_int(bis))
        return result


class MultiDeleteRequestSerializer(RequestSerializer):

    def serialize(self, request, bos, serial_version):
        BinaryProtocol.write_op_code(bos, SerdeUtil.OP_CODE.MULTI_DELETE)
        BinaryProtocol.serialize_request(request, bos)
        SerdeUtil.write_string(bos, request.get_table_name())
        BinaryProtocol.write_durability(request, bos, serial_version)
        BinaryProtocol.write_field_value(bos, request.get_key())
        BinaryProtocol.write_field_range(bos, request.get_range())
        SerdeUtil.write_packed_int(bos, request.get_max_write_kb())
        SerdeUtil.write_bytearray(bos, request.get_continuation_key())

    def deserialize(self, request, bis, serial_version):
        result = operations.MultiDeleteResult()
        BinaryProtocol.deserialize_consumed_capacity(bis, result)
        result.set_num_deletions(SerdeUtil.read_packed_int(bis))
        result.set_continuation_key(SerdeUtil.read_bytearray(bis, False))
        return result


class PrepareRequestSerializer(RequestSerializer):

    # Prepare a query.
    def serialize(self, request, bos, serial_version):
        BinaryProtocol.write_op_code(bos, SerdeUtil.OP_CODE.PREPARE)
        BinaryProtocol.serialize_request(request, bos)
        SerdeUtil.write_string(bos, request.get_statement())
        bos.write_short_int(QueryDriver.QUERY_VERSION)
        bos.write_boolean(request.get_query_plan())

    def deserialize(self, request, bis, serial_version):
        result = operations.PrepareResult()
        BinaryProtocol.deserialize_consumed_capacity(bis, result)
        prep_stmt = PrepareRequestSerializer.deserialize_internal(
            request.get_statement(), request.get_query_plan(), bis)
        result.set_prepared_statement(prep_stmt)
        return result

    @staticmethod
    def deserialize_internal(sql_text, get_query_plan, bis):
        """
        Extract the table name and namespace from the prepared query. This dips
        into the portion of the prepared query that is normally opaque.

        int (4 byte)
        byte[] (32 bytes -- hash)
        byte (number of tables)
        namespace (string)
        tablename (string)
        operation (1 byte)
        """
        saved_offset = bis.get_offset()
        bis.set_offset(saved_offset + 37)
        namespace = SerdeUtil.read_string(bis)
        table_name = SerdeUtil.read_string(bis)
        operation = bis.read_byte()
        bis.set_offset(saved_offset)

        proxy_statement = SerdeUtil.read_bytearray_with_int(bis)
        num_iterators = 0
        num_registers = 0
        external_vars = None
        topology_info = None
        query_plan = None
        if get_query_plan:
            query_plan = SerdeUtil.read_string(bis)
        driver_plan = PlanIter.deserialize_iter(bis)
        if driver_plan is not None:
            num_iterators = bis.read_int()
            num_registers = bis.read_int()
            SerdeUtil.trace(
                'PREP-RESULT: Query Plan:\n' + driver_plan.display() + '\n', 1)
            length = bis.read_int()
            if length > 0:
                external_vars = dict()
                for i in range(length):
                    var_name = SerdeUtil.read_string(bis)
                    var_id = bis.read_int()
                    external_vars[var_name] = var_id
            topology_info = BinaryProtocol.read_topology_info(bis)
        return PreparedStatement(
            sql_text, query_plan, topology_info, proxy_statement, driver_plan,
            num_iterators, num_registers, external_vars,
            namespace, table_name, operation)


class PutRequestSerializer(RequestSerializer):
    """
    The flag indicates if the serializer is used for a standalone request or a
    sub operation of WriteMultiple request.

    If it is used to serialize the sub operation, then some information like
    timeout, namespace and table_name will be skipped during serialization.
    """

    def __init__(self, is_sub_request=False):
        self._is_sub_request = is_sub_request

    def serialize(self, request, bos, serial_version):
        op = SerdeUtil._get_op_code(request)
        BinaryProtocol.write_op_code(bos, op)
        if self._is_sub_request:
            bos.write_boolean(request.get_return_row())
        else:
            BinaryProtocol.serialize_write_request(request, bos, serial_version)
        bos.write_boolean(request.get_exact_match())
        SerdeUtil.write_packed_int(bos, request.get_identity_cache_size())
        BinaryProtocol.write_record(bos, request.get_value())
        bos.write_boolean(request.get_update_ttl())
        BinaryProtocol.write_ttl(bos, request.get_ttl())
        if request.get_match_version() is not None:
            BinaryProtocol.write_version(bos, request.get_match_version())

    def deserialize(self, request, bis, serial_version):
        result = operations.PutResult()
        BinaryProtocol.deserialize_consumed_capacity(bis, result)
        success = bis.read_boolean()
        if success:
            result.set_version(BinaryProtocol.read_version(bis))
        # return row info.
        BinaryProtocol.deserialize_write_response(bis, result, serial_version)
        # generated identity column value
        BinaryProtocol.deserialize_generated_value(bis, result)
        return result

class QueryRequestSerializer(RequestSerializer):

    def serialize(self, request, bos, serial_version):
        # write unconditional state first.
        BinaryProtocol.write_op_code(bos, SerdeUtil.OP_CODE.QUERY)
        BinaryProtocol.serialize_request(request, bos)
        bos.write_byte(request.get_consistency())
        SerdeUtil.write_packed_int(bos, request.get_limit())
        SerdeUtil.write_packed_int(bos, request.get_max_read_kb())
        SerdeUtil.write_bytearray(bos, request.get_cont_key())
        bos.write_boolean(request.is_prepared())
        # The following 7 fields were added in V2.
        bos.write_short_int(QueryDriver.QUERY_VERSION)
        bos.write_byte(request.get_trace_level())
        SerdeUtil.write_packed_int(bos, request.get_max_write_kb())
        BinaryProtocol.write_math_context(bos, request.get_math_context())
        SerdeUtil.write_packed_int(bos, request.topology_seq_num())
        SerdeUtil.write_packed_int(bos, request.get_shard_id())
        bos.write_boolean(request.is_prepared() and request.is_simple_query())
        if request.is_prepared():
            ps = request.get_prepared_statement()
            SerdeUtil.write_bytearray_with_int(bos, ps.get_statement())
            if ps.get_variables() is not None:
                variables = ps.get_variables()
                SerdeUtil.write_packed_int(bos, len(variables))
                for key in variables:
                    SerdeUtil.write_string(bos, key)
                    BinaryProtocol.write_field_value(bos, variables[key])
            else:
                SerdeUtil.write_packed_int(bos, 0)
        else:
            SerdeUtil.write_string(bos, request.get_statement())

    def deserialize(self, request, bis, serial_version):
        prep = request.get_prepared_statement()
        is_prepared = prep is not None
        result = operations.QueryResult(request)
        num_rows = bis.read_int()
        is_sort_phase1_result = bis.read_boolean()
        results = list()
        count = 0
        while count < num_rows:
            results.append(BinaryProtocol.read_field_value(bis))
            count += 1
        if is_sort_phase1_result:
            result.set_is_in_phase1(bis.read_boolean())
            pids = SerdeUtil.read_packed_int_array(bis)
            if pids is not None:
                result.set_pids(pids)
                result.set_num_results_per_pid(
                    SerdeUtil.read_packed_int_array(bis))
                cont_keys = list()
                for i in range(len(pids)):
                    cont_keys.append(SerdeUtil.read_bytearray(bis, False))
                result.set_partition_cont_keys(cont_keys)
        BinaryProtocol.deserialize_consumed_capacity(bis, result)
        result.set_continuation_key(SerdeUtil.read_bytearray(bis, False))
        request.set_cont_key(result.get_continuation_key())
        # In V2, if the QueryRequest was not initially prepared, the prepared
        # statement created at the proxy is returned back along with the query
        # results, so that the preparation does not need to be done during each
        # query batch.
        if not is_prepared:
            prep = PrepareRequestSerializer.deserialize_internal(
                request.get_statement(), False, bis)
            request.set_prepared_statement(prep)
        if prep is not None and not prep.is_simple_query():
            if not is_prepared:
                assert num_rows == 0
                driver = QueryDriver(request)
                driver.set_topology_info(prep.topology_info())
                driver.set_prep_cost(result.get_read_kb())
                result.set_computed(False)
            else:
                # In this case, the QueryRequest is an "internal" one.
                result.set_reached_limit(bis.read_boolean())
                topology_info = BinaryProtocol.read_topology_info(bis)
                driver = request.get_driver()
                if topology_info is not None:
                    prep.set_topology_info(topology_info)
                    driver.set_topology_info(topology_info)
        else:
            results = SerdeUtil.convert_value_to_none(results)
        result.set_results(results)
        return result


class SystemRequestSerializer(RequestSerializer):

    def serialize(self, request, bos, serial_version):
        BinaryProtocol.write_op_code(
            bos, SerdeUtil.OP_CODE.SYSTEM_REQUEST)
        BinaryProtocol.serialize_request(request, bos)
        SerdeUtil.write_string(bos, request.get_statement())

    def deserialize(self, request, bis, serial_version):
        return BinaryProtocol.deserialize_system_result(bis)


class SystemStatusRequestSerializer(RequestSerializer):

    def serialize(self, request, bos, serial_version):
        BinaryProtocol.write_op_code(
            bos, SerdeUtil.OP_CODE.SYSTEM_STATUS_REQUEST)
        BinaryProtocol.serialize_request(request, bos)
        SerdeUtil.write_string(bos, request.get_operation_id())
        SerdeUtil.write_string(bos, request.get_statement())

    def deserialize(self, request, bis, serial_version):
        return BinaryProtocol.deserialize_system_result(bis)


class TableRequestSerializer(RequestSerializer):

    def serialize(self, request, bos, serial_version):
        BinaryProtocol.write_op_code(bos, SerdeUtil.OP_CODE.TABLE_REQUEST)
        BinaryProtocol.serialize_request(request, bos)
        SerdeUtil.write_string(bos, request.get_statement())
        limits = request.get_table_limits()
        if limits is not None:
            bos.write_boolean(True)
            bos.write_int(limits.get_read_units())
            bos.write_int(limits.get_write_units())
            bos.write_int(limits.get_storage_gb())
            if serial_version > 2:
                bos.write_byte(limits.get_mode())
            if request.get_table_name() is not None:
                bos.write_boolean(True)
                SerdeUtil.write_string(bos, request.get_table_name())
            else:
                bos.write_boolean(False)
        else:
            bos.write_boolean(False)

    def deserialize(self, request, bis, serial_version):
        result = operations.TableResult()
        BinaryProtocol.deserialize_table_result(bis, result, serial_version)
        return result


class TableUsageRequestSerializer(RequestSerializer):

    def serialize(self, request, bos, serial_version):
        BinaryProtocol.write_op_code(
            bos, SerdeUtil.OP_CODE.GET_TABLE_USAGE)
        BinaryProtocol.serialize_request(request, bos)
        SerdeUtil.write_string(bos, request.get_table_name())
        SerdeUtil.write_packed_long(bos, request.get_start_time())
        SerdeUtil.write_packed_long(bos, request.get_end_time())
        SerdeUtil.write_packed_int(bos, request.get_limit())

    def deserialize(self, request, bis, serial_version):
        result = operations.TableUsageResult()
        # don't use tenant_id, but it's in the result
        SerdeUtil.read_string(bis)
        result.set_table_name(SerdeUtil.read_string(bis))
        num_results = SerdeUtil.read_packed_int(bis)
        usage_records = list()
        count = 0
        while count < num_results:
            usage_records.append(self._deserialize_usage(bis))
            count += 1
        result.set_usage_records(usage_records)
        return result

    @staticmethod
    def _deserialize_usage(bis):
        start_time_ms = SerdeUtil.read_packed_long(bis)
        seconds_in_period = SerdeUtil.read_packed_int(bis)
        read_units = SerdeUtil.read_packed_int(bis)
        write_units = SerdeUtil.read_packed_int(bis)
        storage_gb = SerdeUtil.read_packed_int(bis)
        read_throttle_count = SerdeUtil.read_packed_int(bis)
        write_throttle_count = SerdeUtil.read_packed_int(bis)
        storage_throttle_count = SerdeUtil.read_packed_int(bis)
        usage = TableUsage(start_time_ms, seconds_in_period, read_units,
                           write_units, storage_gb, read_throttle_count,
                           write_throttle_count, storage_throttle_count)
        return usage


class WriteMultipleRequestSerializer(RequestSerializer):

    def serialize(self, request, bos, serial_version):
        put_serializer = PutRequestSerializer(True)
        delete_serializer = DeleteRequestSerializer(True)
        num = request.get_num_operations()

        # OpCode
        BinaryProtocol.write_op_code(
            bos, SerdeUtil.OP_CODE.WRITE_MULTIPLE)
        BinaryProtocol.serialize_request(request, bos)

        # TableName
        # If all ops use the same table name, write that
        # single table name to the output stream.
        # If any of them are different, write all table
        # names, comma-separated.
        if request.is_single_table():
            SerdeUtil.write_string(bos, request.get_table_name())
        else:
            table_names = ""
            for op in request.get_operations():
                if len(table_names) > 0:
                    table_names += ","
                table_names += op.get_request().get_table_name()
            SerdeUtil.write_string(bos, table_names)

        # Number of operations
        SerdeUtil.write_packed_int(bos, num)

        # Durability settings
        BinaryProtocol.write_durability(request, bos, serial_version)

        # Operations
        for op in request.get_operations():
            start = bos.get_offset()

            # Abort if successful flag
            bos.write_boolean(op.is_abort_if_unsuccessful())
            req = op.get_request()
            if str(req) == 'PutRequest':
                put_serializer.serialize(req, bos, serial_version)
            else:
                assert str(req) == 'DeleteRequest'
                delete_serializer.serialize(req, bos, serial_version)

    def deserialize(self, request, bis, serial_version):
        result = operations.WriteMultipleResult()
        # Success flag
        succeed = bis.read_boolean()
        BinaryProtocol.deserialize_consumed_capacity(bis, result)
        if succeed:
            num = SerdeUtil.read_packed_int(bis)
            count = 0
            while count < num:
                result.add_result(self._create_operation_result(bis, serial_version))
                count += 1
        else:
            result.set_failed_operation_index(bis.read_byte())
            result.add_result(self._create_operation_result(bis, serial_version))
        return result

    @staticmethod
    def _create_operation_result(bis, serial_version):
        op_result = operations.OperationResult()
        op_result.set_success(bis.read_boolean())
        if bis.read_boolean():
            op_result.set_version(BinaryProtocol.read_version(bis))
        BinaryProtocol.deserialize_write_response(bis, op_result, serial_version)
        # generated identity column value
        BinaryProtocol.deserialize_generated_value(bis, op_result)
        return op_result
