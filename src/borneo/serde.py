#
# Copyright (C) 2018, 2019 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
#
# Please see LICENSE.txt file included in the top-level directory of the
# appropriate download for a copy of the license and additional information.
#

from datetime import datetime, timedelta
from decimal import Decimal
from sys import version_info

from .common import (
    CheckValue, IndexInfo, PackedInteger, PutOption, State, TableLimits,
    TableUsage, TimeUnit, Version, enum)
from .exception import (
    BatchOperationNumberLimitException, DeploymentException,
    EvolutionLimitException, IllegalArgumentException, IllegalStateException,
    IndexExistsException, IndexLimitException, IndexNotFoundException,
    InvalidAuthorizationException, KeySizeLimitException, NoSQLException,
    OperationThrottlingException, ReadThrottlingException,
    RequestSizeLimitException, RequestTimeoutException, RowSizeLimitException,
    SecurityInfoNotReadyException, SystemException, TableBusyException,
    TableExistsException, TableLimitException, TableNotFoundException,
    TableSizeException, UnauthorizedException, WriteThrottlingException)
from .operations import WriteMultipleRequest


class BinaryProtocol:
    """
    A base class for binary protocol serialization and constant protocol values.
    Constants are used instead of relying on ordering of values in enumerations
    or other derived protocol state.
    """
    # The limit on the max read KB during a operation
    READ_KB_LIMIT = 2 * 1024

    # The row size limit.
    ROW_SIZE_LIMIT = 512 * 1024

    # Serial version of the protocol.
    SERIAL_VERSION = 1

    # The limit on the max write KB during a operation.
    WRITE_KB_LIMIT = 2 * 1024

    # The max number of operations in a WriteMultiple request.
    BATCH_OP_NUMBER_LIMIT = 50

    # The max size of WriteMultiple request.
    BATCH_REQUEST_SIZE_LIMIT = 25 * 1024 * 1024

    # The max size of request.
    REQUEST_SIZE_LIMIT = 2 * 1024 * 1024

    # Field value type.
    FIELD_VALUE_TYPE = enum(ARRAY=0,
                            BINARY=1,
                            BOOLEAN=2,
                            DOUBLE=3,
                            INTEGER=4,
                            LONG=5,
                            MAP=6,
                            STRING=7,
                            TIMESTAMP=8,
                            NUMBER=9,
                            JSON_NULL=10,
                            NULL=11)

    # Operation codes
    OP_CODE = enum(DELETE=0,
                   DELETE_IF_VERSION=1,
                   GET=2,
                   PUT=3,
                   PUT_IF_ABSENT=4,
                   PUT_IF_PRESENT=5,
                   PUT_IF_VERSION=6,
                   QUERY=7,
                   PREPARE=8,
                   WRITE_MULTIPLE=9,
                   MULTI_DELETE=10,
                   GET_TABLE=11,
                   GET_INDEXES=12,
                   GET_TABLE_USAGE=13,
                   LIST_TABLES=14,
                   TABLE_REQUEST=15,
                   SCAN=16,
                   INDEX_SCAN=17,
                   CREATE_TABLE=18,
                   ALTER_TABLE=19,
                   DROP_TABLE=20,
                   CREATE_INDEX=21,
                   DROP_INDEX=22)

    # Table state.
    TABLE_STATE = enum(ACTIVE=0,
                       CREATING=1,
                       DROPPED=2,
                       DROPPING=3,
                       UPDATING=4)

    """
    Error codes for user-generated errors, range from 1 to 50(exclusive).
    These include illegal arguments, exceeding size limits for some objects,
    resource not found, etc.
    """
    USER_ERROR = enum(NO_ERROR=0,
                      UNKNOWN_OPERATION=1,
                      TABLE_NOT_FOUND=2,
                      INDEX_NOT_FOUND=3,
                      ILLEGAL_ARGUMENT=4,
                      ROW_SIZE_LIMIT_EXCEEDED=5,
                      KEY_SIZE_LIMIT_EXCEEDED=6,
                      BATCH_OP_NUMBER_LIMIT_EXCEEDED=7,
                      REQUEST_SIZE_LIMIT_EXCEEDED=8,
                      TABLE_EXISTS=9,
                      INDEX_EXISTS=10,
                      INVALID_AUTHORIZATION=11,
                      INSUFFICIENT_PERMISSION=12,
                      RESOURCE_EXISTS=13,
                      RESOURCE_NOT_FOUND=14,
                      TABLE_LIMIT_EXCEEDED=15,
                      INDEX_LIMIT_EXCEEDED=16,
                      BAD_PROTOCOL_MESSAGE=17,
                      EVOLUTION_LIMIT_EXCEEDED=18,
                      TABLE_DEPLOYMENT_LIMIT_EXCEEDED=19,
                      TENANT_DEPLOYMENT_LIMIT_EXCEEDED=20)

    # Error codes for user throttling, range from 50 to 100(exclusive).
    THROTTLING_ERROR = enum(READ_LIMIT_EXCEEDED=50,
                            WRITE_LIMIT_EXCEEDED=51,
                            SIZE_LIMIT_EXCEEDED=52,
                            OPERATION_LIMIT_EXCEEDED=53)

    """
    Retry-able server issues, range from 100 to 125(exclusive).
    These are internal problems, presumably temporary, and need to be sent back
    to the application for retry.
    """
    SERVER_RETRY_ERROR = enum(REQUEST_TIMEOUT=100,
                              SERVER_ERROR=101,
                              SERVICE_UNAVAILABLE=102,
                              TABLE_BUSY=103,
                              SECURITY_INFO_UNAVAILABLE=104)

    """
    Other server issues, begin from 125.
    These include server illegal state, unknown server error, etc.
    They might be retry-able, or not.
    """
    SERVER_OTHER_ERROR = enum(UNKNOWN_ERROR=125,
                              ILLEGAL_STATE=126)

    @staticmethod
    def check_request_size_limit(request, request_size):
        # Checks if the request size exceeds the limit.
        request_size_limit = (
            BinaryProtocol.BATCH_REQUEST_SIZE_LIMIT if
            isinstance(request, WriteMultipleRequest) else
            BinaryProtocol.REQUEST_SIZE_LIMIT)
        if request_size > request_size_limit:
            raise RequestSizeLimitException(
                'The request size of ' + str(request_size) +
                ' exceeded the limit of ' + str(request_size_limit))

    @staticmethod
    def deserialize_consumed_capacity(bis, result):
        result.set_read_units(BinaryProtocol.read_packed_int(bis))
        result.set_read_kb(BinaryProtocol.read_packed_int(bis))
        result.set_write_kb(BinaryProtocol.read_packed_int(bis))

    @staticmethod
    def deserialize_table_result(bis, result):
        has_info = bis.read_boolean()
        if has_info:
            # don't use tenant_id, but it's in the result
            BinaryProtocol.read_string(bis)
            result.set_table_name(BinaryProtocol.read_string(bis))
            result.set_state(
                BinaryProtocol.__get_table_state(bis.read_byte()))
            has_static_state = bis.read_boolean()
            if has_static_state:
                read_kb = BinaryProtocol.read_packed_int(bis)
                write_kb = BinaryProtocol.read_packed_int(bis)
                storage_gb = BinaryProtocol.read_packed_int(bis)
                limits = TableLimits(read_kb, write_kb, storage_gb)
                result.set_table_limits(limits)
                result.set_schema(BinaryProtocol.read_string(bis))
            result.set_operation_id(BinaryProtocol.read_string(bis))

    @staticmethod
    def deserialize_write_response(bis, result):
        return_info = bis.read_boolean()
        if not return_info:
            return
        # Existing info always includes both value and version.
        result.set_existing_value(BinaryProtocol.read_field_value(bis))
        result.set_existing_version(BinaryProtocol.read_version(bis))

    @staticmethod
    def map_exception(code, msg):
        # Maps the error code returned from the server into a local string.
        if (code == BinaryProtocol.SERVER_OTHER_ERROR.UNKNOWN_ERROR or
                code == BinaryProtocol.USER_ERROR.UNKNOWN_OPERATION):
            return NoSQLException('Unknown error: ' + msg)
        elif code == BinaryProtocol.SERVER_OTHER_ERROR.ILLEGAL_STATE:
            return IllegalStateException(msg)
        elif code == BinaryProtocol.SERVER_RETRY_ERROR.REQUEST_TIMEOUT:
            return RequestTimeoutException(msg)
        elif code == (
                BinaryProtocol.SERVER_RETRY_ERROR.SECURITY_INFO_UNAVAILABLE):
            return SecurityInfoNotReadyException(msg)
        elif (code == BinaryProtocol.SERVER_RETRY_ERROR.SERVICE_UNAVAILABLE or
              code == BinaryProtocol.SERVER_RETRY_ERROR.SERVER_ERROR):
            return SystemException(msg)
        elif code == BinaryProtocol.SERVER_RETRY_ERROR.TABLE_BUSY:
            return TableBusyException(msg)
        elif code == BinaryProtocol.THROTTLING_ERROR.OPERATION_LIMIT_EXCEEDED:
            return OperationThrottlingException(msg)
        elif code == BinaryProtocol.THROTTLING_ERROR.READ_LIMIT_EXCEEDED:
            return ReadThrottlingException(msg)
        elif code == BinaryProtocol.THROTTLING_ERROR.SIZE_LIMIT_EXCEEDED:
            return TableSizeException(msg)
        elif code == BinaryProtocol.THROTTLING_ERROR.WRITE_LIMIT_EXCEEDED:
            return WriteThrottlingException(msg)
        elif code == BinaryProtocol.USER_ERROR.BAD_PROTOCOL_MESSAGE:
            return IllegalArgumentException('Bad protocol message: ' + msg)
        elif (code ==
              BinaryProtocol.USER_ERROR.BATCH_OP_NUMBER_LIMIT_EXCEEDED):
            return BatchOperationNumberLimitException(msg)
        elif code == BinaryProtocol.USER_ERROR.EVOLUTION_LIMIT_EXCEEDED:
            return EvolutionLimitException(msg)
        elif code == BinaryProtocol.USER_ERROR.ILLEGAL_ARGUMENT:
            return IllegalArgumentException(msg)
        elif code == BinaryProtocol.USER_ERROR.INDEX_EXISTS:
            return IndexExistsException(msg)
        elif code == BinaryProtocol.USER_ERROR.INDEX_LIMIT_EXCEEDED:
            return IndexLimitException(msg)
        elif code == BinaryProtocol.USER_ERROR.INDEX_NOT_FOUND:
            return IndexNotFoundException(msg)
        elif code == BinaryProtocol.USER_ERROR.INSUFFICIENT_PERMISSION:
            return UnauthorizedException(msg)
        elif code == BinaryProtocol.USER_ERROR.INVALID_AUTHORIZATION:
            return InvalidAuthorizationException(msg)
        elif code == BinaryProtocol.USER_ERROR.KEY_SIZE_LIMIT_EXCEEDED:
            return KeySizeLimitException(msg)
        elif (code ==
              BinaryProtocol.USER_ERROR.REQUEST_SIZE_LIMIT_EXCEEDED):
            return RequestSizeLimitException(msg)
        elif code == BinaryProtocol.USER_ERROR.ROW_SIZE_LIMIT_EXCEEDED:
            return RowSizeLimitException(msg)
        elif code == BinaryProtocol.USER_ERROR.TABLE_EXISTS:
            return TableExistsException(msg)
        elif code == BinaryProtocol.USER_ERROR.TABLE_LIMIT_EXCEEDED:
            return TableLimitException(msg)
        elif code == BinaryProtocol.USER_ERROR.TABLE_NOT_FOUND:
            return TableNotFoundException(msg)
        elif (code == BinaryProtocol.USER_ERROR.TABLE_DEPLOYMENT_LIMIT_EXCEEDED
              or code == (
                  BinaryProtocol.USER_ERROR.TENANT_DEPLOYMENT_LIMIT_EXCEEDED)):
            return DeploymentException(msg)
        else:
            return NoSQLException(
                'Unknown error code ' + str(code) + ': ' + msg)

    @staticmethod
    def read_bytearray(bis):
        """
        Reads a possibly None byte array as a
        :py:meth:`read_sequence_length` followed by the array contents.

        :param bis: the byte input stream.
        :returns: the array or None.
        """
        length = BinaryProtocol.read_sequence_length(bis)
        if length == -1:
            return None
        if length == 0:
            return bytearray()
        buf = bytearray(length)
        bis.read_fully(buf)
        return buf

    @staticmethod
    def read_bytearray_with_int(bis):
        # Reads a byte array that has a not-packed integer size.
        length = bis.read_int()
        if length <= 0:
            raise IOError('Invalid length for prepared query: ' + str(length))
        buf = bytearray(length)
        bis.read_fully(buf)
        return buf

    @staticmethod
    def read_datetime(bis):
        # Deserialize a datetime value.
        dt = BinaryProtocol.read_string(bis)
        if '.' in dt:
            place = dt.index('.')
            if len(dt[place + 1:]) <= 6:
                return datetime.strptime(dt, '%Y-%m-%dT%H:%M:%S.%f')
            else:
                rounding = int(dt[place + 7])
                dt = dt[0:place + 7]
                dt = datetime.strptime(dt, '%Y-%m-%dT%H:%M:%S.%f')
                return dt if rounding < 5 else (dt + timedelta(microseconds=1))
        else:
            return datetime.strptime(dt, '%Y-%m-%dT%H:%M:%S')

    @staticmethod
    def read_decimal(bis):
        # Deserialize a decimal value.
        a = BinaryProtocol.read_string(bis)
        return Decimal(a)

    @staticmethod
    def read_dict(bis):
        # Read length.
        bis.read_int()

        size = bis.read_int()
        result = dict()
        count = 0
        while count < size:
            key = BinaryProtocol.read_string(bis)
            value = BinaryProtocol.read_field_value(bis)
            result[key] = value
            count += 1
        return result

    @staticmethod
    def read_field_value(bis):
        # Deserialize a generic field value.
        t = bis.read_byte()
        if t == BinaryProtocol.FIELD_VALUE_TYPE.ARRAY:
            return BinaryProtocol.read_list(bis)
        elif t == BinaryProtocol.FIELD_VALUE_TYPE.BINARY:
            return BinaryProtocol.read_bytearray(bis)
        elif t == BinaryProtocol.FIELD_VALUE_TYPE.BOOLEAN:
            return bis.read_boolean()
        elif t == BinaryProtocol.FIELD_VALUE_TYPE.DOUBLE:
            return bis.read_float()
        elif t == BinaryProtocol.FIELD_VALUE_TYPE.INTEGER:
            return BinaryProtocol.read_packed_int(bis)
        elif t == BinaryProtocol.FIELD_VALUE_TYPE.LONG:
            return BinaryProtocol.read_packed_long(bis)
        elif t == BinaryProtocol.FIELD_VALUE_TYPE.MAP:
            return BinaryProtocol.read_dict(bis)
        elif t == BinaryProtocol.FIELD_VALUE_TYPE.STRING:
            return BinaryProtocol.read_string(bis)
        elif t == BinaryProtocol.FIELD_VALUE_TYPE.TIMESTAMP:
            return BinaryProtocol.read_datetime(bis)
        elif t == BinaryProtocol.FIELD_VALUE_TYPE.NUMBER:
            return BinaryProtocol.read_decimal(bis)
        elif (t == BinaryProtocol.FIELD_VALUE_TYPE.NULL or
              t == BinaryProtocol.FIELD_VALUE_TYPE.JSON_NULL):
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
    def read_packed_int(bis):
        """
        Reads a packed integer from the input and returns it.

        :param bis: the byte input stream.
        :returns: the integer that was read.
        """
        buf = bytearray(PackedInteger.MAX_LENGTH)
        bis.read_fully(buf, 0, 1)
        length = PackedInteger.get_read_sorted_int_length(buf, 0)
        bis.read_fully(buf, 1, length)
        return PackedInteger.read_sorted_int(buf, 0)

    @staticmethod
    def read_packed_long(bis):
        """
        Reads a packed long from the input and returns it.

        :param bis: the byte input stream.
        :returns: the long that was read.
        """
        buf = bytearray(PackedInteger.MAX_LONG_LENGTH)
        bis.read_fully(buf, 0, 1)
        length = PackedInteger.get_read_sorted_long_length(buf, 0)
        bis.read_fully(buf, 1, length)
        return PackedInteger.read_sorted_long(buf, 0)

    @staticmethod
    def read_sequence_length(bis):
        """
        Reads the length of a possibly None sequence.  The length is represented
        as a :py:meth:`PackedInteger.read_packed_int`, with -1 interpreted as
        meaning None, and other negative values not permitted. Although we don't
        enforce maximum sequence lengths yet, this entrypoint provides a place
        to do that.

        :param bis: the byte input stream.
        :returns: the sequence length or -1 for None.
        """
        result = BinaryProtocol.read_packed_int(bis)
        if result < -1:
            raise IOError('Invalid sequence length: ' + str(result))
        return result

    @staticmethod
    def read_string(bis):
        """
        Reads a possibly None string from an input stream in standard UTF-8
        format.

        First reads a packed int representing the length of the UTF-8 encoding
        of the string, or a negative value for None, followed by the string
        contents in UTF-8 format for a non-empty string, if any.

        :param bis: the byte input stream.
        :returns: the string or None.
        """
        length = BinaryProtocol.read_packed_int(bis)
        if length < -1:
            raise IOError('Invalid length of String: ' + str(length))
        if length == -1:
            return None
        if length == 0:
            return str()
        buf = bytearray(length)
        bis.read_fully(buf)
        if version_info.major == 2:
            return str(buf)
        return buf.decode()

    @staticmethod
    def read_version(bis):
        return Version.create_version(BinaryProtocol.read_bytearray(bis))

    # Writes fields from ReadRequest.
    @staticmethod
    def serialize_read_request(request, bos):
        BinaryProtocol.serialize_request(request, bos)
        BinaryProtocol.write_string(bos, request.get_table_name_internal())
        bos.write_byte(request.get_consistency_internal())

    # Writes fields from WriteRequest
    @staticmethod
    def serialize_write_request(request, bos):
        BinaryProtocol.serialize_request(request, bos)
        BinaryProtocol.write_string(bos, request.get_table_name_internal())
        bos.write_boolean(request.get_return_row_internal())

    @staticmethod
    def serialize_request(request, bos):
        BinaryProtocol.write_packed_int(bos, request.get_timeout())

    @staticmethod
    def write_bytearray(bos, value):
        """
        Writes a possibly None byte array as a sequence length followed by the
        array contents.

        :param bos: the byte output stream.
        :param value: the byte array of None.
        """
        length = -1 if value is None else len(value)
        BinaryProtocol.write_sequence_length(bos, length)
        if length > 0:
            bos.write_bytearray(value)

    @staticmethod
    def write_bytearray_with_int(bos, value):
        # Writes a byte array with a full 4-byte int length.
        bos.write_int(len(value))
        bos.write_bytearray(value)

    @staticmethod
    def write_datetime(bos, value):
        # Serialize a datetime value.
        BinaryProtocol.write_string(bos, value.isoformat())

    @staticmethod
    def write_decimal(bos, value):
        # Serialize a decimal value.
        BinaryProtocol.write_string(bos, str(value))

    @staticmethod
    def write_dict(bos, value):
        # Serialize a dict.
        # Leave an integer-sized space for length.
        offset = bos.get_offset()
        bos.write_int(0)
        start = bos.get_offset()
        bos.write_int(len(value))
        for key in value:
            CheckValue.check_str(key, 'key')
            BinaryProtocol.write_string(bos, key)
            BinaryProtocol.write_field_value(bos, value[key])
        # Update the length value.
        bos.write_int_at_offset(offset, bos.get_offset() - start)

    @staticmethod
    def write_field_range(bos, field_range):
        if field_range is None:
            bos.write_boolean(False)
            return
        bos.write_boolean(True)
        BinaryProtocol.write_string(bos, field_range.get_field_path())
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
        bos.write_byte(BinaryProtocol.__get_type(value))
        if value is not None:
            if isinstance(value, list):
                BinaryProtocol.write_list(bos, value)
            elif isinstance(value, bytearray):
                BinaryProtocol.write_bytearray(bos, value)
            elif isinstance(value, bool):
                bos.write_boolean(value)
            elif isinstance(value, float):
                bos.write_float(value)
            elif CheckValue.is_int(value):
                BinaryProtocol.write_packed_long(bos, value)
            elif isinstance(value, dict):
                BinaryProtocol.write_dict(bos, value)
            elif CheckValue.is_str(value):
                BinaryProtocol.write_string(bos, value)
            elif isinstance(value, datetime):
                BinaryProtocol.write_datetime(bos, value)
            elif isinstance(value, Decimal):
                BinaryProtocol.write_decimal(bos, value)
            else:
                raise IllegalStateException('Unknown type ' + str(type(value)))

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
    def write_op_code(bos, op):
        # Writes the opcode for the operation.
        bos.write_byte(op)

    @staticmethod
    def write_packed_int(bos, value):
        """
        Writes a packed integer to the byte output stream.

        :param bos: the byte output stream.
        :param value: the integer to be written.
        :returns: the length of bytes written.
        """
        buf = bytearray(PackedInteger.MAX_LENGTH)
        offset = PackedInteger.write_sorted_int(buf, 0, value)
        bos.write_bytearray(buf, 0, offset)
        return offset

    @staticmethod
    def write_packed_long(bos, value):
        """
        Writes a packed long to the byte output stream.

        :param bos: the byte output stream.
        :param value: the long to be written.
        """
        buf = bytearray(PackedInteger.MAX_LONG_LENGTH)
        offset = PackedInteger.write_sorted_long(buf, 0, value)
        bos.write_bytearray(buf, 0, offset)

    @staticmethod
    def write_record(bos, record):
        """
        Writes a dict.

        This is public to allow a caller to get the size of a value outside of
        the context of serialization.
        """
        BinaryProtocol.write_field_value(bos, record)

    @staticmethod
    def write_sequence_length(bos, length):
        """
        Writes a sequence length. The length is represented as a packed int,
        with -1 representing None. Although we don't enforce maximum sequence
        lengths yet, this entrypoint provides a place to do that.

        :param bos: the byte output stream.
        :param length: the sequence length or -1.
        :raises IllegalArgumentException: raises the exception if length is less
            than -1.
        """
        if length < -1:
            raise IllegalArgumentException(
                'Invalid sequence length: ' + str(length))
        BinaryProtocol.write_packed_int(bos, length)

    @staticmethod
    def write_serial_version(bos):
        # Writes the (short) serial version
        bos.write_short_int(BinaryProtocol.SERIAL_VERSION)

    @staticmethod
    def write_string(bos, value):
        """
        Writes a possibly None or empty string to an output stream using
        standard UTF-8 format.

        First writes a packed int representing the length of the UTF-8 encoding
        of the string, or -1 if the string is None, followed by the UTF-8
        encoding for non-empty strings.

        :param bos: the byte output stream.
        :param value: the string or None.
        :returns: the number of bytes that the string take.
        """
        if value is None:
            return BinaryProtocol.write_packed_int(bos, -1)
        try:
            buf = bytearray(value.encode())
        except UnicodeDecodeError:
            buf = bytearray(value)
        length = len(buf)
        int_len = BinaryProtocol.write_packed_int(bos, length)
        if length > 0:
            bos.write_bytearray(buf)
        return int_len + length

    @staticmethod
    def write_ttl(bos, ttl):
        if ttl is None:
            BinaryProtocol.write_packed_long(bos, -1)
            return
        BinaryProtocol.write_packed_long(bos, ttl.get_value())
        if ttl.unit_is_days():
            bos.write_byte(TimeUnit.DAYS)
        elif ttl.unit_is_hours():
            bos.write_byte(TimeUnit.HOURS)
        else:
            raise IllegalStateException('Invalid TTL unit in ttl ' + str(ttl))

    @staticmethod
    def write_version(bos, version):
        CheckValue.check_not_none(version, 'array')
        BinaryProtocol.write_bytearray(bos, version.get_bytes())

    @staticmethod
    def __get_table_state(state):
        if state == BinaryProtocol.TABLE_STATE.ACTIVE:
            return State.ACTIVE
        elif state == BinaryProtocol.TABLE_STATE.CREATING:
            return State.CREATING
        elif state == BinaryProtocol.TABLE_STATE.DROPPED:
            return State.DROPPED
        elif state == BinaryProtocol.TABLE_STATE.DROPPING:
            return State.DROPPING
        elif state == BinaryProtocol.TABLE_STATE.UPDATING:
            return State.UPDATING
        else:
            raise IllegalStateException('Unknown table state ' + str(state))

    @staticmethod
    def __get_type(value):
        if isinstance(value, list):
            return BinaryProtocol.FIELD_VALUE_TYPE.ARRAY
        elif isinstance(value, bytearray):
            return BinaryProtocol.FIELD_VALUE_TYPE.BINARY
        elif isinstance(value, bool):
            return BinaryProtocol.FIELD_VALUE_TYPE.BOOLEAN
        elif isinstance(value, float):
            return BinaryProtocol.FIELD_VALUE_TYPE.DOUBLE
        elif CheckValue.is_int(value):
            return BinaryProtocol.FIELD_VALUE_TYPE.LONG
        elif isinstance(value, dict):
            return BinaryProtocol.FIELD_VALUE_TYPE.MAP
        elif CheckValue.is_str(value):
            return BinaryProtocol.FIELD_VALUE_TYPE.STRING
        elif isinstance(value, datetime):
            return BinaryProtocol.FIELD_VALUE_TYPE.STRING
        elif isinstance(value, Decimal):
            return BinaryProtocol.FIELD_VALUE_TYPE.STRING
        elif value is None:
            return BinaryProtocol.FIELD_VALUE_TYPE.NULL
        else:
            raise IllegalStateException(
                'Unknown value type ' + str(type(value)))


class DeleteRequestSerializer:
    """
    The flag indicates if the serializer is used for a standalone request or a
    sub operation of WriteMultiple request.

    If it is used to serialize the sub operation, then some information like
    timeout, namespace and table_name will be skipped during serialization.
    """

    def __init__(self, is_sub_request=False, cls_result=None):
        self.__is_sub_request = is_sub_request
        self.__cls_result = cls_result

    def serialize(self, request, bos):
        match_version = request.get_match_version()
        op_code = (BinaryProtocol.OP_CODE.DELETE if match_version is None else
                   BinaryProtocol.OP_CODE.DELETE_IF_VERSION)
        BinaryProtocol.write_op_code(bos, op_code)
        if self.__is_sub_request:
            bos.write_boolean(request.get_return_row())
        else:
            BinaryProtocol.serialize_write_request(request, bos)
        BinaryProtocol.write_field_value(bos, request.get_key())
        if match_version is not None:
            BinaryProtocol.write_version(bos, match_version)

    def deserialize(self, bis):
        result = self.__cls_result()
        BinaryProtocol.deserialize_consumed_capacity(bis, result)
        success = bis.read_boolean()
        result.set_success(success)
        BinaryProtocol.deserialize_write_response(bis, result)
        return result


class GetIndexesRequestSerializer:
    def __init__(self, cls_result=None):
        self.__cls_result = cls_result

    def serialize(self, request, bos):
        BinaryProtocol.write_op_code(bos, BinaryProtocol.OP_CODE.GET_INDEXES)
        BinaryProtocol.serialize_request(request, bos)
        BinaryProtocol.write_string(bos, request.get_table_name())
        if request.get_index_name() is not None:
            bos.write_boolean(True)
            BinaryProtocol.write_string(bos, request.get_index_name())
        else:
            bos.write_boolean(False)

    def deserialize(self, bis):
        result = self.__cls_result()
        num_indexes = BinaryProtocol.read_packed_int(bis)
        indexes = list()
        count = 0
        while count < num_indexes:
            indexes.append(self.__deserialize_index_info(bis))
            count += 1
        result.set_indexes(indexes)
        return result

    def __deserialize_index_info(self, bis):
        index_name = BinaryProtocol.read_string(bis)
        num_fields = BinaryProtocol.read_packed_int(bis)
        field_names = list()
        count = 0
        while count < num_fields:
            field_names.append(BinaryProtocol.read_string(bis))
            count += 1
        return IndexInfo(index_name, field_names)


class GetRequestSerializer:
    def __init__(self, cls_result=None):
        self.__cls_result = cls_result

    def serialize(self, request, bos):
        BinaryProtocol.write_op_code(bos, BinaryProtocol.OP_CODE.GET)
        BinaryProtocol.serialize_read_request(request, bos)
        BinaryProtocol.write_field_value(bos, request.get_key())

    def deserialize(self, bis):
        result = self.__cls_result()
        BinaryProtocol.deserialize_consumed_capacity(bis, result)
        has_row = bis.read_boolean()
        if has_row:
            result.set_value(BinaryProtocol.read_field_value(bis))
            result.set_expiration_time(BinaryProtocol.read_packed_long(bis))
            result.set_version(BinaryProtocol.read_version(bis))
        return result


class GetTableRequestSerializer:
    def __init__(self, cls_result=None):
        self.__cls_result = cls_result

    def serialize(self, request, bos):
        BinaryProtocol.write_op_code(bos, BinaryProtocol.OP_CODE.GET_TABLE)
        BinaryProtocol.serialize_request(request, bos)
        BinaryProtocol.write_string(bos, request.get_table_name())
        BinaryProtocol.write_string(bos, request.get_operation_id())

    def deserialize(self, bis):
        result = self.__cls_result()
        BinaryProtocol.deserialize_table_result(bis, result)
        return result


class ListTablesRequestSerializer:
    def __init__(self, cls_result=None):
        self.__cls_result = cls_result

    def serialize(self, request, bos):
        BinaryProtocol.write_op_code(bos, BinaryProtocol.OP_CODE.LIST_TABLES)
        BinaryProtocol.serialize_request(request, bos)
        bos.write_int(request.get_start_index())
        bos.write_int(request.get_limit())

    def deserialize(self, bis):
        result = self.__cls_result()
        num_tables = BinaryProtocol.read_packed_int(bis)
        tables = list()
        count = 0
        while count < num_tables:
            tables.append(BinaryProtocol.read_string(bis))
            count += 1
        result.set_tables(tables)
        result.set_last_index_returned(BinaryProtocol.read_packed_int(bis))
        return result


class MultiDeleteRequestSerializer:
    def __init__(self, cls_result=None):
        self.__cls_result = cls_result

    def serialize(self, request, bos):
        BinaryProtocol.write_op_code(bos, BinaryProtocol.OP_CODE.MULTI_DELETE)
        BinaryProtocol.serialize_request(request, bos)
        BinaryProtocol.write_string(bos, request.get_table_name())
        BinaryProtocol.write_field_value(bos, request.get_key())
        BinaryProtocol.write_field_range(bos, request.get_range())
        BinaryProtocol.write_packed_int(bos, request.get_max_write_kb())
        BinaryProtocol.write_bytearray(bos, request.get_continuation_key())

    def deserialize(self, bis):
        result = self.__cls_result()
        BinaryProtocol.deserialize_consumed_capacity(bis, result)
        result.set_num_deletions(BinaryProtocol.read_packed_int(bis))
        result.set_continuation_key(BinaryProtocol.read_bytearray(bis))
        return result


class PrepareRequestSerializer:
    # Prepare a query.
    def __init__(self, cls_result=None):
        self.__cls_result = cls_result

    def serialize(self, request, bos):
        BinaryProtocol.write_op_code(bos, BinaryProtocol.OP_CODE.PREPARE)
        BinaryProtocol.serialize_request(request, bos)
        BinaryProtocol.write_string(bos, request.get_statement())

    def deserialize(self, bis):
        result = self.__cls_result()
        BinaryProtocol.deserialize_consumed_capacity(bis, result)
        result.set_prepared_statement(
            BinaryProtocol.read_bytearray_with_int(bis))
        return result


class PutRequestSerializer:
    """
    The flag indicates if the serializer is used for a standalone request or a
    sub operation of WriteMultiple request.

    If it is used to serialize the sub operation, then some information like
    timeout, namespace and table_name will be skipped during serialization.
    """

    def __init__(self, is_sub_request=False, cls_result=None):
        self.__is_sub_request = is_sub_request
        self.__cls_result = cls_result

    def serialize(self, request, bos):
        op = self.__get_op_code(request)
        BinaryProtocol.write_op_code(bos, op)
        if self.__is_sub_request:
            bos.write_boolean(request.get_return_row())
        else:
            BinaryProtocol.serialize_write_request(request, bos)
        BinaryProtocol.write_record(bos, request.get_value())
        bos.write_boolean(request.get_update_ttl())
        BinaryProtocol.write_ttl(bos, request.get_ttl())
        if request.get_match_version() is not None:
            BinaryProtocol.write_version(bos, request.get_match_version())

    def deserialize(self, bis):
        result = self.__cls_result()
        BinaryProtocol.deserialize_consumed_capacity(bis, result)
        success = bis.read_boolean()
        if success:
            result.set_version(BinaryProtocol.read_version(bis))
        # return row info.
        BinaryProtocol.deserialize_write_response(bis, result)
        return result

    def __get_op_code(self, request):
        """
        Assumes that the request has been validated and only one of the if
        options is set, if any.
        """
        request_op = request.get_option()
        if request_op is None:
            return BinaryProtocol.OP_CODE.PUT
        elif request_op is PutOption.IF_ABSENT:
            return BinaryProtocol.OP_CODE.PUT_IF_ABSENT
        elif request_op is PutOption.IF_PRESENT:
            return BinaryProtocol.OP_CODE.PUT_IF_PRESENT
        elif request_op is PutOption.IF_VERSION:
            return BinaryProtocol.OP_CODE.PUT_IF_VERSION
        else:
            raise IllegalStateException('Unknown Options ' + str(request_op))


class QueryRequestSerializer:
    def __init__(self, cls_result=None):
        self.__cls_result = cls_result

    def serialize(self, request, bos):
        # write unconditional state first.
        BinaryProtocol.write_op_code(bos, BinaryProtocol.OP_CODE.QUERY)
        BinaryProtocol.serialize_request(request, bos)
        bos.write_byte(request.get_consistency())
        BinaryProtocol.write_packed_int(bos, request.get_limit())
        BinaryProtocol.write_packed_int(bos, request.get_max_read_kb())
        BinaryProtocol.write_bytearray(bos, request.get_continuation_key())
        if request.get_prepared_statement() is not None:
            ps = request.get_prepared_statement()
            bos.write_boolean(True)
            BinaryProtocol.write_bytearray_with_int(bos, ps.get_statement())
            if ps.get_variables() is not None:
                variables = ps.get_variables()
                BinaryProtocol.write_packed_int(bos, len(variables))
                for key in variables:
                    BinaryProtocol.write_string(bos, key)
                    BinaryProtocol.write_field_value(bos, variables[key])
            else:
                BinaryProtocol.write_packed_int(bos, 0)
        else:
            bos.write_boolean(False)
            BinaryProtocol.write_string(bos, request.get_statement())

    def deserialize(self, bis):
        result = self.__cls_result()
        # the size is an uncompressed int so don't use utility method.
        num_rows = bis.read_int()
        results = list()
        count = 0
        while count < num_rows:
            results.append(BinaryProtocol.read_field_value(bis))
            count += 1
        result.set_results(results)
        BinaryProtocol.deserialize_consumed_capacity(bis, result)
        result.set_continuation_key(BinaryProtocol.read_bytearray(bis))
        return result


class TableRequestSerializer:
    def __init__(self, cls_result=None):
        self.__cls_result = cls_result

    def serialize(self, request, bos):
        BinaryProtocol.write_op_code(bos, BinaryProtocol.OP_CODE.TABLE_REQUEST)
        BinaryProtocol.serialize_request(request, bos)
        BinaryProtocol.write_string(bos, request.get_statement())
        limits = request.get_table_limits()
        if limits is not None:
            bos.write_boolean(True)
            bos.write_int(limits.get_read_units())
            bos.write_int(limits.get_write_units())
            bos.write_int(limits.get_storage_gb())
            if request.get_table_name() is not None:
                bos.write_boolean(True)
                BinaryProtocol.write_string(bos, request.get_table_name())
            else:
                bos.write_boolean(False)
        else:
            bos.write_boolean(False)

    def deserialize(self, bis):
        result = self.__cls_result()
        BinaryProtocol.deserialize_table_result(bis, result)
        return result


class TableUsageRequestSerializer:
    def __init__(self, cls_result=None):
        self.__cls_result = cls_result

    def serialize(self, request, bos):
        BinaryProtocol.write_op_code(
            bos, BinaryProtocol.OP_CODE.GET_TABLE_USAGE)
        BinaryProtocol.serialize_request(request, bos)
        BinaryProtocol.write_string(bos, request.get_table_name())
        BinaryProtocol.write_packed_long(bos, request.get_start_time())
        BinaryProtocol.write_packed_long(bos, request.get_end_time())
        BinaryProtocol.write_packed_int(bos, request.get_limit())

    def deserialize(self, bis):
        result = self.__cls_result()
        # don't use tenant_id, but it's in the result
        BinaryProtocol.read_string(bis)
        result.set_table_name(BinaryProtocol.read_string(bis))
        num_results = BinaryProtocol.read_packed_int(bis)
        usage_records = list()
        count = 0
        while count < num_results:
            usage_records.append(self.__deserialize_usage(bis))
            count += 1
        result.set_usage_records(usage_records)
        return result

    def __deserialize_usage(self, bis):
        start_time_ms = BinaryProtocol.read_packed_long(bis)
        seconds_in_period = BinaryProtocol.read_packed_int(bis)
        read_units = BinaryProtocol.read_packed_int(bis)
        write_units = BinaryProtocol.read_packed_int(bis)
        storage_gb = BinaryProtocol.read_packed_int(bis)
        read_throttle_count = BinaryProtocol.read_packed_int(bis)
        write_throttle_count = BinaryProtocol.read_packed_int(bis)
        storage_throttle_count = BinaryProtocol.read_packed_int(bis)
        usage = TableUsage(start_time_ms, seconds_in_period, read_units,
                           write_units, storage_gb, read_throttle_count,
                           write_throttle_count, storage_throttle_count)
        return usage


class WriteMultipleRequestSerializer:
    def __init__(self, cls_update_multiple_result=None,
                 cls_operation_result=None):
        self.__cls_update_multiple_result = cls_update_multiple_result
        self.__cls_operation_result = cls_operation_result

    def serialize(self, request, bos):
        put_serializer = PutRequestSerializer(True)
        delete_serializer = DeleteRequestSerializer(True)
        num = request.get_num_operations()
        BinaryProtocol.write_op_code(
            bos, BinaryProtocol.OP_CODE.WRITE_MULTIPLE)
        BinaryProtocol.serialize_request(request, bos)
        BinaryProtocol.write_string(bos, request.get_table_name())
        BinaryProtocol.write_packed_int(bos, num)
        for op in request.get_operations():
            start = bos.get_offset()
            bos.write_boolean(op.is_abort_if_unsuccessful())
            req = op.get_request()
            if str(req) == 'PutRequest':
                put_serializer.serialize(req, bos)
            else:
                assert str(req) == 'DeleteRequest'
                delete_serializer.serialize(req, bos)
            # Check each sub request size limit.
            BinaryProtocol.check_request_size_limit(
                req, (bos.get_offset() - start))

    def deserialize(self, bis):
        result = self.__cls_update_multiple_result()
        succeed = bis.read_boolean()
        BinaryProtocol.deserialize_consumed_capacity(bis, result)
        if succeed:
            num = BinaryProtocol.read_packed_int(bis)
            count = 0
            while count < num:
                result.add_result(self.__create_operation_result(bis))
                count += 1
        else:
            result.set_failed_operation_index(bis.read_byte())
            result.add_result(self.__create_operation_result(bis))
        return result

    def __create_operation_result(self, bis):
        op_result = self.__cls_operation_result()
        op_result.set_success(bis.read_boolean())
        if bis.read_boolean():
            op_result.set_version(BinaryProtocol.read_version(bis))
        BinaryProtocol.deserialize_write_response(bis, op_result)
        return op_result
