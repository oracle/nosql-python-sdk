#
# Copyright (c) 2018, 2023 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#

from abc import ABCMeta, abstractmethod
from datetime import datetime
from dateutil import parser, tz
from decimal import (
    Decimal, ROUND_05UP, ROUND_CEILING, ROUND_DOWN, ROUND_FLOOR,
    ROUND_HALF_DOWN, ROUND_HALF_EVEN, ROUND_HALF_UP, ROUND_UP)
from sys import version_info
from time import mktime

from .common import (
    CheckValue, Empty, JsonNone, PackedInteger, PutOption, State, SystemState, enum)
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

from .kv.exception import AuthenticationException


#
# Abstract classes/interfaces related to serialization
#

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


class NsonEventHandler:
    """
    Base class of NsonEvent
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def boolean_value(self, value):
        pass

    @abstractmethod
    def binary_value(self, value):
        pass

    @abstractmethod
    def string_value(self, value):
        pass

    @abstractmethod
    def integer_value(self, value):
        pass

    @abstractmethod
    def long_value(self, value):
        pass

    @abstractmethod
    def double_value(self, value):
        pass

    @abstractmethod
    def number_value(self, value):
        pass

    @abstractmethod
    def timestamp_value(self, value):
        pass

    @abstractmethod
    def json_null_value(self):
        pass

    @abstractmethod
    def null_value(self):
        pass

    @abstractmethod
    def empty_value(self):
        pass

    @abstractmethod
    def start_map(self, size=None):
        pass

    @abstractmethod
    def start_array(self, size=None):
        pass

    @abstractmethod
    def end_map(self, size=None):
        pass

    @abstractmethod
    def end_array(self, size=None):
        pass

    @abstractmethod
    def start_map_field(self, key):
        pass

    @abstractmethod
    def end_map_field(self, key=None):
        pass

    @abstractmethod
    def start_array_field(self, index=None):
        pass

    @abstractmethod
    def end_array_field(self, index=None):
        pass

    @abstractmethod
    def stop(self):
        pass


class SerdeUtil(object):
    """
    A class to encapsulte static methods used by serialization and
    deserialization of requests. These utility methods can be used by
    multiple protocols. It also includes constants that are shared across
    protocols.
    """
    TRACE_LEVEL = 0

    # protocol serial versions
    SERIAL_VERSION_3 = 3
    SERIAL_VERSION_4 = 4
    DEFAULT_SERIAL_VERSION = SERIAL_VERSION_4

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
                            NULL=11,
                            EMPTY=12)

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
                   DROP_INDEX=22,
                   # added in V2.
                   SYSTEM_REQUEST=23,
                   SYSTEM_STATUS_REQUEST=24)

    # System Operation state.
    SYSTEM_STATE = enum(COMPLETE=0,
                        WORKING=1)

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
    USER_ERROR = enum(UNKNOWN_OPERATION=1,
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
                      TENANT_DEPLOYMENT_LIMIT_EXCEEDED=20,
                      # added in V2.
                      OPERATION_NOT_SUPPORTED=21,
                      ETAG_MISMATCH=22,
                      CANNOT_CANCEL_WORK_REQUEST=23,
                      # added in V3
                      UNSUPPORTED_PROTOCOL=24)

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
                              SECURITY_INFO_UNAVAILABLE=104,
                              # added in V2.
                              RETRY_AUTHENTICATION=105)

    """
    Other server issues, begin from 125.
    These include server illegal state, unknown server error, etc.
    They might be retry-able, or not.
    """
    SERVER_OTHER_ERROR = enum(UNKNOWN_ERROR=125,
                              ILLEGAL_STATE=126)

    """
    in V3 and above, TableLimits includes a mode
    """
    CAPACITY_MODE = enum(PROVISIONED=1,
                         ON_DEMAND=2)

    @staticmethod
    def convert_value_to_none(value):
        if isinstance(value, dict):
            return {key: SerdeUtil.convert_value_to_none(val)
                    for (key, val) in value.items()}
        if isinstance(value, list):
            return [SerdeUtil.convert_value_to_none(val) for val in
                    value]
        if isinstance(value, Empty) or isinstance(value, JsonNone):
            return None
        return value

    @staticmethod
    def get_operation_state(state):
        if state == SerdeUtil.SYSTEM_STATE.COMPLETE:
            return SystemState.COMPLETE
        elif state == SerdeUtil.SYSTEM_STATE.WORKING:
            return SystemState.WORKING
        else:
            raise IllegalStateException(
                'Unknown system operation state ' + str(state))

    @staticmethod
    def get_put_op_code(request):
        """
        Assumes that the request has been validated and only one of the if
        options is set, if any.
        """
        request_op = request.get_option()
        if request_op is None:
            return SerdeUtil.OP_CODE.PUT
        elif request_op is PutOption.IF_ABSENT:
            return SerdeUtil.OP_CODE.PUT_IF_ABSENT
        elif request_op is PutOption.IF_PRESENT:
            return SerdeUtil.OP_CODE.PUT_IF_PRESENT
        elif request_op is PutOption.IF_VERSION:
            return SerdeUtil.OP_CODE.PUT_IF_VERSION
        else:
            raise IllegalStateException('Unknown Options ' + str(request_op))

    @staticmethod
    def map_exception(code, msg):
        # Maps the error code returned from the server into a local string.
        if (code == SerdeUtil.SERVER_OTHER_ERROR.UNKNOWN_ERROR or
                code == SerdeUtil.USER_ERROR.UNKNOWN_OPERATION):
            return NoSQLException('Unknown error: ' + msg)
        elif code == SerdeUtil.SERVER_OTHER_ERROR.ILLEGAL_STATE:
            return IllegalStateException(msg)
        elif code == SerdeUtil.SERVER_RETRY_ERROR.REQUEST_TIMEOUT:
            return RequestTimeoutException(msg)
        elif code == SerdeUtil.SERVER_RETRY_ERROR.RETRY_AUTHENTICATION:
            return AuthenticationException(msg)
        elif code == (
                SerdeUtil.SERVER_RETRY_ERROR.SECURITY_INFO_UNAVAILABLE):
            return SecurityInfoNotReadyException(msg)
        elif (code == SerdeUtil.SERVER_RETRY_ERROR.SERVICE_UNAVAILABLE or
              code == SerdeUtil.SERVER_RETRY_ERROR.SERVER_ERROR):
            return SystemException(msg)
        elif code == SerdeUtil.THROTTLING_ERROR.OPERATION_LIMIT_EXCEEDED:
            return OperationThrottlingException(msg)
        elif code == SerdeUtil.THROTTLING_ERROR.READ_LIMIT_EXCEEDED:
            return ReadThrottlingException(msg)
        elif code == SerdeUtil.THROTTLING_ERROR.SIZE_LIMIT_EXCEEDED:
            return TableSizeException(msg)
        elif code == SerdeUtil.THROTTLING_ERROR.WRITE_LIMIT_EXCEEDED:
            return WriteThrottlingException(msg)
        elif code == SerdeUtil.USER_ERROR.BAD_PROTOCOL_MESSAGE:
            # V2 proxy will return this message if V3 is used in the driver
            if "Invalid driver serial version" in msg:
                return UnsupportedProtocolException(msg)
            return IllegalArgumentException('Bad protocol message: ' + msg)
        elif (code ==
              SerdeUtil.USER_ERROR.BATCH_OP_NUMBER_LIMIT_EXCEEDED):
            return BatchOperationNumberLimitException(msg)
        elif code == SerdeUtil.USER_ERROR.EVOLUTION_LIMIT_EXCEEDED:
            return EvolutionLimitException(msg)
        elif code == SerdeUtil.USER_ERROR.ILLEGAL_ARGUMENT:
            return IllegalArgumentException(msg)
        elif code == SerdeUtil.USER_ERROR.INDEX_EXISTS:
            return IndexExistsException(msg)
        elif code == SerdeUtil.USER_ERROR.INDEX_LIMIT_EXCEEDED:
            return IndexLimitException(msg)
        elif code == SerdeUtil.USER_ERROR.INDEX_NOT_FOUND:
            return IndexNotFoundException(msg)
        elif code == SerdeUtil.USER_ERROR.INSUFFICIENT_PERMISSION:
            return UnauthorizedException(msg)
        elif code == SerdeUtil.USER_ERROR.INVALID_AUTHORIZATION:
            return InvalidAuthorizationException(msg)
        elif code == SerdeUtil.USER_ERROR.KEY_SIZE_LIMIT_EXCEEDED:
            return KeySizeLimitException(msg)
        elif code == SerdeUtil.USER_ERROR.OPERATION_NOT_SUPPORTED:
            return OperationNotSupportedException(msg)
        elif (code ==
              SerdeUtil.USER_ERROR.REQUEST_SIZE_LIMIT_EXCEEDED):
            return RequestSizeLimitException(msg)
        elif code == SerdeUtil.USER_ERROR.RESOURCE_EXISTS:
            return ResourceExistsException(msg)
        elif code == SerdeUtil.USER_ERROR.RESOURCE_NOT_FOUND:
            return ResourceNotFoundException(msg)
        elif code == SerdeUtil.USER_ERROR.ROW_SIZE_LIMIT_EXCEEDED:
            return RowSizeLimitException(msg)
        elif code == SerdeUtil.USER_ERROR.TABLE_EXISTS:
            return TableExistsException(msg)
        elif code == SerdeUtil.USER_ERROR.TABLE_LIMIT_EXCEEDED:
            return TableLimitException(msg)
        elif code == SerdeUtil.USER_ERROR.TABLE_NOT_FOUND:
            return TableNotFoundException(msg)
        elif (code == SerdeUtil.USER_ERROR.TABLE_DEPLOYMENT_LIMIT_EXCEEDED
              or code == (
                      SerdeUtil.USER_ERROR.TENANT_DEPLOYMENT_LIMIT_EXCEEDED)):
            return DeploymentException(msg)
        elif code == SerdeUtil.USER_ERROR.UNSUPPORTED_PROTOCOL:
            return UnsupportedProtocolException(msg)
        else:
            return NoSQLException(
                'Unknown error code ' + str(code) + ': ' + msg)

    @staticmethod
    def read_bytearray(bis, skip):
        """
        Reads a possibly None byte array as a
        :py:meth:`read_sequence_length` followed by the array contents.

        :param bis: the byte input stream.
        :type bis: ByteInputStream
        :param skip: True if skipping vs reading
        :type skip: boolean
        :returns: the array or None.
        :rtype: bytearray
        """
        length = SerdeUtil.read_sequence_length(bis)
        if length < -1:
            raise IOError('Invalid length of byte array: ' + str(length))
        if length == -1:
            return None
        if length == 0 and not skip:
            return bytearray()
        if skip:
            bis.set_offset(bis.get_offset() + length)
            return None
        buf = bytearray(length)
        bis.read_fully(buf)
        return buf

    @staticmethod
    def read_full_int(bis):
        # Reads a full, 4-byte int
        return bis.read_int()

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
        # Deserialize a datetime value. Timezone is UTC, object is naive, not
        # timezone aware
        return parser.parse(SerdeUtil.read_string(bis))


    @staticmethod
    def read_decimal(bis):
        # Deserialize a decimal value.
        a = SerdeUtil.read_string(bis)
        return Decimal(a)

    @staticmethod
    def read_packed_int(bis):
        """
        Reads a packed integer from the input and returns it.

        :param bis: the byte input stream.
        :type bis: ByteInputStream
        :returns: the integer that was read.
        :rtype: int
        """
        buf = bytearray(PackedInteger.MAX_LENGTH)
        bis.read_fully(buf, 0, 1)
        length = PackedInteger.get_read_sorted_int_length(buf, 0)
        bis.read_fully(buf, 1, length)
        return PackedInteger.read_sorted_int(buf, 0)

    @staticmethod
    def read_packed_int_array(bis):
        """
        Reads a possibly None int array as a sequence length followed by the
        array contents.

        :param bis: the byte input stream.
        :type bis: ByteInputStream
        :returns: the array or None.
        :rtype: list
        """
        length = SerdeUtil.read_sequence_length(bis)
        if length < -1:
            raise IOError('Invalid length of byte array: ' + str(length))
        if length == -1:
            return None
        array = [0] * length
        for i in range(length):
            array[i] = SerdeUtil.read_packed_int(bis)
        return array

    @staticmethod
    def read_packed_long(bis):
        """
        Reads a packed long from the input and returns it.

        :param bis: the byte input stream.
        :type bis: ByteInputStream
        :returns: the long that was read.
        :rtype: int for python 3 and long for python 2
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
        :type bis: ByteInputStream
        :returns: the sequence length or -1 for None.
        :rtype: int
        """
        result = SerdeUtil.read_packed_int(bis)
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
        :type bis: ByteInputStream
        :returns: the string or None.
        :rtype: str or None
        """
        length = SerdeUtil.read_packed_int(bis)
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
        return buf.decode('utf-8')

    @staticmethod
    def read_string_array(bis):
        length = SerdeUtil.read_sequence_length(bis)
        if length < -1:
            raise IOError('Invalid length of byte array: ' + str(length))
        if length == -1:
            return None
        array = list()
        for i in range(length):
            array.append(SerdeUtil.read_string(bis))
        return array

    @staticmethod
    def read_float(bis):
        """
        Reads a float, which is a double-precision floating point
        """
        return bis.read_float()

    @staticmethod
    def trace(msg, level):
        if level <= SerdeUtil.TRACE_LEVEL:
            print('DRIVER: ' + msg)

    @staticmethod
    def write_bytearray(bos, value):
        """
        Writes a possibly None byte array as a sequence length followed by the
        array contents.

        :param bos: the byte output stream.
        :type bos: ByteOutputStream
        :param value: the bytearray or None.
        :type value: bytearray or None
        """
        length = -1 if value is None else len(value)
        SerdeUtil.write_sequence_length(bos, length)
        if length > 0:
            bos.write_bytearray(value)

    @staticmethod
    def write_bytearray_with_int(bos, value):
        # Writes a byte array with a full 4-byte int length.
        bos.write_int(len(value))
        bos.write_bytearray(value)

    @staticmethod
    def write_int_at_offset(bos, offset, value):
        # Writes a full 4-byte int at the specified offset
        bos.write_int_at_offset(offset, value)


    # Used by datetime_to_iso to deal with padding ISO 8601 values with '0'
    # in front of numbers to keep the number of digits consistent. E.g.
    # months is always 2 digits, years 4, days 2, etc
    @staticmethod
    def append_with_pad(str, newstr, num):
        while num > 0 and len(newstr) < num:
            newstr = '0' + newstr
        if str is not None:
            return str + newstr
        return newstr

    #
    # For consistency with ISO 8601 and other SDKs there is a custom writer
    # for datetime. Format is YYYY-MM-DD[THH:MM:SS.usecZ]
    #
    @staticmethod
    def datetime_to_iso(date):
        daysep = '-'
        timesep = ':'
        val = SerdeUtil.append_with_pad(None, str(date.year), 4)
        val = val + daysep
        val = SerdeUtil.append_with_pad(val, str(date.month), 2)
        val = val + daysep
        val = SerdeUtil.append_with_pad(val, str(date.day), 2)
        # always add time even if it's 0; simpler that way
        val = val + 'T'
        val = SerdeUtil.append_with_pad(val, str(date.hour), 2)
        val = val + timesep
        val = SerdeUtil.append_with_pad(val, str(date.minute), 2)
        val = val + timesep
        val = SerdeUtil.append_with_pad(val, str(date.second), 2)
        if date.microsecond > 0:
            # usecs need to be 6 digits, so pad to 6 chars but strip
            # trailing 0. The strip isn't strictly necessary but plays
            # better with Java. Anything after the "." is effectively
            # microseconds or milliseconds. E.g. these are the same:
            #  .1, .100, .100000 and they all mean 100ms (100000us)
            # In other words the trailing 0 values are implied
            val = val + '.'
            val = SerdeUtil.append_with_pad(val,
                                        str(date.microsecond), 6).rstrip('0')
        val = val + 'Z'
        return val

    @staticmethod
    def write_datetime(bos, value):
        # Serialize a datetime value.
        if value.tzinfo is not None:
            value = value.astimezone(tz.UTC)
        SerdeUtil.write_string(bos, SerdeUtil.datetime_to_iso(value))

    @staticmethod
    def iso_time_to_ms(iso_string):
        dt = parser.parse(iso_string)
        if dt.tzinfo is not None:
            dt = dt.astimezone(tz.UTC)
        return int(mktime(dt.timetuple()) * 1000) + dt.microsecond // 1000

    @staticmethod
    def write_decimal(bos, value):
        # Serialize a decimal value.
        SerdeUtil.write_string(bos, str(value))

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
    def write_packed_int(bos, value):
        """
        Writes a packed integer to the byte output stream.

        :param bos: the byte output stream.
        :type bos: ByteOutputStream
        :param value: the integer to be written.
        :type value: int
        :returns: the length of bytes written.
        :rtype: int
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
        :type bos: ByteOutputStream
        :param value: the long to be written.
        :type value: int for python 3 and long for python 2
        """
        buf = bytearray(PackedInteger.MAX_LONG_LENGTH)
        offset = PackedInteger.write_sorted_long(buf, 0, value)
        bos.write_bytearray(buf, 0, offset)

    @staticmethod
    def write_full_int(bos, value):
        # Writes a full, 4-byte int
        bos.write_int(value)

    @staticmethod
    def write_sequence_length(bos, length):
        """
        Writes a sequence length. The length is represented as a packed int,
        with -1 representing None. Although we don't enforce maximum sequence
        lengths yet, this entrypoint provides a place to do that.

        :param bos: the byte output stream.
        :type bos: ByteOutputStream
        :param length: the sequence length or -1.
        :type length: int
        :raises IllegalArgumentException: raises the exception if length is less
            than -1.
        """
        if length < -1:
            raise IllegalArgumentException(
                'Invalid sequence length: ' + str(length))
        SerdeUtil.write_packed_int(bos, length)

    @staticmethod
    def write_float(bos, value):
        """
        Writes a float, which is a double-precision floating point
        :param bos: the byte output stream.
        :type bos: ByteOutputStream
        :param value: the float to be written.
        :type value: float
        """
        bos.write_float(value)

    @staticmethod
    def write_serial_version(bos, serial_version):
        # Writes the (short) serial version
        bos.write_short_int(serial_version)

    @staticmethod
    def write_string(bos, value):
        """
        Writes a possibly None or empty string to an output stream using
        standard UTF-8 format.

        First writes a packed int representing the length of the UTF-8 encoding
        of the string, or -1 if the string is None, followed by the UTF-8
        encoding for non-empty strings.

        :param bos: the byte output stream.
        :type bos: ByteOutputStream
        :param value: the string or None.
        :type value: str or None
        :returns: the number of bytes that the string take.
        :rtype: int
        """
        if value is None:
            return SerdeUtil.write_packed_int(bos, -1)
        try:
            buf = bytearray(value.encode('utf-8'))
        except UnicodeDecodeError:
            buf = bytearray(value)
        length = len(buf)
        int_len = SerdeUtil.write_packed_int(bos, length)
        if length > 0:
            bos.write_bytearray(buf)
        return int_len + length

    @staticmethod
    def get_table_state(state):
        if state == SerdeUtil.TABLE_STATE.ACTIVE:
            return State.ACTIVE
        elif state == SerdeUtil.TABLE_STATE.CREATING:
            return State.CREATING
        elif state == SerdeUtil.TABLE_STATE.DROPPED:
            return State.DROPPED
        elif state == SerdeUtil.TABLE_STATE.DROPPING:
            return State.DROPPING
        elif state == SerdeUtil.TABLE_STATE.UPDATING:
            return State.UPDATING
        else:
            raise IllegalStateException('Unknown table state ' + str(state))

    @staticmethod
    def get_type(value):
        if isinstance(value, list):
            return SerdeUtil.FIELD_VALUE_TYPE.ARRAY
        elif isinstance(value, bytearray):
            return SerdeUtil.FIELD_VALUE_TYPE.BINARY
        elif isinstance(value, bool):
            return SerdeUtil.FIELD_VALUE_TYPE.BOOLEAN
        elif isinstance(value, float):
            return SerdeUtil.FIELD_VALUE_TYPE.DOUBLE
        elif CheckValue.is_int(value):
            return SerdeUtil.FIELD_VALUE_TYPE.INTEGER
        elif CheckValue.is_long(value):
            return SerdeUtil.FIELD_VALUE_TYPE.LONG
        elif isinstance(value, dict):
            return SerdeUtil.FIELD_VALUE_TYPE.MAP
        elif CheckValue.is_str(value):
            return SerdeUtil.FIELD_VALUE_TYPE.STRING
        elif isinstance(value, datetime):
            return SerdeUtil.FIELD_VALUE_TYPE.TIMESTAMP
        elif isinstance(value, Decimal) or CheckValue.is_overlong(value):
            return SerdeUtil.FIELD_VALUE_TYPE.NUMBER
        elif value is None:
            return SerdeUtil.FIELD_VALUE_TYPE.NULL
        elif isinstance(value, Empty):
            return SerdeUtil.FIELD_VALUE_TYPE.EMPTY
        else:
            raise IllegalStateException(
                'Unknown value type ' + str(type(value)))
