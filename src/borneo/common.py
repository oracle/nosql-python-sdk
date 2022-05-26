#
# Copyright (c) 2018, 2022 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#

from datetime import datetime
from decimal import Decimal
from functools import wraps
from logging import Logger
from struct import pack, unpack
from sys import version_info
from threading import Lock
from time import time
from warnings import simplefilter, warn

from dateutil import tz
from requests import adapters

from .exception import IllegalArgumentException


def enum(**enums):
    return type('Enum', (object,), enums)


def deprecated(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        simplefilter('default', DeprecationWarning)
        warn("Call to deprecated function {}.".format(func.__name__),
             DeprecationWarning)
        return func(*args, **kwargs)

    return wrapper


def synchronized(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        with self.lock:
            return func(self, *args, **kwargs)

    return wrapper


class ByteInputStream(object):
    """
    The ByteInputStream provides methods to get data with different type from
    a bytearray.
    """

    def __init__(self, content):
        self._content = content
        self._offset = 0

    def get_offset(self):
        return self._offset

    def read_boolean(self):
        res = bool(self.read_byte())
        return res

    def read_byte(self):
        res = self._content[self._offset]
        self._offset += 1
        if res > 127:
            return res - 256
        else:
            return res

    def read_float(self):
        buf = bytearray(8)
        self.read_fully(buf)
        res, = unpack('>d', buf)
        return res

    def read_fully(self, buf, start=0, end=None):
        if end is None:
            end = len(buf)
        for index in range(start, end):
            buf[index] = self._content[self._offset]
            self._offset += 1

    def read_int(self):
        buf = bytearray(4)
        self.read_fully(buf)
        res, = unpack('>i', buf)
        return res

    def read_long(self):
        buf = bytearray(8)
        self.read_fully(buf)
        res, = unpack('>q', buf)
        return res

    def read_short_int(self):
        buf = bytearray(2)
        self.read_fully(buf)
        res, = unpack('>h', buf)
        return res

    def set_offset(self, offset):
        self._offset = offset


class ByteOutputStream(object):
    """
    The ByteOutputStream provides methods to write data with different type into
    a bytearray.
    """

    def __init__(self, content):
        self._content = content

    def get_offset(self):
        return len(self._content)

    def write_boolean(self, value):
        val_s = pack('?', value)
        self.write_value(val_s)

    def write_byte(self, value):
        val_s = pack('B', value)
        self.write_value(val_s)

    def write_bytearray(self, value, start=0, end=None):
        if end is None:
            end = len(value)
        for index in range(start, end):
            self._content.append(value[index])

    def write_float(self, value):
        val_s = pack('>d', value)
        self.write_value(val_s)

    def write_int(self, value):
        val_s = pack('>i', value)
        self.write_value(val_s)

    def write_int_at_offset(self, offset, value):
        val_s = pack('>i', value)
        val_b = bytearray(val_s)
        for index in range(len(val_b)):
            self._content[offset + index] = val_b[index]

    def write_short_int(self, value):
        val_s = pack('>h', value)
        self.write_value(val_s)

    def write_value(self, value):
        val_b = bytearray(value)
        self.write_bytearray(val_b)


class CheckValue(object):

    @staticmethod
    def check_boolean(data, name):
        if data is not True and data is not False:
            raise IllegalArgumentException(name + ' must be True or False.')

    @staticmethod
    def check_dict(data, name):
        if data is not None and not isinstance(data, dict):
            raise IllegalArgumentException(name + ' must be a dict.')

    @staticmethod
    def check_float_gt_zero(data, name):
        if not CheckValue.is_digit(data) or data <= 0.0:
            raise IllegalArgumentException(
                name + ' must be a positive digital number. Got:' + str(data))

    @staticmethod
    def check_int(data, name):
        if not (CheckValue.is_int(data) or CheckValue.is_long(data)):
            raise IllegalArgumentException(
                name + ' must be an integer. Got:' + str(data))

    @staticmethod
    def check_int_ge_zero(data, name):
        if (not (CheckValue.is_int(data) or CheckValue.is_long(data)) or
                data < 0):
            raise IllegalArgumentException(
                name + ' must be an integer that is not negative. Got:' +
                str(data))

    @staticmethod
    def check_int_gt_zero(data, name):
        if (not (CheckValue.is_int(data) or CheckValue.is_long(data)) or
                data <= 0):
            raise IllegalArgumentException(
                name + ' must be an positive integer. Got:' + str(data))

    @staticmethod
    def check_list(data, name):
        if not isinstance(data, list):
            raise IllegalArgumentException(name + ' must be a list.')

    @staticmethod
    def check_logger(data, name):
        if data is not None and not isinstance(data, Logger):
            raise IllegalArgumentException(name + ' must be a Logger.')

    @staticmethod
    def check_not_none(data, name):
        if data is None:
            raise IllegalArgumentException(name + ' must be not-none.')

    @staticmethod
    def check_str(data, name, allow_none=False):
        if (not allow_none and data is None or data is not None and
                (not CheckValue.is_str(data) or len(data) == 0)):
            raise IllegalArgumentException(
                name + ' must be a string that is not empty.')

    @staticmethod
    def is_digit(data):
        if (CheckValue.is_int(data) or CheckValue.is_long(data) or
                isinstance(data, float) or isinstance(data, Decimal)):
            return True
        return False

    @staticmethod
    def is_int(data):
        if ((version_info.major == 2 and isinstance(data, int) or
             version_info.major == 3 and isinstance(data, int)) and
                -pow(2, 31) <= data < pow(2, 31)):
            return True
        return False

    @staticmethod
    def is_long(data):
        if ((version_info.major == 2 and isinstance(data, (int, long)) or
             version_info.major == 3 and isinstance(data, int)) and
                not CheckValue.is_int(data) and
                -pow(2, 63) <= data < pow(2, 63)):
            return True
        return False

    @staticmethod
    def is_overlong(data):
        if ((version_info.major == 2 and isinstance(data, long) or
             version_info.major == 3 and isinstance(data, int)) and
                (data < -pow(2, 63) or data >= pow(2, 63))):
            return True
        return False

    @staticmethod
    def is_str(data):
        if (version_info.major == 2 and isinstance(data, (str, unicode)) or
                version_info.major == 3 and isinstance(data, str)):
            return True
        return False


class Consistency(object):
    """
    Set the consistency for read requests.
    """
    ABSOLUTE = 0
    """
    Set Consistency.ABSOLUTE to use absolute consistency for read requests.
    """
    EVENTUAL = 1
    """
    Set Consistency.EVENTUAL to use eventual consistency for read requests.
    This is the default value for operations.
    """


class Empty(object):
    """
    Represents an EMPTY field value.
    """

    def __str__(self):
        return 'Empty'


class FieldRange(object):
    """
    FieldRange defines a range of values to be used in a
    :py:meth:`NoSQLHandle.multi_delete` operation, as specified in
    :py:meth:`MultiDeleteRequest.set_range`. FieldRange is only relevant
    if a primary key has multiple components because all values in the
    range must share the same shard key.

    FieldRange is used as the least significant component in a partially
    specified key value in order to create a value range for an operation that
    returns multiple rows or keys. The data types supported by FieldRange are
    limited to the atomic types which are valid for primary keys.

    The least significant component of a key is the first component of the key
    that is not fully specified. For example, if the primary key for a table is
    defined as the tuple (a, b, c), a FieldRange can be specified for "a" if the
    primary key supplied is empty. A FieldRange can be specified for "b" if the
    primary key supplied to the operation has a concrete value for "a" but not
    for "b" or "c".

    This object is used to scope a :py:meth:`NoSQLHandle.multi_delete`
    operation. The field_path specified must name a field in a table's primary
    key. The values used must be of the same type and that type must match the
    type of the field specified.

    Validation of this object is performed when is it used in an operation.
    Validation includes verifying that the field is in the required key and, in
    the case of a composite key, that the field is in the proper order relative
    to the key used in the operation.

    :param field_path: the path to the field used in the range.
    :type field_path: str
    :raises IllegalArgumentException: raises the exception if field_path is not
        a string.
    """

    def __init__(self, field_path):
        # Create a value based on a specific field.
        CheckValue.check_str(field_path, 'field_path')
        self._field_path = field_path
        self._start = None
        self._start_inclusive = False
        self._end = None
        self._end_inclusive = False

    def __str__(self):
        return ('{Path=' + self._field_path + ', Start=' + str(self._start) +
                ', End=' + str(self._end) + ', StartInclusive=' +
                str(self._start_inclusive) + ', EndInclusive=' +
                str(self._end_inclusive) + '}')

    def get_field_path(self):
        """
        Returns the name for the field used in the range.

        :returns: the name of the field.
        :rtype: str
        """
        return self._field_path

    def set_start(self, value, is_inclusive):
        """
        Sets the start value of the range to the specified value.

        :param value: the value to set.
        :type value: any
        :param is_inclusive: set to True if the range is inclusive of the value,
            False if it is exclusive.
        :type is_inclusive: bool
        :returns: self.
        :raises IllegalArgumentException: raises the exception if parameters are
            not expected type.
        """
        CheckValue.check_not_none(value, 'value')
        CheckValue.check_boolean(is_inclusive, 'is_inclusive')
        self._start = value
        self._start_inclusive = is_inclusive
        return self

    def get_start(self):
        """
        Returns the field value that defines lower bound of the range, or None
        if no lower bound is enforced.

        :returns: the start field value.
        """
        return self._start

    def get_start_inclusive(self):
        """
        Returns whether start is included in the range, i.e., start is less than
        or equal to the first field value in the range. This value is valid only
        if the start value is not None.

        :returns: True if the start value is inclusive.
        :rtype: bool
        """
        return self._start_inclusive

    def set_end(self, value, is_inclusive):
        """
        Sets the end value of the range to the specified value.

        :param value: the value to set.
        :type value: any
        :param is_inclusive: set to True if the range is inclusive of the value,
            False if it is exclusive.
        :type is_inclusive: bool
        :returns: self.
        :raises IllegalArgumentException: raises the exception if parameters are
            not expected type.
        """
        CheckValue.check_not_none(value, 'value')
        CheckValue.check_boolean(is_inclusive, 'is_inclusive')
        self._end = value
        self._end_inclusive = is_inclusive
        return self

    def get_end(self):
        """
        Returns the field value that defines upper bound of the range, or None
        if no upper bound is enforced.

        :returns: the end field value.
        """
        return self._end

    def get_end_inclusive(self):
        """
        Returns whether end is included in the range, i.e., end is greater than
        or equal to the last field value in the range. This value is valid only
        if the end value is not None.

        :returns: True if the end value is inclusive.
        :rtype: bool
        """
        return self._end_inclusive

    def validate(self):
        # Ensures that the object is self-consistent and if not, throws
        # IllegalArgumentException. Validation of the range values themselves is
        # done remotely.
        start_type = None if self._start is None else type(self._start)
        end_type = None if self._end is None else type(self._end)
        if start_type is None and end_type is None:
            raise IllegalArgumentException(
                'FieldRange: must specify a start or end value.')
        if (start_type is not None and end_type is not None and
                start_type is not end_type):
            raise IllegalArgumentException(
                'FieldRange: Mismatch of start and end types. Start type is ' +
                str(start_type) + ', end type is ' + str(end_type))


class HttpConstants(object):
    # The name of the Authorization header.
    AUTHORIZATION = 'Authorization'

    # The name of the date header.
    DATE = 'date'

    # The current version of the protocol
    NOSQL_VERSION = 'V2'

    # The name of the opc obo token header.
    OPC_OBO_TOKEN = 'opc-obo-token'

    # A header for transferring the compartment id on an http request.
    REQUEST_COMPARTMENT_ID = 'x-nosql-compartment-id'

    # The http header that identifies the client scoped unique request id
    # associated with each request. The request header is returned by the
    # server, as part of the response and serves to associate the response with
    # the request.
    #
    # Note: We could use stream ids to associate a request and response.
    # However, the current handler pipeline on the client side operates at the
    # http level rather than the frame level, and consequently does not have
    # access to the stream id.
    REQUEST_ID_HEADER = 'x-nosql-request-id'

    # The name of the (request-target) header.
    REQUEST_TARGET = '(request-target)'

    # Creates a URI path from the arguments
    def _make_path(*args):
        path = args[0]
        for index in range(1, len(args)):
            path += '/' + args[index]
        return path

    # The base path to the on-premise security services. All users need a
    # leading "/" so add it here.
    KV_SECURITY_PATH = _make_path('/' + NOSQL_VERSION, 'nosql/security')

    # The path denoting a NoSQL request
    NOSQL_DATA_PATH = _make_path(NOSQL_VERSION, 'nosql/data')


class IndexInfo(object):
    """
    IndexInfo represents the information about a single index including its name
    and field names. Instances of this class are returned in
    :py:class:`GetIndexesResult`.
    """

    def __init__(self, index_name, field_names):
        self._index_name = index_name
        self._field_names = field_names

    def __str__(self):
        return ('IndexInfo [indexName=' + self._index_name + ', fields=[' +
                ','.join(self._field_names) + ']]')

    def get_index_name(self):
        """
        Returns the name of the index.

        :returns: the index name.
        :rtype: str
        """
        return self._index_name

    def get_field_names(self):
        """
        Returns the list of field names that define the index.

        :returns: the field names.
        :rtype: list(str)
        """
        return self._field_names


class JsonNone(object):
    """
    Represents an JSON NONE field value.
    """

    def __str__(self):
        return 'JsonNone'


class LogUtils(object):

    # Utility methods to facilitate Logging.
    def __init__(self, logger=None):
        self._logger = logger

    def log_critical(self, msg):
        if self._logger is not None:
            self._logger.critical(msg)

    def log_error(self, msg):
        if self._logger is not None:
            self._logger.error(msg)

    def log_warning(self, msg):
        if self._logger is not None:
            self._logger.warning(msg)

    def log_info(self, msg):
        if self._logger is not None:
            self._logger.info(msg)

    def log_debug(self, msg):
        if self._logger is not None:
            self._logger.debug(msg)

    def is_enabled_for(self, level):
        return self._logger is not None and self._logger.isEnabledFor(level)

    def set_level(self, level):
        return self._logger is not None and self._logger.setLevel(level)

    def get_logger(self):
        return self._logger


class Memoize(object):

    # A cache that used for saving the access token.
    def __init__(self, duration=60):
        self._cache = {}
        self._duration = duration
        self.lock = Lock()

    @synchronized
    def set(self, key, value):
        self._cache[key] = {'value': value, 'time': time()}

    @synchronized
    def get(self, key):
        if key in self._cache and not self._is_obsolete(self._cache[key]):
            return self._cache[key]['value']
        return None

    def _is_obsolete(self, entry):
        return time() - entry['time'] > self._duration


class PackedInteger(object):
    # The maximum number of bytes needed to store an int value (5).
    MAX_LENGTH = 5

    # The maximum number of bytes needed to store a long value (9).
    MAX_LONG_LENGTH = 9

    @staticmethod
    def write_sorted_int(buf, offset, value):
        """
        Writes a packed sorted integer starting at the given buffer offset and
        returns the next offset to be written.

        :param buf: the buffer to write to.
        :type buf: bytearray
        :param offset: the offset in the buffer at which to start writing.
        :type offset: int
        :param value: the integer to be written.
        :type value: int
        :returns: the offset past the bytes written.
        :rtype: int

        Values in the inclusive range [-119,120] are stored in a single byte.
        For values outside that range, the first byte stores the number of
        additional bytes. The additional bytes store (value + 119 for negative
        and value - 121 for positive) as an unsigned big endian integer.
        """
        b1 = offset
        offset += 1
        if value < -119:
            """
            If the value < -119, then first adjust the value by adding 119. Then
            the adjusted value is stored as an unsigned big endian integer.
            """
            value += 119

            """
            Store the adjusted value as an unsigned big endian integer. For an
            negative integer, from left to right, the first significant byte is
            the byte which is not equal to 0xFF. Also please note that, because
            the adjusted value is stored in big endian integer, we extract the
            significant byte from left to right.

            In the left to right order, if the first byte of the adjusted value
            is a significant byte, it will be stored in the 2nd byte of the buf.
            Then we will look at the 2nd byte of the adjusted value to see if
            this byte is the significant byte, if yes, this byte will be stored
            in the 3rd byte of the buf, and the like.
            """
            if value | 0x00FFFFFF != 0xFFFFFFFF:
                buf[offset] = value >> 24 & 0xFF
                offset += 1
            if value | 0x0000FFFF != 0xFFFFFFFF:
                buf[offset] = value >> 16 & 0xFF
                offset += 1
            if value | 0x000000FF != 0xFFFFFFFF:
                buf[offset] = value >> 8 & 0xFF
                offset += 1
            buf[offset] = value & 0xFF
            offset += 1

            """
            value_len is the length of the value part stored in buf. Because
            the first byte of buf is used to stored the length, we need to
            subtract one.
            """
            value_len = offset - b1 - 1

            """
            The first byte stores the number of additional bytes. Here we store
            the result of 0x08 - value_len, rather than directly store
            value_len. The reason is to implement natural sort order for
            byte-by-byte comparison.
            """
            buf[b1] = (0x08 - value_len) & 0xFF
        elif value > 120:
            """
            If the value > 120, then first adjust the value by subtracting 121.
            Then the adjusted value is stored as an unsigned big endian integer.
            """
            value -= 121

            """
            Store the adjusted value as an unsigned big endian integer. For a
            positive integer, from left to right, the first significant byte is
            the byte which is not equal to 0x00.

            In the left to right order, if the first byte of the adjusted value
            is a significant byte, it will be stored in the 2nd byte of the buf.
            Then we will look at the 2nd byte of the adjusted value to see if
            this byte is the significant byte, if yes, this byte will be stored
            in the 3rd byte of the buf, and the like.
            """
            if value & 0xFF000000 != 0:
                buf[offset] = value >> 24 & 0xFF
                offset += 1
            if value & 0xFFFF0000 != 0:
                buf[offset] = value >> 16 & 0xFF
                offset += 1
            if value & 0xFFFFFF00 != 0:
                buf[offset] = value >> 8 & 0xFF
                offset += 1
            buf[offset] = value & 0xFF
            offset += 1

            """
            value_len is the length of the value part stored in buf. Because the
            first byte of buf is used to stored the length, we need to subtract
            one.
            """
            value_len = offset - b1 - 1

            """
            The first byte stores the number of additional bytes. Here we store
            the result of 0xF7 + value_len, rather than directly store
            value_len. The reason is to implement natural sort order for
            byte-by-byte comparison.
            """
            buf[b1] = (0xF7 + value_len) & 0xFF
        else:
            """
            If -119 <= value <= 120, only one byte is needed to store the value.
            The stored value is the original value plus 127.
            """
            buf[b1] = (value + 127) & 0xFF
        return offset

    @staticmethod
    def write_sorted_long(buf, offset, value):
        """
        Writes a packed sorted long integer starting at the given buffer offset
        and returns the next offset to be written.

        :param buf: the buffer to write to.
        :type buf: bytearray
        :param offset: the offset in the buffer at which to start writing.
        :type offset: int
        :param value: the long integer to be written.
        :type value: int for python 3 and long for python 2
        :returns: the offset past the bytes written.
        :rtype: int

        Values in the inclusive range [-119,120] are stored in a single byte.
        For values outside that range, the first byte stores the number of
        additional bytes. The additional bytes store (value + 119 for negative
        and value - 121 for positive) as an unsigned big endian integer.
        """
        b1 = offset
        offset += 1
        if value < -119:
            """
            If the value < -119, then first adjust the value by adding 119. Then
            the adjusted value is stored as an unsigned big endian integer.
            """
            value += 119

            """
            Store the adjusted value as an unsigned big endian integer. For an
            negative integer, from left to right, the first significant byte is
            the byte which is not equal to 0xFF. Also please note that, because
            the adjusted value is stored in big endian integer, we extract the
            significant byte from left to right.

            In the left to right order, if the first byte of the adjusted value
            is a significant byte, it will be stored in the 2nd byte of the buf.
            Then we will look at the 2nd byte of the adjusted value to see if
            this byte is the significant byte, if yes, this byte will be stored
            in the 3rd byte of the buf, and the like.
            """
            if value | 0x00FFFFFFFFFFFFFF != 0xFFFFFFFFFFFFFFFF:
                buf[offset] = value >> 56 & 0xFF
                offset += 1
            if value | 0x0000FFFFFFFFFFFF != 0xFFFFFFFFFFFFFFFF:
                buf[offset] = value >> 48 & 0xFF
                offset += 1
            if value | 0x000000FFFFFFFFFF != 0xFFFFFFFFFFFFFFFF:
                buf[offset] = value >> 40 & 0xFF
                offset += 1
            if value | 0x00000000FFFFFFFF != 0xFFFFFFFFFFFFFFFF:
                buf[offset] = value >> 32 & 0xFF
                offset += 1
            if value | 0x0000000000FFFFFF != 0xFFFFFFFFFFFFFFFF:
                buf[offset] = value >> 24 & 0xFF
                offset += 1
            if value | 0x000000000000FFFF != 0xFFFFFFFFFFFFFFFF:
                buf[offset] = value >> 16 & 0xFF
                offset += 1
            if value | 0x00000000000000FF != 0xFFFFFFFFFFFFFFFF:
                buf[offset] = value >> 8 & 0xFF
                offset += 1
            buf[offset] = value & 0xFF
            offset += 1

            """
            value_len is the length of the value part stored in buf. Because the
            first byte of buf is used to stored the length, so we need to minus
            one.
            """
            value_len = offset - b1 - 1

            """
            The first byte stores the number of additional bytes. Here we store
            the result of 0x08 - value_len, rather than directly store
            value_len. The reason is to implement nature sort order for
            byte-by-byte comparison.
            """
            buf[b1] = (0x08 - value_len) & 0xFF
        elif value > 120:
            """
            If the value > 120, then first adjust the value by subtracting 119.
            Then the adjusted value is stored as an unsigned big endian integer.
            """
            value -= 121

            """
            Store the adjusted value as an unsigned big endian integer. For a
            positive integer, from left to right, the first significant byte is
            the byte which is not equal to 0x00.

            In the left to right order, if the first byte of the adjusted value
            is a significant byte, it will be stored in the 2nd byte of the buf.
            Then we will look at the 2nd byte of the adjusted value to see if
            this byte is the significant byte, if yes, this byte will be stored
            in the 3rd byte of the buf, and the like.
            """
            if value & 0xFF00000000000000 != 0:
                buf[offset] = value >> 56 & 0xFF
                offset += 1
            if value & 0xFFFF000000000000 != 0:
                buf[offset] = value >> 48 & 0xFF
                offset += 1
            if value & 0xFFFFFF0000000000 != 0:
                buf[offset] = value >> 40 & 0xFF
                offset += 1
            if value & 0xFFFFFFFF00000000 != 0:
                buf[offset] = value >> 32 & 0xFF
                offset += 1
            if value & 0xFFFFFFFFFF000000 != 0:
                buf[offset] = value >> 24 & 0xFF
                offset += 1
            if value & 0xFFFFFFFFFFFF0000 != 0:
                buf[offset] = value >> 16 & 0xFF
                offset += 1
            if value & 0xFFFFFFFFFFFFFF00 != 0:
                buf[offset] = value >> 8 & 0xFF
                offset += 1
            buf[offset] = value & 0xFF
            offset += 1

            """
            value_en is the length of the value part stored in buf. Because the
            first byte of buf is used to stored the length, so we need to minus
            one.
            """
            value_len = offset - b1 - 1

            """
            The first byte stores the number of additional bytes. Here we store
            the result of 0xF7 + value_len, rather than directly store
            value_len. The reason is to implement nature sort order for
            byte-by-byte comparison.
            """
            buf[b1] = (0xF7 + value_len) & 0xFF
        else:
            """
            If -119 <= value <= 120, only one byte is needed to store the value.
            The stored value is the original value adds 127.
            """
            buf[b1] = (value + 127) & 0xFF
        return offset

    @staticmethod
    def get_read_sorted_int_length(buf, offset):
        """
        Returns the number of bytes that would be read by
        :py:meth:`read_sorted_int`.

        Because the length is stored in the first byte, this method may be
        called with only the first byte of the packed integer in the given
        buffer. This method only accesses one byte at the given offset.

        :param buf: the buffer to read from.
        :type buf: bytearray
        :param offset: the offset in the buffer at which to start reading.
        :type offset: int
        :returns: the number of bytes that would be read.
        :rtype: int
        """
        # The first byte of the buf stores the length of the value part.
        b1 = buf[offset] & 0xff
        if b1 < 0x08:
            return 1 + 0x08 - b1
        if b1 > 0xf7:
            return 1 + b1 - 0xf7
        return 1

    @staticmethod
    def get_read_sorted_long_length(buf, offset):
        """
        Returns the number of bytes that would be read by
        :py:meth:`read_sorted_long`.

        Because the length is stored in the first byte, this method may be
        called with only the first byte of the packed integer in the given
        buffer. This method only accesses one byte at the given offset.

        :param buf: the buffer to read from.
        :type buf: bytearray
        :param offset: the offset in the buffer at which to start reading.
        :type offset: int
        :returns: the number of bytes that would be read.
        :rtype: int
        """
        # The length is stored in the same way for int and long.
        return PackedInteger.get_read_sorted_int_length(buf, offset)

    @staticmethod
    def read_sorted_int(buf, offset):
        """
        Reads a sorted packed integer at the given buffer offset and returns it.

        :param buf: the buffer to read from.
        :type buf: bytearray
        :param offset: the offset in the buffer at which to start reading.
        :type offset: int
        :returns: the integer that was read.
        :rtype: int
        """
        # The first byte of the buf stores the length of the value part.
        b1 = buf[offset] & 0xff
        offset += 1
        # Adjust the byte_len to the real length of the value part.
        if b1 < 0x08:
            byte_len = 0x08 - b1
            negative = True
        elif b1 > 0xf7:
            byte_len = b1 - 0xf7
            negative = False
        else:
            return b1 - 127

        """
        The following bytes on the buf store the value as a big endian integer.
        We extract the significant bytes from the buf and put them into the
        value in big endian order.
        """
        if negative:
            value = -1
        else:
            value = 0
        if byte_len > 3:
            value = (value << 8) | (buf[offset] & 0xFF)
            offset += 1
        if byte_len > 2:
            value = (value << 8) | (buf[offset] & 0xFF)
            offset += 1
        if byte_len > 1:
            value = (value << 8) | (buf[offset] & 0xFF)
            offset += 1
        value = (value << 8) | (buf[offset] & 0xFF)
        offset += 1

        """
        After get the adjusted value, we have to adjust it back to the original
        value.
        """
        if negative:
            value -= 119
        else:
            value += 121
        return value

    @staticmethod
    def read_sorted_long(buf, offset):
        """
        Reads a sorted packed long integer at the given buffer offset and
        returns it.

        :param buf: the buffer to read from.
        :type buf: bytearray
        :param offset: the offset in the buffer at which to start reading.
        :type offset: int
        :returns: the long integer that was read.
        :rtype: int for python 3 and long for python 2
        """
        # The first byte of the buf stores the length of the value part.
        b1 = buf[offset] & 0xff
        offset += 1
        # Adjust the byte_len to the real length of the value part.
        if b1 < 0x08:
            byte_len = 0x08 - b1
            negative = True
        elif b1 > 0xf7:
            byte_len = b1 - 0xf7
            negative = False
        else:
            try:
                return long(b1 - 127)
            except NameError:
                return b1 - 127

        """
        The following bytes on the buf store the value as a big endian integer.
        We extract the significant bytes from the buf and put them into the
        value in big endian order.
        """
        if negative:
            value = -1
        else:
            value = 0
        if byte_len > 7:
            value = (value << 8) | (buf[offset] & 0xFF)
            offset += 1
        if byte_len > 6:
            value = (value << 8) | (buf[offset] & 0xFF)
            offset += 1
        if byte_len > 5:
            value = (value << 8) | (buf[offset] & 0xFF)
            offset += 1
        if byte_len > 4:
            value = (value << 8) | (buf[offset] & 0xFF)
            offset += 1
        if byte_len > 3:
            value = (value << 8) | (buf[offset] & 0xFF)
            offset += 1
        if byte_len > 2:
            value = (value << 8) | (buf[offset] & 0xFF)
            offset += 1
        if byte_len > 1:
            value = (value << 8) | (buf[offset] & 0xFF)
            offset += 1
        value = (value << 8) | (buf[offset] & 0xFF)
        offset += 1
        """
        After obtaining the adjusted value, we have to adjust it back to the
        original value.
        """
        if negative:
            value -= 119
        else:
            value += 121
        try:
            return long(value)
        except NameError:
            return value


class PreparedStatement(object):
    """
    A class encapsulating a prepared query statement. It includes state that can
    be sent to a server and executed without re-parsing the query. It includes
    bind variables which may be set for each successive use of the query. The
    prepared query itself is read-only but this object contains a dictionary of
    bind variables and is not thread-safe if variables are used.

    PreparedStatement instances are returned inside :py:class:`PrepareResult`
    objects returned by :py:meth:`NoSQLHandle.prepare`

    A single instance of PreparedStatement is thread-safe if bind variables are
    not used. If bind variables are to be used and the statement shared among
    threads additional instances of PreparedStatement can be constructed using
    :py:meth:`copy_statement`.
    """
    OPCODE_SELECT = 5

    def __init__(self, sql_text, query_plan, topology_info, proxy_statement,
                 driver_plan, num_iterators, num_registers, external_vars,
                 namespace, table_name, operation):
        """
        Constructs a PreparedStatement. Construction is hidden to eliminate
        application access to the underlying statement, reducing the chance of
        corruption.
        """
        # 10 is arbitrary. TODO: put magic number in it for validation?
        if proxy_statement is None or len(proxy_statement) < 10:
            raise IllegalArgumentException(
                'Invalid prepared query, cannot be None.')

        self._sql_text = sql_text
        self._query_plan = query_plan
        # Applicable to advanced queries only.
        self._topology_info = topology_info
        # The serialized PreparedStatement created at the backend store. It is
        # opaque for the driver. It is received from the proxy and sent back to
        # the proxy every time a new batch of results is needed.
        self._proxy_statement = proxy_statement
        # The part of the query plan that must be executed at the driver. It is
        # received from the proxy when the query is prepared there. It is
        # deserialized by the driver and not sent back to the proxy again.
        # Applicable to advanced queries only.
        self._driver_query_plan = driver_plan
        # The number of iterators in the full query plan
        # Applicable to advanced queries only.
        self._num_iterators = num_iterators
        # The number of registers required to run the full query plan.
        # Applicable to advanced queries only.
        self._num_registers = num_registers
        # Maps the name of each external variable to its id, which is a position
        # in a field value array stored in the RuntimeControlBlock and holding
        # the values of the variables. Applicable to advanced queries only.
        self._variables = external_vars
        # The values for the external variables of the query. This dict is
        # populated by the application. It is sent to the proxy every time a new
        # batch of results is needed. The values in this dict are also placed in
        # the RuntimeControlBlock field value array, just before the query
        # starts its execution at the driver.
        self._bound_variables = None
        self._namespace = namespace
        self._table_name = table_name
        self._operation = operation
        self.lock = Lock()

    def clear_variables(self):
        """
        Clears all bind variables from the statement.
        """
        if self._bound_variables is not None:
            self._bound_variables.clear()

    def copy_statement(self):
        """
        Returns a new instance that shares this object's prepared query, which
        is immutable, but does not share its variables.

        :returns: a new PreparedStatement using this instance's prepared query.
            Bind variables are uninitialized.
        :rtype: PreparedStatement
        """
        return PreparedStatement(
            self._sql_text, self._query_plan, self._topology_info,
            self._proxy_statement, self._driver_query_plan, self._num_iterators,
            self._num_registers, self._variables, self._namespace,
            self._table_name, self._operation)

    def does_writes(self):
        # if it's not SELECT, it does writes.
        return self._operation != PreparedStatement.OPCODE_SELECT

    def driver_plan(self):
        return self._driver_query_plan

    def get_namespace(self):
        # Return namespace from prepared statement, if any.
        return self._namespace

    def get_query_plan(self):
        """
        Returns a string representation of the query execution plan, if it was
        requested in the :py:class:`PrepareRequest`; None otherwise.

        :returns: the string representation of the query execution plan.
        :rtype: bool
        """
        return self._query_plan

    def get_sql_text(self):
        """
        Returns the SQL text of this PreparedStatement.

        :returns: the SQL text of this PreparedStatement.
        :rtype: str
        """
        return self._sql_text

    def get_statement(self):
        # internal use to return the serialized, prepared query, opaque
        return self._proxy_statement

    def get_table_name(self):
        # Return table name from prepared statement, if any.
        return self._table_name

    def get_variables(self):
        """
        Returns the dictionary of variables to use for a prepared query
        with variables.

        :returns: the dictionary.
        :rtype: dict
        """
        return self._bound_variables

    def get_variable_values(self):
        if self._bound_variables is None:
            return None
        values = [0] * len(self._bound_variables)
        for key in self._bound_variables:
            varid = self._variables.get(key)
            values[varid] = self._bound_variables[key]
        return values

    def is_simple_query(self):
        return self._driver_query_plan is None

    def num_iterators(self):
        return self._num_iterators

    def num_registers(self):
        return self._num_registers

    def print_driver_plan(self):
        if self._driver_query_plan is not None:
            return self._driver_query_plan.display()
        return None

    @synchronized
    def set_topology_info(self, topology_info):
        if topology_info is None:
            return
        if self._topology_info is None:
            self._topology_info = topology_info
            return
        if self._topology_info.get_seq_num() < topology_info.get_seq_num():
            self._topology_info = topology_info

    def set_variable(self, variable, value):
        """
        Binds an external variable to a given value. The variable is identified
        by its name or its position within the query string. The variable that
        appears first in the query text has position 1, the variable that
        appears second has position 2 and so on.

        :param variable: the name or the position of the variable.
        :type variable: str or int
        :param value: the value.
        :type value: a value matching the type of the field
        :returns: self.
        :raises IllegalArgumentException: raises the exception if variable is
            not a string or positive integer.
        """
        if (not (CheckValue.is_str(variable) or CheckValue.is_int(variable)) or
                CheckValue.is_int(variable) and variable <= 0):
            raise IllegalArgumentException(
                'variable must be a string or positive integer.')
        if isinstance(variable, str):
            if self._bound_variables is None:
                self._bound_variables = dict()
            if (self._variables is not None and
                    self._variables.get(variable) is None):
                raise IllegalArgumentException(
                    'The query does not contain the variable: ' + variable)
            self._bound_variables[variable] = value
            return self
        else:
            if self._variables is None:
                name = '#' + str(variable)
                return self.set_variable(name, value)
            search_id = variable - 1
            for (k, v) in self._variables.items():
                if v == search_id:
                    return self.set_variable(k, value)
            raise IllegalArgumentException(
                'There is no external variable at position ' + str(variable))

    def topology_info(self):
        return self._topology_info

    @synchronized
    def topology_seq_num(self):
        return (-1 if self._topology_info is None else
                self._topology_info.get_seq_num())


class PutOption(object):
    """
    Set the put option for put requests.
    """
    IF_ABSENT = 0
    """Set PutOption.IF_ABSENT to perform put if absent operation."""
    IF_PRESENT = 1
    """Set PutOption.IF_PRESENT to perform put if present operation."""
    IF_VERSION = 2
    """Set PutOption.IF_VERSION to perform put if version operation."""


class ResourcePrincipalClaimKeys(object):
    """
    Claim keys in the resource principal session token(RPST).

    They can be used to retrieve resource principal metadata such as its
    compartment and tenancy OCID.

    """
    COMPARTMENT_ID_CLAIM_KEY = 'res_compartment'
    """
    The claim name that the RPST holds for the resource compartment. This can be
    passed to
    :py:meth:`borneo.iam.SignatureProvider.get_resource_principal_claim` to
    retrieve the resource's compartment OCID.
    """
    TENANT_ID_CLAIM_KEY = 'res_tenant'
    """
    The claim name that the RPST holds for the resource tenancy. This can be
    passed to
    :py:meth:`borneo.iam.SignatureProvider.get_resource_principal_claim` to
    retrieve the resource's tenancy OCID.
    """


class SSLAdapter(adapters.HTTPAdapter):
    """
    Internal use only.

    An HTTPS Transport Adapter that uses an arbitrary SSLContext.
    """

    def __init__(self, ssl_context=None, **kwargs):
        self._ssl_context = ssl_context
        super(SSLAdapter, self).__init__(**kwargs)

    def init_poolmanager(self, *args, **kwargs):
        kwargs['ssl_context'] = self._ssl_context
        return super(SSLAdapter, self).init_poolmanager(*args, **kwargs)

    def proxy_manager_for(self, *args, **kwargs):
        kwargs['ssl_context'] = self._ssl_context
        return super(SSLAdapter, self).proxy_manager_for(*args, **kwargs)


class State(object):
    """
    Represents the table state.
    """
    ACTIVE = 'ACTIVE'
    """Represents the table is active."""
    CREATING = 'CREATING'
    """Represents the table is creating."""
    DROPPED = 'DROPPED'
    """Represents the table is dropped."""
    DROPPING = 'DROPPING'
    """Represents the table is dropping."""
    UPDATING = 'UPDATING'
    """Represents the table is updating."""


class SystemState(object):
    """
    On-premise only.

    The current state of the system request.
    """
    COMPLETE = 'COMPLETE'
    """
    The operation is complete and was successful. Failures are thrown as
    exceptions.
    """
    WORKING = 'WORKING'
    """The operation is in progress."""


class Durability(object):
    """
    Durability defines the durability characteristics associated with a standalone write
    (put or update) operation.

    This is currently only supported in On-Prem installations. It is ignored
    in the cloud service.

    The overall durability is a function of the SYNC_POLICY and
    ACK_POLICY in effect for the Master, and the SYNC_POLICY in
    effect for each Replica.

    SYNC_POLICY represents policies to be used when committing a
    transaction. High levels of synchronization offer a greater guarantee
    that the transaction is persistent to disk, but trade that off for
    lower performance. The possible SYNC_POLICY values are:

    * SYNC writes and synchronously flushes the log on transaction commit.
      Transactions exhibit all the ACID (atomicity, consistency,
      isolation, and durability) properties.

    * NO_SYNC does not write or synchronously flush the log on transaction
      commit. Transactions exhibit the ACI (atomicity, consistency, and
      isolation) properties, but not D (durability); that is, database
      integrity will be maintained, but if the application or system fails,
      it is possible some number of the most recently committed transactions
      may be undone during recovery. The number of transactions at risk is
      governed by how many log updates can fit into the log buffer, how
      often the operating system flushes dirty buffers to disk, and how
      often log checkpoints occur.

    * WRITE_NO_SYNC writes but does not synchronously flush the log on
      transaction commit. Transactions exhibit the ACI (atomicity, consistency,
      and isolation) properties, but not D (durability); that is, database
      integrity will be maintained, but if the operating system fails, it is
      possible some number of the most recently committed transactions may be
      undone during recovery. The number of transactions at risk is
      governed by how often the operating system flushes dirty buffers to
      disk, and how often log checkpoints occur.

    REPLICA_ACK_POLICY defines the policy for how replicated commits are handled.
    A replicated environment makes it possible to increase an application's
    transaction commit guarantees by committing changes to its replicas on
    the network.

    Possible REPLICA_ACK_POLICY values include:

    * ALL defines that all replicas must acknowledge that they
      have committed the transaction. This policy should be selected only if
      your replication group has a small number of replicas, and those
      replicas are on extremely reliable networks and servers.

    * NONE defines that no transaction commit acknowledgments
      are required and the master will never wait for replica acknowledgments.
      In this case, transaction durability is determined entirely by the type
      of commit that is being performed on the master.

    * SIMPLE_MAJORITY defines that a simple majority of replicas
      must acknowledge that they have committed the transaction. This
      acknowledgment policy, in conjunction with an election policy which
      requires at least a simple majority, ensures that the changes made by
      the transaction remains durable if a new election is held.

    The default Durability is configured in the proxy server with which this
    SDK communicates. It is an optional startup parameter.
    """

    SYNC_POLICY = enum(SYNC=1,
                       NO_SYNC=2,
                       WRITE_NO_SYNC=3)
    """
    SYNC_POLICY
    """

    REPLICA_ACK_POLICY = enum(ALL=1,
                              NONE=2,
                              SIMPLE_MAJORITY=2)
    """
    REPLICA_ACK_POLICY
    """

    def __init__(self, master_sync, replica_sync, replica_ack):
        """
        Create a Durability object

        :param master_sync: the master sync policy
        :type master_sync: SYNC_POLICY
        :param replica_sync: the replica sync policy
        :type replica_sync: SYNC_POLICY
        :param replica_ack: the replica ack policy
        :type replica_ack: REPLICA_ACK_POLICY
        """
        self.master_sync = master_sync
        self.replica_sync = replica_sync
        self.replica_ack = replica_ack

    # noinspection PyUnresolvedReferences
    def validate(self):
        # an all-zero Durability specifies "use the durability settings on the server."
        if self.master_sync == 0 and self.replica_sync == 0 and self.replica_ack == 0:
            return
        if (self.master_sync != Durability.SYNC_POLICY.SYNC and
                self.master_sync != Durability.SYNC_POLICY.NO_SYNC and
                self.master_sync != Durability.SYNC_POLICY.WRITE_NO_SYNC):
            raise IllegalArgumentException(
                'Invalid value for Durability master_sync.')
        if (self.replica_sync != Durability.SYNC_POLICY.SYNC and
                self.replica_sync != Durability.SYNC_POLICY.NO_SYNC and
                self.replica_sync != Durability.SYNC_POLICY.WRITE_NO_SYNC):
            raise IllegalArgumentException(
                'Invalid value for Durability replica_sync.')
        if (self.replica_ack != Durability.REPLICA_ACK_POLICY.ALL and
                self.replica_ack != Durability.REPLICA_ACK_POLICY.NONE and
                self.replica_ack != Durability.REPLICA_ACK_POLICY.SIMPLE_MAJORITY):
            raise IllegalArgumentException(
                'Invalid value for Durability replica_ack.')


class TableLimits(object):
    """
    Cloud service only.

    A TableLimits instance is used during table creation to specify the
    throughput and capacity to be consumed by the table. It is also used in an
    operation to change the limits of an existing table.
    :py:meth:`NoSQLHandle.table_request` and :py:class:`TableRequest` are used
    to perform these operations. These values are enforced by the system and
    used for billing purposes.

    Throughput limits are defined in terms of read units and write units. A read
    unit represents 1 eventually consistent read per second for data up to 1 KB
    in size. A read that is absolutely consistent is double that, consuming 2
    read units for a read of up to 1 KB in size. This means that if an
    application is to use Consistency.ABSOLUTE it may need to specify additional
    read units when creating a table. A write unit represents 1 write per second
    of data up to 1 KB in size.

    In addition to throughput table capacity must be specified to indicate the
    maximum amount of storage, in gigabytes, allowed for the table.

    In provisioned mode, all 3 values must be used whenever using this object.
    There are no defaults and no mechanism to indicate "no change."

    In on demand mode, only the storage_gb parameter must be set.

    :param read_units: the desired throughput of read operation in terms of read
        units. A read unit represents 1 eventually consistent read per second
        for data up to 1 KB in size. A read that is absolutely consistent is
        double that, consuming 2 read units for a read of up to 1 KB in size.
    :type read_units: int
    :param write_units: the desired throughput of write operation in terms of
        write units. A write unit represents 1 write per second of data up to 1
        KB in size.
    :type write_units: int
    :param storage_gb: the maximum storage to be consumed by the table, in
        gigabytes.
    :type storage_gb: int
    :param mode: the mode of the table: provisioned (the default) or on demand.
    :type mode: CAPACITY_MODE
    :raises IllegalArgumentException: raises the exception if parameters are not
        valid.
    :versionchanged: 5.3.0, added optional CAPACITY_MODE
    """

    """
    TableLimits includes an optional mode

    :versionadded: 5.3.0
    """
    CAPACITY_MODE = enum(PROVISIONED=1,
                         ON_DEMAND=2)

    # noinspection PyUnresolvedReferences
    def __init__(self, read_units, write_units, storage_gb,
                 mode=CAPACITY_MODE.PROVISIONED):
        """
        Creates a TableLimits object

        :param read_units: the desired throughput of read operation in terms
         of read units. A read unit represents 1 eventually consistent read
         per second for data up to 1 KB in size. A read that is absolutely
         consistent is double that, consuming 2 read units for a read of up to
         1 KB in size.
        :type read_units: int
        :param write_units: the desired throughput of write operation in terms
         of write units. A write unit represents 1 write per second of data up
         to 1 KB in size.
        :type write_units: int
        :param storage_gb: the maximum storage to be consumed by the table, in
         gigabytes.
        :type storage_gb: int
        :param mode: the mode of the table: provisioned (the default) or on
         demand.
        :type mode: CAPACITY_MODE
        :raises IllegalArgumentException: raises the exception if parameters
         are not valid.
        :versionchanged: 5.3.0, added optional CAPACITY_MODE
       """

        # Constructs a TableLimits instance.
        self._mode = None
        CheckValue.check_int(read_units, 'read_units')
        CheckValue.check_int(write_units, 'write_units')
        CheckValue.check_int(storage_gb, 'storage_gb')
        self._read_units = read_units
        self._write_units = write_units
        self._storage_gb = storage_gb
        self.set_mode(mode)

    def __str__(self):
        return ('[' + str(self._read_units) + ', ' + str(self._write_units) +
                ', ' + str(self._storage_gb) + ']')

    def set_read_units(self, read_units):
        """
        Sets the read throughput in terms of read units.

        :param read_units: the throughput to use, in read units.
        :type read_units: int
        :returns: self.
        :raises IllegalArgumentException: raises the exception if read_units is
            not a integer.
        """
        CheckValue.check_int(read_units, 'read_units')
        self._read_units = read_units
        return self

    def get_read_units(self):
        """
        Returns the read throughput in terms of read units.

        :returns: the read units.
        :rtype: int
        """
        return self._read_units

    def set_write_units(self, write_units):
        """
        Sets the write throughput in terms of write units.

        :param write_units: the throughput to use, in write units.
        :type write_units: int
        :returns: self.
        :raises IllegalArgumentException: raises the exception if write_units is
            not a integer.
        """
        CheckValue.check_int(write_units, 'write_units')
        self._write_units = write_units
        return self

    def get_write_units(self):
        """
        Returns the write throughput in terms of write units.

        :returns: the write units.
        :rtype: int
        """
        return self._write_units

    def set_storage_gb(self, storage_gb):
        """
        Sets the storage capacity in gigabytes.

        :param storage_gb: the capacity to use, in gigabytes.
        :type storage_gb: int
        :returns: self.
        :raises IllegalArgumentException: raises the exception if storage_gb is
            not a integer.
        """
        CheckValue.check_int(storage_gb, 'storage_gb')
        self._storage_gb = storage_gb
        return self

    def get_storage_gb(self):
        """
        Returns the storage capacity in gigabytes.

        :returns: the storage capacity in gigabytes.
        :rtype: int
        """
        return self._storage_gb

    # noinspection PyUnresolvedReferences
    def set_mode(self, mode):
        """
        Sets the mode of the table:
            PROVISIONED: Fixed maximum read/write units. This is the default.
            ON_DEMAND: Flexible read/write limits.

        :param mode: the capacity to use, in gigabytes.
        :type mode: TableLimits.CAPACITY_MODE
        :returns: self.
        :raises IllegalArgumentException: raises the exception if mode is
            invalid.
        :versionadded: 5.3.0
        """
        if (mode != TableLimits.CAPACITY_MODE.PROVISIONED and
                mode != TableLimits.CAPACITY_MODE.ON_DEMAND):
            raise IllegalArgumentException(
                'TableLimits mode must be one of PROVISIONED or ON_DEMAND')
        self._mode = mode

    def get_mode(self):
        """
        Returns the capacity mode of the table.

        :returns: mode: PROVISIONED or ON_DEMAND
        :versionadded: 5.3.0
        """
        return self._mode

    # noinspection PyUnresolvedReferences
    def validate(self):
        if self._storage_gb <= 0:
            raise IllegalArgumentException(
                'TableLimits values must be non-negative.')
        if (self._mode != TableLimits.CAPACITY_MODE.ON_DEMAND and
                (self._read_units <= 0 or self._write_units <= 0)):
            raise IllegalArgumentException(
                'TableLimits values must be non-negative.')


class TableUsage(object):
    """
    TableUsage represents a single usage record, or slice, that includes
    information about read and write throughput consumed during that period as
    well as the current information regarding storage capacity. In addition the
    count of throttling exceptions for the period is reported.
    """

    def __init__(self, start_time_ms, seconds_in_period, read_units,
                 write_units, storage_gb, read_throttle_count,
                 write_throttle_count, storage_throttle_count):
        # Internal use only.
        self._start_time_ms = start_time_ms
        self._seconds_in_period = seconds_in_period
        self._read_units = read_units
        self._write_units = write_units
        self._storage_gb = storage_gb
        self._read_throttle_count = read_throttle_count
        self._write_throttle_count = write_throttle_count
        self._storage_throttle_count = storage_throttle_count

    def __str__(self):
        return ('TableUsage [start_time_ms=' + str(self._start_time_ms) +
                ', seconds_in_period=' + str(self._seconds_in_period) +
                ', read_units=' + str(self._read_units) + ', write_units=' +
                str(self._write_units) + ', storage_gb=' +
                str(self._storage_gb) + ', read_throttle_count=' +
                str(self._read_throttle_count) + ', write_throttle_count=' +
                str(self._write_throttle_count) + ', storage_throttle_count=' +
                str(self._storage_throttle_count) + ']')

    def get_start_time(self):
        """
        Returns the start time for this usage record in milliseconds since
        the Epoch.

        :returns: the start time.
        :rtype: int
        """
        return self._start_time_ms

    def get_start_time_string(self):
        """
        Returns the start time as an ISO 8601 formatted string. If the start
        timestamp is not set, None is returned.

        :returns: the start time, or None if not set.
        :rtype: str or None
        """
        if self._start_time_ms == 0:
            return None
        return datetime.fromtimestamp(
            float(self._start_time_ms) / 1000).replace(
            tzinfo=tz.UTC).isoformat()

    def get_seconds_in_period(self):
        """
        Returns the number of seconds in this usage record.

        :returns: the number of seconds.
        :rtype: int
        """
        return self._seconds_in_period

    def get_read_units(self):
        """
        Returns the number of read units consumed during this period.

        :returns: the read units.
        :rtype: int
        """
        return self._read_units

    def get_write_units(self):
        """
        Returns the number of write units consumed during this period.

        :returns: the write units.
        :rtype: int
        """
        return self._write_units

    def get_storage_gb(self):
        """
        Returns the amount of storage consumed by the table. This
        information may be out of date as it is not maintained in real time.

        :returns: the size in gigabytes.
        :rtype: int
        """
        return self._storage_gb

    def get_read_throttle_count(self):
        """
        Returns the number of read throttling exceptions on this table in
        the time period.

        :returns: the number of throttling exceptions.
        :rtype: int
        """
        return self._read_throttle_count

    def get_write_throttle_count(self):
        """
        Returns the number of write throttling exceptions on this table in
        the time period.

        :returns: the number of throttling exceptions.
        :rtype: int
        """
        return self._write_throttle_count

    def get_storage_throttle_count(self):
        """
        Returns the number of storage throttling exceptions on this table in
        the time period.

        :returns: the number of throttling exceptions.
        :rtype: int
        """
        return self._storage_throttle_count


class TimeUnit(object):
    """
    The time unit to use.
    """
    HOURS = 1
    """Set TimeUnit.HOURS to use hour as time unit"""
    DAYS = 2
    """Set TimeUnit.DAYS to use day as time unit"""


class TimeToLive(object):
    """
    TimeToLive is a utility class that represents a period of time, similar to
    java.time.Duration in Java, but specialized to the needs of this driver.

    This class is restricted to durations of days and hours. It is only used as
    input related to time to live (TTL) for row instances.

    Construction allows only day and hour durations for efficiency reasons.
    Durations of days are recommended as they result in the least amount of
    storage overhead. Only positive durations are allowed on input.

    :param value: value of time.
    :type value: int
    :param timeunit: unit of time, cannot be None.
    :type timeunit: TimeUnit
    :raises IllegalArgumentException: raises the exception if parameters are not
        expected type.
    """

    def __init__(self, value, timeunit):
        """
        All construction is done via this constructor, which validates the
        arguments.
        """
        CheckValue.check_int(value, 'value')
        if (timeunit is None or timeunit != TimeUnit.DAYS and
                timeunit != TimeUnit.HOURS):
            raise IllegalArgumentException(
                'Invalid time unit in TimeToLive construction. Must be ' +
                'not-none and should be DAYS or HOURS.')
        self._value = value
        self._timeunit = timeunit

    def __str__(self):
        timeunit = 'HOURS' if self._timeunit == TimeUnit.HOURS else 'DAYS'
        return str(self._value) + ' ' + timeunit

    @staticmethod
    def of_hours(hours):
        """
        Creates a duration using a period of hours.

        :param hours: the number of hours in the duration, must be a
            non-negative number.
        :type hours: int
        :returns: the duration.
        :rtype: TimeToLive
        :raises IllegalArgumentException: raises the exception if a negative
            value is provided.
        """
        CheckValue.check_int_ge_zero(hours, 'hours')
        return TimeToLive(hours, TimeUnit.HOURS)

    @staticmethod
    def of_days(days):
        """
        Creates a duration using a period of 24 hour days.

        :param days: the number of days in the duration, must be a non-negative
            number.
        :type days: int
        :returns: the duration.
        :rtype: TimeToLive
        :raises IllegalArgumentException: raises the exception if a negative
            value is provided.
        """
        CheckValue.check_int_ge_zero(days, 'days')
        return TimeToLive(days, TimeUnit.DAYS)

    def to_days(self):
        """
        Returns the number of days in this duration, which may be negative.

        :returns: the number of days.
        :rtype: int
        """
        return (self._value if self._timeunit == TimeUnit.DAYS else
                self._value // 24)

    def to_hours(self):
        """
        Returns the number of hours in this duration, which may be negative.

        :returns: the number of hours.
        :rtype: int
        """
        return (self._value if self._timeunit == TimeUnit.HOURS else
                self._value * 24)

    def to_expiration_time(self, reference_time):
        """
        Returns an absolute time representing the duration plus the absolute
        time reference parameter. If an expiration time from the current time is
        desired the parameter should be the current system time in millisecond.
        If the duration of this object is 0, indicating no expiration time, this
        method will return 0, regardless of the reference time.

        :param reference_time: an absolute time in milliseconds since January
            1, 1970.
        :type reference_time: int
        :returns: time in milliseconds, 0 if this object's duration is 0.
        :rtype: int
        :raises IllegalArgumentException: raises the exception if reference_time
            is not positive.
        """
        CheckValue.check_int_gt_zero(reference_time, 'reference_time')
        if self._value == 0:
            return 0
        hours = 24 if self._timeunit == TimeUnit.DAYS else 1
        return reference_time + hours * self._value * 60 * 60 * 1000

    def get_value(self):
        """
        Returns the numeric duration value.

        :returns: the duration value, independent of unit.
        :rtype: int
        """
        return self._value

    def get_unit(self):
        """
        Returns the time unit used for the duration.

        :returns: the timeunit.
        :rtype: TimeUnit
        """
        return self._timeunit

    def unit_is_days(self):
        return self._timeunit == TimeUnit.DAYS

    def unit_is_hours(self):
        return self._timeunit == TimeUnit.HOURS


class UserInfo(object):
    """
    On-premise only.

    A class that encapsulates the information associated with a user including
    the user id and name in the system.
    """

    def __init__(self, user_id, user_name):
        """
        Constructs an instance of UserInfo as result returned by
        :py:meth:`NoSQLHandle.list_users`.
        """
        self._user_id = user_id
        self._user_name = user_name

    def __str__(self):
        return 'id: ' + self._user_id + ', name: ' + self._user_name

    def get_id(self):
        """
        Returns the id associated with the user.

        :returns: the user id string.
        :rtype: str
        """
        return self._user_id

    def get_name(self):
        """
        Returns the name associated with the user.

        :returns: the user name string.
        :rtype: str
        """
        return self._user_name


class Version(object):
    """
    Version is an opaque class that represents the version of a row in the
    database. It is returned by successful :py:class:`GetRequest` and
    can be used in :py:meth:`PutRequest.set_match_version` and
    :py:meth:`DeleteRequest.set_match_version` to conditionally perform those
    operations to ensure an atomic read-modify-write cycle. This is an opaque
    object from an application perspective.

    Use of Version in this way adds cost to operations so it should be done only
    if necessary.

    :param version: a bytearray.
    :type version: bytearray
    :raises IllegalArgumentException: raises the exception if version is not
        a bytearray.
    """

    def __init__(self, version):
        Version._check_version(version)
        self._version = version

    def get_bytes(self):
        """
        Returns the bytearray from the Version.

        :returns: the bytearray from the Version.
        :rtype: bytearray
        """
        return self._version

    @staticmethod
    def create_version(version):
        """
        Returns an instance of :py:class:`Version`.

        :param version: a bytearray or None.
        :type version: bytearray
        :returns: an instance of Version.
        :rtype: Version
        :raises IllegalArgumentException: raises the exception if version is not
            a bytearray or None.
        """
        if version is None:
            return None
        return Version(version)

    @staticmethod
    def _check_version(version):
        if not isinstance(version, bytearray):
            raise IllegalArgumentException(
                'version must be an bytearray. Got:' + str(version))
