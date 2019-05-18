#
# Copyright (C) 2018, 2019 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl
#
# Please see LICENSE.txt file included in the top-level directory of the
# appropriate download for a copy of the license and additional information.
#

from datetime import datetime
from logging import Logger
from struct import pack, unpack
from sys import version_info
from time import ctime, time

from .exception import IllegalArgumentException


def enum(**enums):
    return type('Enum', (object,), enums)


class ByteInputStream:
    """
    The ByteInputStream provides methods to get data with different type from
    a bytearray.
    """

    def __init__(self, content):
        self.__content = content
        self.__content.reverse()

    def read_boolean(self):
        res = bool(self.read_byte())
        return res

    def read_byte(self):
        return self.__content.pop()

    def read_float(self):
        buf = bytearray(8)
        self.read_fully(buf)
        res, = unpack('>d', buf)
        return res

    def read_fully(self, buf, start=0, end=None):
        if end is None:
            end = len(buf)
        for index in range(start, end):
            buf[index] = self.__content.pop()

    def read_int(self):
        buf = bytearray(4)
        self.read_fully(buf)
        res, = unpack('>i', buf)
        return res


class ByteOutputStream:
    """
    The ByteOutputStream provides methods to write data with different type into
    a bytearray.
    """

    def __init__(self, content):
        self.__content = content

    def get_offset(self):
        return len(self.__content)

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
            self.__content.append(value[index])

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
            self.__content[offset + index] = val_b[index]

    def write_short_int(self, value):
        val_s = pack('>h', value)
        self.write_value(val_s)

    def write_value(self, value):
        val_b = bytearray(value)
        self.write_bytearray(val_b)


class CheckValue:
    @staticmethod
    def check_boolean(data, name):
        if data is not True and data is not False:
            raise IllegalArgumentException(name + ' must be True or False.')

    @staticmethod
    def check_dict(data, name):
        if data is not None and not isinstance(data, dict):
            raise IllegalArgumentException(name + ' must be a dict.')

    @staticmethod
    def check_int(data, name):
        if not CheckValue.is_int(data):
            raise IllegalArgumentException(
                name + ' must be an integer. Got:' + str(data))

    @staticmethod
    def check_int_ge_zero(data, name):
        if not CheckValue.is_int(data) or data < 0:
            raise IllegalArgumentException(
                name + ' must be an integer that is not negative. Got:' +
                str(data))

    @staticmethod
    def check_int_gt_zero(data, name):
        if not CheckValue.is_int(data) or data <= 0:
            raise IllegalArgumentException(
                name + ' must be an positive integer. Got:' + str(data))

    @staticmethod
    def check_list(data, name):
        if not isinstance(data, list):
            raise IllegalArgumentException(name + ' must be a list.')

    @staticmethod
    def check_not_none(data, name):
        if data is None:
            raise IllegalArgumentException(name + ' must be not-none.')

    @staticmethod
    def check_str(data, name):
        if not CheckValue.is_str(data):
            raise IllegalArgumentException(name + ' must be a string type.')

    @staticmethod
    def check_logger(data, name):
        if not isinstance(data, Logger):
            raise IllegalArgumentException(name + ' must be a Logger.')

    @staticmethod
    def is_int(data):
        if ((version_info.major == 2 and isinstance(data, (int, long)) or
                version_info.major == 3 and isinstance(data, int)) and
                -pow(2, 63) <= data < pow(2, 63)):
            return True
        return False

    @staticmethod
    def is_str(data):
        if (version_info.major == 2 and isinstance(data, (str, unicode)) or
                version_info.major == 3 and isinstance(data, str)):
            return True
        return False


class Consistency:
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


class FieldRange:
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
    :raises IllegalArgumentException: raises the exception if field_path is not
        a string.
    """

    def __init__(self, field_path):
        # Create a value based on a specific field.
        CheckValue.check_str(field_path, 'field_path')
        self.__field_path = field_path
        self.__start = None
        self.__start_inclusive = False
        self.__end = None
        self.__end_inclusive = False

    def __str__(self):
        return ('{Path=' + self.__field_path +
                ', Start=' + str(self.__start) + ', End=' + str(self.__end) +
                ', StartInclusive=' + str(self.__start_inclusive) +
                ', EndInclusive=' + str(self.__end_inclusive) + '}')

    def get_field_path(self):
        """
        Returns the name for the field used in the range.

        :returns: the name of the field.
        """
        return self.__field_path

    def set_start(self, value, is_inclusive):
        """
        Sets the start value of the range to the specified value.

        :param value: the value to set.
        :param is_inclusive: set to True if the range is inclusive of the value,
            False if it is exclusive.
        :returns: self.
        :raises IllegalArgumentException: raises the exception if parameters are
            not expected type.
        """
        CheckValue.check_not_none(value, 'value')
        CheckValue.check_boolean(is_inclusive, 'is_inclusive')
        self.__start = value
        self.__start_inclusive = is_inclusive
        return self

    def get_start(self):
        """
        Returns the FieldValue that defines lower bound of the range, or None if
        no lower bound is enforced.

        :returns: the start field value.
        """
        return self.__start

    def get_start_inclusive(self):
        """
        Returns whether start is included in the range, i.e., start is less than
        or equal to the first FieldValue in the range. This value is valid only
        if the start value is not None.

        :returns: True if the start value is inclusive.
        """
        return self.__start_inclusive

    def set_end(self, value, is_inclusive):
        """
        Sets the end value of the range to the specified value.

        :param value: the value to set.
        :param is_inclusive: set to True if the range is inclusive of the value,
            False if it is exclusive.
        :returns: self.
        :raises IllegalArgumentException: raises the exception if parameters are
            not expected type.
        """
        CheckValue.check_not_none(value, 'value')
        CheckValue.check_boolean(is_inclusive, 'is_inclusive')
        self.__end = value
        self.__end_inclusive = is_inclusive
        return self

    def get_end(self):
        """
        Returns the FieldValue that defines upper bound of the range, or None if
        no upper bound is enforced.

        :returns: the end field value.
        """
        return self.__end

    def get_end_inclusive(self):
        """
        Returns whether end is included in the range, i.e., end is greater than
        or equal to the last FieldValue in the range. This value is valid only
        if the end value is not None.

        :returns: True if the end value is inclusive.
        """
        return self.__end_inclusive

    def validate(self):
        # Ensures that the object is self-consistent and if not, throws
        # IllegalArgumentException. Validation of the range values themselves is
        # done remotely.
        start_type = None if self.__start is None else type(self.__start)
        end_type = None if self.__end is None else type(self.__end)
        if start_type is None and end_type is None:
            raise IllegalArgumentException(
                'FieldRange: must specify a start or end value.')
        if (start_type is not None and end_type is not None and
                start_type is not end_type):
            raise IllegalArgumentException(
                'FieldRange: Mismatch of start and end types. Start type is ' +
                str(start_type) + ', end type is ' + str(end_type))


class HttpConstants:
    # The current version of the protocol
    NOSQL_VERSION = 'V0'

    # The service name prefix for public NoSQL services
    NOSQL_PATH_NAME = 'nosql'

    # The service name of the NoSQL data service (the driver protocol)
    DATA_PATH_NAME = 'data'

    # Creates a URI path from the arguments
    @staticmethod
    def make_path(*args):
        path = args[0]
        for index in range(1, len(args)):
            path += '/' + args[index]
        return path

    # The service name of the nosql prefix
    NOSQL_PREFIX = make_path.__func__(NOSQL_VERSION, NOSQL_PATH_NAME)

    # The path denoting a NoSQL request
    NOSQL_DATA_PATH = make_path.__func__(NOSQL_PREFIX, DATA_PATH_NAME)


class IndexInfo:
    """
    IndexInfo represents the information about a single index including its name
    and field names. Instances of this class are returned in
    :py:meth:`GetIndexesResult`.
    """

    def __init__(self, index_name, field_names):
        self.__index_name = index_name
        self.__field_names = field_names

    def __str__(self):
        return ('IndexInfo [indexName=' + self.__index_name + ', fields=[' +
                ','.join(self.__field_names) + ']]')

    def get_index_name(self):
        """
        Returns the name of the index.

        :returns: the index name.
        :rtype: str
        """
        return self.__index_name

    def get_field_names(self):
        """
        Returns the list of field names that define the index.

        :returns: the field names.
        :rtype: list(str)
        """
        return self.__field_names


class LogUtils:
    # Utility methods to facilitate Logging.
    def __init__(self, logger=None):
        self.__logger = logger

    def log_critical(self, msg):
        if self.__logger is not None:
            self.__logger.critical(ctime() + '[CRITICAL]' + msg)

    def log_error(self, msg):
        if self.__logger is not None:
            self.__logger.error(ctime() + '[ERROR]' + msg)

    def log_warning(self, msg):
        if self.__logger is not None:
            self.__logger.warning(ctime() + '[WARNING]' + msg)

    def log_info(self, msg):
        if self.__logger is not None:
            self.__logger.info(ctime() + '[INFO]' + msg)

    def log_debug(self, msg):
        if self.__logger is not None:
            self.__logger.debug(ctime() + '[DEBUG]' + msg)

    # Trace == debug
    def log_trace(self, msg):
        if self.__logger is not None:
            self.__logger.debug(ctime() + '[DEBUG]' + msg)

    def is_enabled_for(self, level):
        return self.__logger is not None and self.__logger.isEnabledFor(level)


class Memoize:
    # A cache that used for saving the access token.
    def __init__(self, duration=60):
        self.__cache = {}
        self.__duration = duration

    def set(self, key, value):
        self.__cache[key] = {'value': value, 'time': time()}

    def get(self, key):
        if key in self.__cache and not self.__is_obsolete(self.__cache[key]):
            return self.__cache[key]['value']
        return None

    def __is_obsolete(self, entry):
        return time() - entry['time'] > self.__duration


class PackedInteger:
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
        :param offset: the offset in the buffer at which to start writing.
        :param value: the integer to be written.
        :returns: the offset past the bytes written.

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
        :param offset: the offset in the buffer at which to start writing.
        :param value: the long integer to be written.
        :returns: the offset past the bytes written.

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
        :param offset: the offset in the buffer at which to start reading.
        :returns: the number of bytes that would be read.
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
        :param offset: the offset in the buffer at which to start reading.
        :returns: the number of bytes that would be read.
        """
        # The length is stored in the same way for int and long.
        return PackedInteger.get_read_sorted_int_length(buf, offset)

    @staticmethod
    def read_sorted_int(buf, offset):
        """
        Reads a sorted packed integer at the given buffer offset and returns it.

        :param buf: the buffer to read from.
        :param offset: the offset in the buffer at which to start reading.
        :returns: the integer that was read.
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
            value = 0xFFFFFFFF
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
        :param offset: the offset in the buffer at which to start reading.
        :returns: the long integer that was read.
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
            value = 0xFFFFFFFFFFFFFFFF
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


class PreparedStatement:
    """
    A class encapsulating a prepared query statement. It includes state that can
    be sent to a server and executed without re-parsing the query. It includes
    bind variables which may be set for each successive use of the query. The
    prepared query itself is read-only but this object contains a dictionary of
    bind variables and is not thread-safe if variables are used.

    PreparedStatement instances are returned inside :py:meth:`PrepareResult`
    objects returned by :py:meth:`NoSQLHandle.prepare`

    A single instance of PreparedStatement is thread-safe if bind variables are
    not used. If bind variables are to be used and the statement shared among
    threads additional instances of PreparedStatement can be constructed using
    :py:meth:`copy_statement`.
    """

    def __init__(self, statement):
        """
        Constructs a PreparedStatement. Construction is hidden to eliminate
        application access to the underlying statement, reducing the chance of
        corruption.
        """
        # 10 is arbitrary. TODO: put magic number in it for validation?
        if statement is None or len(statement) < 10:
            raise IllegalArgumentException(
                'Invalid prepared query, cannot be None.')
        self.__statement = statement
        self.__variables = dict()

    def copy_statement(self):
        """
        Returns a new instance that shares this object's prepared query, which
        is immutable, but does not share its variables.

        :returns: a new PreparedStatement using this instance's prepared query.
            Bind variables are uninitialized.
        :rtype: PreparedStatement
        """
        return PreparedStatement(self.__statement)

    def get_statement(self):
        # internal use to return the serialized, prepared query, opaque
        return self.__statement

    def set_variable(self, name, value):
        """
        Sets the named variable in the map of variables to use for the query.
        Existing variables with the same name are silently overwritten. The
        names and types are validated when the query is executed.

        :param name: the variable name used in the query statement.
        :type name: str
        :param value: the value.
        :type value: a value matching the type of the field
        :returns: self.
        :raises IllegalArgumentException: raises the exception if name is not a
            string.
        """
        CheckValue.check_str(name, 'name')
        self.__variables[name] = value
        return self

    def get_variables(self):
        """
        Returns the dictionary of variables to use for a prepared query
        with variables.

        :returns: the dictionary.
        :rtype: dict
        """
        return self.__variables

    def clear_variables(self):
        """
        Clears all bind variables from the statement.
        """
        self.__variables = dict()


class PutOption:
    """
    Set the put option for put requests.
    """
    IF_ABSENT = 0
    """Set PutOption.IF_ABSENT to perform put if absent operation."""
    IF_PRESENT = 1
    """Set PutOption.IF_PRESENT to perform put if present operation."""
    IF_VERSION = 2
    """Set PutOption.IF_VERSION to perform put if version operation."""


class State:
    """
    Represents the table state, usually used when
    :py:meth:`TableResult.wait_for_state` is called.
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


class TableLimits:
    """
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

    All 3 values must be used whenever using this object. There are no defaults
    and no mechanism to indicate "no change."

    :param read_units: the desired throughput of read operation in terms of read
        units. A read unit represents 1 eventually consistent read per second
        for data up to 1 KB in size. A read that is absolutely consistent is
        double that, consuming 2 read units for a read of up to 1 KB in size.
    :param write_units: the desired throughput of write operation in terms of
        write units. A write unit represents 1 write per second of data up to 1
        KB in size.
    :param storage_gb: the maximum storage to be consumed by the table, in
        gigabytes.
    :raises IllegalArgumentException: raises the exception if parameters are not
        validate.
    """

    def __init__(self, read_units, write_units, storage_gb):
        # Constructs a TableLimits instance.
        CheckValue.check_int(read_units, 'read_units')
        CheckValue.check_int(write_units, 'write_units')
        CheckValue.check_int(storage_gb, 'storage_gb')
        self.__read_units = read_units
        self.__write_units = write_units
        self.__storage_gb = storage_gb

    def __str__(self):
        return ('[' + str(self.__read_units) + ', ' + str(self.__write_units) +
                ', ' + str(self.__storage_gb) + ']')

    def set_read_units(self, read_units):
        """
        Sets the read throughput in terms of read units.

        :param read_units: the throughput to use, in read units.
        :returns: self.
        :raises IllegalArgumentException: raises the exception if read_units is
            not a integer.
        """
        CheckValue.check_int(read_units, 'read_units')
        self.__read_units = read_units
        return self

    def get_read_units(self):
        """
        Returns the read throughput in terms of read units.

        :returns: the read units.
        """
        return self.__read_units

    def set_write_units(self, write_units):
        """
        Sets the write throughput in terms of write units.

        :param write_units: the throughput to use, in write units.
        :returns: self.
        :raises IllegalArgumentException: raises the exception if write_units is
            not a integer.
        """
        CheckValue.check_int(write_units, 'write_units')
        self.__write_units = write_units
        return self

    def get_write_units(self):
        """
        Returns the write throughput in terms of write units.

        :returns: the write units.
        """
        return self.__write_units

    def set_storage_gb(self, storage_gb):
        """
        Sets the storage capacity in gigabytes.

        :param storage_gb: the capacity to use, in gigabytes.
        :returns: self.
        :raises IllegalArgumentException: raises the exception if storage_gb is
            not a integer.
        """
        CheckValue.check_int(storage_gb, 'storage_gb')
        self.__storage_gb = storage_gb
        return self

    def get_storage_gb(self):
        """
        Returns the storage capacity in gigabytes.

        :returns: the storage capacity in gigabytes.
        """
        return self.__storage_gb

    def validate(self):
        if (self.__read_units <= 0 or self.__write_units <= 0 or
                self.__storage_gb <= 0):
            raise IllegalArgumentException(
                'TableLimits values must be non-negative.')


class TableUsage:
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
        self.__start_time_ms = start_time_ms
        self.__seconds_in_period = seconds_in_period
        self.__read_units = read_units
        self.__write_units = write_units
        self.__storage_gb = storage_gb
        self.__read_throttle_count = read_throttle_count
        self.__write_throttle_count = write_throttle_count
        self.__storage_throttle_count = storage_throttle_count

    def __str__(self):
        return ('TableUsage [start_time_ms=' + str(self.__start_time_ms) +
                ', seconds_in_period=' + str(self.__seconds_in_period) +
                ', read_units=' + str(self.__read_units) +
                ', write_units=' + str(self.__write_units) +
                ', storage_gb=' + str(self.__storage_gb) +
                ', read_throttle_count=' + str(self.__read_throttle_count) +
                ', write_throttle_count=' + str(self.__write_throttle_count) +
                ', storage_throttle_count=' +
                str(self.__storage_throttle_count) + ']')

    def get_start_time(self):
        """
        Returns the start time for this usage record in milliseconds since
        the Epoch.

        :returns: the start time.
        """
        return self.__start_time_ms

    def get_start_time_string(self):
        """
        Returns the start time as an ISO 8601 formatted string. If the start
        timestamp is not set, None is returned.

        :returns: the start time, or None if not set.
        """
        if self.__start_time_ms == 0:
            return None
        return datetime.fromtimestamp(
            float(self.__start_time_ms) / 1000).isoformat()

    def get_seconds_in_period(self):
        """
        Returns the number of seconds in this usage record.

        :returns: the number of seconds.
        """
        return self.__seconds_in_period

    def get_read_units(self):
        """
        Returns the number of read units consumed during this period.

        :returns: the read units.
        """
        return self.__read_units

    def get_write_units(self):
        """
        Returns the number of write units consumed during this period.

        :returns: the write units.
        """
        return self.__write_units

    def get_storage_gb(self):
        """
        Returns the amount of storage consumed by the table. This
        information may be out of date as it is not maintained in real time.

        :returns: the size in gigabytes.
        """
        return self.__storage_gb

    def get_read_throttle_count(self):
        """
        Returns the number of read throttling exceptions on this table in
        the time period.

        :returns: the number of throttling exceptions.
        """
        return self.__read_throttle_count

    def get_write_throttle_count(self):
        """
        Returns the number of write throttling exceptions on this table in
        the time period.

        :returns: the number of throttling exceptions.
        """
        return self.__write_throttle_count

    def get_storage_throttle_count(self):
        """
        Returns the number of storage throttling exceptions on this table in
        the time period.

        :returns: the number of throttling exceptions.
        """
        return self.__storage_throttle_count


class TimeUnit:
    """
    The time unit to use.
    """
    HOURS = 1
    """Set TimeUnit.HOURS to use hour as time unit"""
    DAYS = 2
    """Set TimeUnit.DAYS to use day as time unit"""


class TimeToLive:
    """
    TimeToLive is a utility class that represents a period of time, similar to
    java.time.Duration in Java, but specialized to the needs of this driver.

    This class is restricted to durations of days and hours. It is only used as
    input related to time to live (TTL) for row instances.

    Construction allows only day and hour durations for efficiency reasons.
    Durations of days are recommended as they result in the least amount of
    storage overhead. Only positive durations are allowed on input.

    :param value: value of time.
    :param timeunit: unit of time, cannot be None.
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
        self.__value = value
        self.__timeunit = timeunit

    def __str__(self):
        timeunit = 'HOURS' if self.__timeunit == TimeUnit.HOURS else 'DAYS'
        return str(self.__value) + ' ' + timeunit

    @staticmethod
    def of_hours(hours):
        """
        Creates a duration using a period of hours.

        :param hours: the number of hours in the duration, must be a
            non-negative number.
        :returns: the duration.
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
        :returns: the duration.
        :raises IllegalArgumentException: raises the exception if a negative
            value is provided.
        """
        CheckValue.check_int_ge_zero(days, 'days')
        return TimeToLive(days, TimeUnit.DAYS)

    def to_days(self):
        """
        Returns the number of days in this duration, which may be negative.

        :returns: the number of days.
        """
        return (self.__value if self.__timeunit == TimeUnit.DAYS else
                self.__value // 24)

    def to_hours(self):
        """
        Returns the number of hours in this duration, which may be negative.

        :returns: the number of hours.
        """
        return (self.__value if self.__timeunit == TimeUnit.HOURS
                else self.__value * 24)

    def to_expiration_time(self, reference_time):
        """
        Returns an absolute time representing the duration plus the absolute
        time reference parameter. If an expiration time from the current time is
        desired the parameter should be the current system time in millisecond.
        If the duration of this object is 0, indicating no expiration time, this
        method will return 0, regardless of the reference time.

        :param reference_time: an absolute time in milliseconds since January
            1, 1970.
        :returns: time in milliseconds, 0 if this object's duration is 0.
        :raises IllegalArgumentException: raises the exception if reference_time
            is not positive.
        """
        CheckValue.check_int_gt_zero(reference_time, 'reference_time')
        if self.__value == 0:
            return 0
        hours = 24 if self.__timeunit == TimeUnit.DAYS else 1
        return reference_time + hours * self.__value * 60 * 60 * 1000

    def get_value(self):
        """
        Returns the numeric duration value.

        :returns: the duration value, independent of unit.
        """
        return self.__value

    def get_unit(self):
        """
        Returns the time unit used for the duration.

        :returns: the timeunit.
        """
        return self.__timeunit

    def unit_is_days(self):
        return self.__timeunit == TimeUnit.DAYS

    def unit_is_hours(self):
        return self.__timeunit == TimeUnit.HOURS


class Version:
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
    :raises IllegalArgumentException: raises the exception if version is not
        a bytearray.
    """

    def __init__(self, version):
        Version.__check_version(version)
        self.__version = version

    def get_bytes(self):
        """
        Returns the bytearray from the Version.

        :returns: the bytearray from the Version.
        """
        return self.__version

    @staticmethod
    def create_version(version):
        """
        Returns an instance of :py:class:`Version`.

        :param version: a bytearray or None.
        :returns: an instance of Version.
        :raises IllegalArgumentException: raises the exception if version is not
            a bytearray or None.
        """
        if version is None:
            return None
        return Version(version)

    @staticmethod
    def __check_version(version):
        if not isinstance(version, bytearray):
            raise IllegalArgumentException(
                'version must be an bytearray. Got:' + str(version))
