#
# Copyright (c) 2018, 2022 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#

import json
#
from base64 import b64encode
from collections import OrderedDict

import borneo.operations
from .common import (
    ByteInputStream, ByteOutputStream, Empty, IndexInfo, PreparedStatement,
    Replica, ReplicaStats, TableLimits, TableUsage, Version)
from .exception import IllegalArgumentException
from .nson_protocol import *
from .query import PlanIter, QueryDriver, TopologyInfo
from .serde import (math_name_to_value)
from .serdeutil import (SerdeUtil, RequestSerializer, NsonEventHandler)

#
# Contains methods to serialize and deserialize NSON
#
# Maps, arrays, other supported NSON datatypes
# The supported types and their associated numeric values
# are defined in serdeutil in SerdeUtil.FIELD_VALUE_TYPE
#


#
# TODO:
#
#  add tests for new fields (see Java)
#  MR operations?
#

class Nson(object):

    #
    # Primitive type write methods. These write NSON -- that is, they write
    # the type, then the value
    #

    @staticmethod
    def write_int(bos, value):
        bos.write_byte(SerdeUtil.FIELD_VALUE_TYPE.INTEGER)
        SerdeUtil.write_packed_int(bos, value)

    @staticmethod
    def write_boolean(bos, value):
        bos.write_byte(SerdeUtil.FIELD_VALUE_TYPE.BOOLEAN)
        bos.write_boolean(value)

    @staticmethod
    def write_long(bos, value):
        bos.write_byte(SerdeUtil.FIELD_VALUE_TYPE.LONG)
        SerdeUtil.write_packed_long(bos, value)

    @staticmethod
    def write_double(bos, value):
        bos.write_byte(SerdeUtil.FIELD_VALUE_TYPE.DOUBLE)
        SerdeUtil.write_float(bos, value)

    @staticmethod
    def write_string(bos, value):
        bos.write_byte(SerdeUtil.FIELD_VALUE_TYPE.STRING)
        SerdeUtil.write_string(bos, value)

    @staticmethod
    def write_timestamp(bos, value):
        bos.write_byte(SerdeUtil.FIELD_VALUE_TYPE.TIMESTAMP)
        SerdeUtil.write_datetime(bos, value)

    @staticmethod
    def write_bytearray(bos, value):
        bos.write_byte(SerdeUtil.FIELD_VALUE_TYPE.BINARY)
        SerdeUtil.write_bytearray(bos, value)

    @staticmethod
    def write_number(bos, value):
        bos.write_byte(SerdeUtil.FIELD_VALUE_TYPE.NUMBER)
        SerdeUtil.write_decimal(bos, value)

    @staticmethod
    def write_type(bos, value):
        bos.write_byte(value)

    #
    # Primitive type read methods
    #

    @staticmethod
    def read_type(bis, expected_type):
        t = bis.read_byte()
        if t != expected_type:
            raise IllegalArgumentException(
                'Expected type ' + str(expected_type) +
                ', received type ' + str(t))

    @staticmethod
    def read_int(bis):
        Nson.read_type(bis, SerdeUtil.FIELD_VALUE_TYPE.INTEGER)
        return SerdeUtil.read_packed_int(bis)

    @staticmethod
    def read_boolean(bis):
        Nson.read_type(bis, SerdeUtil.FIELD_VALUE_TYPE.BOOLEAN)
        return bis.read_boolean()  # turns byte into boolean

    @staticmethod
    def read_long(bis):
        Nson.read_type(bis, SerdeUtil.FIELD_VALUE_TYPE.LONG)
        return SerdeUtil.read_packed_long(bis)

    @staticmethod
    def read_double(bis):
        Nson.read_type(bis, SerdeUtil.FIELD_VALUE_TYPE.DOUBLE)
        return SerdeUtil.read_float(bis)

    @staticmethod
    def read_string(bis):
        Nson.read_type(bis, SerdeUtil.FIELD_VALUE_TYPE.STRING)
        return SerdeUtil.read_string(bis)

    @staticmethod
    def read_timestamp(bis):
        Nson.read_type(bis, SerdeUtil.FIELD_VALUE_TYPE.TIMESTAMP)
        return SerdeUtil.read_datetime(bis)

    @staticmethod
    def read_number(bis):
        Nson.read_type(bis, SerdeUtil.FIELD_VALUE_TYPE.NUMBER)
        return SerdeUtil.read_decimal(bis)

    @staticmethod
    def read_binary(bis, skip=False):
        Nson.read_type(bis, SerdeUtil.FIELD_VALUE_TYPE.BINARY)
        return SerdeUtil.read_bytearray(bis, skip)

    # noinspection PyUnresolvedReferences
    @staticmethod
    def generate_events_from_nson(bis, handler, skip=False):
        """
        Generate NSON "events"
        """
        if handler is None and not skip:
            raise IllegalArgumentException(
                'Handler must have a value if not skipping')
        if handler is not None and handler.stop():
            return
        t = bis.read_byte()
        if t == SerdeUtil.FIELD_VALUE_TYPE.BINARY:
            value = SerdeUtil.read_bytearray(bis, skip)
            if not skip:
                handler.binary_value(value)
        elif t == SerdeUtil.FIELD_VALUE_TYPE.BOOLEAN:
            value = bis.read_boolean()
            if not skip:
                handler.boolean_value(value)
        elif t == SerdeUtil.FIELD_VALUE_TYPE.DOUBLE:
            value = SerdeUtil.read_float(bis)
            if not skip:
                handler.double_value(value)
        elif t == SerdeUtil.FIELD_VALUE_TYPE.INTEGER:
            value = SerdeUtil.read_packed_int(bis)
            if not skip:
                handler.integer_value(value)
        elif t == SerdeUtil.FIELD_VALUE_TYPE.LONG:
            value = SerdeUtil.read_packed_long(bis)
            if not skip:
                handler.long_value(value)
        elif t == SerdeUtil.FIELD_VALUE_TYPE.STRING:
            value = SerdeUtil.read_string(bis)
            if not skip:
                handler.string_value(value)
        elif t == SerdeUtil.FIELD_VALUE_TYPE.TIMESTAMP:
            value = SerdeUtil.read_datetime(bis)
            if not skip:
                handler.timestamp_value(value)
        elif t == SerdeUtil.FIELD_VALUE_TYPE.NUMBER:
            value = SerdeUtil.read_decimal(bis)
            if not skip:
                handler.number_value(value)
        elif t == SerdeUtil.FIELD_VALUE_TYPE.JSON_NULL:
            if not skip:
                handler.json_null_value()
        elif t == SerdeUtil.FIELD_VALUE_TYPE.NULL:
            if not skip:
                handler.null_value()
        elif t == SerdeUtil.FIELD_VALUE_TYPE.EMPTY:
            if not skip:
                handler.empty_value()
        elif t == SerdeUtil.FIELD_VALUE_TYPE.MAP:
            length = SerdeUtil.read_full_int(bis)
            if skip:
                bis.skip(length)
            else:
                num_elements = SerdeUtil.read_full_int(bis)
                handler.start_map(num_elements)
                if handler.stop():
                    return
                for i in range(0, num_elements):
                    key = SerdeUtil.read_string(bis)
                    skip_field = handler.start_map_field(key)
                    if handler.stop():
                        return
                    Nson.generate_events_from_nson(bis, handler, skip_field)
                    if handler.stop():
                        return
                    handler.end_map_field(key)
                    if handler.stop():
                        return
                handler.end_map(num_elements)
        elif t == SerdeUtil.FIELD_VALUE_TYPE.ARRAY:
            length = SerdeUtil.read_full_int(bis)
            if skip:
                bis.skip(length)
            else:
                num_elements = SerdeUtil.read_full_int(bis)
                handler.start_array(num_elements)
                if handler.stop():
                    return
                for i in range(0, num_elements):
                    skip = handler.start_array_field(i)
                    if handler.stop():
                        return
                    Nson.generate_events_from_nson(bis, handler, skip)
                    if handler.stop():
                        return
                    handler.end_array_field(i)
                    if handler.stop():
                        return
                handler.end_array(num_elements)

        else:
            raise IllegalArgumentException(
                'Unknown value type code: ' + str(t))

    @staticmethod
    def generate_events_from_value(value, handler, skip=False):
        """
        Generate NSON "events" from a field value instance
        """
        t = SerdeUtil.get_type(value)
        if t == SerdeUtil.FIELD_VALUE_TYPE.BINARY:
            if not skip:
                handler.binary_value(value)
        elif t == SerdeUtil.FIELD_VALUE_TYPE.BOOLEAN:
            if not skip:
                handler.boolean_value(value)
        elif t == SerdeUtil.FIELD_VALUE_TYPE.DOUBLE:
            if not skip:
                handler.double_value(value)
        elif t == SerdeUtil.FIELD_VALUE_TYPE.INTEGER:
            if not skip:
                handler.integer_value(value)
        elif t == SerdeUtil.FIELD_VALUE_TYPE.LONG:
            if not skip:
                handler.long_value(value)
        elif t == SerdeUtil.FIELD_VALUE_TYPE.STRING:
            if not skip:
                handler.string_value(value)
        elif t == SerdeUtil.FIELD_VALUE_TYPE.TIMESTAMP:
            if not skip:
                handler.timestamp_value(value)
        elif t == SerdeUtil.FIELD_VALUE_TYPE.NUMBER:
            if not skip:
                handler.number_value(value)
        elif t == SerdeUtil.FIELD_VALUE_TYPE.JSON_NULL:
            if not skip:
                handler.json_null_value()
        elif t == SerdeUtil.FIELD_VALUE_TYPE.NULL:
            if not skip:
                handler.null_value()
        elif t == SerdeUtil.FIELD_VALUE_TYPE.EMPTY:
            if not skip:
                handler.empty_value()
        elif t == SerdeUtil.FIELD_VALUE_TYPE.MAP:
            if skip:
                return
            num_elements = len(value)
            handler.start_map(num_elements)
            if handler.stop():
                return
            for key in value:
                skip_field = handler.start_map_field(key)
                if handler.stop():
                    return
                Nson.generate_events_from_value(value[key],
                                                handler,
                                                skip_field)
                if handler.stop():
                    return
                handler.end_map_field(key)
                if handler.stop():
                    return
            handler.end_map(num_elements)
        elif t == SerdeUtil.FIELD_VALUE_TYPE.ARRAY:
            if skip:
                return
            num_elements = len(value)
            handler.start_array(num_elements)
            if handler.stop():
                return
            index = 0
            for item in value:
                skip = handler.start_array_field(index)
                index += 1
                if handler.stop():
                    return
                Nson.generate_events_from_value(item, handler, skip)
                if handler.stop():
                    return
                handler.end_array_field(index)
                if handler.stop():
                    return
            handler.end_array(num_elements)
        else:
            raise IllegalArgumentException(
                'Unknown value type code: ' + str(t))

    @staticmethod
    def iso_time_to_ms(iso_str):
        return SerdeUtil.iso_time_to_ms(iso_str)


class NsonSerializer(NsonEventHandler):
    """
    This class serializes an NSON "document." It maintains state for nested
    maps and arrays.
    """

    def stop(self):
        pass

    def __init__(self, bos):
        # output stream
        self._bos = bos

        # stack of offsets for map and array total size in bytes
        self._offset_stack = []

        # stack of offsets for tracking number of elements in a map or array
        self._size_stack = []

    def get_stream(self):
        return self._bos

    def binary_value(self, value):
        Nson.write_bytearray(self._bos, value)

    def boolean_value(self, value):
        Nson.write_boolean(self._bos, value)

    def double_value(self, value):
        Nson.write_double(self._bos, value)

    def empty_value(self):
        Nson.write_type(self._bos, SerdeUtil.FIELD_VALUE_TYPE.EMPTY)

    def integer_value(self, value):
        Nson.write_int(self._bos, value)

    def json_null_value(self):
        Nson.write_type(self._bos, SerdeUtil.FIELD_VALUE_TYPE.JSON_NULL)

    def long_value(self, value):
        Nson.write_long(self._bos, value)

    def null_value(self):
        Nson.write_type(self._bos, SerdeUtil.FIELD_VALUE_TYPE.NULL)

    def number_value(self, value):
        Nson.write_number(self._bos, value)

    def string_value(self, value):
        Nson.write_string(self._bos, value)

    def timestamp_value(self, value):
        Nson.write_timestamp(self._bos, value)

    def start_map(self, size=None):
        self._start_map_or_array(SerdeUtil.FIELD_VALUE_TYPE.MAP)

    def start_array(self, size=None):
        self._start_map_or_array(SerdeUtil.FIELD_VALUE_TYPE.ARRAY)

    def _start_map_or_array(self, field_type):
        self._bos.write_byte(field_type)
        offset = self._bos.get_offset()
        self._bos.write_int(0)  # size in bytes
        self._bos.write_int(0)  # number of elements
        self._offset_stack.append(offset)
        self._size_stack.append(0)

    def end_map(self, size=None):
        self._end_map_or_array()

    def end_array(self, size=None):
        self._end_map_or_array()

    def _end_map_or_array(self):
        length_offset = self._offset_stack.pop()
        num_elements = self._size_stack.pop()
        start = length_offset + 4
        total_bytes = self._bos.get_offset() - start
        # total # bytes followed by number of elements
        SerdeUtil.write_int_at_offset(self._bos, length_offset, total_bytes)
        SerdeUtil.write_int_at_offset(self._bos, length_offset + 4, num_elements)

    def start_map_field(self, field_name):
        # no type to write so use SerdeUtil
        SerdeUtil.write_string(self._bos, field_name)

    def end_map_field(self, field_name=None):
        self._incr_size()  # add 1 to number of elements

    def start_array_field(self, index=None):
        pass

    def end_array_field(self, index=0):
        self._incr_size()  # add 1 to number of elements

    def _incr_size(self):
        # add one to value on top of size stack. Using an index of -1
        # refers to the last element in the array/list
        self._size_stack[-1] += 1


class MapWalker(object):
    """
    This class "walks" an NSON map, allowing a caller to see each field and
    read each field. It is up to the caller to either (1) deserialize the
    field or (2) call skip() to move to the next one
    """
    # prevent an infinite loop in the event of bad deserialization
    MAX_ELEMS = 10000000

    def __init__(self, bis):
        self._bis = bis
        self._current_name = None
        self._current_index = 0
        t = bis.read_byte()
        if t != SerdeUtil.FIELD_VALUE_TYPE.MAP:
            raise IllegalArgumentException(
                'NSON MapWalker: stream must be located at a MAP')
        SerdeUtil.read_full_int(bis)  # total length in bytes, not relevant
        self._num_elements = SerdeUtil.read_full_int(bis)
        if self._num_elements < 0 or self._num_elements > MapWalker.MAX_ELEMS:
            raise IllegalArgumentException(
                'NSON MapWalker: invalid number of elements: ' +
                str(self._num_elements))

    def get_current_name(self):
        return self._current_name

    def get_stream(self):
        return self._bis

    def has_next(self):
        return self._current_index < self._num_elements

    def next(self):
        if self._current_index >= self._num_elements:
            raise IllegalArgumentException(
                'Cannot call next with no elements remaining')
        self._current_name = SerdeUtil.read_string(self._bis)
        self._current_index += 1

    def skip(self):
        t = self._bis.read_byte()
        if (t == SerdeUtil.FIELD_VALUE_TYPE.MAP or
                t == SerdeUtil.FIELD_VALUE_TYPE.ARRAY):
            length = SerdeUtil.read_full_int(self._bis)
            self._bis.skip(length)
        elif t == SerdeUtil.FIELD_VALUE_TYPE.BINARY:
            SerdeUtil.read_bytearray(self._bis, True)
        elif t == SerdeUtil.FIELD_VALUE_TYPE.BOOLEAN:
            self._bis.read_byte()
        elif t == SerdeUtil.FIELD_VALUE_TYPE.DOUBLE:
            SerdeUtil.read_float(self._bis)
        elif t == SerdeUtil.FIELD_VALUE_TYPE.INTEGER:
            SerdeUtil.read_packed_int(self._bis)
        elif t == SerdeUtil.FIELD_VALUE_TYPE.LONG:
            SerdeUtil.read_packed_long(self._bis)
        elif (t == SerdeUtil.FIELD_VALUE_TYPE.STRING or
              t == SerdeUtil.FIELD_VALUE_TYPE.TIMESTAMP or
              t == SerdeUtil.FIELD_VALUE_TYPE.NUMBER):
            SerdeUtil.read_string(self._bis)
        elif (t == SerdeUtil.FIELD_VALUE_TYPE.JSON_NULL or
              t == SerdeUtil.FIELD_VALUE_TYPE.NULL or
              t == SerdeUtil.FIELD_VALUE_TYPE.EMPTY):
            return
        else:
            raise IllegalArgumentException('Unknown field type: ' + str(t))


class JsonSerializer(NsonEventHandler):
    DQUOTE = '"'
    SQUOTE = '\''
    COMMA = ','
    comma_value = 44
    CR = '\n'
    SP = ' '
    SEP = ' : '

    #
    # Each individually appended piece of JSON is kept in a string that is a
    # member of a list, _builder. The final concatenation is done with a
    # list join() call.
    #
    # Pretty-printing is an option that results in object files on their own
    # lines and indentation for nested objects
    #
    def __init__(self, pretty=False, use_single_quote=False):
        self._builder = []
        self._pretty = pretty
        self._current_indent = 0
        self._incr = 2
        self._indent = ''
        if pretty:
            self._sep = ' : '
        else:
            self._sep = ':'
        if use_single_quote:
            self._quote_char = self.SQUOTE
        else:
            self._quote_char = self.DQUOTE

    def boolean_value(self, value):
        if value:
            val = 'true'
        else:
            val = 'false'
        self._append(val, False)

    def binary_value(self, value):
        self._append(b64encode(value), True)

    def string_value(self, value):
        self._append(str(value), True)

    def integer_value(self, value):
        self._append(str(value), False)

    def long_value(self, value):
        self._append(str(value), False)

    def double_value(self, value):
        self._append(str(value), False)

    def number_value(self, value):
        self._append(str(value), False)

    def timestamp_value(self, value):
        self._append(SerdeUtil.datetime_to_iso(value), True)

    def json_null_value(self):
        self._append('null', False)

    def null_value(self):
        self._append('null', False)

    def empty_value(self):
        self._append('EMPTY', True)

    def start_map(self, size=None):
        self._append('{', False)
        self._change_indent(self._incr)

    def start_array(self, size=None):
        self._append('[', False)

    def end_map(self, size=None):
        if self._builder[-1] == self.COMMA:
            self._builder.pop()
        if self._pretty:
            self._change_indent(-self._incr)
            self._append(self.CR, False)
            self._append(self._indent, False)

        self._append('}', False)

    def end_array(self, size=None):
        if self._builder[-1] == self.COMMA:
            self._builder.pop()
        self._append(']', False)

    def start_map_field(self, key):
        if self._pretty:
            self._append(self.CR, False)
            self._append(self._indent, False)
        self._append(key, True)
        self._append(self._sep, False)

    def end_map_field(self, key=None):
        self._append(',', False)

    def start_array_field(self, index=None):
        pass

    def end_array_field(self, index=None):
        self._append(',', False)

    def stop(self):
        return False

    def _append(self, val, quote):
        if quote:
            self._quote()
        self._builder.append(str(val))
        if quote:
            self._quote()

    def _quote(self):
        self._builder.append(self._quote_char)

    def _change_indent(self, num):
        if self._pretty:
            self._current_indent += num
            new_indent = []
            if self._current_indent == 0:
                self._indent = ''
            for i in range(self._current_indent):
                new_indent.append(self.SP)
                self._indent = "".join(new_indent)

    def __str__(self):
        return "".join(self._builder)


#
# Deserialize NSON into values (e.g. dict)
#


class FieldValueCreator(NsonEventHandler):

    def __init__(self, ordered=True):
        self._map_stack = []
        self._array_stack = []
        self._key_stack = []
        self._current_map = None
        self._current_array = None
        self._current_key = None
        self._current_value = None
        self._ordered = ordered

    def get_current_value(self):
        return self._current_value

    def _push_map(self, value):
        if self._current_map is not None:
            self._map_stack.append(self._current_map)
        self._current_map = value
        self._current_value = value

    def _push_array(self, value):
        if self._current_array is not None:
            self._array_stack.append(self._current_array)
        self._current_array = value
        self._current_value = value

    def _push_key(self, value):
        if self._current_key is not None:
            self._key_stack.append(self._current_key)
        self._current_key = value

    def boolean_value(self, value):
        self._current_value = value

    def binary_value(self, value):
        self._current_value = value

    def string_value(self, value):
        self._current_value = value

    def integer_value(self, value):
        self._current_value = value

    def long_value(self, value):
        self._current_value = value

    def double_value(self, value):
        self._current_value = value

    def number_value(self, value):
        self._current_value = value

    def timestamp_value(self, value):
        self._current_value = value

    def json_null_value(self):
        self._current_value = None  # JsonNone() ?

    def null_value(self):
        self._current_value = None

    def empty_value(self):
        self._current_value = Empty()

    def start_map(self, size=None):
        if self._ordered:
            self._push_map(OrderedDict())
        else:
            self._push_map(dict())

    def start_array(self, size=None):
        self._push_array(list())

    def end_map(self, size=None):
        # the working map (dict) becomes current value
        self._current_value = self._current_map
        if self._map_stack:
            self._current_map = self._map_stack.pop()
        else:
            self._current_map = None

    def end_array(self, size=None):
        # the working array becomes current value
        self._current_value = self._current_array
        if self._array_stack:
            self._current_array = self._array_stack.pop()
        else:
            self._current_array = None

    def start_map_field(self, key):
        self._push_key(key)

    def end_map_field(self, key=None):
        if self._current_key is not None and self._current_map is not None:
            self._current_map[self._current_key] = self._current_value
        if self._key_stack:
            self._current_key = self._key_stack.pop()
        else:
            self._current_key = None
        # current_value is undefined at this time

    def start_array_field(self, index=None):
        pass

    def end_array_field(self, index=None):
        if self._current_array is not None:
            self._current_array.append(self._current_value)

    def stop(self):
        return False


#
# Here down... request serializers
#

# Protocol is an NSON MAP, divided into 2 sections:
# header and payload. Field names here are "long" names and not
# what is actually serialized. See nson_protocol.py for the short names.
# Types referenced are NSON types, and tagged/serialized as such. Fields
# within an NSON map are not ordered
#
# Request header:
# {
#   "header" : {
#     "SERIAL_VERSION" : <int>
#     "TABLE_NAME" : <string>  #optional, present if needed by request
#     "OPCODE" : <int>
#     "TIMEOUT" : <int>
#   }
#
# Responses do not have headers but if there's an error they have a
# generic error object. E.g. here is an error response:
# {
#   "ERROR_CODE": <int> # if non-zero, the rest of this information may be
#                         present. If zero, no error, normal response
#   "exception": <string> # usually present
#   "consumed": {  # at this point consumed capacity isn't available for errors
#      "read_units": int,
#      "read_kb": int,
#      "write_kb": int
#    }
#   }
# }
#
#


# GetTableRequest
# Payload:
# {
#   "payload" : {
#     "OPERATION_ID" : <string> # optional; present if available
#   }
# }
#
# Non-error response:
# TableResult (see deserialize_table_result)
#
class GetTableRequestSerializer(RequestSerializer):

    def serialize(self, request, bos, serial_version):
        ns = NsonSerializer(bos)
        ns.start_map()  # top-level object

        # header
        Proto.start_map(ns, HEADER)
        Proto.write_header(ns, SerdeUtil.OP_CODE.GET_TABLE, request)
        Proto.end_map(ns, HEADER)

        # payload
        Proto.start_map(ns, PAYLOAD)
        Proto.write_string_map_field(ns, OPERATION_ID,
                                     request.get_operation_id())
        Proto.end_map(ns, PAYLOAD)

        ns.end_map()  # top-level object

    def deserialize(self, request, bis, serial_version):
        return Proto.deserialize_table_result(bis)


#
# GetRequest
#
# Payload:
# {
#   "payload" : {
#     "CONSISTENCY" : {        # required
#       "TYPE" : <int>         # required. If the consistency type requires it,
#                              # additional fields will be present (not used)
#     }
#     "KEY" : {                 # Map of the key value
#     }
#   }
# }
#
# Non-error response:
# {
#   "ERROR_CODE" : 0
#   "CONSUMED" : {
#      "READ_UNITS" : <int>
#      "READ_KV" : <int>
#      "WRITE_UNITS" : <int>
#   }
#   "ROW" : {                  # the row plus metadata
#     "MODIFIED" : <long>      # last modified
#     "EXPIRATION" : <long>    # expiration if using TTL
#     "ROW_VERSION" : <binary> # kv version
#     "VALUE" : {              # the row's value in NSON
#     }
#   }
# }
#


class GetRequestSerializer(RequestSerializer):

    def serialize(self, request, bos, serial_version):
        ns = NsonSerializer(bos)
        ns.start_map()  # top-level object

        # header
        Proto.start_map(ns, HEADER)
        Proto.write_header(ns, SerdeUtil.OP_CODE.GET, request)
        Proto.end_map(ns, HEADER)

        # payload
        Proto.start_map(ns, PAYLOAD)
        Proto.write_consistency(ns, request.get_consistency())
        Proto.write_key(ns, request.get_key())
        Proto.end_map(ns, PAYLOAD)

        ns.end_map()  # top-level object

    def deserialize(self, request, bis, serial_version):
        result = borneo.operations.GetResult()
        walker = MapWalker(bis)
        while walker.has_next():
            walker.next()
            name = walker.get_current_name()
            if name == ERROR_CODE:
                Proto.handle_error_code(walker)
            elif name == CONSUMED:
                Proto.read_consumed_capacity(bis, result)
            elif name == ROW:
                Proto.read_row(bis, result)
            elif name == TOPOLOGY_INFO:
                Proto.read_topology_info(bis, result);
            else:
                walker.skip()

        return result


#
# PutRequest
#
# Payload:
# {
#   "payload" : {
# shared with all Write ops
#     "DURABILITY" : <int>
#     "RETURN_ROW" : {
# shared with WriteMultiple
#     "EXACT_MATCH" :
#     "UPDATE_TTL" :
#     "TTL" :
#     "IDENTITY_CACHE_SIZE" :
#     "MATCH_VERSION" :
#     "VALUE" : {
#     }
#   }
# }
#
# Non-error response:
# {
#   "ERROR_CODE" : 0
#   "CONSUMED" : {
#      "READ_UNITS" : <int>
#      "READ_KV" : <int>
#      "WRITE_UNITS" : <int>
#   }
#   "ROW_VERSION" :
#   "RETURN_INFO" :
#   "GENERATED" :
# }
#

#
# PutRequest
#


class PutRequestSerializer(RequestSerializer):

    def serialize(self, request, bos, serial_version):
        ns = NsonSerializer(bos)
        ns.start_map()  # top-level object

        # header
        Proto.start_map(ns, HEADER)
        Proto.write_header(ns, SerdeUtil.get_put_op_code(request), request)
        Proto.end_map(ns, HEADER)

        # payload
        Proto.start_map(ns, PAYLOAD)
        Proto.write_write_request(ns, request)
        PutRequestSerializer.write_put_request(ns, request)
        Proto.end_map(ns, PAYLOAD)

        ns.end_map()  # top-level object

    def deserialize(self, request, bis, serial_version):
        result = borneo.operations.PutResult()
        walker = MapWalker(bis)
        while walker.has_next():
            walker.next()
            name = walker.get_current_name()
            if name == ERROR_CODE:
                Proto.handle_error_code(walker)
            elif name == CONSUMED:
                Proto.read_consumed_capacity(bis, result)
            elif name == ROW_VERSION:
                result.set_version(Version.create_version(
                    Nson.read_binary(bis)))
            elif name == RETURN_INFO:
                Proto.read_return_info(bis, result)
            elif name == GENERATED:
                result.set_generated_value(Proto.nson_to_value(bis))
            elif name == TOPOLOGY_INFO:
                Proto.read_topology_info(bis, result);
            else:
                walker.skip()

        return result

    @staticmethod
    def write_put_request(ns, request):
        if request.get_exact_match():
            Proto.write_bool_map_field(ns, EXACT_MATCH, True)
        if request.get_update_ttl():
            Proto.write_bool_map_field(ns, UPDATE_TTL, True)
        if request.get_ttl() is not None:
            # TTL is written as string, e.g. '5 DAYS'
            Proto.write_string_map_field(ns, TTL, str(request.get_ttl()))
        if request.get_identity_cache_size() != 0:
            Proto.write_int_map_field(ns, IDENTITY_CACHE_SIZE,
                                      request.get_identity_cache_size())
        if request.get_match_version() is not None:
            Proto.write_bin_map_field(
                ns, ROW_VERSION, request.get_match_version().get_bytes())
        Proto.write_value(ns, request.get_value())


#
# DeleteRequest
#
# Payload:
# {
#   "payload" : {
# shared with all Delete ops
#     "DURABILITY" : <int>
#     "RETURN_ROW" : {
# shared with WriteMultiple
#     "MATCH_VERSION" :
#     "KEY" : {}
#   }
# }
#
# Non-error response:
# {
#   "ERROR_CODE" : 0
#   "CONSUMED" : {
#      "READ_UNITS" : <int>
#      "READ_KV" : <int>
#      "WRITE_UNITS" : <int>
#   }
#   "RETURN_INFO" :
#   "SUCCESS" :
# }
#
class DeleteRequestSerializer(RequestSerializer):

    def serialize(self, request, bos, serial_version):
        ns = NsonSerializer(bos)
        ns.start_map()  # top-level object

        # header
        Proto.start_map(ns, HEADER)
        match_version = request.get_match_version()
        op_code = (SerdeUtil.OP_CODE.DELETE if match_version is None else
                   SerdeUtil.OP_CODE.DELETE_IF_VERSION)
        Proto.write_header(ns, op_code, request)
        Proto.end_map(ns, HEADER)

        # payload
        Proto.start_map(ns, PAYLOAD)
        Proto.write_write_request(ns, request)
        DeleteRequestSerializer.write_delete_request(ns, request)
        Proto.end_map(ns, PAYLOAD)

        ns.end_map()  # top-level object

    def deserialize(self, request, bis, serial_version):
        result = borneo.operations.DeleteResult()
        walker = MapWalker(bis)
        while walker.has_next():
            walker.next()
            name = walker.get_current_name()
            if name == ERROR_CODE:
                Proto.handle_error_code(walker)
            elif name == CONSUMED:
                Proto.read_consumed_capacity(bis, result)
            elif name == SUCCESS:
                result.set_success(Nson.read_boolean(bis))
            elif name == RETURN_INFO:
                Proto.read_return_info(bis, result)
            elif name == TOPOLOGY_INFO:
                Proto.read_topology_info(bis, result);
            else:
                walker.skip()

        return result

    @staticmethod
    def write_delete_request(ns, request):
        if request.get_match_version() is not None:
            Proto.write_bin_map_field(
                ns, ROW_VERSION, request.get_match_version().get_bytes())
        Proto.write_key(ns, request.get_key())


#
# TableRequest
#
# Payload:
# {
#   "payload" : {
#     "STATEMENT" : <optional, string>
#     "LIMITS": {<optional limits object>}
#     "ETAG" : {<optional>}
#   }
# }
#
# Non-error response (all optional)
# See deserialize_table_result
#
class TableRequestSerializer(RequestSerializer):

    def serialize(self, request, bos, serial_version):
        ns = NsonSerializer(bos)
        ns.start_map()  # top-level object

        # header
        Proto.start_map(ns, HEADER)
        Proto.write_header(ns, SerdeUtil.OP_CODE.TABLE_REQUEST, request)
        Proto.end_map(ns, HEADER)

        # payload
        Proto.start_map(ns, PAYLOAD)
        Proto.write_string_map_field(ns, STATEMENT,
                                     request.get_statement())
        Proto.write_limits(ns, request.get_table_limits())
        Proto.write_tags(ns, request)
        Proto.write_string_map_field(ns, ETAG, request.get_match_etag())
        Proto.end_map(ns, PAYLOAD)

        ns.end_map()  # top-level object

    def deserialize(self, request, bis, serial_version):
        return Proto.deserialize_table_result(bis)


#
# TableUsageRequest
#
# Payload:
# {
#   "payload" : {
#     "START" : <optional start time string>
#     "END":<optional end time string>
#     "LIST_MAX_TO_READ" : <optional int>
#     "LIST_START_INDEX" : <optional int>
#   }
# }
#
# Non-error response
# {
#   TABLE_NAME : <string>
#   LAST_INDEX : <int>
#   TABLE_USAGE: [
#      {
#        START: <long>
#        TABLE_USAGE_PERIOD: <int>
#        READ_UNITS: <int>
#        WRITE_UNITS: <int>
#        STORAGE_GB: <int>
#        READ_THROTTLE_COUNT: <int>
#        WRITE_THROTTLE_COUNT: <int>
#        STORAGE_THROTTLE_COUNT: <int>
#        MAX_SHARD_USAGE_PERCENT: <int>
#      }
#   ]
# }
class TableUsageRequestSerializer(RequestSerializer):

    def serialize(self, request, bos, serial_version):
        ns = NsonSerializer(bos)
        ns.start_map()  # top-level object

        # header
        Proto.start_map(ns, HEADER)
        Proto.write_header(ns, SerdeUtil.OP_CODE.GET_TABLE_USAGE, request)
        Proto.end_map(ns, HEADER)

        # payload
        Proto.start_map(ns, PAYLOAD)
        Proto.write_string_map_field(ns, START, request.get_start_time_string())
        Proto.write_string_map_field(ns, END, request.get_end_time_string())
        Proto.write_int_map_field(ns, LIST_MAX_TO_READ, request.get_limit())
        Proto.write_int_map_field(ns, LIST_START_INDEX,
                                  request.get_start_index())
        Proto.end_map(ns, PAYLOAD)

        ns.end_map()  # top-level object

    def deserialize(self, request, bis, serial_version):
        result = borneo.operations.TableUsageResult()
        walker = MapWalker(bis)
        while walker.has_next():
            walker.next()
            name = walker.get_current_name()
            if name == ERROR_CODE:
                Proto.handle_error_code(walker)
            elif name == TABLE_NAME:
                result.set_table_name(Nson.read_string(bis))
            elif name == LAST_INDEX:
                result.set_last_index_returned(Nson.read_int(bis))
            elif name == TABLE_USAGE:
                t = bis.read_byte()
                if t != SerdeUtil.FIELD_VALUE_TYPE.ARRAY:
                    raise IllegalArgumentException(
                        'Bad type in table usage result: ' + str(t) +
                        ' should be ARRAY')
                SerdeUtil.read_full_int(bis)  # consume total bytes
                num_elements = SerdeUtil.read_full_int(bis)
                usage_records = list()
                for i in range(num_elements):
                    usage_records.append(self._read_usage_record(bis))
                result.set_usage_records(usage_records)
            else:
                walker.skip()

        return result

    @staticmethod
    def _read_usage_record(bis):
        walker = MapWalker(bis)
        start_time = 0
        period = 0
        ru = 0
        wu = 0
        sgb = 0
        rtc = 0
        wtc = 0
        stc = 0
        msup = 0

        while walker.has_next():
            walker.next()
            name = walker.get_current_name()
            if name == START:
                start_time = SerdeUtil.iso_time_to_ms(Nson.read_string(bis))
            elif name == TABLE_USAGE_PERIOD:
                period = Nson.read_int(bis)
            elif name == READ_UNITS:
                ru = Nson.read_int(bis)
            elif name == WRITE_UNITS:
                wu = Nson.read_int(bis)
            elif name == STORAGE_GB:
                sgb = Nson.read_int(bis)
            elif name == READ_THROTTLE_COUNT:
                rtc = Nson.read_int(bis)
            elif name == WRITE_THROTTLE_COUNT:
                wtc = Nson.read_int(bis)
            elif name == STORAGE_THROTTLE_COUNT:
                stc = Nson.read_int(bis)
            elif name == MAX_SHARD_USAGE_PERCENT:
                msup = Nson.read_int(bis)
            else:
                walker.skip()

        return TableUsage(start_time, period, ru, wu, sgb, rtc, wtc, stc, msup)


#
# ListTablesRequest
#
# Payload:
# {
#   "payload" : {
#     LIST_MAX_TO_READ : <optional int>
#     LIST_START_INDEX : <optional int>
#     NAMESPACE : <optional string, onprem only>
#   }
# }
#
# Non-error response
# {
#   TABLES: [name1, name2, ...]
#   LAST_INDEX: <int>
# }

class ListTablesRequestSerializer(RequestSerializer):

    def serialize(self, request, bos, serial_version):
        ns = NsonSerializer(bos)
        ns.start_map()  # top-level object

        # header
        Proto.start_map(ns, HEADER)
        Proto.write_header(ns, SerdeUtil.OP_CODE.LIST_TABLES, request)
        Proto.end_map(ns, HEADER)

        # payload
        Proto.start_map(ns, PAYLOAD)
        Proto.write_int_map_field(ns, LIST_MAX_TO_READ, request.get_limit())
        Proto.write_int_map_field(ns, LIST_START_INDEX,
                                  request.get_start_index())
        Proto.write_string_map_field(ns, NAMESPACE, request.get_namespace())
        Proto.end_map(ns, PAYLOAD)

        ns.end_map()  # top-level object

    def deserialize(self, request, bis, serial_version):
        result = borneo.operations.ListTablesResult()
        table_list = list()
        walker = MapWalker(bis)
        while walker.has_next():
            walker.next()
            name = walker.get_current_name()
            if name == ERROR_CODE:
                Proto.handle_error_code(walker)
            elif name == LAST_INDEX:
                result.set_last_index_returned(Nson.read_int(bis))
            elif name == TABLES:
                t = bis.read_byte()
                if t != SerdeUtil.FIELD_VALUE_TYPE.ARRAY:
                    raise IllegalArgumentException(
                        'Bad type in list tables result: ' + str(t) +
                        ' should be ARRAY')
                SerdeUtil.read_full_int(bis)  # consume total bytes
                num_elements = SerdeUtil.read_full_int(bis)
                for i in range(num_elements):
                    table_list.append(Nson.read_string(bis))
            else:
                walker.skip()

        # if no tables, use empty list
        result.set_tables(table_list)
        return result


#
# GetIndexesRequest
#
# Payload:
# {
#   "payload" : {
#     INDEX : <optional index name string>
#   }
# }
#
# Non-error response
# {
#   INDEXES: [
#    {
#     NAME: <string>
#     FIELDS: [ {PATH: <string>, TYPE: <string>}]
#    }
#   ]
# }

class GetIndexesRequestSerializer(RequestSerializer):

    def serialize(self, request, bos, serial_version):
        ns = NsonSerializer(bos)
        ns.start_map()  # top-level object

        # header
        Proto.start_map(ns, HEADER)
        Proto.write_header(ns, SerdeUtil.OP_CODE.GET_INDEXES, request)
        Proto.end_map(ns, HEADER)

        # payload
        Proto.start_map(ns, PAYLOAD)
        Proto.write_string_map_field(ns, INDEX, request.get_index_name())
        Proto.end_map(ns, PAYLOAD)

        ns.end_map()  # top-level object

    def deserialize(self, request, bis, serial_version):
        result = borneo.operations.GetIndexesResult()
        index_list = list()
        walker = MapWalker(bis)
        while walker.has_next():
            walker.next()
            name = walker.get_current_name()
            if name == ERROR_CODE:
                Proto.handle_error_code(walker)
            elif name == INDEXES:
                t = bis.read_byte()
                if t != SerdeUtil.FIELD_VALUE_TYPE.ARRAY:
                    raise IllegalArgumentException(
                        'Bad type in get indexes result: ' + str(t) +
                        ' should be ARRAY')
                SerdeUtil.read_full_int(bis)  # consume total bytes
                num_elements = SerdeUtil.read_full_int(bis)
                for i in range(num_elements):
                    index_list.append(self._read_index_info(bis))
            else:
                walker.skip()

        # if no indexes, use empty list
        result.set_indexes(index_list)
        return result

    @staticmethod
    def _read_index_info(bis):
        walker = MapWalker(bis)
        index_name = None
        fields = list()
        types = list()
        while walker.has_next():
            walker.next()
            name = walker.get_current_name()
            if name == NAME:
                index_name = Nson.read_string(bis)
            elif name == FIELDS:
                t = bis.read_byte()
                if t != SerdeUtil.FIELD_VALUE_TYPE.ARRAY:
                    raise IllegalArgumentException(
                        'Bad type in get indexes result: ' + str(t) +
                        ' should be ARRAY')
                SerdeUtil.read_full_int(bis)  # consume total bytes
                num_elements = SerdeUtil.read_full_int(bis)
                # array of map with PATH, TYPE elements
                for i in range(num_elements):
                    iwalker = MapWalker(bis)
                    while iwalker.has_next():
                        iwalker.next()
                        fname = iwalker.get_current_name()
                        if fname == PATH:
                            fields.append(Nson.read_string(bis))
                        elif fname == TYPE:
                            types.append(Nson.read_string(bis))
                        else:
                            iwalker.skip()
            else:
                walker.skip()

        if index_name is None:
            raise IllegalArgumentException('Missing name in index info')

        return IndexInfo(index_name, fields, types)


#
# SystemRequest
#
# Payload:
# {
#   "payload" : {
#     "STATEMENT" : <utf-8 encoded version of string>
#   }
# }
#
# Non-error response
# See deserialize_system_result
# {acd
#   SYSOP_STATE: <int>
#   SYSOP_RESULT: <string>
#   STATEMENT: <string>
#   OPERATION_ID: <string>
# }
#
class SystemRequestSerializer(RequestSerializer):

    def serialize(self, request, bos, serial_version):
        ns = NsonSerializer(bos)
        ns.start_map()  # top-level object

        # header
        Proto.start_map(ns, HEADER)
        Proto.write_header(ns, SerdeUtil.OP_CODE.SYSTEM_REQUEST, request)
        Proto.end_map(ns, HEADER)

        # payload
        Proto.start_map(ns, PAYLOAD)
        # use a byte array
        buf = bytearray(request.get_statement().encode('utf-8'))
        Proto.write_bin_map_field(ns, STATEMENT, buf)
        Proto.end_map(ns, PAYLOAD)

        ns.end_map()  # top-level object

    def deserialize(self, request, bis, serial_version):
        return Proto.deserialize_system_result(bis)


#
# SystemStatus
#
# Payload:
# {
#   "payload" : {
#     STATEMENT : <string>
#     OPERATION_ID : <string>
#   }
# }
#
# Non-error response
# See deserialize_system_result
#
class SystemStatusRequestSerializer(RequestSerializer):

    def serialize(self, request, bos, serial_version):
        ns = NsonSerializer(bos)
        ns.start_map()  # top-level object

        # header
        Proto.start_map(ns, HEADER)
        Proto.write_header(ns, SerdeUtil.OP_CODE.SYSTEM_STATUS_REQUEST, request)
        Proto.end_map(ns, HEADER)

        # payload
        Proto.start_map(ns, PAYLOAD)
        Proto.write_string_map_field(ns, STATEMENT, request.get_statement())
        Proto.write_string_map_field(ns, OPERATION_ID,
                                     request.get_operation_id())
        Proto.end_map(ns, PAYLOAD)

        ns.end_map()  # top-level object

    def deserialize(self, request, bis, serial_version):
        return Proto.deserialize_system_result(bis)


#
# MultiDeleteRequest
#
# Payload:
# {
#   "payload" : {
# shared with all Delete ops
#     DURABILITY : <int>
#     KEY : {}
#     RANGE : {}
#     MAX_WRITE_KB : <int>
#     CONTINUATION_KEY : <binary>
#   }
# }
#
# Non-error response:
# {
#   "CONSUMED" : {
#      "READ_UNITS" : <int>
#      "READ_KV" : <int>
#      "WRITE_UNITS" : <int>
#   }
#   NUM_DELETIONS: <int>
#   CONTINUATION_KEY: <binary>
# }
#
class MultiDeleteRequestSerializer(RequestSerializer):

    def serialize(self, request, bos, serial_version):

        ns = NsonSerializer(bos)
        ns.start_map()  # top-level object

        # header
        Proto.start_map(ns, HEADER)
        Proto.write_header(ns, SerdeUtil.OP_CODE.MULTI_DELETE, request)
        Proto.end_map(ns, HEADER)

        # payload
        Proto.start_map(ns, PAYLOAD)
        Proto.write_int_map_field(ns, MAX_WRITE_KB, request.get_max_write_kb())
        Proto.write_bin_map_field(ns, CONTINUATION_KEY,
                                  request.get_continuation_key())
        Proto.write_durability(ns, request)
        self._write_field_range(ns, request.get_range())
        Proto.write_key(ns, request.get_key())

        Proto.end_map(ns, PAYLOAD)

        ns.end_map()  # top-level object

    def deserialize(self, request, bis, serial_version):
        result = borneo.operations.MultiDeleteResult()
        walker = MapWalker(bis)
        while walker.has_next():
            walker.next()
            name = walker.get_current_name()
            if name == ERROR_CODE:
                Proto.handle_error_code(walker)
            elif name == CONSUMED:
                Proto.read_consumed_capacity(bis, result)
            elif name == NUM_DELETIONS:
                result.set_num_deletions(Nson.read_int(bis))
            elif name == CONTINUATION_KEY:
                result.set_continuation_key(Nson.read_binary(bis))
            elif name == TOPOLOGY_INFO:
                Proto.read_topology_info(bis, result);
            else:
                walker.skip()

        return result

    #
    # "range": {
    #   "path": path to field (string)
    #   "start" {
    #      "value": {FieldValue}
    #      "inclusive": bool
    #   }
    #   "end" {
    #      "value": {FieldValue}
    #      "inclusive": bool
    #   }
    #
    @staticmethod
    def _write_field_range(ns, key_range):
        if key_range is None:
            return
        Proto.start_map(ns, RANGE)
        Proto.write_string_map_field(ns, RANGE_PATH, key_range.get_field_path())
        if key_range.get_start() is not None:
            Proto.start_map(ns, START)
            Proto.write_value(ns, key_range.get_start())
            Proto.write_bool_map_field(ns, INCLUSIVE,
                                       key_range.get_start_inclusive())
            Proto.end_map(ns, START)
        if key_range.get_end() is not None:
            Proto.start_map(ns, END)
            Proto.write_value(ns, key_range.get_end())
            Proto.write_bool_map_field(ns, INCLUSIVE,
                                       key_range.get_end_inclusive())
            Proto.end_map(ns, END)
        Proto.end_map(ns, RANGE)


#
# WriteMultipleRequest
#
# Header:
#  Normal header but...
#    if one table, table name, if > 1 table, table names are in the ops
#
# Payload:
# {
#   "payload" : {
#     DURABILITY : <int>
#     NUM_OPERATIONS: <int>
#     OPERATIONS: [ array, for each op...
#       TABLE_NAME: <optional string, if using parent/child>
#       OP_CODE: <int>
#       ABORT_ON_FAIL: <bool>
#       RETURN_ROW:
#       delete or put op info w/o durability
#   }
# }
#
# Non-error response:
# {
#   "CONSUMED" : {
#      "READ_UNITS" : <int>
#      "READ_KV" : <int>
#      "WRITE_UNITS" : <int>
#   }
#   WM_SUCCESS: [{}]
#   WM_FAILURE: {
#     WM_FAIL_INDEX: <int>
#     WM_FAIL_RESULT: {}
#   }
# }
#
class WriteMultipleRequestSerializer(RequestSerializer):

    def serialize(self, request, bos, serial_version):

        ns = NsonSerializer(bos)
        ns.start_map()  # top-level object

        # header -- special header handling because of single vs multiple tables
        Proto.start_map(ns, HEADER)
        Proto.write_int_map_field(ns, VERSION,
                                  SerdeUtil.SERIAL_VERSION_4)
        if request.is_single_table():
            Proto.write_string_map_field(ns, TABLE_NAME,
                                         request.get_table_name())
        Proto.write_int_map_field(ns, OP_CODE, SerdeUtil.OP_CODE.WRITE_MULTIPLE)
        Proto.write_int_map_field(ns, TIMEOUT, request.get_timeout())
        Proto.end_map(ns, HEADER)

        # payload
        # IMPORTANT: durability MUST be ordered
        # ahead of the operations or the server can't easily
        # deserialize efficiently
        Proto.start_map(ns, PAYLOAD)
        # common to all ops
        Proto.write_durability(ns, request)
        Proto.write_int_map_field(ns, NUM_OPERATIONS,
                                  request.get_num_operations())
        Proto.start_array(ns, OPERATIONS)
        for op in request.get_operations():
            _write_multi_op(ns, op, request.is_single_table())
        Proto.end_array(ns, OPERATIONS)
        Proto.end_map(ns, PAYLOAD)

        ns.end_map()  # top-level object

    def deserialize(self, request, bis, serial_version):
        result = borneo.operations.WriteMultipleResult()
        walker = MapWalker(bis)
        while walker.has_next():
            walker.next()
            name = walker.get_current_name()
            if name == ERROR_CODE:
                Proto.handle_error_code(walker)
            elif name == CONSUMED:
                Proto.read_consumed_capacity(bis, result)
            elif name == WM_SUCCESS:
                # array of map
                t = bis.read_byte()
                if t != SerdeUtil.FIELD_VALUE_TYPE.ARRAY:
                    raise IllegalArgumentException(
                        'Bad type in write multiple: ' + str(t) +
                        ' should be ARRAY')
                SerdeUtil.read_full_int(bis)  # consume total bytes
                num_elements = SerdeUtil.read_full_int(bis)
                # array of map
                for i in range(num_elements):
                    result.add_result(self._read_operation_result(bis))
            elif name == WM_FAILURE:
                # a map
                fwalker = MapWalker(bis)
                while fwalker.has_next():
                    fwalker.next()
                    fname = fwalker.get_current_name()
                    if fname == WM_FAIL_INDEX:
                        result.set_failed_operation_index(Nson.read_int(bis))
                    elif fname == WM_FAIL_RESULT:
                        result.add_result(self._read_operation_result(bis))
                    else:
                        fwalker.skip()
            elif name == TOPOLOGY_INFO:
                Proto.read_topology_info(bis, result);
            else:
                walker.skip()
        return result

    #
    # Each op is an anonymous map inside and array of maps
    #

    @staticmethod
    def _read_operation_result(bis):
        opres = borneo.operations.OperationResult()
        walker = MapWalker(bis)
        while walker.has_next():
            walker.next()
            name = walker.get_current_name()
            if name == SUCCESS:
                opres.set_success(Nson.read_boolean(bis))
            elif name == ROW_VERSION:
                opres.set_version(Version.create_version(
                    Nson.read_binary(bis)))
            elif name == GENERATED:
                opres.set_generated_value(Proto.nson_to_value(bis))
            elif name == RETURN_INFO:
                Proto.read_return_info(bis, opres)
            else:
                walker.skip()
        return opres


def _write_multi_op(ns, op, is_single_table):
    ns.start_array_field()
    ns.start_map()
    rq = op.get_request()
    is_put = isinstance(rq, borneo.operations.PutRequest)
    if is_put:
        opcode = SerdeUtil.get_put_op_code(rq)
    else:
        match_version = rq.get_match_version()
        opcode = (SerdeUtil.OP_CODE.DELETE if match_version is None else
                  SerdeUtil.OP_CODE.DELETE_IF_VERSION)

    # write op first -- important!
    if not is_single_table:
        Proto.write_string_map_field(ns, TABLE_NAME, rq.get_table_name())
    Proto.write_int_map_field(ns, OP_CODE, opcode)
    if is_put:
        PutRequestSerializer.write_put_request(ns, rq)
    else:
        DeleteRequestSerializer.write_delete_request(ns, rq)

    # common to delete and put
    Proto.write_bool_map_field(ns, RETURN_ROW, rq.get_return_row())
    Proto.write_bool_map_field(ns, ABORT_ON_FAIL,
                               op.is_abort_if_unsuccessful())
    ns.end_map()
    ns.end_array_field()


#
# PrepareRequest
#
# Header:
#  Normal header but no table name
#
# Payload:
# {
#   "payload" : {
#     QUERY_VERSION : <int>
#     STATEMENT: <string>
#     GET_QUERY_PLAN: <bool>
#     GET_QUERY_SCHEMA: <bool>
#   }
# }
#
# Non-error response:
#   See deserialize_prepare_or_query
#
class PrepareRequestSerializer(RequestSerializer):

    def serialize(self, request, bos, serial_version):
        ns = NsonSerializer(bos)
        ns.start_map()  # top-level object

        Proto.start_map(ns, HEADER)
        Proto.write_header(ns, SerdeUtil.OP_CODE.PREPARE, request)
        Proto.end_map(ns, HEADER)

        Proto.start_map(ns, PAYLOAD)
        Proto.write_int_map_field(ns, QUERY_VERSION, request.get_query_version())
        Proto.write_string_map_field(ns, STATEMENT, request.get_statement())
        if request.get_query_plan():
            Proto.write_bool_map_field(ns, GET_QUERY_PLAN, request.get_query_plan())
        if request.get_query_schema():
            Proto.write_bool_map_field(ns, GET_QUERY_SCHEMA,
                                       request.get_query_schema())

        Proto.end_map(ns, PAYLOAD)

        ns.end_map()  # top-level object

    def deserialize(self, request, bis, serial_version):
        result = borneo.operations.PrepareResult()
        Proto.deserialize_prepare_or_query(None, None,  # query request/result
                                           request, result,
                                           bis)
        return result


#
# QueryRequest
#
# Header:
#  Normal header but no table name
#
# Payload:
# {
#   "payload" : {
#     QUERY_VERSION : <int>
#     if non-zero (all int):
#       MAX_READ_KB, MAX_WRITE_KB, NUMBER_LIMIT, TRACE_LEVEL
#     if prepared:
#       IS_PREPARED: <bool>
#       IS_SIMPLE_QUERY: <bool>
#       PREPARED_QUERY: <string -- orig query>
#       bind variables
#     else
#       STATEMENT: <string>
#     CONTINUATION_KEY: <binary>
#     math context
#     SHARD_ID: <int>
#     TOPO_SEQ_NUM: <int>
#   }
# }
#
# Non-error response:
#   See deserialize_prepare_or_query
#
class QueryRequestSerializer(RequestSerializer):

    def serialize(self, request, bos, serial_version):

        ns = NsonSerializer(bos)
        ns.start_map()  # top-level object

        Proto.start_map(ns, HEADER)
        Proto.write_header(ns, SerdeUtil.OP_CODE.QUERY, request)
        Proto.end_map(ns, HEADER)

        Proto.start_map(ns, PAYLOAD)
        Proto.write_int_map_field(ns, QUERY_VERSION, request.get_query_version())
        Proto.write_int_map_field_not_zero(
            ns, MAX_READ_KB, request.get_max_read_kb())
        Proto.write_int_map_field_not_zero(
            ns, MAX_WRITE_KB, request.get_max_write_kb())
        Proto.write_int_map_field_not_zero(
            ns, NUMBER_LIMIT, request.get_limit())
        Proto.write_int_map_field_not_zero(
            ns, TRACE_LEVEL, request.get_trace_level())
        if request.get_trace_level() > 0:
            Proto.write_bool_map_field(ns, TRACE_TO_LOG_FILES,
                                       request.get_log_file_tracing())
            Proto.write_int_map_field(ns, BATCH_COUNTER, request.get_batch_counter())
        Proto.write_consistency(ns, request.get_consistency())
        Proto.write_consistency(ns, request.get_consistency())
        Proto.write_durability(ns, request)

        if request.is_prepared():
            Proto.write_bool_map_field(ns, IS_PREPARED, True)
            Proto.write_bool_map_field(ns, IS_SIMPLE_QUERY, request.is_simple_query())
            Proto.write_bin_map_field(
                ns, PREPARED_QUERY, request.get_prepared_statement().get_statement())
            self._write_bind_variables(
                ns, request.get_prepared_statement().get_variables())
        else:
            Proto.write_string_map_field(ns, STATEMENT, request.get_statement())

        if request.get_cont_key() is not None:
            Proto.write_bin_map_field(
                ns, CONTINUATION_KEY, request.get_cont_key())
        #
        # server memory consumption is purposely left out as not necessary
        # at this time (see Java)
        #

        if request.get_math_context() is not None:
            self._write_math_context(ns, request.get_math_context())
        if request.get_shard_id() != -1:
            Proto.write_int_map_field(ns, SHARD_ID, request.get_shard_id())

        if request.get_query_version() >= QueryDriver.QUERY_V4:
            if request.get_query_name() is not None:
                Proto.write_string_map_field(ns, QUERY_NAME,
                                             request.get_query_name())
            if request.get_virtual_scan() is not None:
                Proto.write_virtual_scan(ns, request.get_virtual_scan())

        Proto.end_map(ns, PAYLOAD)

        ns.end_map()  # top-level object

    @staticmethod
    def _write_bind_variables(ns, variables):
        if variables is None or len(variables) == 0:
            return
        Proto.start_array(ns, BIND_VARIABLES)
        for key in variables:
            ns.start_array_field()
            ns.start_map(0)
            Proto.write_string_map_field(ns, NAME, key)
            ns.start_map_field(VALUE)
            Proto.write_field_value(ns, variables[key])
            ns.end_map_field(VALUE)
            ns.end_map(0)
            ns.end_array_field()
        Proto.end_array(ns, BIND_VARIABLES)

    @staticmethod
    def _write_math_context(ns, context):
        Proto.write_int_map_field(ns, MATH_CONTEXT_CODE, 5)
        Proto.write_int_map_field(ns, MATH_CONTEXT_PRECISION, context.prec)
        Proto.write_int_map_field(
            ns, MATH_CONTEXT_ROUNDING_MODE,
            math_name_to_value.get(context.rounding))

    def deserialize(self, request, bis, serial_version):
        result = borneo.operations.QueryResult(request)
        Proto.deserialize_prepare_or_query(
            request, result,
            None, None,  # prepare request/result
            bis)
        return result


#
# AddReplicaRequest
#
# Header:
#  Normal header table name required
#
# Payload:
# {
#   "payload" : {
#      region: str, required
#      if not 0 or None, read units, write units, match_etag
#   }
# }
#
# Non-error response:
#  TableResult
#
class AddReplicaRequestSerializer(RequestSerializer):

    def serialize(self, request, bos, serial_version):
        ns = NsonSerializer(bos)
        ns.start_map()  # top-level object

        Proto.start_map(ns, HEADER)
        Proto.write_header(ns, SerdeUtil.OP_CODE.ADD_REPLICA, request)
        Proto.end_map(ns, HEADER)

        Proto.start_map(ns, PAYLOAD)
        Proto.write_string_map_field(ns, REGION, request.get_replica_name())
        Proto.write_int_map_field_not_zero(ns, READ_UNITS,
                                               request.get_read_units())
        Proto.write_int_map_field_not_zero(ns, WRITE_UNITS,
                                               request.get_write_units())
        Proto.write_string_map_field(ns, ETAG, request.get_match_etag())
        Proto.end_map(ns, PAYLOAD)

        ns.end_map()  # top-level object

    def deserialize(self, request, bis, serial_version):
        return Proto.deserialize_table_result(bis)

#
# DropReplicaRequest
#
# Header:
#  Normal header table name required
#
# Payload:
# {
#   "payload" : {
#      region: str, required
#      if not None,  match_etag
#   }
# }
#
# Non-error response:
#  TableResult
#
class DropReplicaRequestSerializer(RequestSerializer):

    def serialize(self, request, bos, serial_version):
        ns = NsonSerializer(bos)
        ns.start_map()  # top-level object

        Proto.start_map(ns, HEADER)
        Proto.write_header(ns, SerdeUtil.OP_CODE.DROP_REPLICA, request)
        Proto.end_map(ns, HEADER)

        Proto.start_map(ns, PAYLOAD)
        Proto.write_string_map_field(ns, REGION, request.get_replica_name())
        Proto.write_string_map_field(ns, ETAG, request.get_match_etag())
        Proto.end_map(ns, PAYLOAD)

        ns.end_map()  # top-level object

    def deserialize(self, request, bis, serial_version):
        return Proto.deserialize_table_result(bis)

#
# ReplicaStatsRequest
#
# Header:
#  Normal header table name required
#
# Payload:
# {
#   "payload" : {
#      region: str, required
#      optional start_time str
#      optional limit int
#   }
# }
#
# Non-error response:
#  GetReplicaStatsResult:
#
#   table_name (string)
#   next_start_time (int)
#   replica_stats (Map<string, Array<ReplicaStats>>)
#       key - region (string)
#       value - array of ReplicaStats
#       ReplicaStats:
#         time (int)
#         replica_lag (int)
#
class ReplicaStatsRequestSerializer(RequestSerializer):

    def serialize(self, request, bos, serial_version):
        ns = NsonSerializer(bos)
        ns.start_map()  # top-level object

        Proto.start_map(ns, HEADER)
        Proto.write_header(ns, SerdeUtil.OP_CODE.GET_REPLICA_STATS, request)
        Proto.end_map(ns, HEADER)

        Proto.start_map(ns, PAYLOAD)
        Proto.write_string_map_field(ns, REGION, request.get_replica_name())
        Proto.write_string_map_field(ns, START, request.get_start_time_str())
        Proto.write_int_map_field_not_zero(ns, LIST_MAX_TO_READ,
                                               request.get_limit())
        Proto.end_map(ns, PAYLOAD)

        ns.end_map()  # top-level object

    def deserialize(self, request, bis, serial_version):
        result = borneo.operations.ReplicaStatsResult()
        walker = MapWalker(bis)
        while walker.has_next():
            walker.next()
            name = walker.get_current_name()
            if name == ERROR_CODE:
                Proto.handle_error_code(walker)
            elif name == TABLE_NAME:
                result.set_table_name(Nson.read_string(bis))
            elif name == NEXT_START_TIME:
                result.set_next_start_time(Nson.read_long(bis))
            elif name == REPLICA_STATS:
                ReplicaStatsRequestSerializer.read_replica_stats(bis, result)
            else:
                walker.skip()

        return result

    #
    # Map, key is string, value is ReplicaStats
    #  key - region (str)
    #  value -- array of ReplicaStats
    #      time (long)
    #      lag (int)
    #
    @staticmethod
    def read_replica_stats(bis, result):
        records_map = dict()
        walker = MapWalker(bis)
        while walker.has_next():
            walker.next()
            replica_name = walker.get_current_name()
            # value is array of stats
            t = bis.read_byte()
            if t != SerdeUtil.FIELD_VALUE_TYPE.ARRAY:
                raise IllegalArgumentException(
                    'Bad type in GetReplicaStats: ' + str(t) +
                    ' should be ARRAY')
            # consume total size of array
            SerdeUtil.read_full_int(bis)  # total length in bytes
            num_elements = SerdeUtil.read_full_int(bis)
            records = list()
            for i in range(0,num_elements):
                stats = ReplicaStats()
                swalker = MapWalker(bis)
                while swalker.has_next():
                    swalker.next()
                    name = swalker.get_current_name()
                    if name == TIME:
                        stats._collection_time_millis = Nson.read_long(bis)
                    elif name == REPLICA_LAG:
                        stats._replica_lag = Nson.read_int(bis)
                    else:
                        swalker.skip()

                records.append(stats);
            records_map[replica_name] = records
        result.set_stats_records(records_map)

# noinspection PyArgumentEqualDefault
class Proto(object):
    #
    # Common methods for serializers
    #

    #
    # Serialization/write methods first
    #
    @staticmethod
    def write_header(ns, op, request):
        Proto.write_int_map_field(ns, VERSION,
                                  SerdeUtil.SERIAL_VERSION_4)
        if request.get_table_name() is not None:
            Proto.write_string_map_field(ns, TABLE_NAME,
                                         request.get_table_name())
        Proto.write_int_map_field(ns, OP_CODE, op)
        Proto.write_int_map_field(ns, TOPO_SEQ_NUM, request.get_topo_seq_num())
        Proto.write_int_map_field(ns, TIMEOUT, request.get_timeout())

    @staticmethod
    def write_consistency(ns, consistency):
        if consistency is not None:
            Proto.start_map(ns, CONSISTENCY)
            Proto.write_int_map_field(ns, TYPE, consistency)
            Proto.end_map(ns, CONSISTENCY)

    @staticmethod
    def write_limits(ns, limits):
        if limits is not None:
            Proto.start_map(ns, LIMITS)
            Proto.write_int_map_field(ns, READ_UNITS, limits.get_read_units())
            Proto.write_int_map_field(ns, WRITE_UNITS, limits.get_write_units())
            Proto.write_int_map_field(ns, STORAGE_GB, limits.get_storage_gb())
            Proto.write_int_map_field(ns, LIMITS_MODE, limits.get_mode())
            Proto.end_map(ns, LIMITS)

    #
    # tags are dict() of string in Python but sent and received as a single
    # JSON string
    #
    @staticmethod
    def write_tags(ns, request):
        if request.get_defined_tags() is not None:
            Proto.write_string_map_field(
                ns, DEFINED_TAGS, Proto.value_to_json(
                    request.get_defined_tags()))
        if request.get_free_form_tags() is not None:
            Proto.write_string_map_field(
                ns, FREE_FORM_TAGS, Proto.value_to_json(
                    request.get_free_form_tags()))

    @staticmethod
    def write_key(ns, key):
        # use ns to start/end the field; the key serialization will start
        # and end the map itself that represents the key value
        ns.start_map_field(KEY)
        Proto.write_field_value(ns, key)
        ns.end_map_field(KEY)

    @staticmethod
    def write_write_request(ns, request):
        Proto.write_durability(ns, request)
        Proto.write_bool_map_field(ns, RETURN_ROW, request.get_return_row())

    @staticmethod
    def write_durability(ns, request):
        dur = request.get_durability()
        # Don't bother writing it if not set, use proxy default
        if dur is None:
            return
        dur_val = dur.master_sync
        dur_val |= (dur.replica_sync << 2)
        dur_val |= (dur.replica_ack << 4)
        Proto.write_int_map_field(ns, DURABILITY, dur_val)

    #
    # This writes a "value" key in a map with a value of a FieldValue
    #
    # The value in this path must be a dict
    #
    @staticmethod
    def write_value(ns, value):
        if value is not None:
            ns.start_map_field(VALUE)
            Nson.generate_events_from_value(value, ns)
            ns.end_map_field(VALUE)

    #
    # This writes a field_value by generating NSON events. The ns parameter
    # is a serializer that turns those events into NSON in the output stream
    #
    # The value in this path must be a dict
    #
    @staticmethod
    def write_field_value(ns, value):
        Nson.generate_events_from_value(value, ns)

    # atomic fields
    # Java uses type-specific overloads to differentiate the atomic values
    # integer, string, boolean, binary
    # All the callers know the type, so in Python make it part of the
    # method vs checking types, which is inefficient
    #
    @staticmethod
    def write_int_map_field(ns, name, value):
        ns.start_map_field(name)
        ns.integer_value(value)
        ns.end_map_field(name)

    @staticmethod
    def write_int_map_field_not_zero(ns, name, value):
        if value != 0:
            ns.start_map_field(name)
            ns.integer_value(value)
            ns.end_map_field(name)

    @staticmethod
    def write_string_map_field(ns, name, value):
        if value is not None:
            ns.start_map_field(name)
            ns.string_value(value)
            ns.end_map_field(name)

    @staticmethod
    def write_bool_map_field(ns, name, value):
        if value is not None:
            ns.start_map_field(name)
            ns.boolean_value(value)
            ns.end_map_field(name)

    @staticmethod
    def write_bin_map_field(ns, name, value):
        if value is not None:
            ns.start_map_field(name)
            ns.binary_value(value)
            ns.end_map_field(name)

    @staticmethod
    def write_int_array_map_field(ns, name, value):
        if value is not None:
            Proto.start_array(ns, name)
            for v in values:
                ns.start_array_field()
                ns.integer_value(v)
                ns.end_array_field()
            Proto.end_array(ns, name)

    #
    # start/end complex types
    #
    @staticmethod
    def start_map(ns, name):
        ns.start_map_field(name)
        ns.start_map()

    @staticmethod
    def end_map(ns, name):
        ns.end_map()
        ns.end_map_field(name)

    @staticmethod
    def start_array(ns, name):
        ns.start_map_field(name)
        ns.start_array()

    @staticmethod
    def end_array(ns, name):
        ns.end_array()
        ns.end_map_field(name)

    #
    #
    #
    @staticmethod
    def write_virtual_scan(ns, vs):
        start_map(ns, VIRTUAL_SCAN)
        Proto.write_int_map_field(ns, VIRTUAL_SCAN_SID, vs[VIRTUAL_SCAN_SID])
        Proto.write_int_map_field(ns, VIRTUAL_SCAN_PID, vs[VIRTUAL_SCAN_PID])
        info_sent = vs['info_sent']
        if not info_sent:
            Proto.write_bin_map_field(ns, VIRTUAL_SCAN_PRIM_KEY,
                                          vs[VIRTUAL_SCAN_PRIM_KEY])
            Proto.write_bin_map_field(ns, VIRTUAL_SCAN_SEC_KEY,
                                          vs[VIRTUAL_SCAN_SEC_KEY])
            Proto.write_bool_map_field(ns, VIRTUAL_SCAN_MOVE_AFTER,
                                           vs[VIRTUAL_SCAN_MOVE_AFTER])
            Proto.write_bin_map_field(ns, VIRTUAL_SCAN_JOIN_DESC_RESUME_KEY,
                                          vs[VIRTUAL_SCAN_JOIN_DESC_RESUME_KEY])
            Proto.write_int_array_map_field(ns,
                                            VIRTUAL_SCAN_JOIN_PATH_TABLES,
                                            vs[VIRTUAL_SCAN_JOIN_PATH_TABLES])
            Proto.write_bin_map_field(ns, VIRTUAL_SCAN_JOIN_PATH_KEY,
                                          vs[VIRTUAL_SCAN_JOIN_PATH_KEY])
            Proto.write_bin_map_field(ns, VIRTUAL_SCAN_JOIN_PATH_SEC_KEY,
                                          vs[VIRTUAL_SCAN_JOIN_PATH_SEC_KEY])
            Proto.write_bool_map_field(ns, VIRTUAL_SCAN_JOIN_PATH_MATCHED,
                                           vs[VIRTUAL_SCAN_JOIN_PATH_MATCHED])

        end_map(ns, VIRTUAL_SCAN)


    #
    # Deserialization/read methods
    #

    # TableResult
    # {
    #   "ERROR_CODE" : 0
    #   "TABLE_NAME" : <string>        # required
    #   "TABLE_STATE" : int            # required
    #   "LIMITS" : {                   # required
    #      "READ_UNITS" : <int>
    #      "WRITE_UNITS" : <int>
    #      "STORAGE_GB" : <int>
    #      "LIMITS_MODE" : <int>       # PROVISIONED vs ON-DEMAND
    #   "COMPARTMENT_OCID" : <string>  # optional - cloud only
    #   "NAMESPACE" : <string>         # optional, on-prem only
    #   "TABLE_OCID" " : <string>      # optional, cloud-only
    #   "TABLE_SCHEMA" : <string>      # optional
    #   "TABLE_DDL" : <string>         # optional
    #   "OPERATION_ID" : <string>      # optional
    #   "FREE_FORM_TAGS" : <string>    # optional, cloud-only
    #   "DEFINED_TAGS" :   <string>    # optional, cloud-only
    # }

    @staticmethod
    def deserialize_table_result(bis):
        result = borneo.operations.TableResult()
        # save and reset offset in stream
        bis.set_offset(0)
        walker = MapWalker(bis)

        while walker.has_next():
            walker.next()
            name = walker.get_current_name()
            if name == ERROR_CODE:
                Proto.handle_error_code(walker)
            elif name == COMPARTMENT_OCID:
                result.set_compartment_id(Nson.read_string(bis))
            elif name == NAMESPACE:
                result.set_namespace(Nson.read_string(bis))
            elif name == TABLE_OCID:
                result.set_table_id(Nson.read_string(bis))
            elif name == TABLE_NAME:
                result.set_table_name(Nson.read_string(bis))
            elif name == TABLE_STATE:
                result.set_state(SerdeUtil.get_table_state(
                    Nson.read_int(bis)))
            elif name == TABLE_SCHEMA:
                result.set_schema(Nson.read_string(bis))
            elif name == TABLE_DDL:
                result.set_ddl(Nson.read_string(bis))
            elif name == OPERATION_ID:
                result.set_operation_id(Nson.read_string(bis))
            elif name == FREE_FORM_TAGS:
                result.set_free_form_tags(json.loads(Nson.read_string(bis)))
            elif name == DEFINED_TAGS:
                result.set_defined_tags(json.loads(Nson.read_string(bis)))
            elif name == ETAG:
                result.set_match_etag(Nson.read_string(bis))
            elif name == SCHEMA_FROZEN:
                result.set_schema_frozen(Nson.read_boolean(bis))
            elif name == INITIALIZED:
                result.set_local_replica_initialized(Nson.read_boolean(bis))
            elif name == REPLICAS:
                Proto.read_replicas(bis, result)
            elif name == LIMITS:
                lw = MapWalker(bis)
                ru = 0
                wu = 0
                sg = 0
                mode = SerdeUtil.CAPACITY_MODE.PROVISIONED
                while lw.has_next():
                    lw.next()
                    name = lw.get_current_name()
                    if name == READ_UNITS:
                        ru = Nson.read_int(bis)
                    elif name == WRITE_UNITS:
                        wu = Nson.read_int(bis)
                    elif name == STORAGE_GB:
                        sg = Nson.read_int(bis)
                    elif name == LIMITS_MODE:
                        mode = Nson.read_int(bis)
                    else:
                        lw.skip()
                result.set_table_limits(TableLimits(ru, wu, sg, mode))
            else:
                walker.skip()
        return result

    @staticmethod
    def read_replicas(bis, result):
        # array of replicas
        t = bis.read_byte()
        if t != SerdeUtil.FIELD_VALUE_TYPE.ARRAY:
            raise IllegalArgumentException(
                'Bad type in ReadReplicas in TableResult: ' + str(t) +
                ' should be ARRAY')
        SerdeUtil.read_full_int(bis)  # total length in bytes
        num_elements = SerdeUtil.read_full_int(bis)
        replicas = list()
        for i in range(0,num_elements):
            replica = Replica()
            Proto.read_replica(bis, replica)
            replicas.append(replica)
        result.set_replicas(replicas)


    @staticmethod
    def read_replica(bis, replica):
        walker = MapWalker(bis)
        while walker.has_next():
            walker.next()
            name = walker.get_current_name()
            if name == REGION:
                replica._replica_name = Nson.read_string(bis)
            elif name == TABLE_OCID:
                replica._replica_ocid = Nson.read_string(bis)
            elif name == WRITE_UNITS:
                replica._write_units = Nson.read_int(bis)
            elif name == LIMITS_MODE:
                replica._capacity_mode = Nson.read_int(bis)
            elif name == TABLE_STATE:
                replica._replica_state = (
                    SerdeUtil.get_table_state(Nson.read_int(bis)))
            else:
                walker.skip()


    # {
    #   SYSOP_STATE: <int>
    #   SYSOP_RESULT: <string>
    #   STATEMENT: <string>
    #   OPERATION_ID: <string>
    # }
    @staticmethod
    def deserialize_system_result(bis):
        result = borneo.operations.SystemResult()
        bis.set_offset(0)
        walker = MapWalker(bis)

        while walker.has_next():
            walker.next()
            name = walker.get_current_name()
            if name == ERROR_CODE:
                Proto.handle_error_code(walker)
            elif name == SYSOP_STATE:
                result.set_state(
                    SerdeUtil.get_operation_state(Nson.read_int(bis)))
            elif name == SYSOP_RESULT:
                result.set_result_string(Nson.read_string(bis))
            elif name == STATEMENT:
                result.set_statement(Nson.read_string(bis))
            elif name == OPERATION_ID:
                result.set_operation_id(Nson.read_string(bis))
            else:
                walker.skip()
        return result

    @staticmethod
    def read_consumed_capacity(bis, result):
        walker = MapWalker(bis)
        while walker.has_next():
            walker.next()
            name = walker.get_current_name()
            if name == READ_UNITS:
                result.set_read_units(Nson.read_int(bis))
            elif name == READ_KB:
                result.set_read_kb(Nson.read_int(bis))
            elif name == WRITE_KB:
                units = Nson.read_int(bis)
                result.set_write_units(units)
                result.set_write_kb(units)
            else:
                walker.skip()

    @staticmethod
    def read_row(bis, result):
        walker = MapWalker(bis)
        while walker.has_next():
            walker.next()
            name = walker.get_current_name()
            if name == MODIFIED:
                result.set_modification_time(Nson.read_long(bis))
            elif name == EXPIRATION:
                result.set_expiration_time(Nson.read_long(bis))
            elif name == ROW_VERSION:
                result.set_version(
                    Version.create_version(Nson.read_binary(bis)))
            elif name == VALUE:
                result.set_value(Proto.nson_to_value(bis))
            else:
                walker.skip()

    @staticmethod
    def nson_to_value(bis, ordered=True):
        """
        Deserializes NSON into a value

        :param bis: the stream containing NSON
        :type bis: ByteInputStream
        :param ordered: True (default) for using OrderedDict vs dict
        :type ordered: bool
        :returns: object
        """
        fvc = FieldValueCreator(ordered)
        Nson.generate_events_from_nson(bis, fvc, False)
        return fvc.get_current_value()

    @staticmethod
    def nson_to_json(stream, offset=0, pretty=False):
        """
        Serializes the NSON to JSON in a non-destructive manner, leaving
        the original stream unmodified

        :param stream: the stream containing NSON
        :type stream: ByteInputStream
        :param offset: the offset in the stream to use, defaults to 0
        :type offset: int
        :param pretty: controls pretty printing, defaults to not pretty
        :type pretty: bool
        :returns: str
        """
        js = JsonSerializer(pretty)
        new_stream = ByteInputStream(stream.get_content())
        new_stream.set_offset(offset)
        Nson.generate_events_from_nson(new_stream, js, False)
        return str(js)

    @staticmethod
    def value_to_json(value, pretty=False):
        """
        Serializes the value to JSON

        :param value: the value to serialize
        :type value: object
        :param pretty: controls pretty printing, defaults to not pretty
        :type pretty: bool
        :returns: str
        """
        js = JsonSerializer(pretty, use_single_quote=True)
        Nson.generate_events_from_value(value, js, False)
        return str(js)

    @staticmethod
    def value_to_nson(value):
        """
        Serializes the value to an NSON stream in a bytearray

        :param value: the value to serialize
        :type value: object
        :returns: bytearray
        """
        content = bytearray()
        bos = ByteOutputStream(content)
        ns = NsonSerializer(bos)
        Nson.generate_events_from_value(value, ns)
        return content

    #
    # "return_info" : {
    #   "existing_value" : {}
    #   "existing_version" : byte[]
    #   "existing_mod" : long
    #   "existing_expiration" : long  # not yet implemented
    # }
    #
    @staticmethod
    def read_return_info(bis, result):
        walker = MapWalker(bis)
        while walker.has_next():
            walker.next()
            name = walker.get_current_name()
            if name == EXISTING_MOD_TIME:
                result.set_existing_modification_time(Nson.read_long(bis))
            elif name == EXISTING_VERSION:
                result.set_existing_version(Version.create_version(
                    Nson.read_binary(bis)))
            elif name == EXISTING_VALUE:
                result.set_existing_value(Proto.nson_to_value(bis))
            else:
                walker.skip()

    # Queries are complicated... share code for deserializing Query and Prepare
    # results. Because an initial query request has an implied prepare the
    # deserialization of an unprepared query is a superset of a prepare request
    #
    @staticmethod
    def deserialize_prepare_or_query(
            query_request, query_result,
            prepare_request, prepare_result,
            bis):

        # ps is PreparedStatement
        request_was_prepared = False
        if query_request is not None:
            ps = query_request.get_prepared_statement()
            if ps is not None:
                request_was_prepared = True

        # variables used to construct a PreparedStatement as needed
        dpi = None  # driver plan info
        query_plan = None
        query_schema = None
        table_name = None
        namespace = None
        operation = None
        proxy_topo_seqnum = -1 # V3 and earlier
        shard_ids = None # V3 and earlier
        proxy_prepared_query = None
        cont_key = None
        virtual_scans = None # array of dict
        query_traces = None  # dict

        walker = MapWalker(bis)
        while walker.has_next():
            walker.next()
            name = walker.get_current_name()
            if name == ERROR_CODE:
                Proto.handle_error_code(walker)
            elif name == CONSUMED:
                if query_result is not None:
                    Proto.read_consumed_capacity(bis, query_result)
                else:
                    Proto.read_consumed_capacity(bis, prepare_result)
            elif name == PREPARED_QUERY:
                proxy_prepared_query = Nson.read_binary(bis)
            elif name == DRIVER_QUERY_PLAN:
                dpi = DriverPlanInfo()
                dpi.read_plan(Nson.read_binary(bis))
            elif name == QUERY_PLAN_STRING:
                query_plan = Nson.read_string(bis)
            elif name == QUERY_RESULT_SCHEMA:
                query_schema = Nson.read_string(bis)
            elif name == TABLE_NAME:
                table_name = Nson.read_string(bis)
            elif name == NAMESPACE:
                namespace = Nson.read_string(bis)
            elif name == QUERY_OPERATION:
                operation = Nson.read_int(bis)
            elif name == QUERY_RESULTS:
                # query only
                Proto.read_query_results(query_result, bis)
            elif name == CONTINUATION_KEY:
                # query only
                cont_key = Nson.read_binary(bis)
            elif name == SORT_PHASE1_RESULTS:
                # query only
                Proto.read_phase1_results(query_result, bis)
            elif name == REACHED_LIMIT:
                # query only
                if query_result is not None:
                    query_result.set_reached_limit(Nson.read_boolean(bis))
            elif name == TOPOLOGY_INFO:
                Proto.read_topology_info(bis,
                  query_result if query_result is not None else prepare_result);
            # QUERY_V3 and earlier return topo differently
            elif name == PROXY_TOPO_SEQNUM:
                proxy_topo_seqnum = Nson.read_int(bis)
            elif name == SHARD_IDS:
                shard_ids = Proto.read_nson_int_array(bis)
            # new in QUERY_V4
            elif name == VIRTUAL_SCANS:
                Nson.read_type(bis, SerdeUtil.FIELD_VALUE_TYPE.ARRAY)
                SerdeUtil.read_full_int(bis) # array size in bytes
                num_scans = SerdeUtil.read_full_int(bis)
                virtual_scans = list()
                for i in range(num_scans):
                    virtual_scans.append(Proto.read_virtual_scan(bis))
            elif name == QUERY_BATCH_TRACES:
                Nson.read_type(bis, SerdeUtil.FIELD_VALUE_TYPE.ARRAY)
                SerdeUtil.read_full_int(bis) # array size in bytes
                # divide by 2 because each trace is 2 strings
                num_traces = SerdeUtil.read_full_int(bis) / 2
                query_traces = dict()
                for i in range(num_traces):
                    name = Nson.read_string(bis)
                    trace = Nson.read_string(bis)
                    query_traces[name] = trace
            else:
                walker.skip()

        # QUERY_V3 and earlier return topo differently
        res = query_result if query_result is not None else prepare_result
        if res.get_topology_info() is None and proxy_topo_seqnum >= 0:
            res.set_topology_info(proxy_topo_seqnum, shard_ids)

        # ensure that the continuation key is cleared if not returned,
        # meaning the query is done
        if query_result is not None:
            query_result.set_continuation_key(cont_key)
            query_request.set_cont_key(query_result.get_continuation_key())
            query_result.set_virtual_scans(virtual_scans)
            query_result.set_query_traces(query_traces)

        # if this was already prepared and is from a query, we are done
        if request_was_prepared:
            return
        if query_request is not None:
            statement = query_request.get_statement()
        else:
            statement = prepare_request.get_statement()
        prepared_statement = PreparedStatement(
            statement, query_plan, query_schema,
            proxy_prepared_query,
            None if dpi is None else dpi.get_plan(),
            None if dpi is None else dpi.get_num_iters(),
            None if dpi is None else dpi.get_num_regs(),
            None if dpi is None else dpi.get_vars(),
            namespace,
            table_name,
            operation)
        if prepare_result is not None:
            prepare_result.set_prepared_statement(prepared_statement)
        elif query_request is not None:
            query_request.set_prepared_statement(prepared_statement)
            if not prepared_statement.is_simple_query():
                driver = QueryDriver(query_request)
                driver.set_prep_cost(query_result.get_read_kb())
                query_result.set_computed(False)

    @staticmethod
    def read_virtual_scan(bis):
        vs = dict()
        vs[VIRTUAL_SCAN_SID] = -1
        vs[VIRTUAL_SCAN_PID] = -1
        vs[VIRTUAL_SCAN_PRIM_KEY] = None
        vs[VIRTUAL_SCAN_SEC_KEY] = None
        vs[VIRTUAL_SCAN_MOVE_AFTER] = True
        vs[VIRTUAL_SCAN_JOIN_DESC_RESUME_KEY] = None
        vs[VIRTUAL_SCAN_JOIN_PATH_TABLES] = None
        vs[VIRTUAL_SCAN_JOIN_PATH_KEY] = None
        vs[VIRTUAL_SCAN_JOIN_PATH_SEC_KEY] = None
        vs[VIRTUAL_SCAN_JOIN_PATH_MATCHED] = False

        walker = MapWalker(bis)
        while walker.has_next():
            walker.next()
            name = walker.get_current_name()
            if name == VIRTUAL_SCAN_SID:
                vs[VIRTUAL_SCAN_SID] = Nson.read_int(bis)
            if name == VIRTUAL_SCAN_PID:
                vs[VIRTUAL_SCAN_PID] = Nson.read_int(bis)
            if name == VIRTUAL_SCAN_PRIM_KEY:
                vs[VIRTUAL_SCAN_PRIM_KEY] = Nson.read_binary(bis)
            if name == VIRTUAL_SCAN_SEC_KEY:
                vs[VIRTUAL_SCAN_SEC_KEY] = Nson.read_binary(bis)
            if name == VIRTUAL_SCAN_MOVE_AFTER:
                vs[VIRTUAL_SCAN_MOVE_AFTER] = Nson.read_boolean(bis)
            if name == VIRTUAL_SCAN_JOIN_DESC_RESUME_KEY:
                vs[VIRTUAL_SCAN_JOIN_DESC_RESUME_KEY] = Nson.read_binary(bis)
            if name == VIRTUAL_SCAN_JOIN_PATH_KEY:
                vs[VIRTUAL_SCAN_JOIN_PATH_KEY] = Nson.read_binary(bis)
            if name == VIRTUAL_SCAN_JOIN_PATH_SEC_KEY:
                vs[VIRTUAL_SCAN_JOIN_PATH_SEC_KEY] = Nson.read_binary(bis)
            if name == VIRTUAL_SCAN_JOIN_PATH_TABLES:
                vs[VIRTUAL_SCAN_JOIN_PATH_TABLES] = Nson.read_nson_int_array(bis)
            if name == VIRTUAL_SCAN_JOIN_PATH_MATCHED:
                vs[VIRTUAL_SCAN_JOIN_PATH_MATCHED] = Nson.read_boolean(bis)
            else:
                walker.skip()
        return vs


    # array of int for shard_ids in query topology info
    @staticmethod
    def read_nson_int_array(bis):
        t = bis.read_byte()
        if t != SerdeUtil.FIELD_VALUE_TYPE.ARRAY:
            raise IllegalArgumentException(
                'NSON read int array: stream must be located at type ARRAY')
        SerdeUtil.read_full_int(bis)  # total length in bytes
        num_elements = SerdeUtil.read_full_int(bis)
        arr = []
        for i in range(num_elements):
            arr.append(Nson.read_int(bis))
        return arr

    #
    # Methods to read query results and set them in result objects
    #

    @staticmethod
    def read_query_results(query_result, bis):
        # results are array of MAP
        t = bis.read_byte()
        if t != SerdeUtil.FIELD_VALUE_TYPE.ARRAY:
            raise IllegalArgumentException(
                'NSON query results must be of type ARRAY')
        SerdeUtil.read_full_int(bis)  # total length in bytes
        num_elements = SerdeUtil.read_full_int(bis)
        results = list()
        for i in range(num_elements):
            results.append(Proto.nson_to_value(bis))
        query_result.set_results(results)

    @staticmethod
    def read_phase1_results(query_result, bis):
        # binary, wrap byte array in another input stream
        bis1 = ByteInputStream(Nson.read_binary(bis))
        query_result.set_is_in_phase1(bis1.read_boolean())
        pids = SerdeUtil.read_packed_int_array(bis1)
        if pids is not None:
            query_result.set_pids(pids)
            query_result.set_num_results_per_pid(
                SerdeUtil.read_packed_int_array(bis1))
            cont_keys = list()
            for i in range(len(pids)):
                cont_keys.append(SerdeUtil.read_bytearray(bis1, False))
            query_result.set_partition_cont_keys(cont_keys)

    #
    # {
    #   PROXY_TOPO_SEQNUM : int
    #   SHARD_IDS : [int, ...]
    # }
    #
    @staticmethod
    def read_topology_info(bis, result):
        proxy_topo_seqnum = -1
        shard_ids = None

        walker = MapWalker(bis)
        while walker.has_next():
            walker.next()
            name = walker.get_current_name()
            if name == PROXY_TOPO_SEQNUM:
                proxy_topo_seqnum = Nson.read_int(bis)
            elif name == SHARD_IDS:
                 # int array
                 shard_ids = Proto.read_nson_int_array(bis)
            else:
                walker.skip()
        if proxy_topo_seqnum >= 0:
            result.set_topology_info(proxy_topo_seqnum, shard_ids)

    #
    # Handle success/failure in a response. Success is a 0 error code.
    # Failure is a non-zero code and may also include:
    #  Exception message
    #  Consumed capacity
    #  Retry hints if throttling (future)
    # This method throws an appropriately mapped exception on error and
    # nothing on success.
    #
    #   "error_code": int (code)
    #   "exception": "..."
    #   "consumed": {
    #      "read_units": int,
    #      "read_kb": int,
    #      "write_kb": int
    #    }
    #
    # The walker must be positioned at the very first field in the response
    # which *must* be the error code.
    #
    # This method either returns a non-zero error code or throws an
    # exception based on the error code and additional information.

    @staticmethod
    def handle_error_code(walker):
        bis = walker.get_stream()
        code = Nson.read_int(bis)
        if code == 0:
            return
        while walker.has_next():
            walker.next()
            name = walker.get_current_name()
            if name == EXCEPTION:
                msg = Nson.read_string(bis)
                raise SerdeUtil.map_exception(code, msg)
            elif name == CONSUMED:
                # TODO -- this means delaying raise until end
                walker.skip()
            else:
                walker.skip()


# This code is redundant WRT code in serde.py that does the same thing but
# it is easier to do this than share. The V3 code will never change but this
# might evolve with time
#
# Read and encapsulate the driver portion of the query plan for local execution
#
class DriverPlanInfo(object):

    def __init__(self):
        self._num_iters = None
        self._num_regs = None
        self._plan = None
        self._external_vars = None

    def read_plan(self, binary_plan):
        bis = ByteInputStream(binary_plan)
        self._plan = PlanIter.deserialize_iter(bis)
        if self._plan is None:
            return None
        self._num_iters = bis.read_int()
        self._num_regs = bis.read_int()
        SerdeUtil.trace(
            'PREP-RESULT: Query Plan:\n' + self._plan.display() + '\n', 1)
        length = bis.read_int()
        if length > 0:
            self._external_vars = dict()
            for i in range(length):
                var_name = SerdeUtil.read_string(bis)
                var_id = bis.read_int()
                self._external_vars[var_name] = var_id

    def get_plan(self):
        return self._plan

    def get_num_iters(self):
        return self._num_iters

    def get_num_regs(self):
        return self._num_regs

    def get_vars(self):
        return self._external_vars
