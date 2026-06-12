#
# Copyright (c) 2018, 2026 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#

from ..common import CheckValue
from ..exception import IllegalArgumentException, IllegalStateException


class StartLocation(object):
    """
    The position at which a Change Streams consumer should start reading.
    """

    class LocationType(object):
        """
        Internal values used by the Change Streams protocol.
        """
        UNINITIALIZED = 0
        FIRST_UNCOMMITTED = 1
        EARLIEST = 2
        LATEST = 3
        AT_TIME = 4

    def __init__(self, location_type, start_time_ms=0):
        CheckValue.check_int(location_type, 'location_type')
        CheckValue.check_int_ge_zero(start_time_ms, 'start_time_ms')
        if location_type not in (
                self.LocationType.FIRST_UNCOMMITTED,
                self.LocationType.EARLIEST,
                self.LocationType.LATEST,
                self.LocationType.AT_TIME):
            raise IllegalArgumentException(
                'Invalid Change Streams start location type: ' +
                str(location_type))
        if location_type != self.LocationType.AT_TIME and start_time_ms != 0:
            raise IllegalArgumentException(
                'start_time_ms can only be set for AT_TIME start locations.')
        self._location_type = location_type
        self._start_time_ms = start_time_ms

    @staticmethod
    def first_uncommitted():
        """
        Start consuming at the first uncommitted message in the stream.
        """
        return StartLocation(StartLocation.LocationType.FIRST_UNCOMMITTED)

    @staticmethod
    def earliest():
        """
        Start consuming from the earliest available message in the stream.
        """
        return StartLocation(StartLocation.LocationType.EARLIEST)

    @staticmethod
    def latest():
        """
        Start consuming messages published after the consumer starts.
        """
        return StartLocation(StartLocation.LocationType.LATEST)

    @staticmethod
    def at_time(start_time_ms):
        """
        Start consuming from the specified time in milliseconds since the Epoch.
        """
        return StartLocation(StartLocation.LocationType.AT_TIME, start_time_ms)

    def get_location_type(self):
        """
        Returns the protocol location type for this start location.
        """
        return self._location_type

    def get_start_time(self):
        """
        Returns the start time in milliseconds since the Epoch, or 0 if unset.
        """
        return self._start_time_ms

    def __str__(self):
        return ('StartLocation [location_type=' + str(self._location_type) +
                ', start_time_ms=' + str(self._start_time_ms) + ']')


class Image(object):
    """
    The value and metadata for a Change Streams record image.
    """

    def __init__(self, value=None, metadata=None):
        self._value = value
        self._metadata = metadata

    def get_value(self):
        """
        Returns the record image value, or None if it is not present.
        """
        return self._value

    def get_metadata(self):
        """
        Returns the record image metadata, or None if it is not present.
        """
        return self._metadata

    def _set_value(self, value):
        self._value = value
        return self

    def _set_metadata(self, metadata):
        self._metadata = metadata
        return self

    def is_empty(self):
        """
        Returns True if this image has neither value nor metadata.
        """
        return self._value is None and self._metadata is None

    def __str__(self):
        return ('Image [value=' + str(self._value) +
                ', metadata=' + str(self._metadata) + ']')


class Record(object):
    """
    A single Change Streams record.
    """

    def __init__(self, event_id=None, record_key=None, current_image=None,
                 before_image=None, modification_time=0, expiration_time=0,
                 partition_id=0, region_id=0):
        self._event_id = event_id
        self._record_key = record_key
        self._current_image = current_image
        self._before_image = before_image
        self._modification_time = modification_time
        self._expiration_time = expiration_time
        self._partition_id = partition_id
        self._region_id = region_id

    def get_event_id(self):
        return self._event_id

    def get_record_key(self):
        return self._record_key

    def get_current_image(self):
        return self._current_image

    def get_before_image(self):
        return self._before_image

    def get_modification_time(self):
        return self._modification_time

    def get_expiration_time(self):
        return self._expiration_time

    def get_partition_id(self):
        return self._partition_id

    def get_region_id(self):
        return self._region_id

    def _set_event_id(self, event_id):
        self._event_id = event_id
        return self

    def _set_record_key(self, record_key):
        self._record_key = record_key
        return self

    def _set_current_image(self, image):
        self._current_image = image
        return self

    def _set_before_image(self, image):
        self._before_image = image
        return self

    def _set_modification_time(self, modification_time):
        self._modification_time = modification_time
        return self

    def _set_expiration_time(self, expiration_time):
        self._expiration_time = expiration_time
        return self

    def _set_partition_id(self, partition_id):
        self._partition_id = partition_id
        return self

    def _set_region_id(self, region_id):
        self._region_id = region_id
        return self

    def __str__(self):
        return ('Record [event_id=' + str(self._event_id) +
                ', record_key=' + str(self._record_key) +
                ', current_image=' + str(self._current_image) +
                ', before_image=' + str(self._before_image) +
                ', modification_time=' + str(self._modification_time) +
                ', expiration_time=' + str(self._expiration_time) +
                ', partition_id=' + str(self._partition_id) +
                ', region_id=' + str(self._region_id) + ']')


class Event(object):
    """
    A Change Streams event containing one or more records.
    """

    def __init__(self, records=None):
        if records is None:
            self._records = list()
        elif isinstance(records, list):
            self._records = records
        else:
            self._records = [records]

    def get_records(self):
        """
        Returns the records in this event.
        """
        return self._records

    def __str__(self):
        return 'Event [records=' + str(self._records) + ']'


class Message(object):
    """
    A Change Streams message containing events for one table.
    """

    def __init__(self, table_name=None, compartment_ocid=None,
                 table_ocid=None, version=None, events=None):
        self._table_name = table_name
        self._compartment_ocid = compartment_ocid
        self._table_ocid = table_ocid
        self._version = version
        self._events = events

    def get_table_name(self):
        return self._table_name

    def get_compartment_ocid(self):
        return self._compartment_ocid

    def get_table_ocid(self):
        return self._table_ocid

    def get_version(self):
        return self._version

    def get_events(self):
        return self._events

    def _set_table_name(self, table_name):
        self._table_name = table_name
        return self

    def _set_compartment_ocid(self, compartment_ocid):
        self._compartment_ocid = compartment_ocid
        return self

    def _set_table_ocid(self, table_ocid):
        self._table_ocid = table_ocid
        return self

    def _set_version(self, version):
        self._version = version
        return self

    def _set_events(self, events):
        self._events = events
        return self

    def __str__(self):
        return ('Message [table_name=' + str(self._table_name) +
                ', compartment_ocid=' + str(self._compartment_ocid) +
                ', table_ocid=' + str(self._table_ocid) +
                ', version=' + str(self._version) +
                ', events=' + str(self._events) + ']')


class MessageBundle(object):
    """
    One or more messages returned from a Change Streams poll operation.
    """

    def __init__(self, messages=None):
        self._consumer = None
        self._cursor = None
        self._events_remaining = 0
        self._messages = messages

    def get_events_remaining(self):
        """
        Returns an estimate of unconsumed events remaining for this consumer.
        """
        return self._events_remaining

    def get_messages(self):
        """
        Returns the messages in this bundle.
        """
        return self._messages

    def commit(self, timeout_ms=None):
        """
        Marks the messages in this bundle as committed.
        """
        if self._consumer is None:
            raise IllegalStateException(
                'MessageBundle cannot be committed without a consumer.')
        if hasattr(self._consumer, 'commit_bundle'):
            return self._consumer.commit_bundle(self, timeout_ms)
        return self._consumer.commit(timeout_ms)

    def is_empty(self):
        """
        Returns True if this bundle has no messages.
        """
        return self._messages is None or len(self._messages) == 0

    def _get_consumer(self):
        return self._consumer

    def _set_consumer(self, consumer):
        self._consumer = consumer
        return self

    def _get_cursor(self):
        return self._cursor

    def _set_cursor(self, cursor):
        self._cursor = cursor
        return self

    def _set_events_remaining(self, events_remaining):
        self._events_remaining = events_remaining
        return self

    def __str__(self):
        if self._cursor is None:
            cursor = 'None'
        else:
            cursor = 'size=' + str(len(self._cursor))
        return ('MessageBundle [cursor=' + cursor +
                ', events_remaining=' + str(self._events_remaining) +
                ', messages=' + str(self._messages) + ']')
