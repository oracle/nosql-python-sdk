#
# Copyright (c) 2018, 2026 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#

import unittest


try:
    import requests  # noqa: F401
    import dateutil.parser  # noqa: F401
    import dateutil.tz  # noqa: F401
except ImportError as exc:
    raise unittest.SkipTest(
        'Change Streams tests require requests and python-dateutil.') from exc

import borneo
from borneo import (
    IllegalArgumentException, NoSQLException, OperationNotSupportedException)
from borneo.changestream import (
    Consumer, ConsumerBuilder, Event, Image, Message, MessageBundle, Record,
    StartLocation)
from borneo.common import ByteInputStream, ByteOutputStream
from borneo.nson import NsonSerializer, Proto
from borneo.nson_protocol import (
    CHANGE_STREAM_ENABLED, COMPARTMENT_OCID, CONSUMER_TABLES, CURSOR,
    EVENT_BUNDLE, EVENT_EVENTS, EVENT_EXPIRATION_TIME, EVENT_ID,
    EVENT_MODIFICATION_TIME, EVENT_PARTITION_ID, EVENT_PREV_METADATA,
    EVENT_PREV_VALUE, EVENT_RECORD_KEY, EVENT_RECORD_METADATA,
    EVENT_RECORD_VALUE, EVENT_REGION_ID, EVENT_TYPE, EVENT_VERSION,
    EVENTS_REMAINING, FORCE_RESET, GROUP_ID, HEADER, IS_REMOVE,
    MANUAL_COMMIT, MAX_EVENTS, MAX_POLL_INTERVAL, MODE, OP_CODE, PAYLOAD,
    START_LOCATION, START_TIME, TABLE_NAME, TABLE_OCID)
from borneo.operations import (
    ChangeStreamConsumerRequest, ChangeStreamPollRequest,
    ChangeStreamPollResult, TableRequest)
from borneo.serdeutil import SerdeUtil


class FakeTableResult(object):
    def __init__(self, table_id):
        self._table_id = table_id

    def get_table_id(self):
        return self._table_id


class FakeHandle(object):
    def __init__(self, table_id='ocid1.nosqltable.oc1..resolved'):
        self.requests = list()
        self._table_id = table_id

    def get_table(self, request):
        self.requests.append(request)
        return FakeTableResult(self._table_id)


def _serialize_request(request):
    content = bytearray()
    bos = ByteOutputStream(content)
    serializer = request.create_serializer(SerdeUtil.SERIAL_VERSION_4)
    serializer.serialize(request, bos, SerdeUtil.SERIAL_VERSION_4)
    return Proto.nson_to_value(ByteInputStream(content))


def _new_nson_serializer():
    content = bytearray()
    return content, NsonSerializer(ByteOutputStream(content))


class TestChangeStreamsModels(unittest.TestCase):

    def test_start_location_factories(self):
        first = StartLocation.first_uncommitted()
        self.assertEqual(
            first.get_location_type(),
            StartLocation.LocationType.FIRST_UNCOMMITTED)
        self.assertEqual(first.get_start_time(), 0)

        earliest = StartLocation.earliest()
        self.assertEqual(
            earliest.get_location_type(), StartLocation.LocationType.EARLIEST)

        latest = StartLocation.latest()
        self.assertEqual(
            latest.get_location_type(), StartLocation.LocationType.LATEST)

        at_time = StartLocation.at_time(123456789)
        self.assertEqual(
            at_time.get_location_type(), StartLocation.LocationType.AT_TIME)
        self.assertEqual(at_time.get_start_time(), 123456789)

    def test_start_location_validation(self):
        self.assertRaises(IllegalArgumentException, StartLocation, 0)
        self.assertRaises(IllegalArgumentException, StartLocation.at_time, -1)
        self.assertRaises(
            IllegalArgumentException, StartLocation,
            StartLocation.LocationType.EARLIEST, 1)

    def test_image_and_bundle_empty_helpers(self):
        self.assertTrue(Image().is_empty())
        self.assertFalse(Image({'name': 'jane'}).is_empty())
        self.assertTrue(MessageBundle().is_empty())
        self.assertFalse(MessageBundle([Message()]).is_empty())

    def test_message_bundle_commits_bundle_cursor(self):
        class FakeConsumer(object):
            def __init__(self):
                self.bundle = None
                self.timeout_ms = None

            def commit_bundle(self, bundle, timeout_ms=None):
                self.bundle = bundle
                self.timeout_ms = timeout_ms

        bundle = MessageBundle([Message()])
        consumer = FakeConsumer()
        bundle._set_cursor(bytearray(b'bundle-cursor'))
        bundle._set_consumer(consumer)

        bundle.commit(timeout_ms=1234)

        self.assertIs(consumer.bundle, bundle)
        self.assertEqual(consumer.timeout_ms, 1234)


class TestChangeStreamsExports(unittest.TestCase):

    def test_submodule_exports_public_classes(self):
        self.assertIs(borneo.changestream.Consumer, Consumer)
        self.assertIs(borneo.changestream.ConsumerBuilder, ConsumerBuilder)
        self.assertIs(borneo.changestream.Event, Event)
        self.assertIs(borneo.changestream.Image, Image)
        self.assertIs(borneo.changestream.Message, Message)
        self.assertIs(borneo.changestream.MessageBundle, MessageBundle)
        self.assertIs(borneo.changestream.Record, Record)
        self.assertIs(borneo.changestream.StartLocation, StartLocation)

    def test_generic_classes_not_exported_from_top_level_borneo(self):
        for name in ('Consumer', 'ConsumerBuilder', 'Event', 'Image',
                     'Message', 'MessageBundle', 'Record', 'StartLocation'):
            self.assertNotIn(name, borneo.__all__)
            self.assertFalse(hasattr(borneo, name))


class TestChangeStreamsBuilderAndRequests(unittest.TestCase):

    def test_builder_resolves_names_and_deduplicates_tables(self):
        handle = FakeHandle()
        builder = ConsumerBuilder().set_handle(handle).add_table(
            'users', compartment='compartmentA').add_table(
            'users', compartment='compartmentA').add_table(
            'orders', start_location=StartLocation.latest())

        builder.validate()

        self.assertEqual(builder.get_num_tables(), 2)
        self.assertEqual(len(handle.requests), 2)
        self.assertEqual(
            builder.get_tables()[0].get_table_ocid(),
            'ocid1.nosqltable.oc1..resolved')
        self.assertEqual(
            builder.get_tables()[0].get_start_location().get_location_type(),
            StartLocation.LocationType.FIRST_UNCOMMITTED)
        self.assertEqual(
            builder.get_tables()[1].get_start_location().get_location_type(),
            StartLocation.LocationType.LATEST)
        self.assertEqual(
            handle.requests[0].get_compartment(), 'compartmentA')

    def test_builder_accepts_table_ocid_and_remove_config(self):
        table_ocid = 'ocid1.nosqltable.oc1..table'
        builder = ConsumerBuilder().set_handle(FakeHandle()).add_table(
            table_ocid).remove_table(table_ocid)

        builder.validate()

        self.assertEqual(builder.get_num_tables(), 2)
        self.assertEqual(builder.get_tables()[0].get_table_ocid(), table_ocid)
        self.assertFalse(builder.get_tables()[0].is_remove())
        self.assertTrue(builder.get_tables()[1].is_remove())

    def test_consumer_request_modes_and_retry_behavior(self):
        builder = ConsumerBuilder().set_group_id('group')
        cursor = bytearray(b'cursor')

        self.assertTrue(ChangeStreamConsumerRequest(
            ChangeStreamConsumerRequest.RequestMode.CREATE).should_retry())
        self.assertFalse(ChangeStreamPollRequest(cursor, 10).should_retry())

        ChangeStreamConsumerRequest(
            ChangeStreamConsumerRequest.RequestMode.CREATE).set_builder(
                builder).validate()
        ChangeStreamConsumerRequest(
            ChangeStreamConsumerRequest.RequestMode.UPDATE).set_builder(
                builder).set_cursor(cursor).validate()
        ChangeStreamConsumerRequest(
            ChangeStreamConsumerRequest.RequestMode.CLOSE).set_cursor(
                cursor).validate()
        ChangeStreamConsumerRequest(
            ChangeStreamConsumerRequest.RequestMode.DELETE).set_builder(
                builder).validate()
        ChangeStreamConsumerRequest(
            ChangeStreamConsumerRequest.RequestMode.COMMIT).set_cursor(
                cursor).validate()
        ChangeStreamConsumerRequest(
            ChangeStreamConsumerRequest.RequestMode.RESET).set_cursor(
                cursor).validate()

        self.assertRaises(
            IllegalArgumentException,
            ChangeStreamConsumerRequest(0).validate)
        self.assertRaises(
            IllegalArgumentException,
            ChangeStreamConsumerRequest(
                ChangeStreamConsumerRequest.RequestMode.CREATE).validate)
        self.assertRaises(
            IllegalArgumentException,
            ChangeStreamConsumerRequest(
                ChangeStreamConsumerRequest.RequestMode.UPDATE).set_builder(
                    builder).validate)
        self.assertRaises(
            IllegalArgumentException,
            ChangeStreamConsumerRequest(
                ChangeStreamConsumerRequest.RequestMode.COMMIT).validate)
        self.assertRaises(
            IllegalArgumentException,
            ChangeStreamConsumerRequest(
                ChangeStreamConsumerRequest.RequestMode.DELETE).validate)

    def test_poll_request_validation(self):
        ChangeStreamPollRequest(bytearray(b'cursor'), 0).validate()
        ChangeStreamPollRequest(bytearray(b'cursor'), 100).validate()

        self.assertRaises(
            IllegalArgumentException,
            ChangeStreamPollRequest(None, 100).validate)
        self.assertRaises(
            IllegalArgumentException,
            ChangeStreamPollRequest(bytearray(b'cursor'), -1).validate)

    def test_poll_once_rejects_bundle_without_cursor(self):
        consumer = Consumer.__new__(Consumer)
        consumer._cursor = bytearray(b'old-cursor')
        result = ChangeStreamPollResult().set_bundle(MessageBundle([]))
        consumer._execute_request = lambda request: result

        self.assertRaises(NoSQLException, consumer._poll_once, 100)

    def test_v3_serial_version_is_unsupported(self):
        self.assertRaises(
            OperationNotSupportedException,
            ChangeStreamConsumerRequest(
                ChangeStreamConsumerRequest.RequestMode.CREATE
            ).create_serializer,
            SerdeUtil.SERIAL_VERSION_3)
        self.assertRaises(
            OperationNotSupportedException,
            ChangeStreamPollRequest(bytearray(b'cursor'), 10).create_serializer,
            SerdeUtil.SERIAL_VERSION_3)
        table_request = TableRequest().set_table_name(
            'users').set_change_streaming_enabled(True)
        table_serializer = table_request.create_serializer(
            SerdeUtil.SERIAL_VERSION_3)
        self.assertRaises(
            OperationNotSupportedException,
            table_serializer.serialize,
            table_request,
            ByteOutputStream(bytearray()),
            SerdeUtil.SERIAL_VERSION_3)


class TestChangeStreamsSerialization(unittest.TestCase):

    def test_consumer_request_serializer_writes_expected_fields(self):
        table_ocid = 'ocid1.nosqltable.oc1..table'
        request = ChangeStreamConsumerRequest(
            ChangeStreamConsumerRequest.RequestMode.CREATE).set_builder(
                ConsumerBuilder().set_group_id('group1').set_compartment(
                    'compartment1').set_commit_manual().set_max_poll_interval(
                    12345).set_force_reset_start_location().add_table(
                    table_ocid, start_location=StartLocation.at_time(999)))

        value = _serialize_request(request)
        header = value[HEADER]
        payload = value[PAYLOAD]
        table = payload[CONSUMER_TABLES][0]

        self.assertEqual(
            header[OP_CODE], SerdeUtil.OP_CODE.CHANGE_STREAM_CONSUMER)
        self.assertEqual(
            payload[MODE],
            ChangeStreamConsumerRequest.RequestMode.CREATE)
        self.assertEqual(payload[GROUP_ID], 'group1')
        self.assertTrue(payload[MANUAL_COMMIT])
        self.assertEqual(payload[COMPARTMENT_OCID], 'compartment1')
        self.assertEqual(payload[MAX_POLL_INTERVAL], 12345)
        self.assertTrue(payload[FORCE_RESET])
        self.assertEqual(table[TABLE_OCID], table_ocid)
        self.assertEqual(table[START_TIME], 999)
        self.assertEqual(
            table[START_LOCATION], StartLocation.LocationType.AT_TIME)

    def test_consumer_request_serializer_writes_remove_table(self):
        table_ocid = 'ocid1.nosqltable.oc1..table'
        request = ChangeStreamConsumerRequest(
            ChangeStreamConsumerRequest.RequestMode.UPDATE).set_cursor(
                bytearray(b'cursor')).set_builder(
                ConsumerBuilder().set_group_id('group1').remove_table(
                    table_ocid))

        value = _serialize_request(request)
        payload = value[PAYLOAD]
        table = payload[CONSUMER_TABLES][0]

        self.assertEqual(
            payload[MODE],
            ChangeStreamConsumerRequest.RequestMode.UPDATE)
        self.assertEqual(payload[CURSOR], bytearray(b'cursor'))
        self.assertEqual(table[TABLE_OCID], table_ocid)
        self.assertTrue(table[IS_REMOVE])

    def test_poll_request_serializer_writes_expected_fields(self):
        request = ChangeStreamPollRequest(bytearray(b'cursor'), 25)

        value = _serialize_request(request)

        self.assertEqual(
            value[HEADER][OP_CODE], SerdeUtil.OP_CODE.CHANGE_STREAM_POLL)
        self.assertEqual(value[PAYLOAD][MAX_EVENTS], 25)
        self.assertEqual(value[PAYLOAD][CURSOR], bytearray(b'cursor'))

    def test_table_request_serializer_writes_change_streaming_enabled(self):
        request = TableRequest().set_table_name(
            'users').set_change_streaming_enabled(False)

        value = _serialize_request(request)

        self.assertEqual(
            value[HEADER][OP_CODE], SerdeUtil.OP_CODE.TABLE_REQUEST)
        self.assertFalse(value[PAYLOAD][CHANGE_STREAM_ENABLED])

    def test_poll_response_deserializes_event_bundle(self):
        event_binary = self._create_event_binary()
        response = self._create_poll_response(event_binary)
        request = ChangeStreamPollRequest(bytearray(b'cursor'), 100)

        result = request.create_serializer(
            SerdeUtil.SERIAL_VERSION_4).deserialize(
                request, ByteInputStream(response),
                SerdeUtil.SERIAL_VERSION_4)

        self.assertEqual(result.get_cursor(), bytearray(b'next-cursor'))
        self.assertEqual(result.get_events_remaining(), 7)

        bundle = result.get_bundle()
        self.assertIsInstance(bundle, MessageBundle)
        self.assertEqual(len(bundle.get_messages()), 1)

        message = bundle.get_messages()[0]
        self.assertEqual(message.get_table_name(), 'users')
        self.assertEqual(message.get_table_ocid(), 'ocid1.nosqltable.oc1..t')
        self.assertEqual(message.get_compartment_ocid(), 'compartment1')
        self.assertIsNone(message.get_version())
        self.assertEqual(len(message.get_events()), 1)

        event = message.get_events()[0]
        self.assertIsInstance(event, Event)
        self.assertEqual(len(event.get_records()), 1)

        record = event.get_records()[0]
        self.assertIsInstance(record, Record)
        self.assertEqual(record.get_event_id(), 'event1')
        self.assertEqual(record.get_record_key(), {'id': 10})
        self.assertEqual(record.get_modification_time(), 1111)
        self.assertEqual(record.get_expiration_time(), 2222)
        self.assertEqual(record.get_partition_id(), 3)
        self.assertEqual(record.get_region_id(), 4)
        self.assertEqual(record.get_current_image().get_value(),
                         {'name': 'jane'})
        self.assertEqual(record.get_current_image().get_metadata(),
                         {'version': 1})
        self.assertEqual(record.get_before_image().get_value(),
                         {'name': 'jill'})
        self.assertEqual(record.get_before_image().get_metadata(),
                         {'version': 0})

    @staticmethod
    def _create_event_binary():
        content, ns = _new_nson_serializer()
        ns.start_map()
        Proto.write_int_map_field(ns, EVENT_VERSION, 1)
        Proto.write_int_map_field(ns, EVENT_TYPE, 1)
        Proto.start_array(ns, EVENT_EVENTS)
        ns.start_array_field()
        ns.start_map()
        Proto.write_long_map_field(ns, EVENT_MODIFICATION_TIME, 1111)
        Proto.write_long_map_field(ns, EVENT_EXPIRATION_TIME, 2222)
        Proto.write_string_map_field(ns, EVENT_ID, 'event1')
        Proto.write_int_map_field(ns, EVENT_PARTITION_ID, 3)
        Proto.write_int_map_field(ns, EVENT_REGION_ID, 4)
        TestChangeStreamsSerialization._write_value_map_field(
            ns, EVENT_RECORD_KEY, {'id': 10})
        TestChangeStreamsSerialization._write_value_map_field(
            ns, EVENT_RECORD_VALUE, {'name': 'jane'})
        TestChangeStreamsSerialization._write_value_map_field(
            ns, EVENT_RECORD_METADATA, {'version': 1})
        TestChangeStreamsSerialization._write_value_map_field(
            ns, EVENT_PREV_VALUE, {'name': 'jill'})
        TestChangeStreamsSerialization._write_value_map_field(
            ns, EVENT_PREV_METADATA, {'version': 0})
        ns.end_map()
        ns.end_array_field()
        Proto.end_array(ns, EVENT_EVENTS)
        ns.end_map()
        return content

    @staticmethod
    def _create_poll_response(event_binary):
        content, ns = _new_nson_serializer()
        ns.start_map()
        Proto.write_bin_map_field(ns, CURSOR, bytearray(b'next-cursor'))
        Proto.write_long_map_field(ns, EVENTS_REMAINING, 7)
        Proto.start_array(ns, EVENT_BUNDLE)
        ns.start_array_field()
        ns.start_map()
        Proto.write_string_map_field(ns, TABLE_OCID,
                                     'ocid1.nosqltable.oc1..t')
        Proto.write_string_map_field(ns, TABLE_NAME, 'users')
        Proto.write_string_map_field(ns, COMPARTMENT_OCID, 'compartment1')
        Proto.start_array(ns, EVENT_EVENTS)
        ns.start_array_field()
        ns.binary_value(event_binary)
        ns.end_array_field()
        Proto.end_array(ns, EVENT_EVENTS)
        ns.end_map()
        ns.end_array_field()
        Proto.end_array(ns, EVENT_BUNDLE)
        ns.end_map()
        return content

    @staticmethod
    def _write_value_map_field(ns, field_name, value):
        ns.start_map_field(field_name)
        Proto.write_field_value(ns, value)
        ns.end_map_field(field_name)


if __name__ == '__main__':
    unittest.main()
