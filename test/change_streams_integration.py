#
# Copyright (c) 2018, 2026 Oracle and/or its affiliates. All rights reserved.
#
# Licensed under the Universal Permissive License v 1.0 as shown at
#  https://oss.oracle.com/licenses/upl/
#

import os
import unittest


RUN_CHANGE_STREAMS_INTEGRATION = (
    os.environ.get('BORNEO_CHANGE_STREAMS_INTEGRATION') == '1')


if not RUN_CHANGE_STREAMS_INTEGRATION:
    class TestChangeStreamsIntegration(unittest.TestCase):

        @unittest.skip(
            'Set BORNEO_CHANGE_STREAMS_INTEGRATION=1 to run Change Streams '
            'integration tests against a supported cloud service.')
        def test_change_streams_integration_is_gated(self):
            pass
else:
    from time import sleep, time

    from borneo import (
        DeleteRequest, GetRequest, OperationNotSupportedException, PutRequest,
        TableLimits, TableRequest)
    from borneo.changestream import Consumer, ConsumerBuilder, StartLocation
    from parameters import is_onprem, is_pod, tenant_id, wait_timeout
    from testutils import get_handle


    class TestChangeStreamsIntegration(unittest.TestCase):
        """
        Integration coverage for Change Streams.

        These tests require a cloud service environment that supports Change
        Streams. They are intentionally disabled by default because many normal
        SDK test targets, including on-premises and older cloud simulators, do
        not support the service feature.
        """

        POLL_TIMEOUT_MS = 60000

        def setUp(self):
            if is_onprem():
                self.skipTest('Change Streams is a cloud service feature.')
            self.handle = get_handle(tenant_id)
            self.suffix = str(int(time() * 1000))
            self.table_name = 'pytestChangeStreams' + self.suffix
            self.consumers = list()
            self.group_ids = list()
            self.streaming_tables = set()
            self.tables = list()

        def tearDown(self):
            try:
                for consumer in self.consumers:
                    try:
                        consumer.close()
                    except Exception:
                        pass
                for group_id in self.group_ids:
                    try:
                        Consumer.delete_group(
                            self.handle, group_id, force_stop=True)
                    except Exception:
                        pass
                for table_name in sorted(
                        self.streaming_tables, reverse=True):
                    try:
                        self.handle.enable_change_streaming(
                            table_name, enabled=False,
                            timeout_ms=wait_timeout, poll_interval_ms=1000)
                    except Exception:
                        pass
                for table_name in sorted(self.tables, reverse=True):
                    self._drop_table(table_name)
            finally:
                self.handle.close()

        def test_smoke_put_delete_latest(self):
            self._create_table(self.table_name)
            self._enable_change_streaming_or_skip(self.table_name)

            consumer = self._consumer(
                'smoke', [(self.table_name, StartLocation.latest())],
                manual=False)

            self._put(self.table_name, 10, 'jane')
            get_res = self.handle.get(GetRequest().set_table_name(
                self.table_name).set_key({'id': 10}))
            self.assertIsNotNone(get_res.get_value())

            records = self._poll_for_ids(consumer, self.table_name, {10})
            self.assertEqual(
                records[10].get_current_image().get_value().get('name'),
                'jane')

            del_res = self.handle.delete(DeleteRequest().set_table_name(
                self.table_name).set_key({'id': 10}))
            self.assertTrue(del_res.get_success())

            delete_record = self._poll_for_delete(
                consumer, self.table_name, 10)
            self.assertIsNone(delete_record.get_current_image())

        def test_earliest_start_location_reads_existing_records(self):
            self._create_table(self.table_name)
            self._enable_change_streaming_or_skip(self.table_name)
            self._put_range(self.table_name, 0, 3)

            consumer = self._consumer(
                'earliest', [(self.table_name, StartLocation.earliest())])

            records = self._poll_for_ids(
                consumer, self.table_name, {0, 1, 2})
            self.assertEqual(set(records.keys()), {0, 1, 2})

        def test_at_time_start_location(self):
            self._create_table(self.table_name)
            self._enable_change_streaming_or_skip(self.table_name)

            self._put_range(self.table_name, 0, 3)
            sleep(1)
            start_time_ms = int(time() * 1000)
            self._put_range(self.table_name, 3, 6)

            consumer = self._consumer(
                'at-time',
                [(self.table_name, StartLocation.at_time(start_time_ms))])

            records = self._poll_for_ids(
                consumer, self.table_name, {3, 4, 5})
            self.assertEqual(set(records.keys()), {3, 4, 5})

        def test_first_uncommitted_close_reopen_and_manual_reset(self):
            self._create_table(self.table_name)
            self._enable_change_streaming_or_skip(self.table_name)

            group_id = self._group_id('manual')
            consumer = self._consumer(
                'manual',
                [(self.table_name, StartLocation.first_uncommitted())],
                group_id=group_id)

            self._put_range(self.table_name, 0, 3)

            first_records = self._poll_for_ids(
                consumer, self.table_name, {0, 1, 2})
            self.assertEqual(set(first_records.keys()), {0, 1, 2})

            consumer.close()

            consumer = self._consumer(
                'manual-reopen',
                [(self.table_name, StartLocation.first_uncommitted())],
                group_id=group_id)

            reopened_records = self._poll_for_ids(
                consumer, self.table_name, {0, 1, 2})
            self.assertEqual(set(reopened_records.keys()), {0, 1, 2})

            consumer.reset()

            second_records = self._poll_for_ids(
                consumer, self.table_name, {0, 1, 2}, commit=True)
            self.assertEqual(set(second_records.keys()), {0, 1, 2})

        def test_automatic_commit_reset_starts_after_committed_records(self):
            self._create_table(self.table_name)
            self._enable_change_streaming_or_skip(self.table_name)

            consumer = self._consumer(
                'automatic',
                [(self.table_name, StartLocation.first_uncommitted())],
                manual=False)

            self._put_range(self.table_name, 0, 3)
            records = self._poll_for_ids(consumer, self.table_name, {0, 1, 2})
            self.assertEqual(set(records.keys()), {0, 1, 2})

            consumer.reset()

            self._put_range(self.table_name, 3, 6)
            records = self._poll_for_ids(consumer, self.table_name, {3, 4, 5})
            self.assertEqual(set(records.keys()), {3, 4, 5})

        def test_multiple_consumers_same_group(self):
            self._create_table(self.table_name)
            self._enable_change_streaming_or_skip(self.table_name)
            self._put_range(self.table_name, 0, 6)

            group_id = self._group_id('same-group')
            consumer1 = self._consumer(
                'same-group-1',
                [(self.table_name, StartLocation.earliest())],
                group_id=group_id)
            consumer2 = self._consumer(
                'same-group-2',
                [(self.table_name, StartLocation.earliest())],
                group_id=group_id)

            records = self._poll_for_ids_across_consumers(
                [consumer1, consumer2], self.table_name, set(range(6)))
            self.assertEqual(set(records.keys()), set(range(6)))

        def test_multiple_groups_read_same_table(self):
            self._create_table(self.table_name)
            self._enable_change_streaming_or_skip(self.table_name)
            self._put_range(self.table_name, 0, 3)

            consumer1 = self._consumer(
                'group-a', [(self.table_name, StartLocation.earliest())])
            consumer2 = self._consumer(
                'group-b', [(self.table_name, StartLocation.earliest())])

            records1 = self._poll_for_ids(
                consumer1, self.table_name, {0, 1, 2})
            records2 = self._poll_for_ids(
                consumer2, self.table_name, {0, 1, 2})
            self.assertEqual(set(records1.keys()), {0, 1, 2})
            self.assertEqual(set(records2.keys()), {0, 1, 2})

        def test_multiple_tables(self):
            table2 = self._table_name('Second')
            self._create_table(self.table_name)
            self._create_table(table2)
            self._enable_change_streaming_or_skip(self.table_name)
            self._enable_change_streaming_or_skip(table2)

            self._put(self.table_name, 1, 'one')
            self._put(table2, 2, 'two')

            consumer = self._consumer(
                'multi-table',
                [(self.table_name, StartLocation.earliest()),
                 (table2, StartLocation.earliest())])

            records1 = self._poll_for_ids(consumer, self.table_name, {1})
            records2 = self._poll_for_ids(consumer, table2, {2})
            self.assertEqual(set(records1.keys()), {1})
            self.assertEqual(set(records2.keys()), {2})

        def test_add_remove_tables(self):
            table2 = self._table_name('AddRemove2')
            table3 = self._table_name('AddRemove3')
            self._create_table(self.table_name)
            self._create_table(table2)
            self._create_table(table3)
            self._enable_change_streaming_or_skip(self.table_name)
            self._enable_change_streaming_or_skip(table2)
            self._enable_change_streaming_or_skip(table3)

            consumer = self._consumer(
                'add-remove',
                [(self.table_name, StartLocation.earliest())])

            self._put(self.table_name, 1, 'one')
            records = self._poll_for_ids(consumer, self.table_name, {1})
            self.assertEqual(set(records.keys()), {1})

            consumer.add_table(table2, start_location=StartLocation.earliest())
            self._put(table2, 2, 'two')
            records = self._poll_for_ids(consumer, table2, {2})
            self.assertEqual(set(records.keys()), {2})

            consumer.remove_table(table2)
            consumer.add_table(table3, start_location=StartLocation.earliest())
            self._put(table3, 3, 'three')
            records = self._poll_for_ids(consumer, table3, {3})
            self.assertEqual(set(records.keys()), {3})

        def test_child_table(self):
            parent_table = self._table_name('Parent')
            child_table = parent_table + '.child'
            self._create_table(parent_table)
            self._create_child_table(child_table)
            self._enable_change_streaming_or_skip(parent_table)
            self._enable_change_streaming_or_skip(child_table)

            self._put(parent_table, 1, 'parent')
            child_value = {
                'id': 1,
                'childid': 10,
                'childname': 'child',
                'childdata': 'data'
            }
            self._put_value(child_table, child_value)

            consumer = self._consumer(
                'child',
                [(parent_table, StartLocation.earliest()),
                 (child_table, StartLocation.earliest())])

            parent_records = self._poll_for_ids(consumer, parent_table, {1})
            child_records = self._poll_for_child_ids(
                consumer, child_table, {10})
            self.assertEqual(set(parent_records.keys()), {1})
            self.assertEqual(set(child_records.keys()), {10})

        def test_delete_group_force_stop(self):
            self._create_table(self.table_name)
            self._enable_change_streaming_or_skip(self.table_name)

            group_id = self._group_id('delete-force')
            consumer = self._consumer(
                'delete-force',
                [(self.table_name, StartLocation.earliest())],
                group_id=group_id)

            Consumer.delete_group(
                self.handle, group_id, force_stop=True)
            self.group_ids.remove(group_id)
            try:
                consumer.close()
            except Exception:
                pass

        def _create_table(self, table_name):
            statement = (
                'CREATE TABLE IF NOT EXISTS ' + table_name +
                '(id INTEGER, name STRING, PRIMARY KEY(id))')
            request = TableRequest().set_statement(statement).set_table_limits(
                TableLimits(500, 500, 5))
            self._table_request(request)
            self.tables.append(table_name)

        def _create_child_table(self, table_name):
            statement = (
                'CREATE TABLE IF NOT EXISTS ' + table_name +
                '(childid INTEGER, childname STRING, childdata STRING, '
                'PRIMARY KEY(childid))')
            request = TableRequest().set_statement(statement)
            self._table_request(request)
            self.tables.append(table_name)

        def _drop_table(self, table_name):
            request = TableRequest().set_statement(
                'DROP TABLE IF EXISTS ' + table_name)
            try:
                self._table_request(request)
            except Exception:
                pass

        def _table_request(self, request):
            if is_pod():
                sleep(30)
            return self.handle.do_table_request(request, wait_timeout, 1000)

        def _enable_change_streaming_or_skip(self, table_name):
            try:
                self.handle.enable_change_streaming(
                    table_name, enabled=True, timeout_ms=wait_timeout,
                    poll_interval_ms=1000)
                self.streaming_tables.add(table_name)
            except OperationNotSupportedException as exc:
                self.skipTest(str(exc))

        def _consumer(self, name, tables, group_id=None, manual=True):
            if group_id is None:
                group_id = self._group_id(name)
            builder = ConsumerBuilder().set_handle(
                self.handle).set_group_id(group_id)
            if manual:
                builder.set_commit_manual()
            else:
                builder.set_commit_automatic()
            for table_name, start_location in tables:
                builder.add_table(table_name, start_location=start_location)
            consumer = builder.build()
            self.consumers.append(consumer)
            if group_id not in self.group_ids:
                self.group_ids.append(group_id)
            return consumer

        def _group_id(self, name):
            return 'pytest-cs-' + name + '-' + self.suffix

        def _table_name(self, name):
            return 'pytestChangeStreams' + name + self.suffix

        def _put_range(self, table_name, start, stop):
            for value_id in range(start, stop):
                self._put(table_name, value_id, 'name' + str(value_id))

        def _put(self, table_name, value_id, name):
            self._put_value(table_name, {'id': value_id, 'name': name})

        def _put_value(self, table_name, value):
            result = self.handle.put(PutRequest().set_table_name(
                table_name).set_value(value))
            self.assertIsNotNone(result.get_version())

        def _poll_for_ids(self, consumer, table_name, expected_ids,
                          commit=False):
            deadline = time() + float(self.POLL_TIMEOUT_MS) / 1000.0
            records = dict()
            while time() < deadline:
                bundle = consumer.poll(limit=100, wait_ms=5000)
                for record in self._records_for_table(bundle, table_name):
                    key = record.get_record_key()
                    if key is not None and key.get('id') in expected_ids:
                        current = record.get_current_image()
                        if current is not None:
                            records[key.get('id')] = record
                if commit and not bundle.is_empty():
                    bundle.commit(timeout_ms=30000)
                if expected_ids.issubset(set(records.keys())):
                    return records
            self.fail('Timed out waiting for Change Streams records: ' +
                      str(expected_ids))

        def _poll_for_child_ids(self, consumer, table_name, expected_ids):
            deadline = time() + float(self.POLL_TIMEOUT_MS) / 1000.0
            records = dict()
            while time() < deadline:
                bundle = consumer.poll(limit=100, wait_ms=5000)
                for record in self._records_for_table(bundle, table_name):
                    key = record.get_record_key()
                    if key is not None and key.get('childid') in expected_ids:
                        current = record.get_current_image()
                        if current is not None:
                            records[key.get('childid')] = record
                if expected_ids.issubset(set(records.keys())):
                    return records
            self.fail('Timed out waiting for Change Streams child records: ' +
                      str(expected_ids))

        def _poll_for_ids_across_consumers(self, consumers, table_name,
                                           expected_ids):
            deadline = time() + float(self.POLL_TIMEOUT_MS) / 1000.0
            records = dict()
            while time() < deadline:
                for consumer in consumers:
                    bundle = consumer.poll(limit=100, wait_ms=5000)
                    for record in self._records_for_table(bundle, table_name):
                        key = record.get_record_key()
                        if key is not None and key.get('id') in expected_ids:
                            current = record.get_current_image()
                            if current is not None:
                                records[key.get('id')] = record
                    if expected_ids.issubset(set(records.keys())):
                        return records
            self.fail('Timed out waiting for Change Streams records: ' +
                      str(expected_ids))

        def _poll_for_delete(self, consumer, table_name, value_id):
            deadline = time() + float(self.POLL_TIMEOUT_MS) / 1000.0
            while time() < deadline:
                bundle = consumer.poll(limit=100, wait_ms=5000)
                for record in self._records_for_table(bundle, table_name):
                    key = record.get_record_key()
                    if (key is not None and key.get('id') == value_id and
                            record.get_current_image() is None):
                        return record
            self.fail('Timed out waiting for Change Streams delete record: ' +
                      str(value_id))

        @staticmethod
        def _records_for_table(bundle, table_name):
            records = list()
            for message in bundle.get_messages() or []:
                if message.get_table_name() != table_name:
                    continue
                for event in message.get_events() or []:
                    records.extend(event.get_records())
            return records


if __name__ == '__main__':
    unittest.main()
