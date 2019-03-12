#
# Copyright (C) 2018, 2019 Oracle and/or its affiliates. All rights reserved.
#
# This software is licensed with the Universal Permissive License (UPL) version 1.0
#
# Please see LICENSE.txt file included in the top-level directory of the
# appropriate download for a copy of the license and additional information.
#

from json import loads
from math import pow
from numpy.random import RandomState
from string import ascii_letters, digits
from time import sleep, time

from borneo import (
    DeleteRequest, GetRequest, GetTableRequest, IndexExistsException,
    MultiDeleteRequest, PutOption, PutRequest, QueryRequest, RetryableException,
    State, TableExistsException, TableLimits, TableNotFoundException,
    TableRequest, TableResult, WriteMultipleRequest)

from parameters import (
    index_name, num_procs, num_rows, num_tables, num_threads, table_name,
    table_request_timeout, thread_sids)
from testutils import EXERCISE_OPS


class Operations:
    def __init__(self, proc_id, utils, logutils):
        self.__proc_id = int(proc_id)
        self.__utils = utils
        self.__logutils = logutils
        self.__handle = None
        self.__table_fields = dict()
        self.__fld_id = 'fld_id'
        self.__fld_seed = 'fld_seed'
        self.__fld_sid = 'fld_sid'
        self.__fld_str = 'fld_str'
        self.__max_str_len = 62
        self.__max_retry = 120
        self.__retry_interval = 10
        self.__lock = utils.get_lock()
        self.__start_id = self.__proc_id * num_rows * 2
        self.__start_sid = self.__proc_id * num_threads * thread_sids
        self.__last_sid = num_procs * num_threads * thread_sids
        self.__random = RandomState()
        self.__populate = self.__utils.get_populate_count()
        self.__exercise = self.__utils.get_exercise_count()

    def get_handle(self):
        self.__handle = self.__utils.get_handle()

    def create_tables(self):
        for i in range(num_tables):
            self.__create_table(table_name + str(i))

    def create_index_on_tables(self):
        for i in range(num_tables):
            self.__create_index(table_name + str(i))

    def drop_tables(self):
        for i in range(num_tables):
            self.__drop_table(table_name + str(i))

    def populate_tables(self, thread_id):
        self.__logutils.log_info('Start populate thread: ' + str(thread_id))
        now = time()
        for i in range(num_tables):
            self.__populate_table(table_name + str(i), thread_id)
        self.__logutils.log_info(
            'End populate thread: ' + str(thread_id), now)

    def exercise_tables(self, thread_id, end_time):
        self.__logutils.log_info('Start exercise thread: ' + str(thread_id))
        now = time()
        cnt = num_rows // num_threads
        mod = num_rows % num_threads
        start = self.__get_start(thread_id, mod, cnt)
        count = cnt + 1 if thread_id < mod else cnt
        start_sid = self.__start_sid + thread_id * thread_sids
        end_sid = start_sid + thread_sids
        while int(round(time() * 1000)) < end_time:
            for op in range(1000):
                self.__perform_ops(
                    table_name + str(self.__random.randint(num_tables)), start,
                    count, start_sid, end_sid)
        key = dict()
        key['fld_id'] = self.__proc_id * num_threads + thread_id
        key['fld_sid'] = self.__last_sid
        for i in range(num_tables):
            self.__put_if_absent(table_name + str(i), key)
        self.__logutils.log_info(
            'End exercise thread: ' + str(thread_id), now)

    def __check_query_results_num(self, statement, results, limit):
        num_records = len(results)
        if num_records > limit:
            self.__utils.unexpected_result(
                'Query statement: ' + statement + ' failed, get unexpected ' +
                'number of records, expected number is larger than 0 and ' +
                'less than ' + str(limit) + ', actual is ' + str(num_records))
            return False
        return True

    def __check_value(self, tb_name, value):
        expected = self.__get_expected_row(tb_name, value)
        if value != expected:
            self.__utils.unexpected_result(
                'Check the row value failed, actual value is: ' + str(value) +
                ', expected value is: ' + str(expected))
        else:
            self.__logutils.log_debug(
                'Check the row value succeed, value is: ' + str(value))

    def __create_index(self, tb_name):
        now = time()
        statement = (
            'CREATE INDEX ' + index_name + ' ON ' + tb_name + '(fld_str)')
        request = TableRequest().set_statement(statement)
        try:
            self.__do_table_request(tb_name, request, statement, State.ACTIVE)
            self.__logutils.log_info('Execute command: ' + statement)
            self.__logutils.log_info(
                'Index ' + index_name + ' created on ' + tb_name, now)
        except IndexExistsException:
            self.__logutils.log_debug(
                'Index ' + index_name + ' for  ' + tb_name + ' exists.')
        except Exception as e:
            self.__utils.unexpected_result(
                'Execute command \'' + statement + '\' failed. ' + str(e))

    def __create_table(self, tb_name):
        now = time()
        statement = (
            'CREATE TABLE ' + tb_name + '(fld_id LONG, fld_seed LONG, \
fld_sid INTEGER, fld_str STRING, PRIMARY KEY(SHARD(fld_sid), fld_id)) \
USING TTL 30 DAYS')
        request = TableRequest().set_statement(statement).set_table_limits(
            TableLimits(10000, 10000, 50))
        try:
            result = self.__do_table_request(
                tb_name, request, statement, State.ACTIVE)
            self.__logutils.log_info('Execute command: ' + statement)
            self.__logutils.log_info('Table ' + tb_name + ' created.', now)
            schema = result.get_schema()
        except TableExistsException:
            self.__logutils.log_debug('Table ' + tb_name + ' exists.')
            schema = self.__get_table_schema(tb_name)
        except Exception as e:
            self.__utils.unexpected_result(
                'Execute command \'' + statement + '\' failed. ' + str(e))
            self.__wait_for_state(tb_name, State.ACTIVE)
            schema = self.__get_table_schema(tb_name)
        self.__table_fields[tb_name] = loads(schema).get('fields')

    def __delete(self, tb_name, key):
        value = self.__retry_get(tb_name, key)
        try:
            request = DeleteRequest().set_key(key).set_table_name(tb_name)
            success = self.__handle.delete(request).get_success()
            if value is None and success or value is not None and not success:
                self.__utils.unexpected_result(
                    'Delete the row with primary key ' + str(key) + ' failed.')
            else:
                self.__logutils.log_debug(
                    'Delete the row with primary key ' + str(key) + ' succeed.')
        except Exception as e:
            self.__utils.unexpected_result(
                'Delete the row with primary key ' + str(key) + ' failed: ' +
                str(e))
        with self.__lock:
            self.__exercise[EXERCISE_OPS.DELETE] += 1

    def __do_get(self, tb_name, key, check=False):
        try:
            request = GetRequest().set_key(key).set_table_name(tb_name)
            value = self.__handle.get(request).get_value()
            if check:
                if value is None:
                    self.__logutils.log_debug('The row with primary key ' +
                                              str(key) + ' is not exists.')
                else:
                    self.__check_value(tb_name, value)
            return value
        except Exception as e:
            self.__utils.unexpected_result(
                'Get the row with primary key ' + str(key) + ' failed: ' +
                str(e))
            if not check:
                raise e

    def __do_query(self, tb_name, statement, check=False, fld_sid=None,
                   limit=None, dropped=False):
        try:
            query_request = QueryRequest().set_statement(statement)
            if limit is not None:
                query_request.set_limit(limit)
            results = self.__handle.query(query_request).get_results()
            if check:
                if self.__check_query_results_num(statement, results, limit):
                    for result in results:
                        if result.get(self.__fld_sid) != fld_sid:
                            self.__utils.unexpected_result(
                                'Query statement: ' + statement + ' failed, ' +
                                'get result: ' + str(result) + ' with ' +
                                'unexpected shard key ' + str(fld_sid))
                        else:
                            self.__check_value(tb_name, result)
            return results
        except Exception as e:
            if isinstance(e, TableNotFoundException) and dropped:
                raise e
            self.__utils.unexpected_result(
                'Query statement: ' + statement + ' failed: ' + str(e))

    def __do_table_request(self, tb_name, request, statement, state):
        self.__logutils.log_debug('Execute command: ' + statement)
        try:
            result = self.__handle.table_request(request)
            result = result.wait_for_state(self.__handle, tb_name, state,
                                           table_request_timeout, 10000)
            return result
        except RetryableException:
            self.__wait_for_state(tb_name, state)
            if 'CREATE INDEX' in statement:
                raise IndexExistsException('Index on ' + tb_name + ' exists.')
            elif 'CREATE TABLE' in statement:
                raise TableExistsException('Table ' + tb_name + ' exists.')
            else:
                raise TableNotFoundException('Table ' + tb_name + ' dropped.')
        except (IndexExistsException, TableExistsException,
                TableNotFoundException) as e:
            raise e

    def __drop_table(self, tb_name):
        self.__wait_proc_ops_done(tb_name)
        now = time()
        statement = ('DROP TABLE ' + tb_name)
        request = TableRequest().set_statement(statement)
        try:
            self.__do_table_request(tb_name, request, statement, State.DROPPED)
            self.__logutils.log_info('Execute command: ' + statement)
            self.__logutils.log_info('Table ' + tb_name + ' dropped.', now)
        except TableNotFoundException:
            self.__logutils.log_debug('Table ' + tb_name + ' not found.')
        except Exception as e:
            self.__utils.unexpected_result(
                'Execute command \'' + statement + '\' failed. ' + str(e))

    def __get(self, tb_name, key):
        self.__do_get(tb_name, key, True)
        with self.__lock:
            self.__exercise[EXERCISE_OPS.GET] += 1

    def __get_expected_row(self, tb_name, value):
        key = dict()
        key[self.__fld_id] = value.get(self.__fld_id)
        key[self.__fld_sid] = value.get(self.__fld_sid)
        fld_seed = value.get(self.__fld_seed)
        return self.__get_random_row(tb_name, key, fld_seed)

    def __get_random_key(self, start, count, start_sid, end_sid):
        key = dict()
        key['fld_id'] = start + self.__random.randint(count)
        key['fld_sid'] = self.__random.randint(start_sid, end_sid)
        return key

    def __get_random_row(self, tb_name, key, fld_seed=None):
        if fld_seed is None:
            fld_seed = self.__random.randint(int(pow(2, 32)))
        rand = RandomState(fld_seed)
        row = dict()
        for field in key:
            row[field] = key[field]
        row[self.__fld_seed] = fld_seed
        for field in self.__table_fields[tb_name]:
            if (field.get('name') == self.__fld_id or
                    field.get('name') == self.__fld_sid or
                    field.get('name') == self.__fld_seed):
                continue
            elif field.get('name') == self.__fld_str:
                row[self.__fld_str] = self.__get_random_str(rand)
        return row

    def __get_random_str(self, rand):
        length = rand.randint(self.__max_str_len)
        chars = ascii_letters + digits
        return ''.join(rand.choice(list(chars), length))

    def __get_start(self, thread_id, mod, cnt):
        if thread_id == 0:
            return self.__start_id
        elif mod == 0:
            return self.__start_id + thread_id * cnt
        elif thread_id <= mod:
            return self.__start_id + thread_id * (cnt + 1)
        else:
            return self.__start_id + mod * (cnt + 1) + (thread_id - mod) * cnt

    def __get_table_schema(self, tb_name):
        num_retried = 0
        exception = None
        while num_retried < self.__max_retry:
            try:
                request = GetTableRequest().set_table_name(tb_name)
                result = self.__handle.get_table(request)
                if result is not None:
                    schema = result.get_schema()
                    if schema is not None:
                        return schema
                sleep(self.__retry_interval)
            except RetryableException as re:
                exception = re
                num_retried += 1
                self.__logutils.log_debug(
                    'Retry get table request, num retries: ' +
                    str(num_retried) + ', exception: ' + str(re))
                sleep(self.__retry_interval)
            except Exception as e:
                self.__utils.unexpected_result(
                    'Get table ' + tb_name + ' failed: ' + str(e))
        self.__utils.unexpected_result(
            'Retry count exceeded for get table request, last exception is ' +
            str(exception))

    def __multi_delete(self, tb_name, fld_sid):
        statement = ('SELECT * FROM ' + tb_name + ' WHERE fld_sid = ' +
                     str(fld_sid))
        num_records = len(self.__retry_query(tb_name, statement))
        skey = dict()
        skey[self.__fld_sid] = fld_sid
        try:
            request = MultiDeleteRequest().set_table_name(tb_name).set_key(
                skey)
            deletions = self.__handle.multi_delete(request).get_num_deletions()
            if deletions != num_records:
                self.__utils.unexpected_result(
                    'Multi delete the rows with shard key ' + str(skey) +
                    ' failed, expected deletion number is: ' +
                    str(num_records) + ', actual deletion number is: '
                    + str(deletions))
            else:
                self.__logutils.log_debug('Multi delete ' + str(deletions) +
                                          ' rows with shard key ' + str(skey))
        except Exception as e:
            self.__utils.unexpected_result(
                'Multi delete the rows with shard key ' + str(skey) +
                ' failed: ' + str(e))
        with self.__lock:
            self.__exercise[EXERCISE_OPS.MULTI_DELETE] += 1

    def __perform_ops(self, tb_name, start, count, start_sid, end_sid):
        ops = 7
        op = self.__random.randint(ops)
        if op == EXERCISE_OPS.DELETE:
            self.__delete(tb_name, self.__get_random_key(
                start, count, start_sid, end_sid))
        elif op == EXERCISE_OPS.GET:
            self.__get(tb_name, self.__get_random_key(
                start, count, start_sid, end_sid))
        elif op == EXERCISE_OPS.MULTI_DELETE:
            self.__multi_delete(
                tb_name, self.__random.randint(start_sid, end_sid))
        elif op == EXERCISE_OPS.PUT_IF_ABSENT:
            self.__put_if_absent(tb_name, self.__get_random_key(
                start + num_rows, count, start_sid, end_sid))
        elif op == EXERCISE_OPS.PUT_IF_PRESENT:
            self.__put_if_present(tb_name, self.__get_random_key(
                start, count, start_sid, end_sid))
        elif op == EXERCISE_OPS.QUERY:
            self.__query(tb_name, self.__random.randint(start_sid, end_sid))
        elif op == EXERCISE_OPS.WRITE_MULTIPLE:
            self.__write_multiple(
                tb_name, self.__random.randint(start_sid, end_sid))

    def __populate_table(self, tb_name, thread_id):
        cnt = num_rows // num_threads
        mod = num_rows % num_threads
        start = self.__get_start(thread_id, mod, cnt)
        count = cnt + 1 if thread_id < mod else cnt
        start_sid = self.__start_sid + thread_id * thread_sids
        end_sid = start_sid + thread_sids
        key = dict()
        for fld_id in range(start, start + count):
            key['fld_id'] = fld_id
            key['fld_sid'] = self.__random.randint(start_sid, end_sid)
            self.__put(tb_name, key)

    def __put(self, tb_name, key):
        row = self.__get_random_row(tb_name, key)
        try:
            request = PutRequest().set_value(row).set_table_name(tb_name)
            version = self.__handle.put(request).get_version()
            if version is None:
                self.__utils.unexpected_result(
                    'Put the row ' + str(row) + ' failed.')
            else:
                self.__logutils.log_debug(
                    'Put the row ' + str(row) + ' succeed.')
        except Exception as e:
            self.__utils.unexpected_result(
                'Put the row ' + str(row) + ' failed: ' + str(e))
        with self.__lock:
            self.__populate[0] += 1

    def __put_if_absent(self, tb_name, key):
        value = self.__retry_get(tb_name, key)
        row = self.__get_random_row(tb_name, key)
        try:
            request = PutRequest().set_value(row).set_table_name(
                tb_name).set_option(PutOption.IF_ABSENT)
            version = self.__handle.put(request).get_version()
            if (value is None and version is None or
                    value is not None and version is not None):
                self.__utils.unexpected_result(
                    'Put the row ' + str(row) + ' with PutOption.IF_ABSENT ' +
                    'failed.')
            else:
                self.__logutils.log_debug(
                    'Put the row ' + str(row) + ' with PutOption.IF_ABSENT ' +
                    'succeed.')
        except Exception as e:
            self.__utils.unexpected_result(
                'Put the row ' + str(row) + ' with PutOption.IF_ABSENT ' +
                'failed: ' + str(e))
        with self.__lock:
            self.__exercise[EXERCISE_OPS.PUT_IF_ABSENT] += 1

    def __put_if_present(self, tb_name, key):
        value = self.__retry_get(tb_name, key)
        row = self.__get_random_row(tb_name, key)
        try:
            request = PutRequest().set_value(row).set_table_name(
                tb_name).set_option(PutOption.IF_PRESENT)
            version = self.__handle.put(request).get_version()
            if (value is None and version is not None or
                    value is not None and version is None):
                self.__utils.unexpected_result(
                    'Put the row ' + str(row) + ' with PutOption.IF_PRESENT ' +
                    'failed.')
            else:
                self.__logutils.log_debug(
                    'Put the row ' + str(row) + ' with PutOption.IF_PRESENT ' +
                    'succeed.')
        except Exception as e:
            self.__utils.unexpected_result(
                'Put the row ' + str(row) + ' with PutOption.IF_PRESENT ' +
                'failed: ' + str(e))
        with self.__lock:
            self.__exercise[EXERCISE_OPS.PUT_IF_PRESENT] += 1

    def __query(self, tb_name, fld_sid):
        statement = ('SELECT * FROM ' + tb_name + ' WHERE fld_sid = ' +
                     str(fld_sid))
        self.__do_query(tb_name, statement, True, fld_sid, limit=20)
        with self.__lock:
            self.__exercise[EXERCISE_OPS.QUERY] += 1

    def __retry_get(self, tb_name, key):
        while True:
            try:
                return self.__do_get(tb_name, key)
            except Exception:
                pass

    def __retry_query(self, tb_name, statement, limit=None, dropped=False):
        results = None
        while results is None:
            results = self.__do_query(
                tb_name, statement, limit=limit, dropped=dropped)
        return results

    def __wait_for_state(self, tb_name, state):
        while True:
            try:
                TableResult.wait_for_state(self.__handle, tb_name, state,
                                           table_request_timeout, 10000)
                break
            except (RetryableException, TableNotFoundException):
                sleep(1)

    def __wait_proc_ops_done(self, tb_name):
        while True:
            statement = ('SELECT * FROM ' + tb_name + ' WHERE fld_sid = ' +
                         str(self.__last_sid))
            try:
                if (len(self.__retry_query(tb_name, statement, dropped=True)) ==
                        num_procs * num_threads):
                    break
            except TableNotFoundException:
                break
            sleep(self.__retry_interval)

    def __write_multiple(self, tb_name, fld_sid):
        limit = 10
        statement = ('SELECT * FROM ' + tb_name + ' WHERE fld_sid = ' +
                     str(fld_sid))
        query_results = self.__retry_query(tb_name, statement, limit=limit)
        num_records = len(query_results)
        if num_records == 0:
            with self.__lock:
                self.__exercise[EXERCISE_OPS.IGNORE] += 1
        elif self.__check_query_results_num(statement, query_results, limit):
            try:
                request = WriteMultipleRequest()
                key = dict()
                for result in query_results:
                    key[self.__fld_id] = result.get(self.__fld_id)
                    key[self.__fld_sid] = result.get(self.__fld_sid)
                    new_row = self.__get_random_row(tb_name, key)
                    request.add(PutRequest().set_value(new_row).set_table_name(
                        tb_name), True)
                write_multiple_result = self.__handle.write_multiple(request)
                if (write_multiple_result.get_success() and
                        write_multiple_result.size() == num_records):
                    self.__logutils.log_debug(
                        'Write multiple rows with shard key ' + str(fld_sid) +
                        ' succeed, ' + str(write_multiple_result))
                else:
                    self.__utils.unexpected_result(
                        'Write multiple rows with shard key ' + str(fld_sid) +
                        ' failed, ' + str(write_multiple_result))
            except Exception as e:
                self.__utils.unexpected_result(
                    'Write multiple rows with shard key ' + str(fld_sid) +
                    ' failed: ' + str(e))
            with self.__lock:
                self.__exercise[EXERCISE_OPS.WRITE_MULTIPLE] += 1
