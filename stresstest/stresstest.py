#
# Copyright (C) 2018, 2019 Oracle and/or its affiliates. All rights reserved.
#
# This software is licensed with the Universal Permissive License (UPL) version 1.0
#
# Please see LICENSE.txt file included in the top-level directory of the
# appropriate download for a copy of the license and additional information.
#

from __future__ import print_function
from argparse import ArgumentParser
from logging import FileHandler, getLogger
from os import mkdir, path, sep
from sys import argv
from threading import Thread
from time import time
from traceback import format_exc

from operations import Operations
from parameters import (
    iteration_time, logger_level, num_iterations, num_rows, num_tables,
    num_threads)
from testutils import LogUtils, ReportingThread, TestFailedException, Utils


class StressTest:
    def __init__(self):
        self.__proc_id = '0'
        self.__utils = None
        self.__logutils = None
        self.__logfile = None

    def parse_args(self):
        parser = ArgumentParser()
        parser.add_argument('--process', default='0',
                            help='The current process id.')
        args = parser.parse_args()
        self.__proc_id = args.process

    def get_logger(self):
        logger = getLogger(self.__class__.__name__)
        logger.setLevel(logger_level)
        log_dir = (path.abspath(path.dirname(argv[0])) + sep + 'logs')
        if not path.exists(log_dir):
            try:
                mkdir(log_dir)
            except OSError:
                # Ignore the error if the directory is made by other process.
                pass
        self.__logfile = log_dir + sep + 'stress' + self.__proc_id + '.log'
        logger.addHandler(FileHandler(self.__logfile))
        self.__logutils = LogUtils(self.__proc_id, logger)

    def show_args(self):
        setting = 'Process id: ' + self.__proc_id + \
                  '\n\tNumber of iterations: ' + str(num_iterations) + \
                  '\n\tHours of each iteration: ' + str(iteration_time) + \
                  '\n\tNumber of tables: ' + str(num_tables) + \
                  '\n\tNumber of rows of each table: ' + str(num_rows) + \
                  '\n\tNumber of threads: ' + str(num_threads)

        self.__logutils.log_info('\nStress Test:\n\t' + setting)

    def run(self):
        self.__utils = Utils(self.__logutils, self.__proc_id)
        self.__utils.add_test_tier_tenant()
        ops = Operations(self.__proc_id, self.__utils, self.__logutils)
        ops.get_handle()
        for iteration in range(1, num_iterations + 1):
            self.__logutils.log_info('Begin iteration ' + str(iteration))
            now = time()
            end_time = (int(round(time() * 1000)) +
                        iteration_time * 60 * 60 * 1000)
            threads = list()
            for thread_id in range(num_threads):
                t = StressTest.TestThread(ops, thread_id, end_time)
                threads.append(t)
                t.setDaemon(True)
                t.start()
            self.__logutils.log_info(
                'Started test threads: ' + str(num_threads))
            report = ReportingThread(
                self.__utils, self.__logutils, self.__logfile, iteration)
            report.setDaemon(True)
            report.start()
            for t in threads:
                t.join()
            report.join()
            self.__logutils.log_info('End iteration ' + str(iteration), now)
        if self.__utils.get_unexpected_count() == 0:
            self.__logutils.log_info('Test completed - passed.')
        else:
            msg = ('Test completed - failed, unexpected result: ' +
                   str(self.__utils.get_unexpected_count()))
            self.__logutils.log_info(msg)
            raise TestFailedException(msg)

    def shutdown(self):
        self.__utils.delete_test_tier_tenant()
        self.__utils.close_handle()

    class TestThread(Thread):
        def __init__(self, ops, thread_id, end_time):
            super(StressTest.TestThread, self).__init__()
            self.__ops = ops
            self.__thread_id = thread_id
            self.__end_time = end_time

        def run(self):
            self.__ops.create_tables()
            self.__ops.create_index_on_tables()
            self.__ops.populate_tables(self.__thread_id)
            self.__ops.exercise_tables(self.__thread_id, self.__end_time)
            self.__ops.drop_tables()


if __name__ == '__main__':
    test = StressTest()
    try:
        test.parse_args()
        test.get_logger()
        test.show_args()
        test.run()
        exit(0)
    except Exception:
        print(format_exc())
    finally:
        test.shutdown()
    exit(1)
