# -*- coding: utf-8 -*-
"""
test_log_streams.py
"""
from datetime import datetime, timedelta
import logging
import os
import os.path
import shutil
import sys
import time
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from old_log_inn.log_stream import _compute_timestamp, \
    LogStreamWriter, \
    generate_log_stream_from_file, \
    generate_log_stream_from_directory

_test_dir = "/tmp/test_log_streams"
_test_prefix = "logs."
_test_suffix = ".gz"
_output_work_dir = os.path.join(_test_dir, "output_work_dir")
_output_complete_dir = os.path.join(_test_dir, "output_complete_dir")
_time_test_list = [
    (5, datetime(2013, 1, 1, hour=12, minute=13, second=48), 
     "20130101121345", ),
    (5, datetime(2013, 1, 1, hour=12, minute=14), "20130101121400", ),
    (5, datetime(2013, 1, 1, hour=12, minute=15), "20130101121500", ),
    (5, datetime(2013, 1, 1, hour=12, minute=16, second=56), 
     "20130101121655", ),
    (5, datetime(2013, 1, 1, hour=12, minute=17), "20130101121700", ),
    (5, datetime(2013, 1, 1, hour=12, minute=18), "20130101121800", ),
    (5, datetime(2013, 1, 1, hour=12, minute=19), "20130101121900", ),
    (5, datetime(2013, 1, 1, hour=12, minute=20), "20130101122000", ),
    (300, datetime(2013, 1, 1, hour=12, minute=13), "20130101121000", ),
    (300, datetime(2013, 1, 1, hour=12, minute=14, second=2), 
     "20130101121000", ),
    (300, datetime(2013, 1, 1, hour=12, minute=15), "20130101121500", ),
    (300, datetime(2013, 1, 1, hour=12, minute=16), "20130101121500", ),
    (300, datetime(2013, 1, 1, hour=12, minute=17), "20130101121500", ),
    (300, datetime(2013, 1, 1, hour=12, minute=18), "20130101121500", ),
    (300, datetime(2013, 1, 1, hour=12, minute=19), "20130101121500", ),
    (300, datetime(2013, 1, 1, hour=12, minute=20), "20130101122000", ),
    (3600, datetime(2013, 1, 1, hour=12, minute=13), "20130101120000", ),
    (3600, datetime(2013, 1, 1, hour=12, minute=14), "20130101120000", ),
    (3600, datetime(2013, 1, 1, hour=12, minute=15), "20130101120000", ),
    (3600, datetime(2013, 1, 1, hour=12, minute=16), "20130101120000", ),
    (3600, datetime(2013, 1, 1, hour=12, minute=17), "20130101120000", ),
    (3600, datetime(2013, 1, 1, hour=12, minute=18), "20130101120000", ),
    (3600, datetime(2013, 1, 1, hour=12, minute=19), "20130101120000", ),
    (3600, datetime(2013, 1, 1, hour=12, minute=20), "20130101120000", ),
]

class TestLogStreamWriter(unittest.TestCase):
    """
    test LogStreamWriter and LogStreamReader
    """
    def setUp(self):
        self.tearDown()
        os.mkdir(_test_dir)
        os.mkdir(_output_work_dir)
        os.mkdir(_output_complete_dir)
        
    def tearDown(self):
        if os.path.isdir(_test_dir):
            shutil.rmtree(_test_dir)

    def test_compute_timestmap(self):
        """
        test writing a sin
        """
        for granularity, time_value, expected_timestamp in _time_test_list:
            timestamp = _compute_timestamp(timedelta(seconds=granularity), 
                                           time_value)
            self.assertEqual(timestamp, expected_timestamp, (granularity, 
                             time_value, ))

    def test_single_item(self):
        """
        test writing a single event and reading it back
        """
        granularity = 5
        header = b"aaa"
        data = b"bbb"

        # we expect the completed directory to be empty
        completed_list = os.listdir(_output_complete_dir)
        self.assertEqual(len(completed_list), 0, completed_list)

        writer = LogStreamWriter(_test_prefix,
                                 _test_suffix,
                                 granularity, 
                                 _output_work_dir, 
                                 _output_complete_dir)

        writer.write(header, data)

        # wait for the current file to roll over
        time.sleep(granularity+1)
        writer.check_for_rollover()

        # we expect a single file in the completed directory
        completed_list = os.listdir(_output_complete_dir)
        self.assertEqual(len(completed_list), 1, completed_list)

        stream_file_path = os.path.join(_output_complete_dir, 
                                        completed_list[0]) 
        log_stream = generate_log_stream_from_file(stream_file_path)

        read_header, read_data = next(log_stream)
        self.assertEqual(read_header, header)
        self.assertEqual(read_data, data)

        self.assertRaises(StopIteration, next, log_stream)

    def test_multiple_files(self):
        """
        test writing multiple events to a multiple files and reading them back
        """
        granularity = 5
        events = [(b"aaa", b"111"), (b"bbb", b"222"), (b"ccc", b"333"), ]

        completed_file_count = 0

        # we expect the completed directory to be empty
        completed_list = os.listdir(_output_complete_dir)
        self.assertEqual(len(completed_list), 
                         completed_file_count, 
                         completed_list)

        writer = LogStreamWriter(_test_prefix,
                                 _test_suffix,
                                 granularity, 
                                 _output_work_dir, 
                                 _output_complete_dir)

        for header, data in events:
            writer.write(header, data)

            # wait for the current file to roll over
            time.sleep(granularity+1)
            writer.check_for_rollover()

            # we expect a new file in the completed directory
            completed_file_count += 1
            completed_list = os.listdir(_output_complete_dir)
            self.assertEqual(len(completed_list), 
                             completed_file_count, 
                             completed_list)

        log_stream = generate_log_stream_from_directory(_output_complete_dir)
        for event, actual in zip(events, log_stream):
            self.assertEqual(event, actual, (event, actual, ))

        self.assertRaises(StopIteration, next, log_stream)

if __name__ == "__main__":
    unittest.main()
