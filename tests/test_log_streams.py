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
try:
    import unittest2 as unittest
except ImportError:
    import unittest

from old_log_inn.log_stream import _compute_timestamp, \
    LogStreamWriter, \
    LogStreamReader 

_test_dir = "/tmp/test_log_streams"
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
        granularity = 300
        header = "aaa"
        data = "bbb"

        writer = LogStreamWriter(granularity, 
                                 _output_work_dir, 
                                 _output_complete_dir)

        writer.write(header, data)

        reader = LogStreamReader(_output_complete_dir)

        read_header, read_body = reader.next()
        self.assertEqual(read_header, header)
        self.assertEqual(read_body, body)

if __name__ == "__main__":
    unittest.main()
