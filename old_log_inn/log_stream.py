# -*- coding: utf-8 -*-
"""
log_stream.py

objects for managing log streams: LogStreamWriter, LogStreamReader
"""
from datetime import datetime, timedelta
from gzip import GzipFile
import os
import os.path
import re
import struct

class LogStreamError(Exception):
    pass

_frame_format = "!BII"
_frame_size = struct.calcsize(_frame_format)
_frame_protocol_version = 1

def _compute_timestamp(granularity, time_value):
    """
    So if the setting is 300, and the program starts at 13 minutes
    past noon on January 1, then it will have output files with timestamps
    truncated in 5 minute intervals, like so:

        20130101121000  # 12:10pm, because 12:13 truncates to 12:20
        20130101121500
        20130101122000

    """
    # compute the nearest hour <= time_value
    floor_hour = datetime(time_value.year,
                          time_value.month,
                          time_value.day,
                          time_value.hour)

    # compute the delta from the floor hour
    residue = time_value - floor_hour

    # compute the highest multiple of granularity less than the residue
    time_slot = (residue // granularity) * granularity

    # add the multiple of granularity to floor hour to get the timestamp
    timestamp_value = floor_hour + time_slot

    # return a formatted string
    return timestamp_value.strftime("%Y%m%d%H%M%S")

class LogStreamWriter(object):
    def __init__(self, prefix, suffix, granularity, work_dir, complete_dir):
        self._prefix = prefix
        self._suffix = suffix
        self._granularity = timedelta(seconds=granularity)

        self._work_dir = work_dir
        self._complete_dir = complete_dir

        self._output_timestamp = None
        self._output_file_name = None

        self._output_file = None
        self._output_gzip_file = None

    def write(self, header, data):
        """
        write header and data to the stream in a formatted frame
        both header and data must be of type bytes
        """
        timestamp = self._compute_current_timestamp()
        self.check_for_rollover(timestamp)

        if self._output_timestamp is None:
            self._open_output_file(timestamp)

        frame = struct.pack(_frame_format, 
                            _frame_protocol_version, 
                            len(header),
                            len(data))
        self._output_gzip_file.write(frame)
        self._output_gzip_file.write(header)
        self._output_gzip_file.write(data)
        self._output_file.flush()

    def check_for_rollover(self, timestamp=None):
        """
        check the timestamp on the currently open file (if any)
        and close it if it has aged out
        """
        if self._output_timestamp is None:
            return

        if timestamp is None:
            timestamp = self._compute_current_timestamp()

        if timestamp != self._output_timestamp:
            self._close_current_file()

    def _compute_current_timestamp(self):
        """
        return the timestamp for the current moment
        """
        return _compute_timestamp(self._granularity, datetime.utcnow())

    def _close_current_file(self):
        self._output_gzip_file.close()
        self._output_gzip_file = None

        self._output_file.close()
        self._output_file = None

        work_path = os.path.join(self._work_dir, self._output_file_name)
        complete_path = os.path.join(self._complete_dir, 
                                     self._output_file_name)
        os.rename(work_path, complete_path)

        self._output_timestamp = None
        self._output_file_name = None

    def _open_output_file(self, timestamp):
        self._output_timestamp = timestamp
        self._output_file_name = \
            ".".join([self._prefix, timestamp, self._suffix, ])
        work_path = os.path.join(self._work_dir, self._output_file_name)
        self._output_file = open(work_path, "wb")
        self._output_gzip_file = GzipFile(fileobj=self._output_file)

def generate_log_stream_from_file(path):
    """
    yield a sequence of (header, data) tuples from a named file
    """
    input_gzip_file = GzipFile(filename=path)

    packed_frame = input_gzip_file.read(_frame_size)
    if len(packed_frame) < _frame_size:
        raise StopIteration()

    protocol_version, header_size, data_size = struct.unpack(_frame_format,
                                                             packed_frame)
    if protocol_version != _frame_protocol_version:
        raise LogStreamError("Invalid protocol {0} expected {1}".format(
            protocol_version, _frame_protocol_version))

    header = input_gzip_file.read(header_size)
    if len(header) != header_size:
        raise LogStreamError("Invalid header read {0} expected {1}".format(
            len(header), header_size))

    data = input_gzip_file.read(data_size)
    if len(data) != data_size:
        raise LogStreamError("Invalid data read {0} expected {1}".format(
            len(data), data_size))

    yield header, data

def generate_log_stream_from_directory(directory_name):
    """
    yield a sequence of (header, data) tuples from the files in a directory
    """
    for file_name in sorted(os.listdir(directory_name)):
        path = os.path.join(directory_name, file_name)
        # this could be 'yield from' in Python 3.3
        for header, data in generate_log_stream_from_file(path):
            yield  header, data       
