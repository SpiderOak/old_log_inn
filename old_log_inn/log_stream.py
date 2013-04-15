# -*- coding: utf-8 -*-
"""
log_stream.py

objects for managing log streams: LogStreamWriter, LogStreamReader
"""
from datetime import datetime, timedelta
from gzip import GzipFile
import struct

_frame_format = "!BII"
_frame_size = struct.calcsize(_frame_format)
_frame_protocol_version = 1

def _compute_timestamp(granularity, time_value):
    """
    So if the setting is 300, and the program starts at 13 minutes
    past noon on January 1, then it will have output files with timestamps
    truncated in 5 minute intervals, like so:

        20130101121000  # 12:10pm, because 12:13 trunactes to 12:20
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
    def __init__(self, granularity, work_dir, complete_dir):
        self._granularity = timdelta(seconds=granularity)

        self._work_dir = work_dir
        self._complete_dir = complete_dir

        self._output_timestamp = None
        self._output_file_name = None

        self._output_file = None
        self._output_gzip_file = None

    def write(self, header, data):
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
        self.output_file.flush()

    def check_for_rollover(self, timestamp=None):
        """
        check the timestamp on the currently open file (if any)
        and close it if it has aged out
        """
        if self._output_timestamp is None:
            return

        if timestamp is None:
            timestamp = self._compute_current_timestamp()

        if timestamp != self._output_file_timestamp:
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
        self._output_file_name = ".".join(timestamp, "gz")
        work_path = os.path.join(self._work_dir, self._output_file_name)
        self._output_file = open(work_path, "wb")
        self._output_gzip_file = GzipFile(fileobj=self._output_file)

class LogStreamReader(object):
    def __init__(self, stream_dir):
        self._stream_dir = stream_dir

    def next(self):
        return None, None