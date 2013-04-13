# -*- coding: utf-8 -*-
"""
log_stream.py

objects for managing log streams: LogStreamWriter, LogStreamReader
"""
from datetime import datetime, timedelta
import struct

_control_format = "!BII"
_control_size = struct.calcsize(_control_format)
_control_protocol_version = 1

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
        self._granularity = granularity
        self._work_dir = work_dir
        self._complete_dir = complete_dir

        self._output_file = None

    def write(self, header, data):
        pass


class LogStreamReader(object):
    def __init__(self, stream_dir):
        self._stream_dir = stream_dir

    def next(self):
        return None, None