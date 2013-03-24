# -*- coding: utf-8 -*-
"""
zmq_push_log_handler.py

Reads the ENV variable PYTHON_ZMQ_LOG_HANDLER for a space delimited list of 
ZMQ PUSH socket addresses. Sends logging to a PUSH socket targeting each of 
those addresses.

Log messages have two parts (ZMQ multipart messages): a header and a body.

The header is a JSON object with metadata describing the log event.

The body is the content of the log message.

Both header and body are compressed (independently) by zlib.

The header consists of these additional fields:

log_event_id a uuid or similar unique string that won't collide between 
multiple hosts and programs, such that we can de-duplicate log messages. 
should be cheap. uuid plus increment would be sufficient.

hostname

nodename if available (get from ENV PYTHON_ZMQ_LOG_NODE_NAME. 
If it's not present, omit this field from the header)

pid: via os.getpid()

timestamp: time.time()

log_filename: the filename that the program would log to if it were using 
normal file based logging. This may include slashes to indicate directories.
"""
import json
import logging
import os
import os.path
import socket
import uuid
import zlib

import zmq

from old_log_inn.zmq_util import is_ipc_protocol, prepare_ipc_path

class ZMQPushLogHandler(logging.Handler):
    """
    Sends logging to ZMQ PUSH socket(s).

    Log messages have two parts (ZMQ multipart messages): a header and a body.

    The header is a JSON object with metadata describing the log event.

    The body is the content of the log message.   
    """
    def __init__(self, log_path, level=logging.NOTSET):
        super(ZMQPushLogHandler, self).__init__(level)
        self._log_path = log_path
        self._hostname = socket.gethostname()
        self._uuid = uuid.uuid4()
        self._sequence = 0
        self._zmq_context = zmq.Context()
        self._push_sockets = list()
        for address in os.environ["PYTHON_ZMQ_LOG_HANDLER"].split():
            if is_ipc_protocol(address):
                prepare_ipc_path(address)
            push_socket = self._zmq_context.socket(zmq.PUSH)
            push_socket.setsockopt(zmq.LINGER, 5000)
            push_socket.connect(address)
            self._push_sockets.append(push_socket)

    def emit(self, record):
        """
        Do whatever it takes to actually log the specified logging record.
        """
        self._sequence += 1

        header = {"hostname"    : self._hostname,
                  "uuid"        : self._uuid.hex,
                  "sequence"    : self._sequence,
                  "pid"         : record.process,
                  "timestamp"   : record.created,
                  "log_path"    : self._log_path}
        compressed_header = zlib.compress(json.dumps(header))

        compressed_record = zlib.compress(record.getMessage())

        for push_socket in self._push_sockets:
            push_socket.send(compressed_header, zmq.SNDMORE)
            push_socket.send(compressed_record)
        
    def close(self):
        """
        Tidy up any resources used by the handler.
        """
        super(ZMQPushLogHandler, self).close()
        for push_socket in self._push_sockets:
            push_socket.close()
        self._zmq_context.term()
