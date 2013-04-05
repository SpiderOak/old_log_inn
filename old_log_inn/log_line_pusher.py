# -*- coding: utf-8 -*-
"""
log_line_pusher.py

An object that pushes individual log lines over zeromq.
This object is intended for use by both the ZMQPushLogHandler and
the Stdin to ZMQ Push Log Handler.
"""
import json
import os
import socket
import time
import uuid
import zlib

import zmq

from old_log_inn.zmq_util import is_ipc_protocol, prepare_ipc_path

class LogLinePusher(object):
    """
    an object that pushes individual log lines over zeromq
    """
    def __init__(self, zmq_context, log_path):
        """
        zmq_context
            zeromq context
            used for creating sockets in init: we do not hold a reference

        log_path
            The filename that the program would log to if it were using normal 
            file based logging. 
            This may include slashes to indicate directories.
            Used as a key to identify this log.
        """
        self._log_path = log_path
        self._hostname = socket.gethostname()
        self._nodename = os.environ.get("ZMQ_LOG_NODE_NAME")
        self._uuid = uuid.uuid4()
        self._sequence = 0
        self._push_sockets = list()
        for address in os.environ["PYTHON_ZMQ_LOG_HANDLER"].split():
            if is_ipc_protocol(address):
                prepare_ipc_path(address)
            push_socket = zmq_context.socket(zmq.PUSH)
            push_socket.setsockopt(zmq.LINGER, 5000)
            push_socket.connect(address)
            self._push_sockets.append(push_socket)

    def push_log_line(self, log_line):
        """
        log_line
            one log entry
        """
        self._sequence += 1

        header = {"hostname"    : self._hostname,
                  "uuid"        : self._uuid.hex,
                  "sequence"    : self._sequence,
                  "pid"         : os.getpid(),
                  "timestamp"   : time.time(),
                  "log_path"    : self._log_path}
        if self._nodename is not None:
            header["nodename"] = self._nodename

        header_json = json.dumps(header)
        compressed_header = zlib.compress(header_json.encode("utf-8"))

        compressed_record = zlib.compress(log_line.encode("utf-8"))

        for push_socket in self._push_sockets:
            push_socket.send(compressed_header, zmq.SNDMORE)
            push_socket.send(compressed_record)

    def close(self):
        """
        shut down the line pusher
        """
        for push_socket in self._push_sockets:
            push_socket.close()
