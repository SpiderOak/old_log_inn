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
import logging

import zmq

from old_log_inn.log_line_pusher import LogLinePusher

class ZMQPushLogHandler(logging.Handler):
    """
    Sends logging to ZMQ PUSH socket(s).

    Log messages have two parts (ZMQ multipart messages): a header and a body.

    The header is a JSON object with metadata describing the log event.

    The body is the content of the log message.   
    """
    def __init__(self, log_path, zmq_context=None, level=logging.NOTSET):
        """
        log_path
            The filename that the program would log to if it were using normal 
            file based logging. 
            This may include slashes to indicate directories.
            Used as a key to identify this log.

        zmq_context
            zeromq context. A program should have only one zmq context.
            If you don't pass a context in, we will create one.

        level
            minimum log level. This is usually set externally
        """
        super(ZMQPushLogHandler, self).__init__(level)

        # if the caller does not suppy a zeromq context (they really should)
        # we create our own and hold a reference to it so we can terminate it.
        if zmq_context is None:
            self._zmq_context = zmq.Context()
            zmq_context = self._zmq_context
        else:
            self._zmq_context = None

        self._log_line_pusher = LogLinePusher(zmq_context, log_path)

    def emit(self, record):
        """
        Do whatever it takes to actually log the specified logging record.

        record
            python LogRecord object
            we use only the string returned by record.getMessage()
        """
        self._log_line_pusher.push_log_line(record.getMessage())
        
    def close(self):
        """
        Tidy up any resources used by the handler.
        """
        super(ZMQPushLogHandler, self).close()

        self._log_line_pusher.close()

        if self._zmq_context is not None:
            self._zmq_context.term()
