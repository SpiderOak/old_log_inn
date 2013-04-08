# -*- coding: utf-8 -*-
"""
log_spewer.py

A program to write log lines based on text retrieved from a PULL socket
connected to a central data source
"""
import argparse
import errno
import logging
import sys

import zmq

from old_log_inn.zmq_util import is_ipc_protocol, prepare_ipc_path
from old_log_inn.zmq_push_log_handler import ZMQPushLogHandler
from old_log_inn.signal_handler import set_signal_handler

_log_format_template = '%(asctime)s %(levelname)-8s %(name)-20s: %(message)s'
_log = logging.getLogger("main")

def _parse_commandline():
    parser = \
        argparse.ArgumentParser(description='write log entries at intervals.')
    parser.add_argument("-l", "--log-path", dest="log_path")
    parser.add_argument("-s", "--data-source-address", 
                        dest="data_source_address")
    return parser.parse_args()

def _initialize_logging(log_path, zmq_context):
    handler = ZMQPushLogHandler(log_path, zmq_context=zmq_context)
    formatter = logging.Formatter(_log_format_template)
    handler.setFormatter(formatter)
    logging.root.addHandler(handler)
    logging.root.setLevel(logging.DEBUG)

def main():
    """
    main entry point
    """
    args = _parse_commandline()
    context = zmq.Context()

    if is_ipc_protocol(args.data_source_address):
        prepare_ipc_path(args.data_source_address)

    pull_socket = context.socket(zmq.PULL)
    pull_socket.connect(args.data_source_address)

    _initialize_logging(args.log_path, context)

    halt_event = set_signal_handler()
    while not halt_event.is_set():

        try:
            line_number_bytes = pull_socket.recv()
        except zmq.ZMQError:
            instance = sys.exc_info()[1]
            if instance.errno == errno.EINTR and halt_event.is_set():
                break
            raise

        assert pull_socket.rcvmore
        line_number = int(line_number_bytes.decode("utf-8"))

        log_text_bytes = pull_socket.recv()
        assert not pull_socket.rcvmore

        _log.info("{0:08} {1}".format(line_number, 
                                      log_text_bytes.decode("utf-8")))        

    pull_socket.close()

    # we need to shut down logging here to close the log handlers PUSH socket
    logging.shutdown()  
     
    context.term()

    return 0

if __name__ == "__main__":
    sys.exit(main())
