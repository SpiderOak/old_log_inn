# -*- coding: utf-8 -*-
"""
stdout_spewer.py

A program to write log lines based on text retrieved from a PULL socket
connected to a central data source
"""
import argparse
import errno
import logging
import sys

import zmq

from old_log_inn.zmq_util import is_ipc_protocol, prepare_ipc_path
from old_log_inn.signal_handler import set_signal_handler

_log_format_template = '%(asctime)s %(levelname)-8s %(name)-20s: %(message)s'
_log = logging.getLogger("main")

def _parse_commandline():
    parser = \
        argparse.ArgumentParser(description='write log entries at intervals.')
    parser.add_argument("-s", "--data-source-address", 
                        dest="data_source_address")
    return parser.parse_args()

def _initialize_logging():
    handler = logging.StreamHandler(stream=sys.stdout)
    formatter = logging.Formatter(_log_format_template)
    handler.setFormatter(formatter)
    logging.root.addHandler(handler)
    logging.root.setLevel(logging.DEBUG)

def main():
    """
    main entry point
    """
    returncode = 0
    args = _parse_commandline()
    context = zmq.Context()
    
    _initialize_logging()

    if is_ipc_protocol(args.data_source_address):
        prepare_ipc_path(args.data_source_address)

    pull_socket = context.socket(zmq.PULL)
    pull_socket.connect(args.data_source_address)

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

        # 2013-04-15 dougfort -- the logging StreamHandler really wants
        # ascii, and it goes into an endless exception loop if it
        # gets non ascii characters in utf-8
        try:
            log_text = log_text_bytes.decode("ascii")
        except Exception:
            log_text = "??? not ascii ???"
        
        _log.info("{0:08} {1}".format(line_number, log_text))

    pull_socket.close()
    context.term()

    return returncode

if __name__ == "__main__":
    sys.exit(main())
