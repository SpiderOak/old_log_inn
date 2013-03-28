# -*- coding: utf-8 -*-
"""
log_spewer.py

A program to write log lines at intervals
"""
import argparse
import logging
import signal 
import sys
from threading import Event

from old_log_inn.zmq_push_log_handler import ZMQPushLogHandler

def _create_signal_handler(halt_event):
    def cb_handler(*_):
        halt_event.set()
    return cb_handler

def _set_signal_handler(halt_event):
    """
    set a signal handler to set halt_event when SIGTERM is raised
    """
    signal.signal(signal.SIGTERM, _create_signal_handler(halt_event))

def _parse_commandline():
    parser = \
        argparse.ArgumentParser(description='write log entries at intervals.')
    parser.add_argument("-l", "--log-path", dest="log_path")
    return parser.parse_args()

def _initialize_logging(log_path):
    handler = ZMQPushLogHandler(log_path)
    logging.root.addHandler(handler)
    logging.root.setLevel(logging.DEBUG)

def main():
    """
    main entry point
    """
    args = _parse_commandline()
    _initialize_logging(args.log_path)

    halt_event = Event()
    _set_signal_handler(halt_event)

    while not halt_event.is_set():
        halt_event.wait(1.0)

    return 0

if __name__ == "__main__":
    sys.exit(main())
    