# -*- coding: utf-8 -*-
"""
log_spewer.py

A program to write log lines at intervals
"""
import argparse
import logging
import sys

from old_log_inn.zmq_push_log_handler import ZMQPushLogHandler
from old_log_inn.signal_handler import set_signal_handler

_log_format_template = '%(asctime)s %(levelname)-8s %(name)-20s: %(message)s'

def _parse_commandline():
    parser = \
        argparse.ArgumentParser(description='write log entries at intervals.')
    parser.add_argument("-l", "--log-path", dest="log_path")
    return parser.parse_args()

def _initialize_logging(log_path):
    handler = ZMQPushLogHandler(log_path)
    formatter = logging.Formatter(_log_format_template)
    handler.setFormatter(formatter)
    logging.root.addHandler(handler)
    logging.root.setLevel(logging.DEBUG)

def main():
    """
    main entry point
    """
    args = _parse_commandline()
    _initialize_logging(args.log_path)

    log = logging.getLogger("main")

    halt_event = set_signal_handler()
    while not halt_event.is_set():
        log.info("glort")
        halt_event.wait(1.0)

    return 0

if __name__ == "__main__":
    sys.exit(main())
