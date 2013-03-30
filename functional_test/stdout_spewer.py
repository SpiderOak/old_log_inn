# -*- coding: utf-8 -*-
"""
stdout_spewer.py

A program to write log lines to stdout at intervals
"""
import argparse
import logging
import signal 
import sys
from threading import Event

_log_format_template = '%(asctime)s %(levelname)-8s %(name)-20s: %(message)s'

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
    handler = logging.StreamHandler(stream=sys.stdout)
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

    halt_event = Event()
    _set_signal_handler(halt_event)

    log = logging.getLogger("main")

    while not halt_event.is_set():
        log.info("glort")
        halt_event.wait(1.0)

    return 0

if __name__ == "__main__":
    sys.exit(main())
