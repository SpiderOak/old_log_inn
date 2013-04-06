# -*- coding: utf-8 -*-
"""
stdout_spewer.py

A program to write log lines to stdout at intervals
"""
import logging
import sys

from old_log_inn.signal_handler import set_signal_handler

_log_format_template = '%(asctime)s %(levelname)-8s %(name)-20s: %(message)s'
_log = logging.getLogger("main")

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
    _initialize_logging()

    halt_event = set_signal_handler()
    while not halt_event.is_set():
        _log.info("clam")
        halt_event.wait(1.0)

    return 0

if __name__ == "__main__":
    sys.exit(main())
