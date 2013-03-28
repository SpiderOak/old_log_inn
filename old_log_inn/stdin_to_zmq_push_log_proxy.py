# -*- coding: utf-8 -*-
"""
stdin_to_zmq_log_proxy.py

A program to accept log lines from stdin.

This is intended as a drop in substitute for the stdin-to-file logging system 
bundled with runit.
"""
import argparse
import errno
import signal 
import sys
from threading import Event

import zmq

from old_log_inn.log_line_pusher import LogLinePusher

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

def main():
    """
    main entry point
    """
    args = _parse_commandline()

    halt_event = Event()
    _set_signal_handler(halt_event)

    zmq_context = zmq.Context()
    log_line_pusher = LogLinePusher(zmq_context, args.log_path)

    while not halt_event.is_set():
        try:
            line = sys.stdin.readline()
        except IOError:
            instance = sys.exc_info()[1]
            if instance.errno == errno.EINTR and halt_event.is_set():
                break
            raise 
        log_line_pusher.push_log_line(line)

    log_line_pusher.close()
    zmq_context.term()
    return 0

if __name__ == "__main__":
    sys.exit(main())
    