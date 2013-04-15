# -*- coding: utf-8 -*-
"""
data_source.py

provide data for the spewers in the functional test
"""
import argparse
import logging
import sys

import zmq

from old_log_inn.zmq_util import is_ipc_protocol, prepare_ipc_path
from old_log_inn.signal_handler import set_signal_handler

_log_format_template = '%(asctime)s %(levelname)-8s %(name)-20s: %(message)s'
_log = logging.getLogger("main") 

def _parse_commandline():
    parser = argparse.ArgumentParser(description='data_source')
    parser.add_argument("-s", "--source-path", dest="source_path")
    parser.add_argument("-p", "--push-socket-address", 
                        dest="push_socket_address")
    parser.add_argument("--verbose", dest="verbose", action="store_true", 
                        default=False)
    return parser.parse_args()

def _initialize_logging(verbose):
    """
    log to stdout for debugging
    """
    handler = logging.StreamHandler(stream=sys.stdout)
    formatter = logging.Formatter(_log_format_template)
    handler.setFormatter(formatter)
    logging.root.addHandler(handler)
    log_level = (logging.DEBUG if verbose else logging.WARN)
    logging.root.setLevel(log_level)

def main():
    """
    main entry point
    """
    args = _parse_commandline()

    _initialize_logging(args.verbose)
    _log.debug("program starts")

    if is_ipc_protocol(args.push_socket_address):
        prepare_ipc_path(args.push_socket_address)

    context = zmq.Context()

    push_socket = context.socket(zmq.PUSH)
    _log.info("binding push socket to {0}".format(args.push_socket_address))
    push_socket.bind(args.push_socket_address)

    halt_event = set_signal_handler()
    line_count = 0
    _log.debug("opening {0}".format(args.source_path))
    with open(args.source_path, "r") as input_file:
        for line in input_file.readlines():
            if halt_event.is_set():
                _log.warn("breaking read loop: halt_event is set")
                break
            line_count += 1
            push_socket.send(str(line_count).encode("utf-8"), zmq.SNDMORE)
            push_socket.send(line[:-1].encode("utf-8"))

    _log.debug("shutting down: published {0} lines".format(line_count))
    push_socket.close()
    context.term()
    return 0

if __name__ == "__main__":
    sys.exit(main())
