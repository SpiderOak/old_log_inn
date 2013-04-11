# -*- coding: utf-8 -*-
"""
zmq_log_file_logger.py

A Logging to regular rotating log files.

In many situations it is convienient to keep a small amount of regular local 
disk files avaliable for inspecting very recent logs. 
Log events are flushed to files as they are received (no buffering.)
"""
import argparse
import errno
import json
import logging
import logging.handlers
import os
import os.path
import re
import socket
import sys
import zlib 

import zmq

from old_log_inn.zmq_util import is_ipc_protocol, prepare_ipc_path
from old_log_inn.signal_handler import set_signal_handler

_log_format_template = '%(asctime)s %(levelname)-8s %(name)-20s: %(message)s'
_hostname = os.environ.get("HOSTNAME", socket.gethostname())

class DummyLogRecord(object):
    """
    This gives the log handler what it wants to see
    """
    def __init__(self, log_line):
        self._log_line = log_line
        self.exc_info = None
        self.exc_text = None
        self.stack_info = None
    def getMessage(self):
        return self._log_line

def _parse_commandline():
    parser = \
        argparse.ArgumentParser(description='subscription_aggregator')
    parser.add_argument("--sub", dest="zmq_sub_address")
    parser.add_argument("--zmq-identity", dest="zmq_identity",
                        default="file_logger.{0}".format(_hostname))
    parser.add_argument("--from-files", dest="from_files")
    parser.add_argument("--output",  dest="log_dir")
    parser.add_argument("--host-regexp", dest="host_regexp")
    parser.add_argument("--node-regexp", dest="node_regexp")
    parser.add_argument("--log-filename-regexp", dest="log_filename_regexp")
    parser.add_argument("--content-regexp", dest="content_regexp")
    parser.add_argument("--add-hostname-to-path", dest="add_hostname_to_path",
                        action="store_true",  default=False) 
    parser.add_argument("--logfile-max-size", dest="logfile_max_size",
                        type=int, default=1024 ** 2)
    parser.add_argument("--logfile-keep", dest="logfile_keep", 
                        type=int, default=0)

    return parser.parse_args()

def _get_one_message(sub_socket):
    """
    retrieve a message (3 parts)
    decompress the parts
    decode the parts into unicode
    parse the JSON header into python objects
    """
    _topic = sub_socket.recv()
    assert sub_socket.rcvmore
    compressed_header = sub_socket.recv()
    assert sub_socket.rcvmore
    compressed_body = sub_socket.recv()
    assert not sub_socket.rcvmore

    raw_header = zlib.decompress(compressed_header)
    header = json.loads(raw_header.decode("utf-8"))
    raw_body = zlib.decompress(compressed_body)
    body = raw_body.decode("utf-8")

    return header, body

def _compute_log_filename(args, header):
    log_filename = os.path.basename(header["log_path"])
    if args.add_hostname_to_path:
        log_filename = "_".join([header["hostname"], log_filename])
    return log_filename

def _create_log_handler(args, log_filename):
    """
    we create a log handler, but do not attach it to root
    """
    log_path = os.path.join(args.log_dir, log_filename)

    handler = \
        logging.handlers.RotatingFileHandler(log_path,
                                             mode="a", 
                                             maxBytes=args.logfile_max_size,
                                             backupCount=args.logfile_keep,
                                             encoding="utf-8")
    formatter = logging.Formatter("%(message)s")
    handler.setFormatter(formatter)
    return handler

def _initialize_log(args):
    log_level = logging.DEBUG
    handler = _create_log_handler(args, "zmq_log_file_logger.log")
    formatter = logging.Formatter(_log_format_template)
    handler.setFormatter(formatter)

    logging.root.addHandler(handler)
    logging.root.setLevel(log_level)

def _create_header_filters(args):
    filters = list()
    if args.host_regexp is None:
        filters.append(lambda _: True)
    else:
        regex = re.compile(args.host_regexp)
        filters.append(lambda h: regex.match(h["hostname"]) is not None)

    if args.node_regexp is None:
        filters.append(lambda _: True)
    else:
        regex = re.compile(args.node_regexp)
        filters.append(lambda h: "nodename" in h and \
                       regex.match(h["nodename"]) is not None)

    if args.log_filename_regexp is None:
        filters.append(lambda _: True)
    else:
        regex = re.compile(args.log_filename_regexp)
        filters.append(lambda h: regex.match(h["log_path"]) is not None)

    return filters

def _create_body_filter(args):
    if args.content_regexp is None:
        return lambda _: True

    regex = re.compile(args.content_regexp)
    return lambda d: regex.match(d) is not None

def main():
    """
    main entry point
    """
    args = _parse_commandline()

    if not os.path.isdir(args.log_dir):
        os.makedirs(args.log_dir)

    _initialize_log(args)
    log = logging.getLogger("main")
    log.info("program starts")

    header_filters = _create_header_filters(args)
    body_filter = _create_body_filter(args)

    if is_ipc_protocol(args.zmq_sub_address):
        prepare_ipc_path(args.zmq_sub_address)

    identity_bytes = args.zmq_identity.encode("utf-8")

    context = zmq.Context()

    sub_socket = context.socket(zmq.SUB)
    sub_socket.connect(args.zmq_sub_address)
    sub_socket.setsockopt(zmq.IDENTITY, identity_bytes)
    sub_socket.setsockopt(zmq.SUBSCRIBE, "".encode("utf-8"))

    log_handlers = dict()
    halt_event = set_signal_handler()
    while not halt_event.is_set():

        try:
            header, body = _get_one_message(sub_socket)
        except zmq.ZMQError:
            instance = sys.exc_info()[1]
            if instance.errno == errno.EINTR and halt_event.is_set():
                break
            raise
        log.debug("received {0}".format(header))

        if not all([f(header) for f in header_filters]):
            log.debug("header does not pass filters {0}".format(header))
            continue

        if not body_filter(body):
            log.debug("body does not pass filters {0}".format(body))
            continue

        log_filename = _compute_log_filename(args, header)
        if not log_filename in log_handlers:
            log_handlers[log_filename] = _create_log_handler(args, 
                                                             log_filename)
        log_handler = log_handlers[log_filename]
        log_record = DummyLogRecord(body)
        log_handler.emit(log_record)
        log_handler.flush()

    log.info("program shutting down")
    sub_socket.close()
    context.term()

    for log_handler in log_handlers.values():
        log_handler.close()

    return 0

if __name__ == "__main__":
    sys.exit(main())