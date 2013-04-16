# -*- coding: utf-8 -*-
"""
zmq_log_stream_writer.py

This program receives the logs from the ZMQ subscription aggregator, 
and stores them in compressed, rotating, local disk files.
The design here is intended such that multiple aggregator programs on different 
nodes (for redundancy) will put the same log records in similarly timestamped 
output files, yet that they can be aggregate and deduplicated later.

An output file has a filename like maple1.YYYYMMDDHHMMSS.gz. 
The contents are a stream of records, each containing one log event. 
Files are always compressed with gzip as they are written, and the compressor 
and the file are flushed as each record is added.

When it's completed, it will be renamed using the --output-suffix command line 
argument, to something like: maple1.YYYYMMDDHHMMSS.gz.complete
"""
import argparse
import errno
import logging
import os
import os.path
import socket
import sys

import zmq

from old_log_inn.zmq_util import is_ipc_protocol, prepare_ipc_path
from old_log_inn.signal_handler import set_signal_handler
from old_log_inn.log_stream import 

_log_format_template = '%(asctime)s %(levelname)-8s %(name)-20s: %(message)s'
_log = logging.getLogger("main") 
_hostname = os.environ.get("HOSTNAME", socket.gethostname())

def _parse_commandline():
    parser = argparse.ArgumentParser(description='log_stream_writer')
    parser.add_argument("--sub-list", dest="sub_list_path")
    parser.add_argument("--zmq-identity", dest="zmq_identity", 
                        default="log_aggregator.{0}".format(_hostname))
    parser.add_argument("--granularity", dest="granularity", type=int, 
                        default=300)
    parser.add_argument("--output-prefix", dest="output_prefix", 
                        default="logs.")
    parser.add_argument("--output-suffix", dest="output_suffix",
                        default=".{0}.gz".format(_hostname))
    parser.add_argument("--output-work-dir", dest="output_work_dir") 
    parser.add_argument("--output-complete-dir", dest="output_complete_dir")
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

def _load_sub_list(sub_list_path):
    """
    load a list of socket addresses to subscribe to
    """
    with open(sub_list_path) as input_file:
        return [line[:-1] for line in input_file.readlines()]

def main():
    """
    main entry point
    """
    args = _parse_commandline()

    _initialize_logging(args.verbose)

    sub_address_list = _load_sub_list(args.sub_list_path)
    for address in sub_address_list:
        if is_ipc_protocol(address):
            prepare_ipc_path(address)

    for directory in [args.output_work_dir, args.output_complete_dir, ]:
        if not os.path.isdir(directory):
            _log.info("creating {0}".format(directory))
            os.makedirs(directory) 

    context = zmq.Context()

    poller = zmq.Poller()

    sub_socket_list = list()
    for sub_socket_address in sub_address_list:
        sub_socket = context.socket(zmq.SUB)
        sub_socket.setsockopt(zmq.SUBSCRIBE, "".encode("utf-8"))
        _log.info("connecting sub_socket to {0}".format(sub_socket_address))
        sub_socket.connect(sub_socket_address)
        poller.register(sub_socket, zmq.POLLIN)
        sub_socket_list.append(sub_socket)

    stream_writer = LogStreamWriter(args.output_prefix,
                                    args.output_suffix,
                                    args.granularity,
                                    args.output_work_dir,
                                    args.output_complete_dir)

    halt_event = set_signal_handler()
    while not halt_event.is_set():

        try:
            result_list = poller.poll()
        except zmq.ZMQError:
            instance = sys.exc_info()[1]
            if instance.errno == errno.EINTR and halt_event.is_set():
                break
            raise

        for sub_socket, event in result_list: 
            assert event == zmq.POLLIN, event

            _log.debug("traffic on socket {0}".format(sub_socket))

            # we expect topic, compressed header, compressed body
            topic = sub_socket.recv()
            assert sub_socket.rcvmore
            header = sub_socket.recv()
            assert sub_socket.rcvmore
            body = sub_socket.recv()
            assert not sub_socket.rcvmore

            # send out what we got in

    _log.debug("shutting down")
    for sub_socket in sub_socket_list:
        sub_socket.close()
    context.term()
    return 0

if __name__ == "__main__":
    sys.exit(main())