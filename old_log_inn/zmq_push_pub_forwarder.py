# -*- coding: utf-8 -*-
"""
zmq_push_pub_forwarder.py

A program to listen on a ZMQ Pull socket, 
and re-publish every message on a ZMQ Pub socket.
"""
import argparse
import errno
import logging
import os
import socket
import sys

import zmq

from old_log_inn.zmq_util import is_ipc_protocol, prepare_ipc_path
from old_log_inn.signal_handler import set_signal_handler

_log_format_template = '%(asctime)s %(levelname)-8s %(name)-20s: %(message)s'
_log = logging.getLogger("main") 
_hostname = os.environ.get("HOSTNAME", socket.gethostname())

def _parse_commandline():
    parser = \
        argparse.ArgumentParser(description='push_pub_forwarder')
    parser.add_argument("--pull", dest="zmq_pull_socket_address")
    parser.add_argument("--pub", dest="zmq_pub_socket_address")
    parser.add_argument("--topic", dest="topic", default=_hostname)
    parser.add_argument("--hwm", dest="hwm", type=int, default=20000)
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

    for address in [args.zmq_pub_socket_address, args.zmq_pull_socket_address]:
        if is_ipc_protocol(address):
            prepare_ipc_path(address)

    topic_bytes = args.topic.encode("utf-8")

    context = zmq.Context()

    pub_socket = context.socket(zmq.PUB)
    _log.info("binding pub_socket to {0}".format(args.zmq_pub_socket_address))
    pub_socket.bind(args.zmq_pub_socket_address)
    pub_socket.setsockopt(zmq.HWM, args.hwm)

    pull_socket = context.socket(zmq.PULL)
    _log.info("binding pull_socket to {0}".format(args.zmq_pull_socket_address))
    pull_socket.bind(args.zmq_pull_socket_address)

    poller = zmq.Poller()
    poller.register(pull_socket, zmq.POLLIN)

    halt_event = set_signal_handler()
    while not halt_event.is_set():

        try:
            result = dict(poller.poll())
        except zmq.ZMQError:
            instance = sys.exc_info()[1]
            if instance.errno == errno.EINTR and halt_event.is_set():
                break
            raise
        _log.debug("poller received {0}".format(result))

        if pull_socket in result and result[pull_socket] == zmq.POLLIN:

            # we expect a compressed header followed by a compressed body
            header = pull_socket.recv()
            assert pull_socket.rcvmore
            body = pull_socket.recv()
            assert not pull_socket.rcvmore

            # send out what we got in, preceded by the pub topic
            pub_socket.send(topic_bytes, zmq.SNDMORE)
            pub_socket.send(header, zmq.SNDMORE)
            pub_socket.send(body)

    _log.info("shutting down")
    pub_socket.close()
    pull_socket.close()
    context.term()
    return 0

if __name__ == "__main__":
    sys.exit(main())