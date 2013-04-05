# -*- coding: utf-8 -*-
"""
zmq_subscription_aggregator.py

A program to ubscribe to the log streams from every node, and re-publish them 
on a single local pub socket, using HWM to give subscribers some protection 
against disconnects.
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
        argparse.ArgumentParser(description='subscription_aggregator')
    parser.add_argument("--sub-list", dest="sub_list_path")
    parser.add_argument("--pub", dest="zmq_pub_socket_address")
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
    logging.root.setLevel(logging.DEBUG)

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

    if is_ipc_protocol(args.zmq_pub_socket_address):
        prepare_ipc_path(args.zmq_pub_socket_address)

    context = zmq.Context()

    pub_socket = context.socket(zmq.PUB)
    _log.info("binding pub socket to {0}".format(args.zmq_pub_socket_address))
    pub_socket.bind(args.zmq_pub_socket_address)
    pub_socket.setsockopt(zmq.HWM, args.hwm)

    poller = zmq.Poller()

    sub_socket_list = list()
    for sub_socket_address in sub_address_list:
        sub_socket = context.socket(zmq.SUB)
        sub_socket.setsockopt(zmq.SUBSCRIBE, "".encode("utf-8"))
        _log.info("connecting sub_socket to {0}".format(sub_socket_address))
        sub_socket.connect(sub_socket_address)
        poller.register(sub_socket, zmq.POLLIN)
        sub_socket_list.append(sub_socket)

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
            pub_socket.send(topic, zmq.SNDMORE)
            pub_socket.send(header, zmq.SNDMORE)
            pub_socket.send(body)

    _log.debug("shutting down")
    pub_socket.close()
    for sub_socket in sub_socket_list:
        sub_socket.close()
    context.term()
    return 0

if __name__ == "__main__":
    sys.exit(main())