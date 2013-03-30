# -*- coding: utf-8 -*-
"""
zmq_push_pub_forwarder.py

A program to listen on a ZMQ Pull socket, 
and re-publish every message on a ZMQ Pub socket.
"""
import argparse
import errno
import signal 
import socket
import sys

import zmq

from old_log_inn.zmq_util import is_ipc_protocol, \
                                 prepare_ipc_path, \
                                 is_interrupted_system_call
from old_log_inn.signal_handler import set_signal_handler

def _parse_commandline():
    parser = \
        argparse.ArgumentParser(description='push_pub_forwarder')
    parser.add_argument("--pull", dest="zmq_pull_socket_address")
    parser.add_argument("--pub", dest="zmq_pub_socket_address")
    parser.add_argument("--topic", dest="topic", default=socket.gethostname())
    parser.add_argument("--hwm", dest="hwm", type=int, default=20000)

    return parser.parse_args()

def main():
    """
    main entry point
    """
    args = _parse_commandline()
    for address in [args.zmq_pub_socket_address, args.zmq_pull_socket_address]:
        if is_ipc_protocol(address):
            prepare_ipc_path(address)

    context = zmq.Context()

    pub_socket = context.socket(zmq.PUB)
    pub_socket.bind(args.zmq_pub_socket_address)
    pub_socket.setsockopt(zmq.HWM, args.hwm)

    pull_socket = context.socket(zmq.PULL)
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

        if pull_socket in result and result[pull_socket] == zmq.POLLIN:

            # we expect a compressed header followed by a compressed body
            header = pull_socket.recv()
            assert pull_socket.rcvmore
            body = pull_socket.recv()
            assert not pull_socket.rcvmore

            # send out what we got in, preceded by the pub topic
            pub_socket.send(args.topic, zmq.SNDMORE)
            pub_socket.send(header, zmq.SNDMORE)
            pub_socket.send(body)

    pub_socket.close()
    pull_socket.close()
    context.term()
    return 0

if __name__ == "__main__":
    sys.exit(main())