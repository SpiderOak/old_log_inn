# -*- coding: utf-8 -*-
"""
zeromq_util.py

Utility routines to support zeromq messsaging
"""
import os
import os.path

_ipc_protocol_label = "ipc://"

def is_ipc_protocol(address):
    """
    return True if the address is for IPC 
    """
    return address.startswith(_ipc_protocol_label)

def prepare_ipc_path(address):
    """
    IPC sockets need an existing file for an address
    """
    path = address[len(_ipc_protocol_label):]
    dir_name = os.path.dirname(path)
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
    if not os.path.exists(path):
        with open(path, "w") as output_file:
            output_file.write("pork")

def is_interrupted_system_call(zmq_error):
    """
    predicate to test a zmq.ZMQError object for interrupted system call 
    This normally occurs when a process is terminated while blocking on
    a ZMQ socket. It's an error we generally choose to ignore.
    """
    return str(zmq_error) == "Interrupted system call"
