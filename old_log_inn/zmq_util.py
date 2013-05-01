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

def _parse_path(path):
    # we assume this is a directory
    accum = list()
    residue = path
    while len(residue) > 0:
        if residue == os.path.sep:
            residue, name = "", residue
        else:
            residue, name = os.path.split(residue)
        accum.append(name)        
    accum.reverse()
    return accum

def prepare_ipc_path(address):
    """
    IPC sockets need an existing file for an address
    """
    path = address[len(_ipc_protocol_label):]
    dir_path = os.path.dirname(path)

    # 2013-05-01 dougfort -- os.makedirs is unreliable in python 3
    # because it is touchy about mode
    dir_list = _parse_path(dir_path)
    work_dir = ""
    for dir_name in dir_list:
        work_path = os.path.join(work_dir, dir_name)
        if not os.path.exists(work_path):
            os.mkdir(work_path)
        work_dir = work_path

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
