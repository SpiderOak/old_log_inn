# -*- coding: utf-8 -*-
"""
functional_test_main.py

main control program for functional test of the Old Log Inn
"""
import argparse
import json
import logging
import os.path
import subprocess
import sys
import time

from old_log_inn.signal_handler import set_signal_handler

_log_format_template = '%(asctime)s %(levelname)-8s %(name)-20s: %(message)s'

def _parse_commandline():
    parser = argparse.ArgumentParser(description='functional test.')
    parser.add_argument("-l", "--log-path", dest="log_path")
    parser.add_argument("-c", "--config-path", dest="config_path")
    parser.add_argument("-d", "--duration", dest="duration", type=int, 
                        default=30)
    parser.add_argument("-o", "--old-log-inn-path", dest="old_log_inn_path")
    return parser.parse_args()

def _initialize_logging(log_path):
    log_level = logging.DEBUG
    handler = logging.FileHandler(log_path, mode="w", encoding="utf-8")
    formatter = logging.Formatter(_log_format_template)
    handler.setFormatter(formatter)

    logging.root.addHandler(handler)
    logging.root.setLevel(log_level)

def _load_config_file(config_path):
    log = logging.getLogger("_load_config_file")
    log.info("loading config file '{0}'".format(config_path))
    with open(config_path, "r") as input_file:
        return json.load(input_file)

def _constuct_aggregate_sub_socket_file(config, node_names):
    """
    The subscription aggregator expects a disk file of pub socket addresses. 
    We construct it here from the node entries.
    """
    path = config["global"]["aggregate_sub_socket_file"]
    with open(path, "w") as output_file:
        for node_name in node_names:
            address = config[node_name]["node_pub_socket_address"]
            output_file.write("{0}\n".format(address))

def _start_zmq_subscription_aggregator(old_log_inn_path, global_config):
    """
    start the global aggregator
    """
    program_path = os.path.join(old_log_inn_path, 
                                "old_log_inn", 
                                "zmq_subscription_aggregator.py")
    args = [sys.executable, 
            program_path,
            "--sub-list={0}".format(global_config["aggregate_sub_socket_file"]),
            "--pub={0}".format(global_config["aggregate_pub_socket_address"])
    ]

    return _start_subprocess(args, 
                             None, 
                             "global", 
                             "zmq_subscription_aggregator")

def _start_zmq_log_file_logger(old_log_inn_path,  
                               sub_socket_address, 
                               logger_config):
    program_path = os.path.join(old_log_inn_path, 
                                "old_log_inn", 
                                "zmq_log_file_logger.py")
    args = [sys.executable, 
            program_path,
            "--sub={0}".format(sub_socket_address),
            "--zmq-identity={0}".format(logger_config["zmq_identity"]),
            "--output={0}".format(logger_config["log_dir"])]

    if "host_regexp" in logger_config:
        args.append("--host-regexp={0}".format(logger_config["host_regexp"]))
            
    if "node_regexp" in logger_config:
        args.append("--node-regexp={0}".format(logger_config["node_regexp"]))
            
    if "log_filename_regexp" in logger_config:
        args.append("--node-regexp={0}".format(
            logger_config["log_filename_regexp"]))

    if "content_regexp" in logger_config:
        args.append("--content-regexp={0}".format(
            logger_config["content_regexp"]))      

    if "add_hostname_to_path" in logger_config and \
    logger_config["add_hostname_to_path"]:            
        args.append("--add-hostname-to-path")

    return _start_subprocess(args, 
                             None, 
                             "global", 
                             "zmq_log_file_logger")

def _start_zmq_push_pub_forwarder(old_log_inn_path, node_name, node_config):
    program_path = os.path.join(old_log_inn_path, 
                                "old_log_inn", 
                                "zmq_push_pub_forwarder.py")
    args = [sys.executable, 
            program_path,
            "--pull={0}".format(node_config["node_pull_socket_address"]),
            "--pub={0}".format(node_config["node_pull_socket_address"]),
            "--topic={0}".format(node_name)]

    if "zmq_push_pub_forwarder" in node_config:
        forwarder_config = node_config["zmq_push_pub_forwarder"]
        if "hwm" in forwarder_config:
            args.append("--hwm={0}".format(forwarder_config["hwm"]))

    return _start_subprocess(args, 
                             None, 
                             node_name, 
                             "zmq_push_pub_forwarder")

def _start_subprocess(args, env, node_name, program_name):
    """
    start a subprocess set some extra attributes to help track it
    * node_name
    * program_name
    * active
    return the Popen object
    """
    log = logging.getLogger("_start_subprocess")
    log.info("starting node {0} program {1}".format(node_name, program_name))
    process = subprocess.Popen(args, stderr=subprocess.PIPE, env=env)

    setattr(process, "active", True)
    setattr(process, "node_name", node_name)
    setattr(process, "program_name", program_name)

    return process

def _poll_processes(processes):
    log = logging.getLogger("_poll_processes")
    for process in processes:
        process.poll()
        if process.active and process.returncode is not None:            
            _stdoutdata, stderrdata = process.communicate()
            if process.returncode == 0:
                log.info("node {0} program {0} terminated normally {1}".format(
                         process.node_name,
                         process.program_name,
                         stderrdata))
            else:
                log.error("node {0} process {1} failed ({2}) {3}".format(
                          process.node_name,
                          process.program_name,                          
                          process.returncode,
                          stderrdata))
            setattr(process, "active", False)

def main():
    """
    main entry point
    """
    args = _parse_commandline()

    _initialize_logging(args.log_path)
    log = logging.getLogger("main")
    log.info("program starts")

    config = _load_config_file(args.config_path)

    # config is a dict with keys ['global', <node-name-1>...<node-name-n>]
    # not necessarily in order
    node_names = list(config.keys())
    node_names.remove("global")
    node_names.sort()

    _constuct_aggregate_sub_socket_file(config, node_names)

    processes = list()
    process = _start_zmq_subscription_aggregator(args.old_log_inn_path,
                                                 config["global"])
    processes.append(process)

    if "zmq_log_file_logger" in config["global"]:
        process = \
            _start_zmq_log_file_logger(
                args.old_log_inn_path,
                config["global"]["aggregate_pub_socket_address"],
                config["global"]["zmq_log_file_logger"])
        processes.append(process)

    for node_name in node_names:
        process = _start_zmq_push_pub_forwarder(args.old_log_inn_path,
                                                node_name, 
                                                config[node_name])
        processes.append(process)

    halt_event = set_signal_handler()
    start_time = time.time()
    while not halt_event.is_set():
        elapsed_time = int(time.time() - start_time)
        if elapsed_time > args.duration:
            log.info("time expired {0} seconds: stopping test".format(
                     elapsed_time))
            halt_event.set()
            break
        _poll_processes(processes)
        halt_event.wait(5.0)

    log.info("shutting down")
    for process in processes:
        if process.returncode is not None:
            log.warn("node {0} process {1} already terminated {2}".format(
                     process.node_name,
                     process.program_name,
                     process.returncode))
        else:
            log.debug("terminating node {0} process {1}".format(
                      process.node_name,
                      process.program_name))
            process.terminate()
    _poll_processes(processes)

    log.info("program terminates with return code 0")
    return 0

if __name__ == "__main__":
    sys.exit(main())
