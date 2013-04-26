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
    parser.add_argument("-w", "--directory-wormhole-path", 
                        dest="directory_wormhole_path")
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

def _start_directory_wormhole(directory_wormhole_path, global_config):
    """
    start the directory_wormhole
    """
    directory_wormhole_config = global_config["directory_wormhole"]
    log_path = "/tmp/directory_wormhole.log"
    program_path = os.path.join(directory_wormhole_path, 
                                "directory-wormhole-main.py")

    args = [sys.executable, 
            program_path,
            "--log={0}".format(log_path),
            "--watch={0}".format(global_config["log_stream_complete_dir"]),
            "--collection={0}".format(
                directory_wormhole_config["collection_name"]),
            "--verbose"]

    if "nimbusio_identity" in directory_wormhole_config:
        args.append("--identity={0}".format(
                    directory_wormhole_config["nimbusio_identity"]))

    env = { "HOSTNAME"   : global_config["host_name"], 
            "NIMBUSIO_CONNECTION_TIMEOUT" : 
                os.environ["NIMBUSIO_CONNECTION_TIMEOUT"],
            "NIMBUS_IO_SERVICE_PORT" : os.environ["NIMBUS_IO_SERVICE_PORT"],
            "NIMBUS_IO_SERVICE_HOST" : os.environ["NIMBUS_IO_SERVICE_HOST"],
            "NIMBUS_IO_SERVICE_DOMAIN" : 
                os.environ["NIMBUS_IO_SERVICE_DOMAIN"],
            "NIMBUS_IO_SERVICE_SSL" : os.environ["NIMBUS_IO_SERVICE_SSL"],
            "PYTHONPATH" : os.environ["PYTHONPATH"],}

    process = _start_subprocess(args, 
                                env, 
                                "global", 
                                "directory_wormhole")
    return process

def _start_zmq_log_stream_writer(old_log_inn_path, global_config):
    """
    start the log stream writer
    """
    program_path = os.path.join(old_log_inn_path, 
                                "old_log_inn", 
                                "zmq_log_stream_writer.py")
    args = [sys.executable, 
            program_path,
            "--sub-list={0}".format(
                global_config["aggregate_sub_socket_file"]),
            "--output-work-dir={0}".format(
                global_config["log_stream_work_dir"]), 
            "--output-complete-dir={0}".format(
                global_config["log_stream_complete_dir"]),
            "--verbose"
    ]

    env = {"HOSTNAME"   : global_config["host_name"], 
           "PYTHONPATH" : os.environ["PYTHONPATH"],}

    stdout_path = "/tmp/zmq_log_stream_writer.log"
    stdout_file = open(stdout_path, "w")

    process = _start_subprocess(args, 
                                env, 
                                "global", 
                                "zmq_log_stream_writer",
                                stdout=stdout_file)
    stdout_file.close()
    return process

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
            "--pub={0}".format(global_config["aggregate_pub_socket_address"],
            "--verbose")
    ]

    stdout_path = "/tmp/zmq_subscription_aggregator.log"
    stdout_file = open(stdout_path, "w")

    process = _start_subprocess(args, 
                                None, 
                                "global", 
                                "zmq_subscription_aggregator",
                                stdout=stdout_file)
    stdout_file.close()
    return process

def _start_zmq_log_file_logger(old_log_inn_path,  
                               host_name,
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

    env = {"HOSTNAME"               : host_name, 
           "PYTHONPATH" : os.environ["PYTHONPATH"],}

    return _start_subprocess(args, 
                             env, 
                             "global", 
                             "zmq_log_file_logger")

def _start_zmq_push_pub_forwarder(old_log_inn_path, node_name, node_config):
    program_path = os.path.join(old_log_inn_path, 
                                "old_log_inn", 
                                "zmq_push_pub_forwarder.py")
    args = [sys.executable, 
            program_path,
            "--pull={0}".format(node_config["node_pull_socket_address"]),
            "--pub={0}".format(node_config["node_pub_socket_address"]),
            "--topic={0}".format(node_name),
            "--verbose"]

    if "zmq_push_pub_forwarder" in node_config:
        forwarder_config = node_config["zmq_push_pub_forwarder"]
        if "hwm" in forwarder_config:
            args.append("--hwm={0}".format(forwarder_config["hwm"]))

    env = {"HOSTNAME"   : node_config["host_name"], 
           "PYTHONPATH" : os.environ["PYTHONPATH"],}

    stdout_path = "/tmp/zmq_push_pub_forwarder_{0}.log".format(node_name)
    stdout_file = open(stdout_path, "w")

    process = _start_subprocess(args, 
                                env, 
                                node_name, 
                                "zmq_push_pub_forwarder",
                                stdout=stdout_file)
    stdout_file.close()
    return process

def _start_log_spewer(old_log_inn_path,
                      node_name, 
                      node_config,
                      log_spewer_config,
                      data_source_address):

    program_path = os.path.join(old_log_inn_path, 
                                "functional_test", 
                                "log_spewer.py")

    args = [sys.executable, 
            program_path,
            "--log-path={0}".format(log_spewer_config["log_path"]),
            "--data-source-address={0}".format(data_source_address)]

    env = {"HOSTNAME"               : node_config["host_name"], 
           "PYTHON_ZMQ_LOG_HANDLER" : node_config["node_pull_socket_address"],
           "PYTHONPATH" : os.environ["PYTHONPATH"],}

    return _start_subprocess(args, 
                             env, 
                             node_name, 
                             log_spewer_config["name"])

def _start_stdout_spewer(old_log_inn_path, 
                         node_name, 
                         stdout_spewer_config, 
                         data_source_address):

    program_path = os.path.join(old_log_inn_path, 
                                "functional_test", 
                                "stdout_spewer.py")

    args = [sys.executable, 
            program_path,
            "--data-source-address={0}".format(data_source_address)]

    env = {"PYTHONPATH" : os.environ["PYTHONPATH"],}

    return _start_subprocess(args, 
                             env, 
                             node_name, 
                             stdout_spewer_config["spewer_name"],
                             stdout=subprocess.PIPE)
                
def _start_stdin_to_zmq_log_proxy(old_log_inn_path, 
                                  node_name, 
                                  node_config,
                                  stdout_spewer_config,
                                  stdout_spewer_stdout):

    program_path = os.path.join(old_log_inn_path, 
                                "old_log_inn", 
                                "stdin_to_zmq_push_log_proxy.py")

    args = [sys.executable, 
            program_path,
            "--log-path={0}".format(stdout_spewer_config["log_path"])]

    env = {"HOSTNAME"               : node_config["host_name"], 
           "PYTHON_ZMQ_LOG_HANDLER" : node_config["node_pull_socket_address"],
           "PYTHONPATH" : os.environ["PYTHONPATH"],}

    return _start_subprocess(args, 
                             env, 
                             node_name, 
                             stdout_spewer_config["proxy_name"],
                             stdin=stdout_spewer_stdout)

def _start_data_source(old_log_inn_path, data_source_config):
    """
    start the global data source
    """
    program_path = os.path.join(old_log_inn_path, 
                                "functional_test", 
                                "data_source.py")
    args = [sys.executable, 
            program_path,
            "--source-path={0}".format(data_source_config["source_path"]),
            "--push-socket-address={0}".format(
                data_source_config["push_socket_address"]),
            "--verbose"]

    stdout_path = "/tmp/data_source.log"
    stdout_file = open(stdout_path, "w")

    process = _start_subprocess(args, 
                                None, 
                                "global", 
                                "data_source",
                                stdout=stdout_file)
    stdout_file.close()
    return process

def _start_subprocess(args, 
                      env, 
                      node_name, 
                      program_name, 
                      stdout=None, 
                      stdin=None):
    """
    start a subprocess set some extra attributes to help track it
    * node_name
    * program_name
    * active
    return the Popen object
    """
    log = logging.getLogger("_start_subprocess")
    log.info("starting node {0} program {1}".format(node_name, program_name))
    process = subprocess.Popen(args, 
                               stderr=subprocess.PIPE, 
                               stdout=stdout, 
                               stdin=stdin,
                               env=env)

    setattr(process, "active", True)
    setattr(process, "node_name", node_name)
    setattr(process, "program_name", program_name)

    return process

def _check_process_termination(process):
    log = logging.getLogger("_check_process_termination")
    try:
        process.wait()
        stderrdata = process.stderr.read()
    except Exception:
        log.exception("{0} {1}".format(
                      process.node_name, process.program_name))
        return

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

def _poll_processes(processes):
    inactive_processes = list()
    for process in processes:
        process.poll()
        if process.active and process.returncode is not None:
            _check_process_termination(process)            
            setattr(process, "active", False)
            inactive_processes.append(process.program_name)
    return inactive_processes

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

    # we start the processes from back to front

    # optional wormhole to nimbus.io
    if "directory_wormhole" in config["global"]:
        process = _start_directory_wormhole(args.directory_wormhole_path,
                                            config["global"])
        processes.append(process)

    # log stream writer 
    process = _start_zmq_log_stream_writer(args.old_log_inn_path,
                                           config["global"])
    processes.append(process)

    # the aggregator
    process = _start_zmq_subscription_aggregator(args.old_log_inn_path,
                                                 config["global"])
    processes.append(process)

    # optional file logger
    if "zmq_log_file_logger" in config["global"]:
        process = \
            _start_zmq_log_file_logger(
                args.old_log_inn_path,
                config["global"]["host_name"],
                config["global"]["aggregate_pub_socket_address"],
                config["global"]["zmq_log_file_logger"])
        processes.append(process)

    data_source_address = \
        config["global"]["data_source"]["push_socket_address"]

    for node_name in node_names:
        process = _start_zmq_push_pub_forwarder(args.old_log_inn_path,
                                                node_name, 
                                                config[node_name])
        processes.append(process)

        if "log_spewers" in config[node_name]:
            for log_spewer_config in config[node_name]["log_spewers"]:
                process = _start_log_spewer(args.old_log_inn_path,
                                            node_name, 
                                            config[node_name],
                                            log_spewer_config,
                                            data_source_address)
                processes.append(process)

        if "stdout_spewers" in config[node_name]:
            for stdout_spewer_config in config[node_name]["stdout_spewers"]:
                # start a stdout spewer pipelined to a stdin_to_zmq_log_proxy
                spewer_process = \
                    _start_stdout_spewer(args.old_log_inn_path,
                                         node_name, 
                                         stdout_spewer_config,
                                         data_source_address)
                proxy_process = \
                    _start_stdin_to_zmq_log_proxy(args.old_log_inn_path,
                                                  node_name, 
                                                  config[node_name],
                                                  stdout_spewer_config,
                                                  spewer_process.stdout)
                # The p1.stdout.close() call after starting the p2 is important 
                # in order for p1 to receive a SIGPIPE if p2 exits before p1.
                spewer_process.stdout.close()

                processes.append(spewer_process)
                processes.append(proxy_process)

    # start the data source last, hoping everyone has had time to hook up
    process = _start_data_source(args.old_log_inn_path,
                                 config["global"]["data_source"])
    processes.append(process)

    data_source_finished = False
    halt_event = set_signal_handler()
    start_time = time.time()
    while not halt_event.is_set():
        elapsed_time = int(time.time() - start_time)
        if elapsed_time > args.duration:
            log.info("time expired {0} seconds: stopping test".format(
                     elapsed_time))
            halt_event.set()
            break
        terminated_subprocesses = _poll_processes(processes)

        # if the data source is done, we are done
        if "data_source" in terminated_subprocesses:
            log.info("data_source has terminated")
            data_source_finished = True            

        halt_event.wait(5.0)

    log.info("shutting down")
    if not data_source_finished:
        log.warn("data source has not finished")
        
    for process in processes:
        if not process.active:
            log.warn("node {0} process {1} already terminated {2}".format(
                     process.node_name,
                     process.program_name,
                     process.returncode))
        else:
            log.debug("terminating node {0} process {1}".format(
                      process.node_name,
                      process.program_name))
            process.terminate()
            _check_process_termination(process)

    log.info("program terminates with return code 0")
    return 0

if __name__ == "__main__":
    sys.exit(main())
