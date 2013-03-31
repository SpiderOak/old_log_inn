# -*- coding: utf-8 -*-
"""
functional_test_main.py

main control program for functional test of the Old Log Inn
"""
import argparse
import json
import logging
import sys

from old_log_inn.signal_handler import set_signal_handler

_log_format_template = '%(asctime)s %(levelname)-8s %(name)-20s: %(message)s'

def _parse_commandline():
    parser = argparse.ArgumentParser(description='functional test.')
    parser.add_argument("-l", "--log-path", dest="log_path")
    parser.add_argument("-c", "--config-path", dest="config_path")
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

def main():
    """
    main entry point
    """
    args = _parse_commandline()

    _initialize_logging(args.log_path)
    log = logging.getLogger("main")
    log.info("program starts")

    config = _load_config_file(args.config_path)

    halt_event = set_signal_handler()
    while not halt_event.is_set():
        log.info("glort")
        halt_event.wait(1.0)

    log.info("program terminates wiht return code 0")
    return 0

if __name__ == "__main__":
    sys.exit(main())
