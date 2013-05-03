#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
search_and_retrieve.py

A tool for retrieving and searching logs from archives.

The start and stop arguments are used to narrow down a list of archives 
available in a Nimbus.io collection. All matching archives are selected for 
retrieval.

Archives are then grouped by timestamp. 
For example, all the archives having the timestamp "20130101000000" would be 
retrieved locally. They would each be processed by sorting their contents by 
timestamp header and then deduplicating records using the log_event_id header, 
producing a unified sequence of unduplicated records.

Those records would be filtered and output according to the directions 
described by the command line arguments. 

Then any local data files would be removed and the next grouped-by-timestamp 
set of archives would be processed.
"""
import argparse
from collections import defaultdict
from itertools import groupby
import json
import logging
import os
import os.path
import re
import sys

import motoboto

from old_log_inn.log_stream import generate_log_stream_from_file

_log_format_template = '%(asctime)s %(levelname)-8s %(name)-20s: %(message)s'
_log = logging.getLogger("main") 
_program_description = "log_archive_search_and_retrieve" 

def _initialize_logging():
    """
    log to stderr for debugging
    """
    handler = logging.StreamHandler()
    formatter = logging.Formatter(_log_format_template)
    handler.setFormatter(formatter)
    logging.root.addHandler(handler)
    log_level = logging.WARN
    logging.root.setLevel(log_level)

def _parse_commandline():
    """
    organize program arguments
    """
    parser = argparse.ArgumentParser(description=_program_description)
    parser.add_argument("-v", "--verbose", action="store_true", default=False)

    parser.add_argument("--output",  dest="work_dir", default="/tmp")
    parser.add_argument("--add-hostname-to-path", dest="add_hostname_to_path",
                        action="store_true",  default=False) 

    parser.add_argument("--host-regexp", dest="host_regexp", default="")
    parser.add_argument("--node-regexp", dest="node_regexp", default="")
    parser.add_argument("--log-filename-regexp", dest="log_filename_regexp",
                        default="")
    parser.add_argument("--content-regexp", dest="content_regexp", default="")

   # The above are the same arguments as to File Logger above.  In this case, 
   # you probably want to give a temporary directory for output since you are 
   # making a specific search.

    parser.add_argument("--identity", dest="identity_path",
                       help="/path/to/motoboto_identity")
    parser.add_argument("--collection", dest="collection_name",
                       help="name_of_nimbus.io_collection")

    # a key name is of the form <prefix>YYYYMMDDHHMMSS<suffix>

    parser.add_argument("--archive-name-prefix", 
                        dest="archive_name_prefix", default="logs.",
                        help="prefix of the archived key name")
    parser.add_argument("--archive-name-suffix", 
                        dest="archive_name_suffix", default="",
                        help="suffix of the archived key name")

   # Look in logs archives at this timestamp or newer.  Timestamp is in
   # YYYYMMDDHHMMSS format.  
    parser.add_argument("--start", dest="start", help="YYYYMMDDHHMMSS")
 

   # Look in logs archives at for this timestamp or older.  Timestamp is in
   # YYYYMMDDHHMMSS format.  
    parser.add_argument("--stop", dest="stop", help="YYYYMMDDHHMMSS")

   # Set start and stop automatically by specifying an interval, such as "1
   # day", "1 week", "1 month".  
   # [default: '1 month']
    parser.add_argument("--go-back", dest="go_back",  
                        help="Set start automatically by specifying " \
                        "an interval")

    args = parser.parse_args()

    return args

def _organize_timestamps(args):
    """
    return a tuple of low_timestamp, high_timkestamp based on the args
    """
    if args.go_back is None:
        return args.start, args.stop

    raise NotImplemented("args.go_back")

def _construct_keep_header_pred(args):
    """
    return a function that runs all the regular expression tests that
    determine whether we process a header
    """
    host_regexp = re.compile(args.host_regexp)
    node_regexp = re.compile(args.node_regexp)
    log_filename_regexp = re.compile(args.log_filename_regexp)

    host_pred = lambda x: host_regexp.match(x["hostname"]) is not None
    node_pred = \
        lambda x: not "nodename" in x or \
        node_regexp.match(x["nodename"]) is not None
    log_filename_pred = \
        lambda x: log_filename_regexp.match(x["log_path"]) is not None

    predicates = [host_pred, node_pred, log_filename_pred, ]

    def __keep_header_pred(header):
        return all([pred(header) for pred in predicates])

    return __keep_header_pred

def _construct_keep_content_pred(args):
    """
    return a function that runs the regular expression test that
    determines whether we process an event content block
    """
    content_regexp = re.compile(args.content_regexp)

    return lambda x: content_regexp.match(x) is not None

def _iterate_keys(bucket, 
                  prefix=None, 
                  suffix=None, 
                  low_timestamp=None, 
                  high_timestamp=None):
    """
    fetch keys from nimbus.io
    return a tuple of timestamp, key
    """
    timestamp_length = len("YYYYMMDDHHMMSS")

    if prefix is None:
        prefix = ""
    prefix_length = len(prefix)

    if suffix is None:
        suffix = ""

    marker = ""
    while True:
        key_list = bucket.get_all_keys(marker=marker)
        for key in key_list:
            key_name = key.name
            marker = key_name
            if not key_name.startswith(prefix):
                continue
            if not key_name.endswith(suffix):
                continue
            timestamp = key_name[prefix_length:timestamp_length+prefix_length]
            if low_timestamp is not None and timestamp < low_timestamp:
                continue
            if high_timestamp is not None and timestamp > high_timestamp:
                continue
            yield timestamp, key
        if not key_list.truncated:
            raise StopIteration()

def _header_key_function(header):
    return (header["timestamp"], header["uuid"], )

def _iterate_timestamp_content(work_dir,
                               keep_header_pred,
                               keep_content_pred,
                               timestamp_key_dict):

    # put the retrieved timestamps in order
    timestamps = sorted(timestamp_key_dict.keys())

    for timestamp in timestamps:
        _log.info("timestamp {0}".format(timestamp))
        header_list = list()
        for index, key in enumerate(timestamp_key_dict[timestamp]):
            _log.info("    key {0}".format(key.name))
            retrieve_path = os.path.join(work_dir, key.name)

            # retrieve the key from nimbus.io to a disk file
            with open(retrieve_path, "wb") as output_file:
                key.get_contents_to_file(output_file)
                     
            # write uncompressed data blocks to a file while maintaining 
            # a sortable list of headers
            data_file_name = "{0:08}".format(index)
            data_file_path = os.path.join(work_dir, data_file_name)
            with open(data_file_path, "wb") as data_file:            
                for header_json, data in \
                    generate_log_stream_from_file(retrieve_path):
                    header = json.loads(header_json.decode("utf-8"))

                    if not keep_header_pred(header):
                        continue

                    header["data_file_path"] = data_file_path
                    header["data_offset"] = data_file.tell()
                    header["data_size"] = len(data)
                    header_list.append(header)
                    data_file.write(data)

            # we don't need the retrieved file anymore
            os.unlink(retrieve_path)

        # sort the combined header_list on timestamp and uuid
        header_list.sort(key=_header_key_function)

        # de-dupe the headers and retrieve the data
        data_files = dict()
        for _, group in groupby(header_list, key=_header_key_function):
            group_list = list(group)
            header = group_list[0]
            if not header["data_file_path"] in data_files:
                data_files[header["data_file_path"]] = \
                    open(header["data_file_path"])
            data_file = data_files[header["data_file_path"]]
            data_file.seek(header["data_offset"])
            data = data_file.read(header["data_size"])
            if not keep_content_pred(data):
                continue
            yield data

def main():
    """
    main entry point
    """
    return_code = 0

    _initialize_logging()

    args = _parse_commandline()
    if args.verbose:
#        logging.root.setLevel(logging.DEBUG)
        logging.root.setLevel(logging.INFO)

    low_timestamp, high_timestamp = _organize_timestamps(args)

    if not os.path.isdir(args.work_dir):
        os.makedirs(args.work_dir)

    _log.info("program starts")

    if args.identity_path is not None:
        _log.info("loading identity from {0}".format(args.identity_path))
        nimbusio_identity = \
            motoboto.identity.load_identity_from_file(args.identity_path)
        if nimbusio_identity is None:
            _log.error("Unable to load identity from {0}".format(
                      args.identity_path))
            return 1
    else:
        nimbusio_identity = None

    # load all keys whose names fit our extract criteria
    bucket = motoboto.s3.bucket.Bucket(nimbusio_identity, args.collection_name)
    timestamp_key_dict = defaultdict(list)
    for timestamp, key in _iterate_keys(bucket, 
                                        prefix=args.archive_name_prefix,
                                        suffix=args.archive_name_suffix,
                                        low_timestamp=low_timestamp,
                                        high_timestamp=high_timestamp):
        timestamp_key_dict[timestamp].append(key)

    keep_header_pred = _construct_keep_header_pred(args)
    keep_content_pred = _construct_keep_content_pred(args)

    content_generator = _iterate_timestamp_content(args.work_dir,
                                                   keep_header_pred,
                                                   keep_content_pred,
                                                   timestamp_key_dict)
    for content in content_generator:
        print(content)

    _log.info("program terminates return_code = {0}".format(return_code))
    return return_code

if __name__ == "__main__":
    sys.exit(main())
