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
import logging
import sys

import motoboto

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

    parser.add_argument("--output",  dest="log_dir")
    parser.add_argument("--add-hostname-to-path", dest="add_hostname_to_path",
                        action="store_true",  default=False) 

    parser.add_argument("--host-regexp", dest="host_regexp")
    parser.add_argument("--node-regexp", dest="node_regexp")
    parser.add_argument("--log-filename-regexp", dest="log_filename_regexp")
    parser.add_argument("--content-regexp", dest="content_regexp")

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
                        help="Set start and stop automatically by specifying " \
                        "an interval")

    args = parser.parse_args()

    return args

def _iterate_keys(bucket, 
                  prefix=None, 
                  suffix=None, 
                  low_timestamp=None, 
                  high_timestamp=None):
    """
    fetch keys from nimbus.io, allowing for truncation 
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
            print(timestamp)
            yield key
        if not key_list.truncated:
            raise StopIteration()

def main():
    """
    main entry point
    """
    return_code = 0

    _initialize_logging()

    args = _parse_commandline()
    if args.verbose:
        logging.root.setLevel(logging.DEBUG)

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

    bucket = motoboto.s3.bucket.Bucket(nimbusio_identity, args.collection_name)
    for key in _iterate_keys(bucket, 
                             prefix=args.archive_name_prefix,
                             suffix=args.archive_name_suffix):
        print(key.name)

    _log.info("program terminates return_code = {0}".format(return_code))
    return return_code

if __name__ == "__main__":
    sys.exit(main())
