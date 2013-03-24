# -*- coding: utf-8 -*-
"""
test_zmq_push_log_hander.py


"""
import logging
import os
import sys
try:
    import unittest2 as unittest
except ImportError:
    import unittest

import zmq

from old_log_inn.zmq_push_log_handler import ZMQPushLogHandler

_test_addresses = ["ipc:///tmp/sockets/test1.socket", 
                   "tcp://127.0.0.1:4000",
                   "ipc:///tmp/sockets/test2.socket"]
_poll_timeout = 5 * 1000

class TestZMQPushLogHandler(unittest.TestCase):
    """
    """
    def setUp(self):
        self._zmq_context = zmq.Context()

    def tearDown(self):
        if hasattr(self, "_zmq_context") and self._zmq_context is not None:
#            self._zmq_context.term()
            self._zmq_context = None

    def test_single_socket(self):
        """
        test log handler functionality using a single PULL socket
        """
        os.environ["PYTHON_ZMQ_LOG_HANDLER"] = _test_addresses[0]
        log_path = "aaa/bbb/ccc.log"

        # create the handler first so it can do the work of preparing
        # the sockets
        handler = ZMQPushLogHandler(log_path)
        logging.root.addHandler(handler)
        logging.root.setLevel(logging.DEBUG)

        # listen on a single PULL socket
        pull_socket = self._zmq_context.socket(zmq.PULL)
        pull_socket.bind(_test_addresses[0])

        poller = zmq.Poller()
        poller.register(pull_socket, zmq.POLLIN)

        log = logging.getLogger("test")
        log.info("pork")

        result_list = poller.poll(timeout=_poll_timeout)
        self.assertEqual(len(result_list), 1)

        header = pull_socket.recv(zmq.NOBLOCK)
        self.assertTrue(pull_socket.rcvmore)
        body = pull_socket.recv()

if __name__ == "__main__":
    unittest.main()
