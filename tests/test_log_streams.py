# -*- coding: utf-8 -*-
"""
test_stream_writer.py


"""
import logging
import os
import sys
try:
    import unittest2 as unittest
except ImportError:
    import unittest

class TestLogStreamWriter(unittest.TestCase):
    """
    """
    def setUp(self):
        self._zmq_context = zmq.Context()

    def tearDown(self):
        if hasattr(self, "_zmq_context") and self._zmq_context is not None:
            self._zmq_context.term()
            self._zmq_context = None

    def test_single_socket(self):
        """
        test log handler functionality using a single PULL socket
        """
        os.environ["PYTHON_ZMQ_LOG_HANDLER"] = _test_addresses[0]
        log_path = "aaa/bbb/ccc.log"

        # create the handler first so it can do the work of preparing
        # the sockets
        handler = ZMQPushLogHandler(log_path, context=self._zmq_context)
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

        compressed_header = pull_socket.recv(zmq.NOBLOCK)
        self.assertTrue(pull_socket.rcvmore)        
        compressed_body = pull_socket.recv()

        header = json.loads(zlib.decompress(compressed_header))
        body = zlib.decompress(compressed_body)

if __name__ == "__main__":
    unittest.main()
