# -*- coding: utf-8 -*-
from __future__ import absolute_import

# To allow python to run these tests as main script
import functools
import inspect
import multiprocessing
import sys
import os
import threading

import types
from random import randint

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

import time
import pyzmp

import pytest
# http://pytest.org/latest/contents.html
# https://github.com/ionelmc/pytest-benchmark

# TODO : PYPY
# http://pypy.org/

from pyzmp.node import Node, discover


# IPC protocol
# Node as fixture to guarantee cleanup
# Better to have IPC as main class as it is simpler and easier to test than Socket.
class TestNodeIPC(object):
    __test__ = True

    class UnstableNode(Node):
        def __init__(self, name):
            super(TestNodeIPC.UnstableNode, self).__init__(name)
            self.magic_number = 666
            # TODO : improvement : autodetect class own methods
            # TODO : assert static ?
            self.provides(self.crash)

        def crash(self):
            1/0

    def setup_method(self, method):
        # services is already setup globally
        self.testnode = TestNodeIPC.UnstableNode(name="TestNode")

    def teardown_method(self, method):
        if self.testnode.is_alive():
            self.testnode.shutdown(join=True)
        # if it s still alive terminate it.
        if self.testnode.is_alive():
            self.testnode.terminate()

    # @nose.SkipTest  # to help debugging ( FIXME : how to programmatically start only one test - maybe in fixture - ? )
    def test_node_discover(self):
        print("\n" + inspect.currentframe().f_code.co_name)
        assert not self.testnode.is_alive()

        print("Discovering Node...")
        testnode_client = discover("Test.*")
        assert testnode_client is None  # node not found until started.

        self.testnode.start()
        assert self.testnode.is_alive()

        print("Discovering Node...")
        testnode_client = discover("Test.*")  # Note : we should not have to wait here, start() should wait long enough.
        assert not testnode_client is None

        self.testnode.shutdown()
        assert not self.testnode.is_alive()

        print("Discovering Node...")
        testnode_client = discover("Test.*")
        assert testnode_client is None  # node not found any longer.

    def test_node_crash(self):
        print("\n" + inspect.currentframe().f_code.co_name)
        assert not self.testnode.is_alive()

        self.testnode.start()
        assert self.testnode.is_alive()

        print("Discovering Node...")
        testnode_client = discover("Test.*")  # Note : we should not have to wait here, start() should wait long enough.
        assert not testnode_client is None

        # pick the one
        assert len(testnode_client) == 1
        testnode_client = testnode_client[0]

        # calling a method dynamically setup to crash the child process
        testnode_client.crash()
        assert not self.testnode.is_alive()

        print("Discovering Node...")
        testnode_client = discover("Test.*")
        assert testnode_client is None  # node not found any longer.


# test that, after the node started, services are immediately available
# test that, after a node stopped / terminated / crashed, services are not available


if __name__ == '__main__':
    import pytest
    pytest.main(['-s', '-x', __file__])
