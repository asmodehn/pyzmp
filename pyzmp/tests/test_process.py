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

# TODO : Test Node exception : correctly transmitted, node still keeps spinning...


@pytest.mark.timeout(5)
def test_process_termination():
    """Checks that a node can be shutdown without being started and indicate that it never ran"""
    n1 = pyzmp.Process()
    assert not n1.is_alive()
    exitcode = n1.shutdown()  # shutdown should have no effect here (if not started, same as noop )
    assert exitcode is None  # exitcode should be None (process didn't start and didn't stop so no exit code)
    assert not n1.is_alive()


@pytest.mark.timeout(5)
def test_process_creation_termination():
    """Checks that a node can be started and shutdown and indicate that it ran successfully"""
    n1 = pyzmp.Process()
    assert not n1.is_alive()
    svc_url = n1.start()
    assert n1.is_alive()
    assert svc_url
    exitcode = n1.shutdown()
    assert exitcode == 0  # default node should spin without issues
    assert not n1.is_alive()


@pytest.mark.timeout(5)
def test_process_timeout_creation_termination():
    """Checks that a node can be started with timeout and shutdown and indicate that it ran successfully"""
    n1 = pyzmp.Process()
    assert not n1.is_alive()
    svc_url = n1.start(1)
    assert svc_url
    assert n1.is_alive()
    exitcode = n1.shutdown()
    assert exitcode == 0
    assert not n1.is_alive()


# @nose.SkipTest  # to help debugging ( FIXME : how to programmatically start only one test - maybe in fixture - ? )
@pytest.mark.timeout(5)
def test_process_double_creation_termination():
    """Checks that a node can be started twice and shutdown and indicate that it ran successfully"""
    n1 = pyzmp.Process()
    assert not n1.is_alive()
    svc_url1 = n1.start()
    assert n1.is_alive()
    assert svc_url1
    svc_url2 = n1.start()  # this shuts down properly and restart the node
    assert n1.is_alive()
    assert svc_url2

    # the node is the same (same id) so we should get same url
    assert svc_url1 == svc_url2

    exitcode = n1.shutdown()
    assert exitcode == 0
    assert not n1.is_alive()


# @nose.SkipTest  # to help debugging ( FIXME : how to programmatically start only one test - maybe in fixture - ? )
@pytest.mark.timeout(5)
def test_process_timeout_double_creation_termination():
    """Checks that a node can be started twice with timeout and shutdown and indicate that it ran successfully"""
    n1 = pyzmp.Process()
    assert not n1.is_alive()
    svc_url1 = n1.start(1)
    assert n1.is_alive()
    assert svc_url1

    svc_url2 = n1.start(1)  # this shuts down and restart the node
    assert n1.is_alive()
    assert svc_url2

    # the node is the same (same id) so we should get same url
    assert svc_url1 == svc_url2

    exitcode = n1.shutdown()
    assert exitcode == 0
    assert not n1.is_alive()


@pytest.mark.timeout(5)
def test_process_creation_double_termination():
    """Checks that a node can be started and shutdown twice and indicate that it ran successfully"""
    n1 = pyzmp.Process()
    assert not n1.is_alive()

    svc_url1 = n1.start()
    assert n1.is_alive()
    assert svc_url1

    exitcode = n1.shutdown()
    assert exitcode == 0
    assert not n1.is_alive()
    exitcode = n1.shutdown()
    assert exitcode == 0  # the exit code is still 0 since we didn't restart...
    assert not n1.is_alive()


@pytest.mark.timeout(5)
def test_process_creation_args():
    """Checks that a node can be passed an argument using inheritance"""
    ns = multiprocessing.Manager().Namespace()
    ns.arg = 42

    class TestArgNode(pyzmp.Process):
        def target(self, *args):
            ns.arg -= args[0]
            return ns.arg

    n1 = TestArgNode(args=(ns.arg,))
    assert not n1.is_alive()
    svc_url = n1.start()
    # update might not have been called yet
    assert n1.is_alive()
    assert svc_url

    # starting and shutdown should at least guarantee ONE call of update function.

    exitcode = n1.shutdown()
    assert exitcode == 0
    assert not n1.is_alive()

    assert ns.arg == 0


@pytest.mark.timeout(5)
def test_process_creation_args_delegate():
    """Checks that a node can be passed an argument using delegation"""
    ns = multiprocessing.Manager().Namespace()
    ns.arg = 42

    def arguser(fortytwo, **kwargs):  # kwargs is there to accept extra arguments nicely (timedelta)
        ns.arg -= fortytwo
        return ns.arg

    n1 = pyzmp.Process(args=(ns.arg,), target_override=arguser)
    assert not n1.is_alive()
    svc_url = n1.start()
    assert n1.is_alive()
    assert svc_url

    exitcode = n1.shutdown()
    assert exitcode == 0
    assert not n1.is_alive()

    assert ns.arg == 0


@pytest.mark.timeout(5)
def test_process_creation_kwargs():
    """Checks that a node can be passed a keyword argument using inheritance"""
    ns = multiprocessing.Manager().Namespace()
    ns.kwarg = 42

    class TestKWArgNode(pyzmp.Process):
        def target(self, *args, **kwargs):
            ns.kwarg -= kwargs.get('intval')
            return ns.kwarg

    n1 = TestKWArgNode(kwargs={'intval': ns.kwarg, })
    assert not n1.is_alive()
    svc_url = n1.start()
    assert n1.is_alive()
    assert svc_url

    exitcode = n1.shutdown()
    assert exitcode == 0
    assert not n1.is_alive()

    assert ns.kwarg == 0


@pytest.mark.timeout(5)
def test_process_creation_kwargs_delegate():
    """Checks that a node can be passed a keyword argument using delegation"""
    ns = multiprocessing.Manager().Namespace()
    ns.kwarg = 42

    def kwarguser(intval, **kwargs):  # kwargs is there to accept extra arguments nicely (timedelta)
        ns.kwarg -= intval
        return ns.kwarg

    n1 = pyzmp.Process(kwargs={'intval': ns.kwarg, }, target_override=kwarguser)
    assert not n1.is_alive()
    svc_url = n1.start()
    assert n1.is_alive()
    assert svc_url

    exitcode = n1.shutdown()
    assert exitcode == 0
    assert not n1.is_alive()

    assert ns.kwarg == 0


# @nose.SkipTest  # to help debugging ( FIXME : how to programmatically start only one test - maybe in fixture - ? )
@pytest.mark.timeout(5)
def test_process_as_context_manager():
    """Checks that a node can be used as a context manager"""
    with pyzmp.Process() as n1:  # this will __init__ and __enter__
        assert n1.is_alive()
    assert not n1.is_alive()


# @nose.SkipTest  # to help debugging ( FIXME : how to programmatically start only one test - maybe in fixture - ? )
@pytest.mark.timeout(5)
def test_process_running_as_context_manager():
    """Checks that an already running node can be used as a context manager"""
    n1 = pyzmp.Process()
    n1.start()
    with n1:  # hooking to an already started node
        # This might restart the node (might be bad but ideally should not matter.)
        assert n1.is_alive()
    assert not n1.is_alive()


# Process as fixture to guarantee cleanup
class TestProc(object):
    __test__ = True

    def setup_method(self, method):
        # services is already setup globally
        self.testproc = pyzmp.Process(name="TestProcess")

    def teardown_method(self, method):
        if self.testproc.is_alive():
            self.testproc.shutdown(join=True)
        # if it s still alive terminate it.
        if self.testproc.is_alive():
            self.testproc.terminate()

    # @nose.SkipTest  # to help debugging ( FIXME : how to programmatically start only one test - maybe in fixture - ? )
    def test_process_discover(self):
        print("\n" + inspect.currentframe().f_code.co_name)
        assert not self.testproc.is_alive()

        print("Discovering Node...")
        testproc_client = pyzmp.Process.discover("Test.*")
        assert testproc_client is None  # node not found until started.

        self.testproc.start()
        assert self.testproc.is_alive()

        print("Discovering Node...")
        testproc_client = pyzmp.Process.discover("Test.*")  # Note : we should not have to wait here, start() should wait long enough.
        assert not testproc_client is None

        self.testproc.shutdown()
        assert not self.testproc.is_alive()

        print("Discovering Node...")
        testproc_client = pyzmp.Process.discover("Test.*")
        assert testproc_client is None  # node not found any longer.


    def test_process_crash(self):
        print("\n" + inspect.currentframe().f_code.co_name)
        assert not self.testproc.is_alive()

        self.testproc.start()
        assert self.testproc.is_alive()

        print("Discovering Node...")
        testproc_clients = pyzmp.Process.discover("Test.*")  # Note : we should not have to wait here, start() should wait long enough.
        assert not testproc_clients is None
        assert len(testproc_clients) == 1

        # pick the one
        testproc_client = testproc_clients.get("TestProcess")

        # sending a signal to kill the child process
        self.testproc.terminate()
        # TODO : handle all kinds of ways to do that...

        # dies immediately
        assert self.testproc.is_alive()

        # but nothing is cleaned up (finally context managers, etc. are not cleaning)
        while "TestProcess" in pyzmp.Process.discover("Test.*"):
            time.sleep(0.5)

        #TODO : wait a bit (less than gossip period) until processor is not found any more...
        print("Discovering Node...")
        testproc_client = pyzmp.Process.discover("Test.*")
        assert testproc_client is None  # node not found any longer.


def test_update_rate():
    """
    Testing that the update methods get a correct timedelta
    """
    # TODO : investigate if node multiprocessing plugin would help simplify this
    # playing with list to pass a reference to this
    testing_last_update = [time.time()]
    testing_time_delta = []
    acceptable_timedelta = []

    def testing_update(self, timedelta, last_update, time_delta, ok_timedelta):
        time_delta.append(time.time() - last_update[-1])
        last_update.append(time.time())

        # if the time delta measured in test and the one passed as argument differ
        # too much, one time, test is failed
        if abs(time_delta[-1] - timedelta) > 0.005:
            ok_timedelta.append(False)
        else:
            ok_timedelta.append(True)

        # spin like crazy, loads CPU for a bit, and eventually exits.
        # We re here trying to disturb the update rate
        while True:
            if randint(0, 10000) == 42:
                break

    # hack to dynamically change the update method
    testing_update_onearg = functools.partial(testing_update,
                                        last_update=testing_last_update,
                                        time_delta=testing_time_delta,
                                        ok_timedelta=acceptable_timedelta)

    n1 = pyzmp.Process()
    n1.update = types.MethodType(testing_update_onearg, n1)

    assert not n1.is_alive()

    # Starting the node in the same thread, to be able to test simply by shared memory.
    # TODO : A Node that can choose process or thread run ( on start() instead of init() maybe ? )
    runthread = threading.Thread(target=n1.run)
    runthread.daemon = True  # to kill this when test is finished
    runthread.start()
    # n1.start()

    # sleep here for a while
    time.sleep(10)

    # removing init time only used for delta computation
    testing_last_update.pop(0)
    # Check time vars modified by update
    for i in range(0, len(testing_last_update)):
        print("update : {u} | delta: {d} | accept : {a}".format(
            u=testing_last_update[i],
            d=testing_time_delta[i],
            a=acceptable_timedelta[i])
        )

        assert acceptable_timedelta[i]



### TODO : more testing in case of crash in process, exception, signal, etc.

if __name__ == '__main__':
    import pytest
    pytest.main(['-s', '-x', __file__])
