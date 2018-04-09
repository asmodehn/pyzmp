# -*- coding: utf-8 -*-
# This python package is implementing a very simple multiprocess framework
# The point of it is to be able to fully tests the multiprocess behavior,
#     in pure python, without having to run a ROS system.
from __future__ import absolute_import
from __future__ import print_function

import os
import sys
import tempfile
if os.name == 'posix' and sys.version_info[0] < 3:
    import subprocess32 as subprocess
else:
    import subprocess

import multiprocessing, multiprocessing.reduction  # TODO we should probably use subprocess + psutil instead...
import psutil
import types
import uuid

import errno

import re
import zmq
import socket
import logging
import pickle
import contextlib
#import dill as pickle

# allowing pickling of exceptions to transfer it
from collections import namedtuple

from .registry import FileBasedRegistry

import time

try:
    from tblib.decorators import Traceback
    # TODO : potential candidates for pickle + tblib replacement for easier serialization
    # TODO : - https://github.com/uqfoundation/dill
    # TODO : - OR https://github.com/cloudpipe/cloudpickle
    # TODO : - OR https://github.com/irmen/Serpent ?
    # TODO : - OR https://github.com/esnme/ultrajson ?
    # TODO : - OR something else ?
except ImportError:
    Traceback = None

try:
    import setproctitle
except ImportError:
    setproctitle = None


# TODO : Nodelet ( thread, with fast intraprocess zmq comm - entity system design /vs/threadpool ?)

pid_registry = FileBasedRegistry("pid")


def on_terminate(proc):
    print("process {} terminated with exit code {}".format(proc, proc.returncode))


class ProcessObserver(object):
    """A ProcessObserver can observe any local running process (even if we did not launch it and are not the parent)"""

    # local storage of all our child process which we are responsible for
    _watched_pids = {}

    @staticmethod
    def monitor_all():  # TODO : maybe one per processobserver instance is easier ?
        """ function to monitor the registry entry. Needs to be called by the update method of the parent process"""
        # NEED this for a delayed cleanup in case of process termination/crash
        gone_pids = [p for p in ProcessObserver._watched_pids if not psutil.pid_exists(p)]
        for p in gone_pids:
            pid_registry.pop(p)

    def __init__(self, pid=None, infanticist=False):
        """
        Creates a ProcessObserver for the process matching the pid (or hte current process if pid is None).
        If infanticist is set to true, the current process will attempt to kill this pid (his child) when dying.
        :param pid: 
        :param infanticist: 
        """
        self.infanticist = infanticist
        self._process = psutil.Process(pid)
        self._watched_pids[pid] = self

    def monitor(self):
        """
        Function to monitor the registry entry for this process.
        This needs to be called by the update method of the parent process
        """
        # need this for a delayed cleanup in case of process termination/crash
        if not psutil.pid_exists(self._process.pid):
            pid_registry.pop(self._process.pid)

    def __del__(self):
        if self.infanticist:
            self._process.terminate()
            gone, still_alive = psutil.wait_procs(self._process, timeout=3, callback=on_terminate)
            for p in still_alive:
                p.kill()


def discover_process(name_regex='.*', timeout=None):
    """
    Discovers all processes.
    Note : we do not want to make the discovery block undefinitely since we never know for sure if a process is running or not
    TODO : improve with future...
    :param name_regex: regex to filter the nodes by name/uuid
    :param timeout: maximum number of seconds the discover can wait for a discovery matching requirements. if None, doesn't wait.
    """
    start = time.time()
    endtime = timeout if timeout else 0

    reg = re.compile(name_regex)

    while True:
        timed_out = time.time() - start > endtime
        dp = {
            p: ProcessObserver(pid_registry[p])
            for p in pid_registry if reg.match(p)
        # filtering by regex here TODO : move that feature to the Registry
        }  # return right away if we have something

        if dp:
            return dp
        elif timed_out:
            break
        # else we keep looping after a short sleep ( to allow time to refresh services list )
        time.sleep(0.2)  # sleep
    return None


class Process(object):
    """
    Process class that model how a process is started and stopped, can start / stop child processes,
     all in a synchronous deterministic manner.
    It mainly add synchronization primitives to multiprocessing.Process.
    """

    class Observer(object):
        """
        ProcessObserver that provide a observe interface to an already running process.
        """

        def __init__(self, pid=None):
            self.started = multiprocessing.Event()
            self._osproc = psutil.Process(pid)

        # TODO : inverted control flow, but in a nice way ???
        def wait_for_start(self, timeout):
            return self.started.wait(timeout=timeout)

        def has_started(self):
            """
            :return: True if the node has started (update() might not have been called yet). Might still be alive, or not...
            """
            return self.started.is_set()

    # TODO : we need to monitor a process and cleanup pid files if needed...
    class Control(Observer):
        # inheritance since there is no point to try to control without feedback,
        # and users usually expect both in same place...
        """
        ProcessControl that provide a control interface to an already running process.
        """

        def __init__(self, pid=None):
            self.exit = multiprocessing.Event()
            super(Process.Control, self).__init__(pid=pid)

        def set_exit_flag(self):
            """Request a process termination"""
            return self.exit.set()

        def monitor_registry_entry(self):
            """ function to monitor the registry entry. Needs to be called by the update method of the parent process"""
            # need this for a delayed cleanup in case of process termination/crash
            if psutil.pid_exists(self._osproc.pid):
                pid_registry.pop(self._osproc.pid)

    # TODO : we can extend this later (see psutil) for debugging and more...

    def __init__(self, name=None, target_context=None, target_override=None, args=None, kwargs=None):
        """
        Initializes a ZMP Node (Restartable Python Process communicating via ZMQ)
        :param name: Name of the node
        :param target_context: a context_manager to be used with run (in a with statement)
        :param target_override: a function to override this class target method
        :return:
        """
        # TODO check name unicity
        # using process as delegate
        self._pargs = {
            'name': name or str(uuid.uuid4()),
            'args': args or (),
            'kwargs': kwargs or {},
            'target': self.run,  # Careful : our run() is not the same as the one for Process
        }
        # TODO : we should ensure our args + kwargs are compatible with our target (to avoid later errors)
        # Careful : our own target is not the same as the one for Process
        self._target = target_override or self.target
        self.target_call_start = None
        self.target_call_timedelta = None

        #: the actual process instance. lazy creation on start() call only.
        self._process = None
        self._control = Process.Control()

        #: whether or not the node name should be set as the actual process title
        #: replacing the string duplicated from the python interpreter run
        self.new_title = True

        self._target_context = target_context or self.target_context  # TODO: extend to list if possible ( available for python >3.1 only )
        super(Process, self).__init__()

    def __enter__(self):
        # __enter__ is called only if we pass this instance to with statement ( after __init__ )
        # start only if needed (so that we can hook up a context manager to a running node) :
        if not self.is_alive():
            self.start()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        # make sure we cleanup when we exit
        self.shutdown()

    def is_alive(self):
        if self and self._process:
            return self._process.is_alive()

    def join(self, timeout=None):
        if not self._process:
            # blocking on started event before blocking on join
            self._control.started.wait(timeout=timeout)
        return self._process.join(timeout=timeout)

    @property
    def name(self):
        if self and self._process:
            return self._process.name
        else:
            return self._pargs.get('name', "ZMPProcess")

    @name.setter
    def name(self, name):
        if self and self._process:
            self._process.name = name
            # only reset the name arg if it was accepted by the setter
            self._pargs.set('name', self._process.name)
        else:
            # TODO : maybe we should be a bit more strict here ?
            self._pargs.set('name', name)

    @property
    def daemon(self):
        """
        Return whether process is a daemon
        :return:
        """
        if self._process:
            return self._process.daemon
        else:
            return self._pargs.get('daemonic', False)

    @daemon.setter
    def daemon(self, daemonic):
        """
        Set whether process is a daemon
        :param daemonic:
        :return:
        """
        if self._process:
            self._process.daemonic = daemonic
        else:
            self._pargs['daemonic'] = daemonic

    @property
    def authkey(self):
        return self._process.authkey

    @authkey.setter
    def authkey(self, authkey):
        """
        Set authorization key of process
        """
        self._process.authkey = authkey

    @property
    def exitcode(self):
        """
        Return exit code of process or `None` if it has yet to stop
        """
        if self._process:
            return self._process.exitcode
        else:
            return None

    @property
    def ident(self):
        """
        Return identifier (PID) of process or `None` if it has yet to start
        """
        if self._process:
            return self._process.ident
        else:
            return None

    def __repr__(self):
        # TODO : improve this
        return self._process.__repr__()

    def start(self, timeout=None):
        """
        Start child process
        :param timeout: the maximum time to wait for child process to report it has actually started.
        None waits until the update is ready to be called.
        """

        # we lazily create our process delegate (with same arguments)
        if self.daemon:
            daemonic = True
        else:
            daemonic = False

        pargs = self._pargs.copy()
        pargs.pop('daemonic', None)

        self._process = multiprocessing.Process(**pargs)

        self._process.daemon = daemonic

        # CAREFUL here : multiprocessing documentation specifies that a process object can be started only once...
        if self.is_alive():
            # if already started, we shutdown and join before restarting
            # not timeout will bock here (default join behavior).
            # otherwise we simply use the same timeout.
            self.shutdown(join=True, timeout=timeout)  # TODO : only restart if no error (check exitcode)
            self.start(timeout=timeout)  # recursive to try again if needed
        else:
            self._process.start()

        # timeout None means we want to wait and ensure it has started
        # deterministic behavior, like is_alive() from multiprocess.Process is always true after start()
        if self._control.wait_for_start(timeout=timeout):  # blocks until we know true or false
            # TODO: futures, somehow...
            return ProcessObserver(self._process.ident)

    # TODO : Implement a way to redirect stdout/stderr, or even forward to parent ?
    # cf http://ryanjoneil.github.io/posts/2014-02-14-capturing-stdout-in-a-python-child-process.html

    def terminate(self):
        """
        Forcefully terminates the underlying process (using SIGTERM)
        CAREFUL : in that case the finally clauses, and context exits will NOT run.
        """
        return self._process.terminate()
        # TODO : maybe redirect to shutdown here to avoid child process leaks ?

    def shutdown(self, join=True, timeout=None):
        """
        Clean shutdown of the node from the parent.
        :param join: optionally wait for the process to end (default : True)
        :return: None
        """
        if self.is_alive():  # check if process started
            print("Shutdown initiated")
            self._control.set_exit_flag()
            if join:
                self.join(timeout=timeout)
                # TODO : timeout before forcing terminate (SIGTERM)

        exitcode = self._process.exitcode if self._process else None  # we return None if the process was never started
        return exitcode

    @contextlib.contextmanager
    def target_context(self):
        self.target_call_start = time.time()
        self.target_call_timedelta = 0
        yield

    # TODO : extract that into a (asyncio) task...
    def target(self, *args, **kwargs):
        """
        The function to overload if inheriting the Process class to implement a specific behavior.
        :param args: 
        :param kwargs: 
        :return: 
        """
        # tracking time, so a target defining timedelta parameter will get the time delta (should be optional)
        target_call_time = time.time()
        self.target_call_timedelta = target_call_time - self.target_call_start
        self.target_call_start = target_call_time

        # TODO : this is probably where we could implement a sleep to enforce frequency of calls...
        return None

    def run(self, *args, **kwargs):
        """
        The Node main method, running in a child process (similar to Process.run() but also accepts args)
        A children class can override this method, but it needs to call super().run(*args, **kwargs)
        for the node to start properly and call update() as expected.
        :param args: arguments to pass to update()
        :param kwargs: keyword arguments to pass to update()
        :return: last exitcode returned by update()
        """
        # TODO : make use of the arguments ? since run is now the target for Process...

        exitstatus = None  # keeping the semantic of multiprocessing.Process : running process has None

        try :
            # Initializing the required context managers
            with pid_registry.registered(self.name, self.ident) as pcm:  # TODO : careful about reusing PIDs here...

                if setproctitle and self.new_title:
                    setproctitle.setproctitle("{0}".format(self.name))

                print('[{procname}] Process started as [{pid}]'.format(procname=self.name, pid=self.ident))

                with self._target_context() as cm:

                    first_loop = True
                    # loop listening to connection
                    while not self._control.exit.is_set():

                        # signalling startup only the first time, just after having check for exit request.
                        # We need to guarantee at least ONE call to update.
                        if first_loop:
                            self._control.started.set()

                        # replacing the original Process.run() call, passing arguments to our target
                        if self._target:
                            # TODO : use return code to determine when/how we need to run this the next time...
                            # Also we need to keep the exit status to be able to call external process as an update...

                            logging.debug("[{self.name}] calling {self._target.__name__} with args {args} and kwargs {kwargs}...".format(**locals()))
                            exitstatus = self._target(*args, **kwargs)

                        if first_loop:
                            first_loop = False

                        if exitstatus is not None:
                            break

                    if self._control.started.is_set() and exitstatus is None and self._control.exit.is_set():
                        # in the not so special case where we started, we didnt get exit code and we exited,
                        # this is expected as a normal result and we set an exitcode here of 0
                        # As 0 is the conventional success for unix process successful run
                        exitstatus = 0

        except KeyboardInterrupt:
            raise
        except Exception:
            raise
        finally:
            logging.debug("[{self.name}] Process stopped.".format(**locals()))
        return exitstatus  # returning last exit status from the update function




