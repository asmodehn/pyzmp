# -*- coding: utf-8 -*-
# This python package is implementing a very simple multiprocess framework
# The point of it is to be able to fully tests the multiprocess behavior,
#     in pure python, without having to run a ROS system.
from __future__ import absolute_import
from __future__ import print_function

import os
import sys
import tempfile
import multiprocessing, multiprocessing.reduction
import types
import uuid

import errno
import zmq
import socket
import logging
import pickle
import contextlib
#import dill as pickle

# allowing pickling of exceptions to transfer it
from collections import namedtuple

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

### IMPORTANT : COMPOSITION -> A SET OF NODE SHOULD ALSO 'BE' A NODE ###
### IMPORTANT : IDENTITY
### Category Theory https://en.wikipedia.org/wiki/Category_theory

### Data Flow with topic :
#
# object : message
# arrow : topic_listener
# binary op - associativity : listener l1, l2, l3 <= (l1 . l2) .l3 == l1. (l2. l3)
# binary op - identity : noop on listener => msg transfer as is
# -> Expressed in programming language (functional programming follows category theory) :
# msg3 = topic2_listener(topic1_listener(msg1))

# -> Expressed in graph :
# msg3 <--topic2_listener-- msg2 <--topic1_listener-- msg1

### RPC with services :
#
# object : (req, resp)
# arrow : service_call
# binary op - associativity : service s1, s2, s3 <= (s1 . s2) .s3 == s1. (s2. s3)
# binary op - identity : noop on service => (req, resp) transfer as is ( two ways comm )
# -> Expressed in programming language (functional programming follows category theory) :
#  (req3, resp3) = service2_call(service1_call((req1, resp1)))

# -> Expressed in graph :
# msg1 --service1_call--> msg2 --service2_call--> msg3

###### higher level for execution graph to be represented by a category ######

# TODO : CAREFUL topic is probably not a complete concept in itself
# => often we need to get back to pub / sub connections rather than dealing with "topics"
# We need the reverse flow of "service call" which is a distributed generalisation of function call
# We need the distributed generalisation of the callback function ( async calls and futures )

### Service is a first class citizen. node is abstracted in that perspective : implementation requires some discovery mechanism.
# object : service
# arrow : call
# binary op - associativity : call c1, c2, c3 <= (c1 . c2) .c3 == c1 . (c2 . c3)
# binary op - identity : ??? TODO
# -> Expressed in programming language (functional programming follows category theory) :
#  svc1 = lambda x: return ( lambda y: return svc3(y) )( x )  <= currying/partial => svc1 = lambda x, y : return svc23(x,y)

# -> Expressed in graph :
# svc1 --call--> svc2 --call--> svc3

### Node is a first class citizen
# object : node
# arrow : topic_listener / callback
# binary op - associativity : callback cb1. cb2. cb3 <= (cb1 . cb2) . cb3 == cb1 . (cb2 . cb3)
# binary op - identity : ??? TODO
# -> Expressed in programming language (functional programming follows category theory) :
#  node1.cb = lambda x: return ( lambda y: return node3.cb(y) )(x) <= currying/partial => node1.cb = lambda x, y : return node23(x,y)

# -> Expressed in graph :
# node3 <--topic_cb-- node2 <--topic_cb-- node1

from .master import manager
from .exceptions import UnknownServiceException, UnknownRequestTypeException
from .message import ServiceRequest, ServiceRequest_dictparse, ServiceResponse, ServiceException
from .service import service_provider_cm
#from .service import RequestMsg, ResponseMsg, ErrorMsg  # only to access message types

current_node = multiprocessing.current_process

# Lock is definitely needed ( not implemented in proxy objects, unless the object itself already has it, like Queue )
nodes_lock = manager.Lock()
nodes = manager.dict()


@contextlib.contextmanager
def dummy_cm():
    yield None


@contextlib.contextmanager
def node_cm(node_name, svc_address):
    # advertise itself
    nodes_lock.acquire()
    nodes[node_name] = {'service_conn': svc_address}
    nodes_lock.release()

    yield

    # concealing itself
    nodes_lock.acquire()
    nodes[node_name] = {}
    nodes_lock.release()


# TODO : Nodelet ( thread, with fast intraprocess zmq comm - entity system design /vs/threadpool ?)
# CAREFUL here : multiprocessing documentation specifies that a process object can be started only once...
class Node(object):

    EndPoint = namedtuple("EndPoint", "self func")

    # TODO : allow just passing target to be able to make a Node from a simple function, and also via decorator...
    def __init__(self, name=None, socket_bind=None, context_manager=None, target=None, args=None, kwargs=None):
        """
        Initializes a ZMP Node (Restartable Python Process communicating via ZMQ)
        :param name: Name of the node
        :param socket_bind: the string describing how to bind the ZMQ socket ( IPC, TCP, etc. )
        :param context_manager: a context_manager to be used with run (in a with statement)
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
        # Careful : our own target is not the same as the one for Process
        self._target = target or self.update  # we expect the user to overload update in child class.

        #: the actual process instance. lazy creation on start() call only.
        self._process = None

        #: whether or not the node name should be set as the actual process title
        #: replacing the string duplicated from the python interpreter run
        self.new_title = True

        self.context_manager = context_manager or dummy_cm  # TODO: extend to list if possible ( available for python >3.1 only )
        self.exit = multiprocessing.Event()
        self.started = multiprocessing.Event()
        self.listeners = {}
        self._providers = {}
        self.tmpdir = tempfile.mkdtemp(prefix='zmp-' + self.name + '-')
        # if no socket is specified the services of this node will be available only through IPC
        self._svc_address = socket_bind if socket_bind else 'ipc://' + self.tmpdir + '/services.pipe'

    def __enter__(self):
        # __enter__ is called only if we pass this instance to with statement ( after __init__ )
        # start only if needed (so that we can hook up a context manager to a running node) :
        if not self.is_alive():
            self.start()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        # make sure we cleanup when we exit
        self.shutdown()

    def has_started(self):
        """
        :return: True if the node has started (update() might not have been called yet). Might still be alive, or not...
        """
        return self.started.is_set()

    ### Process API delegation ###
    def is_alive(self):
        if self and self._process:
            return self._process.is_alive()

    def join(self, timeout=None):
        if not self._process:
            # blocking on started event before blocking on join
            self.started.wait(timeout=timeout)
        return self._process.join(timeout=timeout)

    @property
    def name(self):
        if self and self._process:
            return self._process.name
        else:
            return self._pargs.get('name', "ZMPNode")

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
        None waits until the update has been called at least once.
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
        if self.started.wait(timeout=timeout):  # blocks until we know true or false
            return self._svc_address  # returning the zmp url as a way to connect to the node
            # CAREFUL : doesnt make sense if this node only run a one-time task...
        # TODO: futures and ThreadPoolExecutor (so we dont need to manage the pool ourselves)
        else:
            return False

    # TODO : Implement a way to redirect stdout/stderr, or even forward to parent ?
    # cf http://ryanjoneil.github.io/posts/2014-02-14-capturing-stdout-in-a-python-child-process.html

    def terminate(self):
        """Forcefully terminates the underlying process (using SIGTERM)"""
        return self._process.terminate()
        # TODO : maybe redirect to shutdown here to avoid child process leaks ?

    ### Node specific API ###
    # TODO : find a way to separate process management and service provider API

    def provides(self, svc_callback, service_name=None):
        service_name = service_name or svc_callback.__name__
        # we store an endpoint ( bound method or unbound function )
        self._providers[service_name] = Node.EndPoint(
            self=getattr(svc_callback, '__self__', None),
            func=getattr(svc_callback, '__func__', svc_callback),
        )

    def withholds(self, service_name):
        service_name = getattr(service_name, '__name__', service_name)
        # we store an endpoint ( bound method or unbound function )
        self._providers.pop(service_name)

    # TODO : shortcut to discover/build only services provided by this node ?

    # Careful : this is NOT the same usage as "run()" from Process :
    # it is called inside a loop that it does not directly control...
    # TOOD : think about it and improve (Entity System integration ? Pool + Futures integration ?)
    def update(self, shutting_down, *args, **kwargs):
        """
        Runs at every update cycle in the node process/thread.
        Usually you want to override this method to extend the behavior of the node in your implementation
        :param shutting_down will be True when it will be the last update call. This allows update function to do some cleanup.
        :return: integer as exitcode to stop the node, or None to keep looping...
        """
        # TODO : Check which way is better (can also be used to run external process, other functions, like Process)
        return None  # we keep looping by default (need to deal with services here...)

    def shutdown(self, join=True, timeout=None):
        """
        Clean shutdown of the node.
        :param join: optionally wait for the process to end (default : True)
        :return: None
        """
        if self.is_alive():  # check if process started
            print("Shutdown initiated")
            self.exit.set()
            if join:
                self.join(timeout=timeout)
                # TODO : timeout before forcing terminate (SIGTERM)

        exitcode = self._process.exitcode if self._process else None  # we return None if the process was never started
        return exitcode

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

        if setproctitle and self.new_title:
            setproctitle.setproctitle("{0}".format(self.name))

        print('[{node}] Node started as [{pid} <= {address}]'.format(node=self.name, pid=self.ident, address=self._svc_address))


        zcontext = zmq.Context()  # check creating context in init ( compatibility with multiple processes )
        # Apparently not needed ? Ref : https://github.com/zeromq/pyzmq/issues/770
        zcontext.setsockopt(socket.SO_REUSEADDR, 1)  # required to make restart easy and avoid debugging traps...
        svc_socket = zcontext.socket(zmq.REP)  # Ref : http://api.zeromq.org/2-1:zmq-socket # TODO : ROUTER instead ?

        try:  # attempting binding socket
            svc_socket.bind(self._svc_address,)
        except zmq.ZMQError as ze:
            if ze.errno == errno.ENOENT:  # No such file or directory
                # TODO : handle all possible cases
                fpath = self._svc_address.split(':')[1]
                if fpath.startswith("//"): fpath = fpath[2:]
                try:
                    os.makedirs(os.path.dirname(fpath))
                except OSError as ose:
                    if ose.errno == errno.EEXIST and os.path.isdir(fpath):
                        pass
                    else:
                        raise
                # try again or break
                svc_socket.bind(self._svc_address, )
            else:
                raise

        except Exception as e:
            raise


        poller = zmq.Poller()
        poller.register(svc_socket, zmq.POLLIN)

        # Initializing all context managers
        with service_provider_cm(
                    self.name, self._svc_address, self._providers
                ), node_cm(self.name, self._svc_address), self.context_manager() as cm:

            # Starting the clock
            start = time.time()

            first_loop = True
            # loop listening to connection
            while not self.exit.is_set():

                # signalling startup only the first time, just after having check for exit request.
                # We need to guarantee at least ONE call to update.
                if first_loop:
                    self.started.set()

                # blocking. messages are received ASAP. timeout only determine update/shutdown speed.
                socks = dict(poller.poll(timeout=100))
                if svc_socket in socks and socks[svc_socket] == zmq.POLLIN:
                    req = None
                    try:
                        req_unparsed = svc_socket.recv()
                        req = ServiceRequest_dictparse(req_unparsed)
                        if isinstance(req, ServiceRequest):
                            if req.service and req.service in self._providers.keys():

                                request_args = pickle.loads(req.args) if req.args else ()
                                # add 'self' if providers[req.service] is a bound method.
                                if self._providers[req.service].self:
                                    request_args = (self, ) + request_args
                                request_kwargs = pickle.loads(req.kwargs) if req.kwargs else {}

                                resp = self._providers[req.service].func(*request_args, **request_kwargs)
                                svc_socket.send(ServiceResponse(
                                    service=req.service,
                                    response=pickle.dumps(resp),
                                ).serialize())

                            else:
                                raise UnknownServiceException("Unknown Service {0}".format(req.service))
                        else:  # should not happen : dictparse would fail before reaching here...
                            raise UnknownRequestTypeException("Unknown Request Type {0}".format(type(req.request)))
                    except Exception:  # we transmit back all errors, and keep spinning...
                        exctype, excvalue, tb = sys.exc_info()
                        # trying to make a pickleable traceback
                        try:
                            ftb = Traceback(tb)
                        except TypeError as exc:
                            ftb = "Traceback manipulation error: {exc}. Verify that python-tblib is installed.".format(exc=exc)

                        # sending back that exception with traceback
                        svc_socket.send(ServiceResponse(
                            service=req.service,
                            exception=ServiceException(
                                exc_type=pickle.dumps(exctype),
                                exc_value=pickle.dumps(excvalue),
                                traceback=pickle.dumps(ftb),
                            )
                        ).serialize())

                # time is ticking
                # TODO : move this out of here. this class should require only generic interface to update method.
                now = time.time()
                timedelta = now - start
                start = now

                # replacing the original Process.run() call, passing arguments to our target
                if self._target:
                    # bwcompat
                    kwargs['timedelta'] = timedelta
                    # adding cleanup on shutdown capabilities
                    kwargs['shutting_down'] = False

                    # TODO : use return code to determine when/how we need to run this the next time...
                    # Also we need to keep the exit status to be able to call external process as an update...

                    logging.debug("[{self.name}] calling {self._target.__name__} with args {args} and kwargs {kwargs}...".format(**locals()))
                    exitstatus = self._target(*args, **kwargs)

                if first_loop:
                    first_loop = False

                if exitstatus is not None:
                    break

            if self.started.is_set() and exitstatus is None and self.exit.is_set():
                # in the not so special case where we started, we didnt get exit code and we exited,
                # this is expected as a normal result and we set an exitcode here of 0
                # As 0 is the conventional success for unix process successful run

                # adding cleanup on shutdown capabilities
                kwargs['shutting_down'] = True

                logging.debug(
                    "[{self.name}] FINAL call {self._target.__name__} with args {args} and kwargs {kwargs}.".format(
                        **locals()))
                exitstatus = self._target(*args, **kwargs)
                # Note this exit status will be ignored by python process anyway.
                # But the user might want to have different behavior on it depending on the target here.
                # The target can be a python function, also can be a shell script, or something else...
                # => Where is the best place for this usecase ?

        logging.debug("[{self.name}] Node stopped.".format(**locals()))
        return exitstatus  # returning last exit status from the update function

        # all context managers are destroyed properly here




