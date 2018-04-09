# -*- coding: utf-8 -*-
# This python package is implementing a very simple multiprocess framework
# The point of it is to be able to fully tests the multiprocess behavior,
#     in pure python, without having to run a ROS system.
from __future__ import absolute_import
from __future__ import print_function

import os
import pty
import shlex
import sys
import tempfile

import signal

import mmap
import tty

if os.name == 'posix' and sys.version_info[0] < 3:
    import subprocess32 as subprocess
else:
    import subprocess

import multiprocessing, multiprocessing.reduction  #TODO we should probably use subprocess + psutil instead...
import threading
import psutil
import pexpect.fdpexpect, pexpect.popen_spawn, pexpect.spawnbase
import types
import uuid
import io
import errno

import re
import zmq
import socket
import logging
import pickle
import contextlib
#import dill as pickle
import concurrent.futures

try:
    from queue import Queue, Empty  # Python 3
except ImportError:
    from Queue import Queue, Empty  # Python 2

# allowing pickling of exceptions to transfer it
from collections import namedtuple, OrderedDict

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


class AttachBase(pexpect.spawnbase.SpawnBase):
    def __init__(self, timeout=30, maxread=2000, searchwindowsize=None,
                 logfile=None, encoding=None, codec_errors='strict'):
        super(AttachBase, self).__init__(timeout=30, maxread=2000, searchwindowsize=None,
                 logfile=None, encoding=None, codec_errors='strict')


class PopenAttach(AttachBase):
    """ Extending http://pexpect.readthedocs.io/en/stable/_modules/pexpect/popen_spawn.html#PopenSpawn to change API """
    if pexpect.spawnbase.PY3:
        crlf = '\n'.encode('ascii')
    else:
        crlf = '\n'

    def __init__(self, cmd, timeout=30, maxread=2000, searchwindowsize=None,
                 logfile=None, cwd=None, env=None, encoding=None,
                 codec_errors='strict'):
        super(PopenAttach, self).__init__(timeout=timeout, maxread=maxread,
                                         searchwindowsize=searchwindowsize, logfile=logfile,
                                         encoding=encoding, codec_errors=codec_errors)

        kwargs = dict(bufsize=0, stdin=subprocess.PIPE,
                      stderr=subprocess.STDOUT, stdout=subprocess.PIPE,
                      cwd=cwd, env=env)

        if sys.platform == 'win32':
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            kwargs['startupinfo'] = startupinfo
            kwargs['creationflags'] = subprocess.CREATE_NEW_PROCESS_GROUP

        if not isinstance(cmd, (list, tuple)):
            cmd = shlex.split(cmd)

        # self.proc = subprocess.Popen(cmd, **kwargs)
        self.opened = False
        self.closed = False
        self._buf = self.string_type()

        self._read_queue = Queue()
        self._read_thread = threading.Thread(target=self._read_incoming)
        self._read_thread.setDaemon(True)
        self._read_thread.start()

    _read_reached_eof = False

    def read_nonblocking(self, size, timeout):
        buf = self._buf
        if self._read_reached_eof:
            # We have already finished reading. Use up any buffered data,
            # then raise EOF
            if buf:
                self._buf = buf[size:]
                return buf[:size]
            else:
                self.flag_eof = True
                raise pexpect.exceptions.EOF('End Of File (EOF).')

        if timeout == -1:
            timeout = self.timeout
        elif timeout is None:
            timeout = 1e6

        t0 = time.time()
        while (time.time() - t0) < timeout and size and len(buf) < size:
            try:
                incoming = self._read_queue.get_nowait()
            except Empty:
                break
            else:
                if incoming is None:
                    self._read_reached_eof = True
                    break

                buf += self._decoder.decode(incoming, final=False)

        r, self._buf = buf[:size], buf[size:]

        self._log(r, 'read')
        return r

    def _read_incoming(self):
        """Run in a thread to move output from a pipe to a queue."""
        while not self.proc:
            fileno = self.proc.stdout.fileno()
        while 1:
            buf = b''
            try:
                buf = os.read(fileno, 1024)
            except OSError as e:
                self._log(e, 'read')

            if not buf:
                # This indicates we have reached EOF
                self._read_queue.put(None)
                return

            self._read_queue.put(buf)

    def write(self, s):
        '''This is similar to send() except that there is no return value.
        '''
        self.send(s)

    def writelines(self, sequence):
        '''This calls write() for each element in the sequence.

        The sequence can be any iterable object producing strings, typically a
        list of strings. This does not add line separators. There is no return
        value.
        '''
        for s in sequence:
            self.send(s)

    def send(self, s):
        '''Send data to the subprocess' stdin.

        Returns the number of bytes written.
        '''
        s = self._coerce_send_string(s)
        self._log(s, 'send')

        b = self._encoder.encode(s, final=False)
        if pexpect.spawnbase.PY3:
            return self.proc.stdin.write(b)
        else:
            # On Python 2, .write() returns None, so we return the length of
            # bytes written ourselves. This assumes they all got written.
            self.proc.stdin.write(b)
            return len(b)

    def sendline(self, s=''):
        '''Wraps send(), sending string ``s`` to child process, with os.linesep
        automatically appended. Returns number of bytes written. '''

        n = self.send(s)
        return n + self.send(self.linesep)

    def wait(self):
        '''Wait for the subprocess to finish.

        Returns the exit code.
        '''
        status = self.proc.wait()
        if status >= 0:
            self.exitstatus = status
            self.signalstatus = None
        else:
            self.exitstatus = None
            self.signalstatus = -status
        self.terminated = True
        return status

    def kill(self, sig):
        '''Sends a Unix signal to the subprocess.

        Use constants from the :mod:`signal` module to specify which signal.
        '''

        if not self.proc:
            return

        if sys.platform == 'win32':
            if sig in [signal.SIGINT, signal.CTRL_C_EVENT]:
                sig = signal.CTRL_C_EVENT
            elif sig in [signal.SIGBREAK, signal.CTRL_BREAK_EVENT]:
                sig = signal.CTRL_BREAK_EVENT
            else:
                sig = signal.SIGTERM

        os.kill(self.proc.pid, sig)

    def sendeof(self):
        '''Closes the stdin pipe from the writing end.'''
        if not self.proc:
            return
        self.proc.stdin.close()


class ProcessPipeInterface(io.BufferedRWPair):
    def __init__(self, buffer_size=io.DEFAULT_BUFFER_SIZE):
        super(ProcessPipeInterface, self).__init__(io.BytesIO(), io.BytesIO(), buffer_size)






class ProcessWatcher(object):
    """A ProcessWatcher can observe any local running process (even if we did not launch it and are not the parent)
    Heavily insprired from http://pexpect.readthedocs.io/en/stable/_modules/pexpect/popen_spawn.html#PopenSpawn """

    @classmethod
    def from_ptyprocess(cls, pexpect_spawn):
        # We want to use pexpect tty interactive feature to control a process
        return cls(pid=pexpect_spawn.pid,
                   expect_out=pexpect_spawn,
                   expect_err=pexpect_spawn)

    @classmethod
    def from_subprocess(cls, subprocess_popen):
        # building pexpect objects on file descriptors
        return cls(pid=subprocess_popen.pid,
                   expect_out=pexpect.fdpexpect.fdspawn(subprocess_popen.stdout),
                   expect_err=pexpect.fdpexpect.fdspawn(subprocess_popen.stderr))

    def __init__(self, out_watchers=None, err_watchers=None, async=True):
        """
        Creates a ProcessObserver for the process matching the pid (or the current process if pid is None).
        :param err_watcher: a list of pattern to watch for, along with the callback to call.
        :param async: On py2 will create another thread to run the err_watcher callbacks. On py3 will use corountines instead.
          Setting async to false means the monitor() method need to be called periodically in order to check for pattern in the output
        """

        self._lock = threading.RLock()
        self._out_watcher = OrderedDict()
        self._out_cpl_pattern = None
        self._err_watcher = OrderedDict()
        self._err_cpl_pattern = None

        # same as add_err_watcher and add_out_watcher
        # careful we need to keep keys order here...
        if out_watchers:
            self.add_out_watcher(out_watchers)
        if err_watchers:
            self.add_err_watcher(err_watchers)

        # if async:
        #     # Optional function, can be used if we are not calling monitor from current process
        #     # CAREFUL : callback will be done in another thread
        #     def event_loop(self):   # TODO : make this an event loop with asyncio and python 3
        #         """
        #         Function to monitor the registry entry for this process.
        #         This needs to be called by the update method of the parent process
        #         """
        #         # with self.monitor_context() as mc:
        #         while self._expect_out.isalive() or self._expect_err.isalive():
        #             stop = self.monitor(mc)
        #             time.sleep(0.1)
        #             if stop:
        #                 break

            # async in python3 doesnt need a thread...
            # executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
            # f = executor.submit(event_loop)  # ignoring return here

            # t = threading.Thread(name='threaded_eventloop', target=event_loop, args=(self,))
            # t.start()

        master_fd, slave_fd = pty.openpty()  # Ref : https://stackoverflow.com/questions/12419198/python-subprocess-readlines-hangs/12471855#12471855

        # p = subprocess.Popen(['python'], stdin=slave, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # pin = os.fdopen(master, 'w')
        # tty.setcbreak(sys.stdin)


        # stdin_fileno, fpath_stdin = tempfile.mkstemp(suffix="-in", prefix="watched-")
        # stdout_fileno, fpath_stdout = tempfile.mkstemp(suffix="-out", prefix="watched-")
        # stderr_fileno, fpath_stderr = tempfile.mkstemp(suffix="-err", prefix="watched-")
        #
        # stdin_fileno.write("\0")

        # mmapped_in = mmap.mmap(slave, length=0, access=mmap.ACCESS_WRITE)
        # mmapped_out = mmap.mmap(master, length=0, access=mmap.ACCESS_READ)
        # #mmapped_err = mmap.mmap(stderr_fileno, length=0, access=mmap.ACCESS_READ)
        #
        # # Here we return a pair of buffered io, to enable starting a subprocess afterwards.
        # # This is required for short run processes to not miss any message.
        # self.in_pipe = io.BufferedWriter(mmapped_in, buffer_size=io.DEFAULT_BUFFER_SIZE)
        # self.out_pipe = io.BufferedReader(mmapped_out, buffer_size=io.DEFAULT_BUFFER_SIZE)
        # self.err_pipe = io.BufferedReader(mmapped_err, buffer_size=io.DEFAULT_BUFFER_SIZE)
        #
        # self._expect_out = pexpect.fdpexpect.fdspawn(self.out_pipe)
        # self._expect_err = pexpect.fdpexpect.fdspawn(self.err_pipe)

        #self._process = psutil.Process(pid=process.pid)
        # TODO : detect if the process has virtual terminal attached or not...

        self.pty_master = master_fd
        self.pty_slave = slave_fd

        self._process = None

    def ppid(self):
        """ delegating to _process if available"""
        if self._process:
            return self._process.ppid()


    # def expect_out(self, pattern_list, timeout=-1, searchwindowsize=-1, async=False):
    #     if self._expect_out:
    #         return self._expect_out.expect(pattern=pattern_list, timeout=timeout, searchwindowsize=searchwindowsize, async=async)
    #
    # def expect_err(self, pattern_list, timeout=-1, searchwindowsize=-1, async=False):
    #     if self._expect_err:
    #         return self._expect_err.expect(pattern=pattern_list, timeout=timeout, searchwindowsize=searchwindowsize, async=async)
    #
    # def expect_out_exact(self, pattern_list, timeout=-1, searchwindowsize=-1, async=False):
    #     if self._expect_out:
    #         return self._expect_out.expect_exact(pattern_list=pattern_list, timeout=timeout, searchwindowsize=searchwindowsize, async=async)
    #
    # def expect_err_exact(self, pattern_list, timeout=-1, searchwindowsize=-1, async=False):
    #     if self._expect_err:
    #         return self._expect_err.expect_exact(pattern_list=pattern_list, timeout=timeout, searchwindowsize=searchwindowsize, async=async)

    def add_err_watcher(self, watchers):
        with self._lock:
            for pattern, fun in watchers.items():
                assert callable(fun)
                self._err_watcher[pattern] = fun

    def add_out_watcher(self, watchers):
        with self._lock:
            for pattern, fun in watchers.items():
                assert callable(fun)
                self._out_watcher[pattern] = fun

    # @contextlib.contextmanager
    # def monitor_context(self):
    #     last_err_cpl = self._err_watcher.keys()
    #     last_out_cpl = self._out_watcher.keys()
    #     self._err_cpl_pattern = self._expect_err.compile_pattern_list(last_err_cpl)
    #     self._out_cpl_pattern = self._expect_out.compile_pattern_list(last_out_cpl)
    #     yield last_out_cpl, last_err_cpl
    #     pass  # we should keep cleanup as minimal as possible (will not be run when process crashes/is killed)

    def attach(self, pid):
        self._process = psutil.Process(pid)
        of = self._process.open_files()

        # write into registry
        pid_registry[pid] = self._process

    def monitor(self): #, monitor_context):
        """
        Function to monitor the registry entry for this process.
        This needs to be called by the update method of the parent process
        """

        # # if there is a change in patterns to watch, we recompile it
        # if monitor_context[1] != self._err_watcher.keys():
        #     self._err_cpl_pattern = self._expect_err.compile_pattern_list(self._err_watcher.keys())
        # if monitor_context[0] != self._out_watcher.keys():
        #     self._out_cpl_pattern = self._expect_out.compile_pattern_list(self._out_watcher.keys())
        #
        # with self._lock:
        #     try:
        #         # TODO : make this a corountine with asyncio and python 3
        #         i = self.expect_err(self._err_cpl_pattern, 1)
        #         if i:
        #             # calling function for this pattern
        #             self._err_watcher[i]()
        #     except pexpect.TIMEOUT:
        #         pass  # we pass after timeout waiting
        #     except pexpect.EOF:
        #         pass  # we pass if there is nothing to read
        #
        #     try:
        #         # TODO : make this a corountine with asyncio and python 3
        #         i = self.expect_out(self._out_cpl_pattern, 1)
        #         if i:
        #             # calling function for this pattern
        #             self._out_watcher[i]()
        #     except pexpect.TIMEOUT:
        #         pass  # we pass after timeout waiting
        #     except pexpect.EOF:
        #         pass  # we pass if there is nothing to read


        # need this for attaching when process is detected
        if self.isalive() and not self._process:
            # find the pid
            for fd in self._process.open_files():
                print(fd)

           #self.attach(self._expect.pid)


        # need this for a delayed cleanup in case of process termination/crash
        if self._process and not self._process.is_running():
            pid_registry.pop(self._process.pid)
            return True  # return True to stop looping

    def terminate(self):
        if self._process:
            return self._process.terminate()

    def kill(self):
        if self._process:
            return self._process.kill()

    def __del__(self):  # TODO : is this really NEEDED ?
        """Upon deletion, we want to get rid of everything, as properly as possible"""
        for p in self._process.children():
            p.terminate()
        gone, still_alive = psutil.wait_procs(self, timeout=3, callback=on_terminate)
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

