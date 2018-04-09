import re
import signal

import six

try:
    import asyncio
except ImportError:
    import trollius as asyncio

import logging.handlers

"""
This is a text-based (ASCII) protocol, for communicating and synchronizing between processes, through pipes.
The goal is to synchronize process startup / shutdown behavior to be able to implement synchronization at a higher level...

Note this does the same job as what is found in systemd, upstart, openrc, etc.
See https://en.wikipedia.org/wiki/Init
TODO : compatibility with those, eventually 

"""

STARTED_FMT = "-STARTED {pid}-"  # string to denote startup
STARTED_REGEX = ".*-STARTED (.+?)-"  # regex to extract pid
SHUTDOWN_FMT = "-SHUTDOWN {exit_code}-"  # string to denote shutdown
SHUTDOWN_REGEX = ".*-SHUTDOWN (.+?)-"  # regex to extract exit code


class SubprocessProtocol(asyncio.SubprocessProtocol):

    def __init__(self, exit_future, loop=None):
        self._logger = logging.getLogger(__name__)
        self.transport = None  # no connection made yet
        self.loop = loop or asyncio.get_event_loop()
        self.exit_future = exit_future
        self.output = bytearray()  # TODO : Change that into a logger to allow parent to redirect output (but not err !)
        self.available = False

    def connection_made(self, transport):
        """Called when a connection is made.

        The argument is the transport representing the pipe connection.
        To receive data, wait for data_received() calls.
        When the connection is closed, connection_lost() is called.
        """
        self.transport = transport
        self._logger.debug("connection made")

        asyncio.run_coroutine_threadsafe(self.on_connected(), self.loop)

    def connection_lost(self, exc):
        """Called when the connection is lost or closed.

        The argument is an exception object or None (the latter
        meaning a regular EOF is received or the connection was
        aborted or closed).
        """

        self.transport = None
        if exc is None:  # EOF or aborted
            pass
        else:
            self._logger.debug("connection lost")

        asyncio.run_coroutine_threadsafe(self.on_disconnected(), self.loop)

    def pause_writing(self):
        """Called when the transport's buffer goes over the high-water mark.

        Pause and resume calls are paired -- pause_writing() is called
        once when the buffer goes strictly over the high-water mark
        (even if subsequent writes increases the buffer size even
        more), and eventually resume_writing() is called once when the
        buffer size reaches the low-water mark.

        Note that if the buffer size equals the high-water mark,
        pause_writing() is not called -- it must go strictly over.
        Conversely, resume_writing() is called when the buffer size is
        equal or lower than the low-water mark.  These end conditions
        are important to ensure that things go as expected when either
        mark is zero.

        NOTE: This is the only Protocol callback that is not called
        through EventLoop.call_soon() -- if it were, it would have no
        effect when it's most needed (when the app keeps writing
        without yielding until pause_writing() is called).
        """
        self._logger.debug("pause writing")

    def resume_writing(self):
        """Called when the transport's buffer drains below the low-water mark.

        See pause_writing() for details.
        """
        self._logger.debug("resume writing")

    """Interface for protocol for subprocess calls."""
    def pipe_data_received(self, fd, data):
        """Called when the subprocess writes data into stdout/stderr pipe.

        fd is int file descriptor.
        data is bytes object.
        """

        self._logger.debug("pipe data received")

        self.output.extend(data)

        if not self.available and re.match(STARTED_REGEX, data):  # startup sequence has finished
            self.available = True
            pid = self.extract_pid(data)  # CAREFUL with intermediary processes (shells especially), and how they forward signals...
            asyncio.run_coroutine_threadsafe(self.on_started(), self.loop)
        # elif data.endswith("STOPPED"):  # process has been stopped (received SIGSTOP / Ctrl^Z)
        #
        # elif data.endswith("RESUMED"):  # process has been stopped (received SIGCONT)

        if self.available and re.match(SHUTDOWN_REGEX, data):  # shutdown sequence has been initiated. (received SIGTERM or normal shutdown)
            # CAREFUL : by design, this is an optimization and should not be necessary for the system to keep working.
            exit_code = self.extract_exit_code(data)  # => other processes must not rely on exit code. Required messages must be propagated at a higher level...
            asyncio.run_coroutine_threadsafe(self.on_shutdown(exit_code), self.loop)
            self.available = False

    @asyncio.coroutine
    def on_connected(self):
        raise NotImplemented

    @asyncio.coroutine
    def on_disconnected(self):
        raise NotImplemented

    @asyncio.coroutine
    def on_started(self):
        raise NotImplemented

    @asyncio.coroutine
    def on_shutdown(self):
        raise NotImplemented

    @asyncio.coroutine
    def on_exited(self):
        raise NotImplemented

    def extract_pid(self, started_str):
        try:
            pid = re.search(STARTED_REGEX, started_str).group(1)
            return pid
        except AttributeError:
            return None

    def extract_exit_code(self, shutdown_str):
        try:
            exit_code = re.search(SHUTDOWN_REGEX, shutdown_str).group(1)
            return exit_code
        except AttributeError:
            return None


    def pipe_connection_lost(self, fd, exc):
        """Called when a file descriptor associated with the child process is
        closed.

        fd is the int file descriptor that was closed.
        """
        self._logger.debug('pipe connection lost')

    def process_exited(self):
        """Called when subprocess has exited."""
        self._logger.debug('process exited')
        asyncio.run_coroutine_threadsafe(self.on_exited(), self.loop)
        self.exit_future.set_result(True)

