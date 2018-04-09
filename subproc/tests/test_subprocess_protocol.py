import os
import sys
# THis needs to be done early to avoid problem with other async-related import
try:
    import asyncio
except ImportError:
    import trollius as asyncio


import asynctest
import pytest

from subproc.subprocess_protocol import SubprocessProtocol

# We need to carefully setup the logging, to make sure we can see the debug logging messages from the protocol.
import logging.config
logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'pid_aware': {
            'format': '%(name)s %(levelname)s : %(message)s'
        },
    },
    'handlers': {
      'console': {
          'class': 'logging.StreamHandler',
          'formatter': 'pid_aware',
      }
    },
    'root': {
      'level': 'DEBUG',
      'handlers': ['console'],
    },
    'subproc.subprocess_protocol': {
      'level': 'DEBUG',
      'handlers': ['console'],
      'propagate': True,
    },
})

# @pytest.mark.skipif(sys.version_info < (2, 7), reason="requires python2.7 minimum")
# @pytest.mark.skipif(sys.version_info >= (3, 4), reason="can do better for python 3.4 and up")
# def test_subprocess_protocol_2_7():
#     """
#     Ref : https://docs.python.org/3/library/asyncio-subprocess.html#asyncio.asyncio.subprocess.Process
#     Adapted for syncio for python 2.7
#     """
#     import trollius


@pytest.mark.asyncio
@asyncio.coroutine
def test_minimal_long_process(event_loop):
    with open(os.path.join(os.path.dirname(__file__), "minimal_long_process.py"), "r") as min_proc:
        code = min_proc.read()  # file should be small enough

    exit_future = asyncio.Future(loop=event_loop)

    # In case mock doesnt work as expected
    class TestSubprocessProtocol(SubprocessProtocol):
        @asyncio.coroutine
        def on_started(self):
            print("ON STARTED")

        @asyncio.coroutine
        def on_shutdown(self):
            print("ON SHUTDOWN")

        @asyncio.coroutine
        def on_exited(self):
            print("ON EXIT")

    def mock_protocol_factory():
        """To mock only the async API"""
        p = TestSubprocessProtocol(exit_future)
        # Just comment out these lines to test the actual behavior with mock
        # p = SubprocessProtocol(exit_future)
        #p.on_shutdown = asynctest.CoroutineMock()
        #p.on_started = asynctest.CoroutineMock()
        #p.on_exited = asynctest.CoroutineMock()
        return p

    # Create the subprocess controlled by the protocol DateProtocol,
    # redirect the standard output into a pipe
    create = event_loop.subprocess_exec(mock_protocol_factory,
                                  sys.executable, '-c', code,
                                  stdin=None, stderr=None)
    try:
        transport, protocol = yield from create
    except Exception as e:  # TODO : catch exact exception on python < 3.4
        transport, protocol = yield asyncio.From(create)

    #protocol.on_started.assert_called_once()


    try:
        # Wait for the subprocess exit using the process_exited() method
        # of the protocol
        yield from exit_future
    except Exception as e:  # TODO : catch exact exception on python < 3.4
        yield asyncio.From(exit_future)

    #protocol.on_shutdown.assert_called_once()

    # Close the stdout pipe
    transport.close()
    #
    # # Read the output which was collected by the pipe_data_received()
    # # method of the protocol
    # data = bytes(protocol.output)
    # return data.decode('ascii').rstrip()

# if sys.platform == "win32":
#     loop = trollius.ProactorEventLoop()
# else:
#     loop = trollius.new_event_loop()
# trollius.set_event_loop(loop)
#
# date = loop.run_until_complete(run_minimal_process(loop))
# print("Current date: %s" % date)
# loop.close()


if __name__ == '__main__':
    pytest.main(['-s', '-x', __file__])
