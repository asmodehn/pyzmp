
import sys

import pytest

"""
Test different version of async framework to verify our original assumptions
"""


@pytest.mark.skipif(sys.version_info < (2, 7), reason="requires python2.7 minimum")
@pytest.mark.skipif(sys.version_info >= (3, 4), reason="can do better for python 3.4 and up")
def test_subprocess_protocol_2_7():
    """
    Ref : https://docs.python.org/3/library/asyncio-subprocess.html#asyncio.asyncio.subprocess.Process
    Adapted for syncio for python 2.7
    """
    import trollius

    class DateProtocol(trollius.SubprocessProtocol):
        def __init__(self, exit_future):
            self.exit_future = exit_future
            self.output = bytearray()

        def pipe_data_received(self, fd, data):
            self.output.extend(data)

        def process_exited(self):
            self.exit_future.set_result(True)

    @trollius.coroutine
    def get_date(loop):
        code = 'import datetime; print(datetime.datetime.now())'
        exit_future = trollius.Future(loop=loop)

        # Create the subprocess controlled by the protocol DateProtocol,
        # redirect the standard output into a pipe
        create = loop.subprocess_exec(lambda: DateProtocol(exit_future),
                                      sys.executable, '-c', code,
                                      stdin=None, stderr=None)
        transport, protocol = yield trollius.From(create)

        # Wait for the subprocess exit using the process_exited() method
        # of the protocol
        yield trollius.From(exit_future)

        # Close the stdout pipe
        transport.close()

        # Read the output which was collected by the pipe_data_received()
        # method of the protocol
        data = bytes(protocol.output)
        return data.decode('ascii').rstrip()

    if sys.platform == "win32":
        loop = trollius.ProactorEventLoop()
    else:
        loop = trollius.new_event_loop()
    trollius.set_event_loop(loop)

    date = loop.run_until_complete(get_date(loop))
    print("Current date: %s" % date)
    loop.close()


@pytest.mark.skipif(sys.version_info < (3, 4), reason="requires python3.4")
def test_subprocess_protocol_3_4():
    import asyncio

    """Ref : https://docs.python.org/3/library/asyncio-subprocess.html#asyncio.asyncio.subprocess.Process"""
    class DateProtocol(asyncio.SubprocessProtocol):
        def __init__(self, exit_future):
            self.exit_future = exit_future
            self.output = bytearray()

        def pipe_data_received(self, fd, data):
            self.output.extend(data)

        def process_exited(self):
            self.exit_future.set_result(True)

    @asyncio.coroutine
    def get_date(loop):
        code = 'import datetime; print(datetime.datetime.now())'
        exit_future = asyncio.Future(loop=loop)

        # Create the subprocess controlled by the protocol DateProtocol,
        # redirect the standard output into a pipe
        create = loop.subprocess_exec(lambda: DateProtocol(exit_future),
                                      sys.executable, '-c', code,
                                      stdin=None, stderr=None)
        transport, protocol = yield from create

        # Wait for the subprocess exit using the process_exited() method
        # of the protocol
        yield from exit_future

        # Close the stdout pipe
        transport.close()

        # Read the output which was collected by the pipe_data_received()
        # method of the protocol
        data = bytes(protocol.output)
        return data.decode('ascii').rstrip()

    if sys.platform == "win32":
        loop = asyncio.ProactorEventLoop()
    else:
        loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    date = loop.run_until_complete(get_date(loop))
    print("Current date: %s" % date)
    loop.close()


@pytest.mark.skipif(sys.version_info < (3, 5), reason="requires python3.5")
def test_subprocess_protocol_3_5():
    import asyncio

    """Ref : https://docs.python.org/3/library/asyncio-subprocess.html#asyncio.asyncio.subprocess.Process"""
    class DateProtocol(asyncio.SubprocessProtocol):
        def __init__(self, exit_future):
            self.exit_future = exit_future
            self.output = bytearray()

        def pipe_data_received(self, fd, data):
            self.output.extend(data)

        def process_exited(self):
            self.exit_future.set_result(True)

    async def get_date(loop):
        code = 'import datetime; print(datetime.datetime.now())'
        exit_future = asyncio.Future(loop=loop)

        # Create the subprocess controlled by the protocol DateProtocol,
        # redirect the standard output into a pipe
        create = loop.subprocess_exec(lambda: DateProtocol(exit_future),
                                      sys.executable, '-c', code,
                                      stdin=None, stderr=None)
        transport, protocol = await create

        # Wait for the subprocess exit using the process_exited() method
        # of the protocol
        await exit_future

        # Close the stdout pipe
        transport.close()

        # Read the output which was collected by the pipe_data_received()
        # method of the protocol
        data = bytes(protocol.output)
        return data.decode('ascii').rstrip()

    if sys.platform == "win32":
        loop = asyncio.ProactorEventLoop()
    else:
        loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    date = loop.run_until_complete(get_date(loop))
    print("Current date: %s" % date)
    loop.close()

if __name__ == '__main__':
    pytest.main(['-s', '-x', __file__])
