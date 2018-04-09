import asyncio.subprocess
import sys

import pytest



@pytest.mark.skipif(sys.version_info < (3,4), reason="requires python3.4")
def test_subprocess_stream_3_4():
    """Ref : https://docs.python.org/3/library/asyncio-subprocess.html#asyncio.asyncio.subprocess.Process"""
    @asyncio.coroutine
    def get_date():
        code = 'import datetime; print(datetime.datetime.now())'

        # Create the subprocess, redirect the standard output into a pipe
        create = asyncio.create_subprocess_exec(sys.executable, '-c', code,
                                                stdout=asyncio.subprocess.PIPE)
        proc = yield from create

        # Read one line of output
        data = yield from proc.stdout.readline()
        line = data.decode('ascii').rstrip()

        # Wait for the subprocess exit
        yield from proc.wait()
        return line

    if sys.platform == "win32":
        loop = asyncio.ProactorEventLoop()
        asyncio.set_event_loop(loop)
    else:
        loop = asyncio.get_event_loop()

    date = loop.run_until_complete(get_date())
    print("Current date: %s" % date)
    loop.close()

@pytest.mark.skipif(sys.version_info < (3,5), reason="requires python3.5")
def test_subprocess_stream_3_5():
    """Ref : https://docs.python.org/3/library/asyncio-subprocess.html#asyncio.asyncio.subprocess.Process"""
    async def get_date():
        code = 'import datetime; print(datetime.datetime.now())'

        # Create the subprocess, redirect the standard output into a pipe
        create = asyncio.create_subprocess_exec(sys.executable, '-c', code,
                                                stdout=asyncio.subprocess.PIPE)
        proc = await create

        # Read one line of output
        data = await proc.stdout.readline()
        line = data.decode('ascii').rstrip()

        # Wait for the subprocess exit
        await proc.wait()
        return line

    if sys.platform == "win32":
        loop = asyncio.ProactorEventLoop()
        asyncio.set_event_loop(loop)
    else:
        loop = asyncio.get_event_loop()

    date = loop.run_until_complete(get_date())
    print("Current date: %s" % date)
    loop.close()

if __name__ == '__main__':
    pytest.main(['-s', '-x', __file__])
