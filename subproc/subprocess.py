
import asyncio.subprocess


class SubProcess():
    """
    Class wrapping :
    - asyncio.process (for async code),
    - subprocess.Popen (for bwcompat with non async code - py2.7)
    """
    # TODO : attach to an existing process ??? is it doable at this level or we need to go higher (stream and sockets ?)


    def __init__(self):
        pass

    def send_signal(self):
        pass

    def terminate(self):
        pass

    def kill(self):
        pass


    # async interface
    async def wait(self):
        await pass

    async def communicate(self):
        await pass



async def create_subprocess_shell(*args, stdin=None, stdout=None, stderr=None, loop=None, limit=None, **kwds):
        await asyncio.create_subprocess_shell(*args, stdin=stdin, stdout=stdout, stderr=stderr, loop=loop, limit=limit, **kwds)

async def create_subprocess_exec(*args, stdin=None, stdout=None, stderr=None, loop=None, limit=None, **kwds):
        await asyncio.create_subprocess_exec(*args, stdin=stdin, stdout=stdout, stderr=stderr, loop=loop, limit=limit, **kwds)


# Simple example code
if __name__ == "__main__":

    testproc = create_subprocess_exec(["ed", "-p\*"])

    try:
        self.testobserver.expect("\\*", timeout=5)

        self.testproc.write('H\n')
        self.testobserver.expect("\\*", timeout=5)

        self.testproc.write('a\n')
        self.testobserver.expect("\\*", timeout=5)

        self.testproc.write("some test string\n")
        self.testproc.write(".\n")
        self.testobserver.expect("\\*", timeout=5)

        self.testproc.write("p\n")
        self.testobserver.expect("\\some test string\n", timeout=5)

        self.testproc.write("Q\n")
        self.testobserver.expect("", timeout=5)

    except:  # something went wrong
        print("Exception was thrown")
        print("debug information:")
        print(str(self.testproc))
        # print("stdout:")
        # while not self.testobserver._expect_out.eof():
        #     print(self.testobserver._expect_out.readline())
        # print("stderr:")
        # while not self.testobserver.expecterr.eof():
        #     print(self.testobserver.expecterr.readline())
        raise

    self.testobserver.expect("")
    self.testobserver.expect("")

    if sys.platform == "win32":
        loop = asyncio.ProactorEventLoop()
        asyncio.set_event_loop(loop)
    else:
        loop = asyncio.get_event_loop()

    date = loop.run_until_complete(get_date(loop))
    print("Current date: %s" % date)
    loop.close()
