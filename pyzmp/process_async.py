# Ref : https://docs.python.org/3.6/library/asyncio-subprocess.html


"""
An asyncio implementation of process.
"""
import contextlib
import uuid

import time

import logging
import setproctitle
import asyncio


class ProcessAsync(object):
    """
    Process class that model how a process is started and stopped, can start / stop child processes,
     all in a synchronous deterministic manner.
    It mainly add synchronization primitives to multiprocessing.Process.
    """

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
        self._control = ProcessAsync.Control()

        #: whether or not the node name should be set as the actual process title
        #: replacing the string duplicated from the python interpreter run
        self.new_title = True

        self._target_context = target_context or self.target_context  # TODO: extend to list if possible ( available for python >3.1 only )
        super(ProcessAsync, self).__init__()

    @asyncio.corountine()
    await def start(self, timeout=None):
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


        # https://github.com/dano/aioprocessing


        # https://kevinmccarthy.org/2016/07/25/streaming-subprocess-stdin-and-stdout-with-asyncio-in-python/
        # self._process = await asyncio.create_subprocess_exec(**pargs, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        #
        # await asyncio.wait([
        #     _read_stream(process.stdout, stdout_cb),
        #     _read_stream(process.stderr, stderr_cb)
        # ])
        # return await process.wait()


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