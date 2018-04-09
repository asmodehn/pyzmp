from __future__ import absolute_import, division, print_function

import inspect

import concurrent.futures._base
import reprlib

from . import events

Error = concurrent.futures._base.Error
CancelledError = concurrent.futures.CancelledError
TimeoutError = concurrent.futures.TimeoutError


class InvalidStateError(Error):
    """The operation is not allowed in this state."""


def _format_callbacks(cb):
    """helper function for Future.__repr__"""
    size = len(cb)
    if not size:
        cb = ''

    def format_cb(callback):
        return events._format_callback_source(callback, ())

    if size == 1:
        cb = format_cb(cb[0])
    elif size == 2:
        cb = '{}, {}'.format(format_cb(cb[0]), format_cb(cb[1]))
    elif size > 2:
        cb = '{}, <{} more>, {}'.format(format_cb(cb[0]),
                                        size - 2,
                                        format_cb(cb[-1]))
    return 'cb=[%s]' % cb


def _future_repr_info(future):
    # (Future) -> str
    """helper function for Future.__repr__"""
    info = [future._state.lower()]
    if future._state == concurrent.futures._base.FINISHED:
        if future._exception is not None:
            info.append('exception={!r}'.format(future._exception))
        else:
            # use reprlib to limit the length of the output, especially
            # for very long strings
            result = reprlib.repr(future._result)
            info.append('result={}'.format(result))
    if future._callbacks:
        info.append(_format_callbacks(future._callbacks))
    if future._source_traceback:
        frame = future._source_traceback[-1]
        info.append('created at %s:%s' % (frame[0], frame[1]))
    return info


def _format_routine(ro):
    if not hasattr(ro, 'cr_code') and not hasattr(ro, 'gi_code'):
        # Most likely a built-in type.

        # Built-in types might not have __qualname__ or __name__.
        ro_name = getattr(
            ro, '__qualname__',
            getattr(ro, '__name__', type(ro).__name__))
        ro_name = '{}()'.format(ro_name)

        running = False
        try:
            running = ro.cr_running
        except AttributeError:
            try:
                running = ro.gi_running
            except AttributeError:
                pass

        if running:
            return '{} running'.format(ro_name)
        else:
            return ro_name

    ro_name = None
    func = ro

    if ro_name is None:
        ro_name = events._format_callback(func, (), {})

    try:
        ro_code = ro.gi_code
    except AttributeError:
        ro_code = ro.cr_code

    try:
        ro_frame = ro.gi_frame
    except AttributeError:
        ro_frame = ro.cr_frame

    filename = ro_code.co_filename
    lineno = 0
    if ro_frame is not None:
        lineno = ro_frame.f_lineno
        ro_repr = ('%s running at %s:%s'
                     % (ro_name, filename, lineno))
    else:
        lineno = ro_code.co_firstlineno
        ro_repr = ('%s done, defined at %s:%s'
                     % (ro_name, filename, lineno))

    return ro_repr


import linecache
import traceback


def _task_repr_info(task):
    info = _future_repr_info(task)

    if task._must_cancel:
        # replace status
        info[0] = 'cancelling'

    ro = _format_routine(task._ro)
    info.insert(1, 'coro=<%s>' % ro)

    if task._fut_waiter is not None:
        info.insert(2, 'wait_for=%r' % task._fut_waiter)
    return info


def _task_get_stack(task, limit):
    frames = []
    try:
        # 'async def' coroutines
        f = task._ro.cr_frame
    except AttributeError:
        f = task._ro.gi_frame
    if f is not None:
        while f is not None:
            if limit is not None:
                if limit <= 0:
                    break
                limit -= 1
            frames.append(f)
            f = f.f_back
        frames.reverse()
    elif task._exception is not None:
        tb = task._exception.__traceback__
        while tb is not None:
            if limit is not None:
                if limit <= 0:
                    break
                limit -= 1
            frames.append(tb.tb_frame)
            tb = tb.tb_next
    return frames


def _task_print_stack(task, limit, file):
    extracted_list = []
    checked = set()
    for f in task.get_stack(limit=limit):
        lineno = f.f_lineno
        co = f.f_code
        filename = co.co_filename
        name = co.co_name
        if filename not in checked:
            checked.add(filename)
            linecache.checkcache(filename)
        line = linecache.getline(filename, lineno, f.f_globals)
        extracted_list.append((filename, lineno, name, line))
    exc = task._exception
    if not extracted_list:
        print('No stack for %r' % task, file=file)
    elif exc is not None:
        print('Traceback for %r (most recent call last):' % task,
              file=file)
    else:
        print('Stack for %r (most recent call last):' % task,
              file=file)
    traceback.print_list(extracted_list, file=file)
    if exc is not None:
        for line in traceback.format_exception_only(exc.__class__, exc):
            print(line, file=file, end='')




import weakref

from . import events


"""
A Task is a completely serializable, atomic, unit of computing, that can be transferred between Threads (and therefore Processes).

"""


class Task(concurrent.futures.Future):  # TODO : asyncio on py3
    """
    A function call wrapped in a Future.
    Provides a similar API to asyncio.Task for upward compatibility.
    
    Here we only have one function call, instead of a set of coroutines.
    It is the most trivial implementation of a py3 asyncio Task, but working on python2.7.
    """

    # An important invariant maintained while a Task not done:
    #
    # - Either _fut_waiter is None, and _step() is scheduled;
    # - or _fut_waiter is some Future, and _step() is *not* scheduled.
    #
    # The only transition from the latter to the former is through
    # _wakeup().  When _fut_waiter is not None, one of its callbacks
    # must be _wakeup().

    # Weak set containing all tasks alive. In our case, only 1.
    _all_tasks = weakref.WeakSet()

    # Dictionary containing tasks that are currently active in
    # all running event loops.  {EventLoop: Task}
    _current_tasks = {}

    # If False, don't log a message if the task is destroyed whereas its
    # status is still pending
    _log_destroy_pending = True

    @classmethod
    def current_task(cls, loop=None):
        """Return the currently running task in an event loop or None.

        By default the current task for the current event loop is returned.

        None is returned when called not in the context of a Task.
        """
        if loop is None:
            loop = events.get_event_loop()
        return cls._current_tasks.get(loop)

    @classmethod
    def all_tasks(cls, loop=None):
        """Return a set of all tasks for an event loop.

        By default all tasks for the current event loop are returned.
        """
        if loop is None:
            loop = events.get_event_loop()
        return {t for t in cls._all_tasks if t._loop is loop}

    def __init__(self, ro, *, loop=None):
        super(Task, self).__init__(loop=loop)
        if self._source_traceback:
            del self._source_traceback[-1]
        self._ro = ro
        self._fut_waiter = None
        self._must_cancel = False
        self._loop.call_soon(self._step)
        self.__class__._all_tasks.add(self)

    def __del__(self):
        if self._state == concurrent.futures._base.PENDING and self._log_destroy_pending:
            context = {
                'task': self,
                'message': 'Task was destroyed but it is pending!',
            }
            if self._source_traceback:
                context['source_traceback'] = self._source_traceback
            self._loop.call_exception_handler(context)
        super(Task, self).__del__(self)

    def _repr_info(self):
        return _task_repr_info(self)

    def get_stack(self, *, limit=None):
        """Return the list of stack frames for this task's coroutine.

        If the coroutine is not done, this returns the stack where it is
        suspended.  If the coroutine has completed successfully or was
        cancelled, this returns an empty list.  If the coroutine was
        terminated by an exception, this returns the list of traceback
        frames.

        The frames are always ordered from oldest to newest.

        The optional limit gives the maximum number of frames to
        return; by default all available frames are returned.  Its
        meaning differs depending on whether a stack or a traceback is
        returned: the newest frames of a stack are returned, but the
        oldest frames of a traceback are returned.  (This matches the
        behavior of the traceback module.)

        For reasons beyond our control, only one stack frame is
        returned for a suspended coroutine.
        """
        return _task_get_stack(self, limit)

    def print_stack(self, *, limit=None, file=None):
        """Print the stack or traceback for this task's coroutine.

        This produces output similar to that of the traceback module,
        for the frames retrieved by get_stack().  The limit argument
        is passed to get_stack().  The file argument is an I/O stream
        to which the output is written; by default output is written
        to sys.stderr.
        """
        return _task_print_stack(self, limit, file)

    def cancel(self):
        """Request that this task cancel itself.

        This arranges for a CancelledError to be thrown into the
        wrapped coroutine on the next cycle through the event loop.
        The coroutine then has a chance to clean up or even deny
        the request using try/except/finally.

        Unlike Future.cancel, this does not guarantee that the
        task will be cancelled: the exception might be caught and
        acted upon, delaying cancellation of the task or preventing
        cancellation completely.  The task may also return a value or
        raise a different exception.

        Immediately after this method is called, Task.cancelled() will
        not return True (unless the task was already cancelled).  A
        task will be marked as cancelled when the wrapped coroutine
        terminates with a CancelledError exception (even if cancel()
        was not called).
        """
        if self.done():
            return False
        if self._fut_waiter is not None:
            if self._fut_waiter.cancel():
                # Leave self._fut_waiter; it may be a Task that
                # catches and ignores the cancellation so we may have
                # to cancel it again later.
                return True
        # It must be the case that self._step is already scheduled.
        self._must_cancel = True
        return True

    def _step(self, exc=None):
        assert not self.done(), \
            '_step(): already done: {!r}, {!r}'.format(self, exc)
        if self._must_cancel:
            if not isinstance(exc, CancelledError):
                exc = CancelledError()
            self._must_cancel = False
        coro = self._coro
        self._fut_waiter = None

        self.__class__._current_tasks[self._loop] = self
        # Call either coro.throw(exc) or coro.send(None).
        try:
            if exc is None:
                # We use the `send` method directly, because coroutines
                # don't have `__iter__` and `__next__` methods.
                result = coro.send(None)
            else:
                result = coro.throw(exc)
        except StopIteration as exc:
            if self._must_cancel:
                # Task is cancelled right before coro stops.
                self._must_cancel = False
                self.set_exception(CancelledError())
            else:
                self.set_result(exc.value)
        except CancelledError:
            super(Task, self).cancel()  # I.e., Future.cancel(self).
        except Exception as exc:
            self.set_exception(exc)
        except BaseException as exc:
            self.set_exception(exc)
            raise
        else:
            blocking = getattr(result, '_asyncio_future_blocking', None)
            if blocking is not None:
                # Yielded Future must come from Future.__iter__().
                if result._loop is not self._loop:
                    self._loop.call_soon(
                        self._step,
                        RuntimeError(
                            'Task {!r} got Future {!r} attached to a '
                            'different loop'.format(self, result)))
                elif blocking:
                    if result is self:
                        self._loop.call_soon(
                            self._step,
                            RuntimeError(
                                'Task cannot await on itself: {!r}'.format(
                                    self)))
                    else:
                        result._asyncio_future_blocking = False
                        result.add_done_callback(self._wakeup)
                        self._fut_waiter = result
                        if self._must_cancel:
                            if self._fut_waiter.cancel():
                                self._must_cancel = False
                else:
                    self._loop.call_soon(
                        self._step,
                        RuntimeError(
                            'yield was used instead of yield from '
                            'in task {!r} with {!r}'.format(self, result)))
            elif result is None:
                # Bare yield relinquishes control for one event loop iteration.
                self._loop.call_soon(self._step)
            elif inspect.isgenerator(result):
                # Yielding a generator is just wrong.
                self._loop.call_soon(
                    self._step,
                    RuntimeError(
                        'yield was used instead of yield from for '
                        'generator in task {!r} with {}'.format(
                            self, result)))
            else:
                # Yielding something else is an error.
                self._loop.call_soon(
                    self._step,
                    RuntimeError(
                        'Task got bad yield: {!r}'.format(result)))
        finally:
            self.__class__._current_tasks.pop(self._loop)
            self = None  # Needed to break cycles when an exception occurs.

    def _wakeup(self, future):
        try:
            future.result()
        except Exception as exc:
            # This may also be a cancellation.
            self._step(exc)
        else:
            # Don't pass the value of `future.result()` explicitly,
            # as `Future.__iter__` and `Future.__await__` don't need it.
            # If we call `_step(value, None)` instead of `_step()`,
            # Python eval loop would use `.send(value)` method call,
            # instead of `__next__()`, which is slower for futures
            # that return non-generator iterators from their `__iter__`.
            self._step()
        self = None  # Needed to break cycles when an exception occurs.
