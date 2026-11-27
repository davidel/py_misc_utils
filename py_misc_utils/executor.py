"""Thread pool executor with dynamic worker scaling and async result support.

This module provides:
- Executor: Thread pool with configurable min/max workers and idle timeout
- Queue: Thread-safe task queue with start/stop control
- Task: Callable wrapper that captures exceptions
- AsyncResult: Future-like object for retrieving task results
- common_executor(): Global singleton executor instance

Example:
  import py_misc_utils.executor as xe

  ex = xe.Executor(max_threads=4, min_threads=1, name_prefix='Demo')
  res = ex.submit_result(lambda a, b: a + b, 2, 3)
  assert res.wait() == 5
  ex.submit(print, 'fire and forget')
  ex.wait_for_idle()
  ex.shutdown()
"""

import collections
import os
import threading
import weakref

from . import alog
from . import cond_waiter as cwait
from . import utils as ut

_ExceptionWrapper = collections.namedtuple('ExceptionWrapper', 'exception')


class Task:
  """Callable task wrapper that executes a function and optionally sets a result.

  Captures exceptions and wraps them in _ExceptionWrapper for later re-raising.
  """

  def __init__(self, fn, args=None, kwargs=None, aresult=None):
    """Initialize a task.

    Args:
      fn: Callable to execute.
      args: Positional arguments (default: empty tuple).
      kwargs: Keyword arguments (default: empty dict).
      aresult: Optional AsyncResult to set with the result.
    """
    self._fn = fn
    self._args = args or ()
    self._kwargs = kwargs or dict()
    self._aresult = aresult

  def __call__(self):
    """Execute the task function and set result if provided."""
    try:
      fnres = self._fn(*self._args, **self._kwargs)
    except Exception as ex:  # noqa: BLE001 -- task boundary: any user-callable failure is delivered to the waiter as _ExceptionWrapper
      alog.exception(ex, exmsg='Exception while running task')
      fnres = _ExceptionWrapper(exception=ex)

    if self._aresult is not None:
      self._aresult.set(fnres)


class _Void:
  """Sentinel class for unset AsyncResult."""


VOID = _Void()


class AsyncResult:
  """Future-like object for retrieving asynchronous task results.

  Thread-safe with blocking wait() and optional timeout. If the task
  raised an exception, wait() re-raises it instead of returning a value.

  Example:
    import py_misc_utils.executor as xe

    res = xe.common_executor().submit_result(len, 'abc')
    assert res.wait() == 3
    # The same instance is returned on every call.
    assert xe.common_executor() is xe.common_executor()
  """

  def __init__(self):
    """Initialize an unset result."""
    self._cond = threading.Condition(lock=threading.Lock())
    self._result = VOID

  def set(self, result):
    """Set the result and notify waiters.

    Args:
      result: Result value or _ExceptionWrapper.
    """
    with self._cond:
      self._result = result
      self._cond.notify_all()

  def wait(self, timeout=None):
    """Wait for and retrieve the result.

    Args:
      timeout: Maximum time to wait in seconds (default: None, wait forever).

    Returns:
      The result value.

    Raises:
      Exception: If the task raised an exception.
      TimeoutError: If timeout expires before result is set.
    """
    with self._cond:
      # No need for a loop here, as the condition is signaled only when result
      # is set by the producer.
      if self._result is VOID:
        self._cond.wait(timeout=timeout)

      if isinstance(self._result, _ExceptionWrapper):
        raise self._result.exception

      return self._result


class Queue:
  """Thread-safe task queue with start/stop control.

  Supports blocking get with timeout and graceful shutdown. stop() just
  increments a counter, so it can be called multiple times, and pending
  items are still returned by get() - only once the queue is empty (and
  stopped) get() stops blocking and returns None.

  Example:
    import py_misc_utils.executor as xe

    q = xe.Queue()
    q.put('a')
    q.put('b')
    assert q.get(timeout=1.0) == 'a'
    q.stop()
    # Pending items are still returned after stop - None means exhausted.
    assert q.get() == 'b'
    assert q.get(timeout=0.1) is None
  """

  def __init__(self):
    """Initialize an empty queue."""
    self._lock = threading.Lock()
    self._cond = threading.Condition(lock=self._lock)
    self._queue = collections.deque()
    self._stopped = 0

  def put(self, task):
    """Add a task to the queue.

    Args:
      task: Task to enqueue.

    Returns:
      New queue length.
    """
    with self._lock:
      self._queue.append(task)
      self._cond.notify()

      return len(self._queue)

  def get(self, timeout=None):
    """Get a task from the queue, blocking until available or timeout.

    Even if the queue is stopped, pending items are still returned. Once
    the queue is both stopped and empty, None is returned - so None is
    the "no more work" sentinel used by the worker loop.

    Args:
      timeout: Maximum time to wait in seconds (default: None, wait forever).

    Returns:
      Task from the queue, or None if stopped and empty.
    """
    with self._lock:
      while True:
        # Even in case of stopped queue, always return pending items if available.
        if self._queue:
          return self._queue.popleft()
        if self._stopped > 0 or not self._cond.wait(timeout=timeout):
          break

  def start(self):
    """Resume the queue (decrement stop counter)."""
    with self._lock:
      self._stopped -= 1

  def stop(self):
    """Stop the queue (increment stop counter) and wake all waiters."""
    with self._lock:
      self._stopped += 1
      self._cond.notify_all()

  def __len__(self):
    """Return the number of pending tasks."""
    with self._lock:
      return len(self._queue)


class _Worker:
  """Internal worker thread that processes tasks from a queue."""

  def __init__(self, executor, queue, name, idle_timeout=None):
    """Initialize and start the worker thread.

    Args:
      executor: Weak reference to parent Executor.
      queue: Queue to pull tasks from.
      name: Thread name.
      idle_timeout: Timeout after which worker exits if no tasks (None = never).
    """
    self.executor = executor
    self.queue = queue
    self.idle_timeout = idle_timeout
    self.thread = threading.Thread(target=self._run, name=name, daemon=True)
    self.thread.start()

  def _unregister(self):
    """Notify executor that this worker is exiting."""
    executor = self.executor()
    if executor is not None:
      executor._unregister_worker(self)

  def _run(self):
    """Main worker loop: fetch and execute tasks until queue returns None."""
    while True:
      task = self.queue.get(timeout=self.idle_timeout)

      if task is None:
        break

      task()
      del task

    self._unregister()

  @property
  def ident(self):
    """Return the thread identifier."""
    return self.thread.ident

  def join(self):
    """Wait for the worker thread to finish."""
    self.thread.join()


def _compute_num_threads(min_threads, max_threads):
  """Compute default min/max thread counts.

  Args:
    min_threads: Minimum threads (None for auto).
    max_threads: Maximum threads (None for auto).

  Returns:
    Tuple of (min_threads, max_threads).
  """
  if max_threads is None:
    max_threads = max(8, int(os.cpu_count() * 1.5))
  if min_threads is None:
    min_threads = max(2, max_threads // 4)

  return min_threads, max_threads


class Executor:
  """Thread pool executor with dynamic worker scaling.

  Maintains a minimum number of persistent workers and scales up to a maximum
  based on queue depth. Workers beyond the minimum have an idle timeout.

  Example:
    import py_misc_utils.executor as xe

    ex = xe.Executor(max_threads=4, min_threads=1, name_prefix='Demo')
    res = ex.submit_result(lambda a, b: a + b, 2, 3)
    assert res.wait() == 5
    ex.submit(print, 'fire and forget')
    ex.wait_for_idle()
    ex.shutdown()
  """

  def __init__(self,
               max_threads=None,
               min_threads=None,
               name_prefix=None,
               idle_timeout=None):
    """Initialize the executor.

    Args:
      max_threads: Maximum number of worker threads (default: auto).
      min_threads: Minimum number of persistent workers (default: auto).
      name_prefix: Prefix for worker thread names (default: 'Executor').
      idle_timeout: Seconds before idle workers exit (default: from
        EXECUTOR_IDLE_TIMEOUT env var or 5).
    """
    self._min_threads, self._max_threads = _compute_num_threads(
        min_threads, max_threads)
    self._name_prefix = name_prefix or 'Executor'
    self._idle_timeout = idle_timeout or ut.getenv(
        'EXECUTOR_IDLE_TIMEOUT', dtype=int, defval=5)
    self._lock = threading.Lock()
    self._queue = Queue()
    self._workers = dict()
    self._thread_counter = 0
    self._idle_cond = threading.Condition(lock=self._lock)

  def _unregister_worker(self, worker):
    """Called by worker when it exits."""
    alog.spam(f'Unregistering worker thread {worker.ident}')
    with self._lock:
      self._workers.pop(worker.ident, None)
      if not self._workers:
        self._idle_cond.notify_all()

  def _new_name(self):
    """Generate a new worker thread name."""
    self._thread_counter += 1

    return f'{self._name_prefix}-{self._thread_counter}'

  def _maybe_add_worker(self, queued):
    """Add a worker if queue depth warrants it or below minimum."""
    num_threads = len(self._workers)
    if ((queued > 1 and num_threads < self._max_threads) or
        num_threads < self._min_threads):
      # Up to min_threads the workers should never quit, so they get None as
      # timeout, while the one after that will get _idle_timeout which will
      # make them quit if no task is fetched within such timeout.
      idle_timeout = self._idle_timeout if num_threads > self._min_threads else None

      worker = _Worker(weakref.ref(self),
                       self._queue,
                       self._new_name(),
                       idle_timeout=idle_timeout)

      self._workers[worker.ident] = worker

      alog.spam(f'New thread #{num_threads} with ID {worker.ident}')

  def _submit_task(self, task):
    """Submit a task and potentially scale up workers.

    Args:
      task: Task to submit.
    """
    with self._lock:
      queued = self._queue.put(task)
      self._maybe_add_worker(queued)

  def submit(self, fn, *args, **kwargs):
    """Submit a fire-and-forget task.

    Args:
      fn: Callable to execute.
      *args: Positional arguments.
      **kwargs: Keyword arguments.
    """
    self._submit_task(Task(fn, args=args, kwargs=kwargs))

  def submit_result(self, fn, *args, **kwargs):
    """Submit a task and return an AsyncResult for retrieving the result.

    Args:
      fn: Callable to execute.
      *args: Positional arguments.
      **kwargs: Keyword arguments.

    Returns:
      AsyncResult that can be waited on for the result.
    """
    aresult = AsyncResult()

    self._submit_task(Task(fn, args=args, kwargs=kwargs, aresult=aresult))

    return aresult

  def shutdown(self):
    """Stop the executor and wait for all workers to exit."""
    alog.debug0('Stopping executor')
    self._queue.stop()
    with self._lock:
      alog.debug0('Waiting executor workers exit')
      while self._workers:
        self._idle_cond.wait()

  def wait_for_idle(self, timeout=None, timegen=None, waiter=None):
    """Wait for all tasks to complete and workers to become idle.

    Temporarily stops the queue to prevent new tasks from being added -
    tasks submitted concurrently are queued but do not spawn extra workers
    during the wait (the queue is restarted before returning).

    Args:
      timeout: Maximum time to wait in seconds (default: None).
      timegen: Optional time generator for CondWaiter.
      waiter: Optional CondWaiter instance.

    Returns:
      True if all workers became idle, False if timeout expired.
    """
    alog.debug0('Waiting for idle ...')

    waiter = waiter or cwait.CondWaiter(timeout=timeout, timegen=timegen)
    self._queue.stop()
    try:
      with self._lock:
        while self._workers:
          if not waiter.wait(self._idle_cond):
            return False
    finally:
      self._queue.start()
      alog.debug0('Waiting for idle ... done')

    return True


_LOCK = threading.Lock()
_EXECUTOR = None


def common_executor():
  """Get the global singleton executor instance.

  Creates the executor on first call with configuration from environment:
  - EXECUTOR_WORKERS: max threads
  - EXECUTOR_NAME: name prefix

  Returns:
    The global Executor instance.

  Example:
    import py_misc_utils.executor as xe

    res = xe.common_executor().submit_result(len, 'abc')
    assert res.wait() == 3
  """
  global _EXECUTOR

  with _LOCK:
    if _EXECUTOR is None:
      _EXECUTOR = Executor(
          max_threads=ut.getenv('EXECUTOR_WORKERS', dtype=int),
          name_prefix=os.getenv('EXECUTOR_NAME', 'CommonExecutor'),
      )

    return _EXECUTOR
