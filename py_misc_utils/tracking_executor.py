"""A task-tracking executor wrapper.

This module provides TrackingExecutor, an executor wrapper which keeps
track of the tasks being executed, allowing to wait for the completion of
specific tasks - or of all the submitted tasks - with completion
notification delivered via condition variables.
"""
import threading
import weakref

from . import cond_waiter as cwait
from . import executor as xe


def _wrap_task(executor, tid, fn, *args, **kwargs):
  """Wraps a task function so that its completion gets reported.

  The executor is held via a weak reference, so that pending task
  functions do not prevent a TrackingExecutor from being garbage
  collected.
  """
  eref = weakref.ref(executor)
  del executor

  def wfn():
    """The task wrapper: runs the task, then reports its completion."""
    try:
      return fn(*args, **kwargs)
    finally:
      xtor = eref()
      if xtor is not None:
        xtor._report_done(tid)

  return wfn


class TrackingExecutor:
  """An executor wrapper tracking the lifecycle of the submitted tasks.

  Each submitted task gets a unique id, and the executor keeps track of
  the tasks still pending - i.e. not yet completed - so that the callers
  can wait for their completion.

  A TrackingExecutor is not the owner of its underlying executor: it does
  not close it when being closed itself.
  """

  def __init__(self, executor=None):
    """Creates a tracking executor.

    Args:
      executor: An optional executor to which the tasks are submitted. If
        None, the common (shared) executor is used.
    """
    self.executor = executor if executor is not None else xe.common_executor()
    self._lock = threading.Lock()
    self._task_id = 0
    self._pending = set()
    self._pending_cv = threading.Condition(lock=self._lock)

  def _report_done(self, tid):
    """Marks a task as completed, notifying the waiters."""
    with self._lock:
      self._pending.remove(tid)
      self._pending_cv.notify_all()

  def _wrap(self, fn, *args, **kwargs):
    """Wraps a task function and registers it as pending.

    Args:
      fn: The function to execute.
      *args: Positional arguments to pass to the function.
      **kwargs: Keyword arguments to pass to the function.

    Returns:
      A (wrapped task function, task id) tuple.
    """
    with self._lock:
      wfn = _wrap_task(self, self._task_id, fn, *args, **kwargs)
      self._pending.add(self._task_id)
      self._task_id += 1

      return wfn, self._task_id - 1

  def submit(self, fn, *args, **kwargs):
    """Submits a task for execution.

    Args:
      fn: The function to execute.
      *args: Positional arguments to pass to the function.
      **kwargs: Keyword arguments to pass to the function.

    Returns:
      The unique task id assigned to the submitted task.

    Raises:
      Exception: If the underlying executor fails to accept the task - in
        which case the task is not tracked as pending anymore.
    """
    wfn, tid = self._wrap(fn, *args, **kwargs)
    try:
      self.executor.submit(wfn)
    except Exception:
      self._report_done(tid)
      raise

    return tid

  def submit_result(self, fn, *args, **kwargs):
    """Submits a task for execution, returning a result-holder object.

    Args:
      fn: The function to execute.
      *args: Positional arguments to pass to the function.
      **kwargs: Keyword arguments to pass to the function.

    Returns:
      A result-holder object - allowing to retrieve the task result and to
      wait for its completion - as returned by the underlying executor.

    Raises:
      Exception: If the underlying executor fails to accept the task - in
        which case the task is not tracked as pending anymore.
    """
    wfn, tid = self._wrap(fn, *args, **kwargs)
    try:
      return self.executor.submit_result(wfn)
    except Exception:
      self._report_done(tid)
      raise

  def shutdown(self):
    """Shuts down the underlying executor, waiting for all the pending
    tasks to complete."""
    self.executor.shutdown()
    self.wait()

  def wait(self, tids=None, timeout=None, timegen=None, waiter=None):
    """Waits for the completion of one or more tasks.

    Args:
      tids: An optional list of task ids to wait for. If None, all the
        tasks still pending are waited for.
      timeout: An optional maximum number of seconds to wait.
      timegen: An optional TimeGen object used to evaluate the timeout.
      waiter: An optional pre-built waiter object to be used - instead of
        building one from the timeout and timegen arguments.

    Returns:
      True if all the waited-for tasks are completed, False if the wait
      ended by timeout expiry.
    """
    waiter = waiter or cwait.CondWaiter(timeout=timeout, timegen=timegen)
    if not tids:
      with self._lock:
        while self._pending:
          if not waiter.wait(self._pending_cv):
            break

        return not self._pending
    else:
      stids = set(tids)
      with self._lock:
        while True:
          rem = stids & self._pending
          if not (rem and waiter.wait(self._pending_cv)):
            break

        return not rem

  def wait_for_idle(self, timeout=None, timegen=None):
    """Waits for both the tracked tasks - and the underlying executor -
    to be idle.

    Args:
      timeout: An optional maximum number of seconds to wait.
      timegen: An optional TimeGen object used to evaluate the timeout.

    Returns:
      True if the executor is idle, False if the wait ended by timeout
      expiry.
    """
    waiter = cwait.CondWaiter(timeout=timeout, timegen=timegen)

    return self.wait(waiter=waiter) and self.executor.wait_for_idle(waiter=waiter)
