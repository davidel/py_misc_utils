"""Periodic task execution on a scheduler with weak reference callbacks.

This module provides :class:`PeriodicTask` which runs a callable repeatedly
at a fixed interval on a scheduler, with support for graceful start/stop,
error handling, and automatic cleanup when the callable is garbage collected.
"""

import threading
import weakref

from . import alog
from . import scheduler as sch
from . import weak_call as wcall


class PeriodicTask:
  """Run a callable periodically on a scheduler.
  """

  def __init__(self, name, periodic_fn, period, scheduler=None, stop_on_error=None):
    """Initialize a PeriodicTask.

    Args:
      name: Human-readable name for the task.
      periodic_fn: The callable to run periodically.
      period: Interval in seconds between runs.
      scheduler: Optional scheduler instance; defaults to the common scheduler.
      stop_on_error: If True (default), stop the task when periodic_fn raises
        an exception. If False, continue running despite errors.
    """
    self._name = name
    self._periodic_fn = wcall.WeakCall(periodic_fn)
    self._period = period
    self._scheduler = scheduler or sch.common_scheduler()
    self._stop_on_error = stop_on_error in (None, True)
    self._lock = threading.Lock()
    self._event = None
    self._completed_event = None

  def start(self):
    """Schedule the first run of the periodic task.

    Returns:
      The PeriodicTask instance for chaining.
    """
    with self._lock:
      if self._event is None:
        self._schedule()

    return self

  def stop(self):
    """Stop the periodic task and wait for any in-flight run to complete.

    If the task is not running, this is a no-op.
    """
    completed_event = None
    with self._lock:
      if self._event is not None:
        # If "events" is empty, we were not able to cancel the task, so it will be
        # in flight, and we need to wait for it to complete before exiting.
        events = self._scheduler.cancel(self._event)
        completed_event = self._completed_event if not events else None
        self._event = None

    if completed_event is not None:
      completed_event.wait()

  def _schedule(self):
    """Enter the runner into the scheduler for the next period."""
    self._event = self._scheduler.enter(self._period, self._runner)
    self._completed_event = threading.Event()

  def _runner(self):
    """Execute the periodic function and reschedule if appropriate.

    Signals completion via _completed_event. If the callable has been garbage
    collected (WeakCall returns GONE) or stop_on_error is set after an
    exception, the task is not rescheduled.
    """
    re_issue = True
    try:
      if self._periodic_fn() is wcall.GONE:
        re_issue = False
    except Exception as ex:
      alog.exception(ex, exmsg=f'Exception while running periodic task "{self._name}"')
      re_issue = not self._stop_on_error
    finally:
      with self._lock:
        self._completed_event.set()
        if self._event is not None:
          if re_issue:
            self._schedule()
          else:
            self._event = None
