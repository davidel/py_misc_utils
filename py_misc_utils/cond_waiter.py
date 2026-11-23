"""Timeout-aware condition variable waiting.

This module provides CondWaiter, a small object holding a timeout and a
time source, used to wait on condition variables with a consistent
remaining-time semantics across multiple waits - which is needed by the
components supporting custom TimeGen objects (like the executors).
"""
from . import abs_timeout as abst
from . import timegen as tg


class CondWaiter:
  """A condition variable waiter holding a timeout and a time source.

  Each wait() call consumes the remaining time held by the object, so
  that a sequence of waits honors a single overall timeout.
  """

  def __init__(self, timeout=None, timegen=None):
    """Creates a condition waiter.

    Args:
      timeout: An optional maximum overall number of seconds to wait. If
        None, the waits never expire.
      timegen: An optional TimeGen object used to evaluate the timeout -
        and to wait on the condition variables. If None, the default
        (system) time source is used.
    """
    self._timegen = tg.TimeGen() if timegen is None else timegen
    self._timeo = abst.AbsTimeout(timeout, timefn=self._timegen.now)

  def wait(self, cond):
    """Waits on a condition variable until notified - or timeout expiry.

    Args:
      cond: The condition variable to wait on. The caller must hold its
        lock.

    Returns:
      True if the condition variable has been notified, False if the
      remaining wait time expired.
    """
    return self._timegen.wait(cond, timeout=self._timeo.get())
