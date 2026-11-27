"""Time source abstraction.

This module provides TimeGen, the time source used by the scheduler (and
other time-related components) to get the current time and to wait on
condition variables. It can be subclassed to provide custom - e.g.
simulated - time sources.
"""
import time


class TimeGen:
  """A time source for time-driven components.

  The default implementation uses the system time and condition variable
  wait semantics. Subclassing TimeGen allows to control the flow of time -
  e.g. for testing time-based logic without real delays.

  Example:
    class ShiftedTimeGen(timegen.TimeGen):

      def now(self):
        return super().now() + 3600  # one hour in the future
  """

  def now(self):
    """Returns the current time.

    Returns:
      The current time, in seconds since the epoch.
    """
    return time.time()

  def wait(self, cond, timeout=None):
    """Waits on a condition variable until it is notified - or timeout.

    Args:
      cond: The threading condition variable to wait on. The caller must
        hold its lock.
      timeout: An optional maximum number of seconds to wait. If None, the
        wait blocks until the condition variable is notified.

    Returns:
      The result of the underlying condition variable wait operation: True
      if it has been notified, False if the timeout expired.
    """
    return cond.wait(timeout=timeout)
