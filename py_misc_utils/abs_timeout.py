"""Absolute timeout helper for managing deadlines.

This module provides a simple timeout class that tracks an absolute
expiration time and returns the remaining time on demand - so that a
fixed deadline is not shifted forward by the time spent between its
creation and its uses.

Example:
  tmo = abs_timeout.AbsTimeout(30)
  time_left = tmo.get()  # seconds remaining, shrinking over time
  if time_left == 0:
    deadline_expired()  # handle the expired 30s deadline
"""

import time


class AbsTimeout:
  """Timeout tracker with absolute expiration time.

  Computes expiration time on creation and returns remaining time
  when get() is called. Useful for passing to functions that accept
  timeout parameters.
  """

  def __init__(self, timeout, timefn=None):
    """Initialize the timeout.

    Args:
      timeout: The timeout duration, in seconds. If None, get() always
        returns None (no timeout).
      timefn: Optional time function (default: time.time). Useful for testing
        or when using monotonic clocks.
    """
    self._timefn = time.time if timefn is None else timefn
    self._expires = self._timefn() + timeout if timeout is not None else None

  def get(self):
    """Get remaining time until expiration.

    Returns:
      Remaining time in seconds (>= 0), or None if no timeout was set.
    """
    return max(0, self._expires - self._timefn()) if self._expires is not None else None
