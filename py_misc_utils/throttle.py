"""Events rate limiting.

This module provides Throttle, an object which limits the rate at which
events - like external API calls - are triggered, by delaying the events
which exceed a given per-second limit.
"""
import threading
import time


class Throttle:
  """A rate limiter.

  A Throttle ensures that trigger() calls are spaced by at least the
  period corresponding to the configured events-per-second limit, blocking
  the caller - if needed - until the next event can be triggered.

  The object is thread-safe: the events spacing is computed over the
  trigger() calls made by all the threads.

  Example:
    throttle = throttle.Throttle(2)  # at most 2 events per second
    with throttle.trigger():
      pass  # perform_event() here - spaced by >= 0.5s from the previous one
  """

  def __init__(self, xsec_limit):
    """Creates a rate limiter.

    Args:
      xsec_limit: The maximum number of events per second. Non-positive
        values disable the rate limiting.
    """
    self._secsx = 1.0 / xsec_limit if xsec_limit > 0 else None
    self._last = None
    self._lock = threading.Lock()

  def _wait_time(self):
    """Returns the time to wait before the next event can be triggered."""
    if self._secsx is None:
      return 0
    with self._lock:
      now = time.time()
      if self._last is None:
        self._last = now - self._secsx
      horizon = self._last + self._secsx
      self._last = max(horizon, now)

      return horizon - now

  def trigger(self):
    """Triggers an event, delaying it if the rate limit would be exceeded.

    Blocks the calling thread until the event can be triggered without
    exceeding the configured events-per-second limit.

    Returns:
      The throttle lock, held for the duration of the event - allowing
      callers to serialize the triggered events.
    """
    wt = self._wait_time()
    if wt > 0:
      time.sleep(wt)

    return self._lock
