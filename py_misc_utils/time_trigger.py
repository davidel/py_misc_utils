"""Periodic time-based triggering.

This module provides TimeTrigger, an object which evaluates to True -
exactly once - every given time interval, so that periodic actions can be
implemented with a simple boolean test in a loop.
"""
import time


class TimeTrigger:
  """A periodic boolean trigger.

  Converting a TimeTrigger to a boolean evaluates whether the time since
  the last True conversion exceeds the configured interval: if so, the
  next trigger time is moved ahead by one interval and True is returned,
  otherwise False is returned.

  Note that the trigger times are computed by adding intervals to the
  previous trigger time, so a delayed evaluation does not accumulate
  drift of the trigger rate.

  Example:
    trig = time_trigger.TimeTrigger(interval=1.0)
    for _ in range(3):
      if trig:
        print('one second elapsed')  # at most once per second
  """

  def __init__(self, interval):
    """Creates a time trigger.

    Args:
      interval: The trigger interval, in seconds. Zero - or a negative
        value - makes the trigger never fire.
    """
    self._interval = interval
    self.next = time.time() + interval

  def __bool__(self):
    """Checks whether the trigger interval has elapsed.

    Returns:
      True exactly once per configured interval, False otherwise.
    """
    if self._interval > 0 and (now := time.time()) >= self.next:
      self.next = now + self._interval

      return True

    return False
