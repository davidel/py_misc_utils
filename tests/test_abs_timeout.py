"""Tests for the py_misc_utils.abs_timeout deadline helper.

Expectations follow the documented API contract: the expiration time is
computed at creation from the provided time function, get() returns the
remaining seconds (never negative) or None when no timeout was set, and
the injected timefn makes the clock fully deterministic.
"""

import time

import py_misc_utils.abs_timeout as pab


def test_get_returns_full_timeout_at_creation():
  """Right after creation the remaining time equals the timeout."""
  at = pab.AbsTimeout(10.0, timefn=lambda: 1000.0)

  assert at.get() == 10.0


def test_get_returns_none_without_timeout():
  """AbsTimeout(None) never expires and always reports None."""
  at = pab.AbsTimeout(None, timefn=lambda: 1000.0)

  assert at.get() is None


def test_get_decreases_with_elapsed_time():
  """get() subtracts the time reported by the injected clock."""
  clock = [1000.0]
  at = pab.AbsTimeout(10.0, timefn=lambda: clock[0])

  clock[0] += 4.0

  assert at.get() == 6.0

  clock[0] += 2.5

  assert at.get() == 3.5


def test_get_floors_at_zero_after_expiry():
  """get() clamps the remaining time to 0 once the deadline has passed."""
  clock = [1000.0]
  at = pab.AbsTimeout(5.0, timefn=lambda: clock[0])

  clock[0] += 100.0

  assert at.get() == 0
  assert at.get() >= 0


def test_get_with_default_time_clock_smoke():
  """With the default clock, the remaining time starts near the timeout."""
  at = pab.AbsTimeout(30.0)

  assert 29.0 <= at.get() <= 30.0


def test_real_clock_expiry_after_sleep():
  """A tiny real timeout expires after a short sleep."""
  at = pab.AbsTimeout(0.02)

  time.sleep(0.05)

  assert at.get() == 0
