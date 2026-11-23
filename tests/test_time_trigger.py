"""Tests for py_misc_utils.time_trigger."""

import time

from py_misc_utils import time_trigger as tt


def test_first_bool_false():
  t = tt.TimeTrigger(interval=0.2)
  assert bool(t) is False


def test_fires_once_per_interval():
  t = tt.TimeTrigger(interval=0.2)
  fires = []
  t0 = time.time()
  while time.time() - t0 < 1.0:
    if bool(t):
      fires.append(time.time())
    time.sleep(0.01)
  # Roughly one fire per 0.2s over the 1s window - 4 to 6 fires.
  assert 3 <= len(fires) <= 6
  for i in range(len(fires) - 1):
    assert fires[i + 1] - fires[i] >= 0.15


def test_interval_zero_never_fires():
  t = tt.TimeTrigger(interval=0)
  for _ in range(3):
    assert bool(t) is False


def test_interval_negative_never_fires():
  # Documented: "a negative value makes the trigger never fire".
  t = tt.TimeTrigger(interval=-1)
  for _ in range(3):
    assert bool(t) is False


def test_catch_up_single_fire():
  t = tt.TimeTrigger(interval=0.15)
  time.sleep(0.35)  # more than two intervals elapse
  fires = sum(bool(t) for _ in range(5))
  # Exactly one True - no burst of accumulated fires.
  assert fires == 1
  # Next fire is one full interval after now (not stacked in the past).
  assert t.next > time.time()
