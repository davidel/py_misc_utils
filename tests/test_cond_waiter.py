"""Tests for py_misc_utils.cond_waiter.

The whole point of CondWaiter is that a sequence of wait() calls shares a
single overall timeout - verified here with a simulated TimeGen, so the
budget arithmetic is fully deterministic (no wall-clock races).
"""

import threading

from py_misc_utils import cond_waiter as cw


class SimTimeGen:
  """A TimeGen-like object with a simulated clock: waits never notify."""

  def __init__(self, step=0.4):
    self.t = 100.0
    self.step = step
    self.waits = []

  def now(self):
    return self.t

  def wait(self, cond, timeout=None):
    self.waits.append(timeout)
    self.t += self.step
    return False


class NotifyTimeGen(SimTimeGen):
  """A TimeGen-like object whose waits always notify."""

  def wait(self, cond, timeout=None):
    self.waits.append(timeout)
    self.t += self.step
    return True


def _cond():
  return threading.Condition()


def test_sequence_shares_budget():
  tg = SimTimeGen()
  w = cw.CondWaiter(timeout=1.0, timegen=tg)
  results = [w.wait(_cond()) for _ in range(3)]
  assert results == [False, False, False]


def test_budget_values():
  tg = SimTimeGen()
  w = cw.CondWaiter(timeout=1.0, timegen=tg)
  w.wait(_cond())
  w.wait(_cond())
  w.wait(_cond())
  # 1.0, then remaining 0.6, then remaining 0.2 - budget fully consumed.
  assert tg.waits[0] == 1.0
  assert abs(tg.waits[1] - 0.6) < 1e-6
  assert abs(tg.waits[2] - 0.2) < 1e-6


def test_notified_wait_consumes_budget():
  tg = NotifyTimeGen()
  w = cw.CondWaiter(timeout=1.0, timegen=tg)
  r1 = w.wait(_cond())
  r2 = w.wait(_cond())
  assert (r1, r2) == (True, True)
  # Both waits consumed the same shared budget.
  assert tg.waits[0] == 1.0
  assert tg.waits[1] < 1.0


def test_no_timeout_never_expires():
  tg = SimTimeGen()
  w = cw.CondWaiter(timeout=None, timegen=tg)
  for _ in range(5):
    assert w.wait(_cond()) is False
  # With timeout=None every wait passes None through.
  assert tg.waits == [None] * 5


def test_default_timegen_wall_clock_expiry():
  w = cw.CondWaiter(timeout=0.05)
  cond = _cond()
  with cond:
    assert w.wait(cond) is False
