"""Tests for py_misc_utils.multi_wait."""

import threading
import time

from py_misc_utils import multi_wait as mw


def test_signal_releases_waiter():
  m = mw.MultiWait(1)
  assert m.wait(timeout=1.0) is False  # partial: timeout expiry
  m.signal()
  assert m.wait() is True


def test_signal_caps_at_count():
  m = mw.MultiWait(2)
  m.signal(5)
  # Capped at 2 - no error, barrier reached.
  assert m.wait() is True


def test_wait_partial_timeout():
  m = mw.MultiWait(3)
  m.signal()
  m.signal()
  t0 = time.time()
  assert m.wait(timeout=0.05) is False
  assert time.time() - t0 >= 0.04


def test_wait_released_by_third_signal():
  m = mw.MultiWait(3)
  m.signal()
  m.signal()
  assert m.wait(timeout=0.05) is False
  m.signal()
  assert m.wait() is True


def test_concurrent_waiters_all_released():
  m = mw.MultiWait(2)
  released = []

  def waiter(i):
    ok = m.wait(timeout=2.0)
    released.append((i, ok))

  ths = [threading.Thread(target=waiter, args=(i,)) for i in range(3)]
  for th in ths:
    th.start()
  time.sleep(0.1)
  m.signal(2)
  for th in ths:
    th.join(timeout=3.0)
  assert sorted(released) == [(0, True), (1, True), (2, True)]
  assert all(not th.is_alive() for th in ths)


def test_already_reached_wait_immediate():
  m = mw.MultiWait(2)
  m.signal()
  m.signal()
  t0 = time.time()
  assert m.wait() is True
  assert time.time() - t0 < 0.05


def test_signal_zero_is_noop():
  m = mw.MultiWait(1)
  m.signal(0)
  assert m.wait(timeout=0.05) is False
  m.signal()
  assert m.wait() is True
