"""Tests for py_misc_utils.timegen."""

import threading
import time

from py_misc_utils import timegen as tg


def test_now_uses_wall_clock():
  t = tg.TimeGen()
  t0 = time.time()
  now = t.now()
  assert now - t0 < 1.0
  assert now - t0 > -1.0


def test_wait_notify():
  cond = threading.Condition()
  t = tg.TimeGen()
  result = []

  def notifier():
    with cond:
      time.sleep(0.1)
      cond.notify()

  th = threading.Thread(target=notifier)
  with cond:
    th.start()
    ok = t.wait(cond, timeout=2.0)
    result.append(ok)
    th.join()
  assert result == [True]


def test_wait_timeout():
  cond = threading.Condition()
  t = tg.TimeGen()
  with cond:
    ok = t.wait(cond, timeout=0.1)
  assert ok is False


def test_wait_timeout_none_blocks_until_notify():
  cond = threading.Condition()
  t = tg.TimeGen()
  ok = []

  def notifier():
    with cond:
      time.sleep(0.1)
      cond.notify()

  th = threading.Thread(target=notifier)
  with cond:
    th.start()
    ok.append(t.wait(cond, timeout=None))
    th.join()
  assert ok == [True]


def test_custom_timegen_drives_now_and_wait():

  class SimTimeGen(tg.TimeGen):

    def __init__(self):
      self.t = 1000.0
      self.wait_calls = []

    def now(self):
      return self.t

    def wait(self, cond, timeout=None):
      # Simulated clock: never actually blocks; advances time.
      self.wait_calls.append(timeout)
      self.t += 0.5
      return False

  stg = SimTimeGen()
  assert stg.now() == 1000.0
  cond = threading.Condition()
  with cond:
    ok = stg.wait(cond, timeout=3.0)
  assert ok is False
  assert stg.wait_calls == [3.0]
  assert stg.now() == 1000.5
