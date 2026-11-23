"""Tests for py_misc_utils.throttle.

Timing-sensitive values are asserted with lower bounds only, all well above
the 50 ms floor; rate-limit debt behavior is probed via _wait_time() (a
white-box value check) instead of wall-clock races.
"""

import threading
import time

from py_misc_utils import throttle as thr


def test_disabled_limit():
  th = thr.Throttle(0)
  assert th._wait_time() == 0
  th2 = thr.Throttle(-5)
  assert th2._wait_time() == 0
  t0 = time.time()
  th.trigger()
  th.trigger()
  assert time.time() - t0 < 0.05


def test_first_trigger_immediate():
  th = thr.Throttle(10)
  t0 = time.time()
  th.trigger()
  assert time.time() - t0 < 0.05


def test_rate_enforced():
  th = thr.Throttle(10)  # 10 events/sec => 0.1s spacing
  t0 = time.time()
  th.trigger()
  th.trigger()
  th.trigger()
  elapsed = time.time() - t0
  # Two enforced spacings: >= 0.2s (with margin), not more than ~0.4s.
  assert elapsed >= 0.18
  assert elapsed < 0.5


def test_no_debt_after_full_pause():
  th = thr.Throttle(10)
  th.trigger()
  th.trigger()  # blocked 0.1s
  time.sleep(0.15)  # longer than one full period
  wt = th._wait_time()
  assert wt <= 0


def test_trigger_returns_lock():
  th = thr.Throttle(0)
  lock = th.trigger()
  assert lock is th._lock
  assert lock.acquire(timeout=0.01) is True
  lock.release()


def test_trigger_returns_unheld_lock():
  # The returned lock is not held by the trigger itself.
  th = thr.Throttle(0)
  lock = th.trigger()
  assert lock.acquire(blocking=False) is True
  lock.release()


def test_threads_serialized():
  th = thr.Throttle(10)
  order = []
  lock = threading.Lock()

  def worker(i):
    with th.trigger(), lock:
      order.append(i)

  ths = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
  t0 = time.time()
  for th_ in ths:
    th_.start()
  for th_ in ths:
    th_.join(timeout=5.0)
  elapsed = time.time() - t0
  assert sorted(order) == [0, 1, 2, 3]
  # 4 events at 10/sec => 3 spacings >= 0.3s.
  assert elapsed >= 0.28
  assert all(not th_.is_alive() for th_ in ths)
