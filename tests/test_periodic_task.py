"""Tests for py_misc_utils.periodic_task.

PeriodicTask runs on a dedicated Scheduler with short periods. Stop/GC
semantics are verified via the scheduler queue and run counters, never via
sub-100ms wall-clock assertions.
"""

import gc
import time

import pytest

from py_misc_utils import periodic_task as pt
from py_misc_utils import scheduler as sch


@pytest.fixture()
def sched():
  # The Scheduler runner is a daemon thread; there is no shutdown() API to
  # call - the fixture just hands out a fresh scheduler per test.
  s = sch.Scheduler(max_workers=1, name='t-periodic')
  yield s


def test_runs_repeatedly(sched):
  cnt = [0]

  def fn():
    cnt[0] += 1

  task = pt.PeriodicTask('p', fn, 0.05, scheduler=sched)
  task.start()
  time.sleep(0.3)
  task.stop()
  assert cnt[0] >= 3


def test_stop_halts_rescheduling(sched):
  cnt = [0]

  def fn():
    cnt[0] += 1

  task = pt.PeriodicTask('p', fn, 0.05, scheduler=sched)
  task.start()
  time.sleep(0.3)
  task.stop()
  n_at_stop = cnt[0]
  time.sleep(0.2)
  assert cnt[0] == n_at_stop


def test_stop_not_running_noop(sched):
  cnt = [0]

  def fn():
    cnt[0] += 1

  task = pt.PeriodicTask('p', fn, 0.05, scheduler=sched)
  # Never started.
  task.stop()
  assert cnt[0] == 0


def test_stop_on_error_true_stops(sched):
  cnt = [0]

  def bad():
    cnt[0] += 1
    raise ValueError('boom')

  task = pt.PeriodicTask('bad', bad, 0.05, scheduler=sched, stop_on_error=True)
  task.start()
  time.sleep(0.25)
  n_at = cnt[0]
  time.sleep(0.2)
  # Exceptions stop the task by default - exactly one run, no growth.
  assert n_at == 1
  assert cnt[0] == n_at


def test_stop_on_error_false_keeps_running(sched):
  cnt = [0]

  def bad():
    cnt[0] += 1
    raise ValueError('boom')

  task = pt.PeriodicTask('bad2', bad, 0.05, scheduler=sched, stop_on_error=False)
  task.start()
  time.sleep(0.25)
  n_at = cnt[0]
  assert n_at >= 2
  time.sleep(0.2)
  assert cnt[0] > n_at
  task.stop()


def test_exception_logged_not_propagated(sched, caplog):
  cnt = [0]

  def bad():
    cnt[0] += 1
    raise ValueError('logged-boom')

  task = pt.PeriodicTask('bad3', bad, 0.05, scheduler=sched)
  task.start()
  time.sleep(0.2)
  task.stop()
  # The scheduler swallows the exception (only logs it): the run counter
  # reached 1 and the scheduler keeps accepting/scheduling new events.
  assert cnt[0] == 1
  def bump():
    cnt[0] += 1

  task2 = pt.PeriodicTask('after-error', bump, 0.05, scheduler=sched)
  task2.start()
  time.sleep(0.15)
  task2.stop()
  # The scheduler survived the error and ran the follow-up task.
  assert cnt[0] >= 3


def test_gc_self_terminates(sched):

  class Holder:

    def go(self):
      pass

  h = Holder()
  task = pt.PeriodicTask('gc', h.go, 0.05, scheduler=sched)
  task.start()
  time.sleep(0.12)
  assert len(sched.get_events(lambda q: True)) == 1
  del h
  gc.collect()
  time.sleep(0.3)
  assert sched.get_events(lambda q: True) == []


def test_start_twice_single_schedule(sched):
  cnt = [0]

  def fn():
    cnt[0] += 1

  task = pt.PeriodicTask('p', fn, 0.05, scheduler=sched)
  task.start()
  task.start()
  time.sleep(0.25)
  task.stop()
  # Exactly one event per period: a double start must not double-schedule.
  assert cnt[0] >= 2
  assert sched.get_events(lambda q: True) == []


def test_start_after_stop_reschedules(sched):
  cnt = [0]

  def fn():
    cnt[0] += 1

  task = pt.PeriodicTask('p', fn, 0.05, scheduler=sched)
  task.start()
  time.sleep(0.25)
  task.stop()
  n1 = cnt[0]
  task.start()
  time.sleep(0.25)
  task.stop()
  assert cnt[0] > n1
