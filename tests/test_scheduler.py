"""Tests for py_misc_utils.scheduler."""

import time

from py_misc_utils import scheduler as sch


def _wait_for(cond, timeout=2.0):
  """Polls cond() until true or timeout."""
  t0 = time.time()
  while not cond() and time.time() - t0 < timeout:
    time.sleep(0.01)


def test_past_event_fires_immediately():
  s = sch.Scheduler(max_workers=1, name='t-sched1')
  got = []
  s.enterabs(time.time() - 10.0, lambda: got.append(1))
  _wait_for(lambda: got)
  assert got == [1]


def test_enter_delay_and_args():
  s = sch.Scheduler(max_workers=1, name='t-sched2')
  got = []
  s.enter(0.05, lambda a, b: got.append(a + b), argument=(2,), kwargs={'b': 3})
  _wait_for(lambda: got)
  assert got == [5]


def test_events_fire_in_time_order():
  s = sch.Scheduler(max_workers=1, name='t-sched3')
  got = []
  s.enterabs(time.time() + 0.15, lambda: got.append('late'))
  s.enter(0.05, lambda: got.append('early'))
  _wait_for(lambda: len(got) >= 2)
  assert got == ['early', 'late']


def test_same_time_events_in_sequence_order():
  s = sch.Scheduler(max_workers=1, name='t-sched4')
  got = []
  ts = time.time() + 0.1
  s.enterabs(ts, lambda: got.append('first'))
  s.enterabs(ts, lambda: got.append('second'))
  _wait_for(lambda: len(got) >= 2)
  assert got == ['first', 'second']


def test_cancel_single_event():
  s = sch.Scheduler(max_workers=1, name='t-sched5')
  got = []
  ev = s.enter(0.05, lambda: got.append('ran'))
  cancelled = s.cancel(ev)
  assert cancelled == [ev]
  time.sleep(0.2)
  assert got == []


def test_cancel_list_of_events():
  s = sch.Scheduler(max_workers=1, name='t-sched6')
  got = []
  e1 = s.enter(10.0, lambda: got.append(1))
  e2 = s.enter(10.0, lambda: got.append(2))
  e3 = s.enter(10.0, lambda: got.append(3))
  cancelled = s.cancel([e1, e3])
  assert {id(e) for e in cancelled} == {id(e1), id(e3)}
  assert s.get_events(lambda q: True) == [e2]
  # e2 is 10s out: nothing fires within the test window.
  time.sleep(0.2)
  assert got == []


def test_cancel_already_run_event_returns_empty():
  s = sch.Scheduler(max_workers=1, name='t-sched7')
  got = []
  ev = s.enter(0.05, lambda: got.append(1))
  _wait_for(lambda: got)
  assert got == [1]
  assert s.cancel(ev) == []


def test_ref_cancel_single():
  s = sch.Scheduler(max_workers=1, name='t-sched8')
  s.enter(10.0, lambda: None, ref='ra')
  s.enter(10.0, lambda: None, ref='rb')
  cancelled = s.ref_cancel('ra')
  assert len(cancelled) == 1
  assert cancelled[0].ref == 'ra'
  assert [e.ref for e in s.get_events(lambda q: True)] == ['rb']


def test_ref_cancel_list():
  s = sch.Scheduler(max_workers=1, name='t-sched9')
  s.enter(10.0, lambda: None, ref='r1')
  s.enter(10.0, lambda: None, ref='r2')
  s.enter(10.0, lambda: None, ref='r3')
  cancelled = s.ref_cancel(['r1', 'r3', 'not-present'])
  assert sorted([e.ref for e in cancelled]) == ['r1', 'r3']
  assert [e.ref for e in s.get_events(lambda q: True)] == ['r2']


def test_get_events_returns_queued_only():
  s = sch.Scheduler(max_workers=1, name='t-sched10')
  got = []
  ev = s.enter(0.05, lambda: got.append(1))
  ev2 = s.enter(10.0, lambda: None)
  assert s.get_events(lambda q: True) == [ev, ev2]
  _wait_for(lambda: got)
  assert got == [1]
  assert s.get_events(lambda q: True) == [ev2]


def test_gen_unique_ref():
  s = sch.Scheduler(max_workers=1, name='t-sched11')
  r1 = s.gen_unique_ref()
  r2 = s.gen_unique_ref()
  assert isinstance(r1, str) and r1
  assert r1 != r2


def test_common_scheduler_singleton():
  s1 = sch.common_scheduler()
  s2 = sch.common_scheduler()
  assert s1 is s2


def test_cancelled_event_does_not_run():
  # Regression: cancel(single Event) used to silently do nothing because
  # Event is a namedtuple - hence a tuple - and was treated as a list of
  # events. A cancelled event must not run.
  s = sch.Scheduler(max_workers=1, name='t-sched12')
  got = []
  ev = s.enter(0.05, lambda: got.append('ran'))
  assert s.cancel(ev) == [ev]
  time.sleep(0.25)
  assert got == []
  assert s.get_events(lambda q: True) == []
