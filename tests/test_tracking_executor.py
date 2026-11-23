"""Tests for the py_misc_utils.tracking_executor wrapper.

Expectations follow the documented API contract: every submitted task
gets a unique id and is tracked as pending until its completion is
reported, wait() blocks until the tracked set drains (or times out),
submit failures are untracked before propagating, the wrapper never
shuts down the underlying executor on its own, and wait_for_idle()
combines the two idle conditions.
"""

import threading
import time

import pytest

import py_misc_utils.executor as xe
import py_misc_utils.tracking_executor as pte


def _shutdown(ex, timeout=5.0):
  """Stop an executor and wait for its workers to exit."""
  ex.shutdown()
  deadline = time.time() + timeout
  while ex._workers and time.time() < deadline:
    time.sleep(0.01)


def test_submit_assigns_unique_increasing_ids():
  """submit() returns distinct, increasing task ids."""
  ex = xe.Executor(min_threads=1, max_threads=2, idle_timeout=0.2)
  tex = pte.TrackingExecutor(ex)

  try:
    tids = [tex.submit(lambda v: v, i) for i in range(4)]

    assert tids == [0, 1, 2, 3]
  finally:
    tex.wait()
    _shutdown(ex)


def test_wait_blocks_until_all_submitted_complete():
  """wait() without tids waits for the whole pending set to drain."""
  ex = xe.Executor(min_threads=1, max_threads=2, idle_timeout=0.2)
  tex = pte.TrackingExecutor(ex)

  try:
    for i in range(8):
      tex.submit(time.sleep, 0.02)

    assert tex.wait(timeout=5)
    assert tex._pending == set()
  finally:
    _shutdown(ex)


def test_wait_specific_tid():
  """wait(tids=...) waits only for the given tasks."""
  ex = xe.Executor(min_threads=1, max_threads=2, idle_timeout=0.2)
  tex = pte.TrackingExecutor(ex)
  done = {}

  def slow(tag, dur):
    time.sleep(dur)
    done[tag] = True

  try:
    tid_fast = tex.submit(slow, 'fast', 0.01)
    tex.submit(slow, 'slow', 0.4)

    # Only the fast task is awaited.
    assert tex.wait(tids=[tid_fast], timeout=5)
    assert 'fast' in done

    assert tex.wait(timeout=5)
    assert 'slow' in done
  finally:
    _shutdown(ex)


def test_submit_result_delivers_values_and_exceptions():
  """submit_result futures expose both values and task exceptions."""
  ex = xe.Executor(min_threads=1, max_threads=2, idle_timeout=0.2)
  tex = pte.TrackingExecutor(ex)

  try:
    ar_ok = tex.submit_result(lambda v: v + 1, 10)
    ar_err = tex.submit_result(lambda: 1 / 0)

    assert ar_ok.wait(timeout=5) == 11
    with pytest.raises(ZeroDivisionError):
      ar_err.wait(timeout=5)
  finally:
    tex.wait()
    _shutdown(ex)


def test_submit_failure_untracks_task():
  """When the underlying executor rejects a task it is untracked."""
  tex = pte.TrackingExecutor()

  class _FailingExecutor:

    def submit(self, *args, **kwargs):
      raise RuntimeError('no room')

    def submit_result(self, *args, **kwargs):
      raise RuntimeError('no room')

  tex.executor = _FailingExecutor()

  with pytest.raises(RuntimeError):
    tex.submit(lambda: None)

  with pytest.raises(RuntimeError):
    tex.submit_result(lambda: None)

  # The failed submissions must not stay pending.
  assert tex._pending == set()


def test_wait_for_idle_combines_tracked_and_executor():
  """wait_for_idle() waits for both pending tasks and idle workers."""
  ex = xe.Executor(min_threads=1, max_threads=2, idle_timeout=0.2)
  tex = pte.TrackingExecutor(ex)

  try:
    tex.submit(time.sleep, 0.05)

    # Pending tasks: not idle yet.
    assert not tex.wait_for_idle(timeout=0.01)
    assert tex.wait_for_idle(timeout=5)
  finally:
    _shutdown(ex)


def test_wrap_task_reports_done_after_completion():
  """The wrapped task removes its id from the pending set on completion."""
  tex = pte.TrackingExecutor()
  done = threading.Event()
  original = tex._report_done

  def spy_report(tid):
    original(tid)
    done.set()

  tex._report_done = spy_report

  wfn, tid = tex._wrap(lambda v: v * 3, 7)

  assert tex._pending == {tid}
  assert wfn() == 21
  assert done.wait(timeout=5)
  assert tid not in tex._pending
