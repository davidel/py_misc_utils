"""Tests for the py_misc_utils.tracking_executor wrapper.

Expectations follow the documented API contract: every submitted task
gets a unique id and is tracked as pending until its completion is
reported, wait() blocks until the tracked set drains (or times out),
submit failures are untracked before propagating, the wrapper never
shuts down the underlying executor on its own, and wait_for_idle()
combines the two idle conditions.
"""

import time

import pytest

import py_misc_utils.executor as xe
import py_misc_utils.tracking_executor as pte


def _shutdown(ex, timeout=5.0):
  """Stop an executor and wait for it to become idle."""
  ex.shutdown()
  # After shutdown() all pending work is drained: wait_for_idle() returns
  # instead of blocking forever.
  assert ex.wait_for_idle(timeout=timeout) is True


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

    # After the wait() the whole tracked set has drained: a fresh wait with
    # a zero timeout returns immediately (nothing pending).
    assert tex.wait(timeout=5)
    assert tex.wait(timeout=0)
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

  # The failed submissions must not stay pending: with nothing tracked the
  # tracked-set wait returns immediately. Restore a working executor and
  # verify a fresh task still tracks and completes.
  assert tex.wait() is True
  tex.executor = xe.Executor(min_threads=1, max_threads=1, idle_timeout=0.2)
  try:
    tid = tex.submit(lambda: None)
    assert tex.wait(tids=[tid], timeout=5) is True
  finally:
    tex.executor.shutdown()


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


def test_task_completes_untracked_and_result_resolves():
  """A submitted task completes and its result future resolves.

  Completion removes the task from the tracked set: wait() on the task id
  returns once the task has run (tracked-until-done semantics pinned via
  the public wait()/submit_result() APIs).
  """
  tex = pte.TrackingExecutor()

  tid = tex.submit(lambda v: v * 3, 7)

  # wait() on the specific task id blocks until the task completed.
  assert tex.wait(tids=[tid], timeout=5) is True

  ares = tex.submit_result(lambda v: v + 1, 1)
  assert ares.wait(timeout=5) == 2
  assert tex.wait(timeout=5) is True
