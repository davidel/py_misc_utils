"""Tests for the py_misc_utils.executor thread pool.

Expectations follow the documented API contract: Task wraps a callable
capturing exceptions for later re-raise, AsyncResult blocks until set and
re-raises task exceptions, Queue gating keeps pending items deliverable
while stopped, and Executor scales workers between min/max bounds,
delivers results via futures and supports idle waits.
"""

import threading
import time

import pytest

import py_misc_utils.executor as xe


def _shutdown(ex, timeout=5.0):
  """Stop an executor and wait for it to become idle."""
  ex.shutdown()
  # After shutdown() all pending work is drained: wait_for_idle() returns
  # instead of blocking forever.
  assert ex.wait_for_idle(timeout=timeout) is True


def test_task_delivers_result_through_aresult():
  """Task.__call__ runs fn and sets the result on the aresult."""
  ar1, ar2 = xe.AsyncResult(), xe.AsyncResult()

  t = xe.Task(lambda x, y: x + y, args=(2, 3), aresult=ar1)
  assert t() is None
  assert ar1.wait(timeout=1) == 5

  t = xe.Task(lambda x: x * 2, kwargs={'x': 4}, aresult=ar2)
  t()
  assert ar2.wait(timeout=1) == 8


def test_task_captures_exception_into_aresult():
  """Task exceptions are wrapped and re-raised through the aresult."""
  aresult = xe.AsyncResult()
  boom = RuntimeError('task boom')

  def boom_fn():
    raise boom

  t = xe.Task(boom_fn, aresult=aresult)

  # __call__ itself never raises: the exception is captured and re-raised
  # by wait() (the original exception instance).
  assert t() is None
  with pytest.raises(RuntimeError) as info:
    aresult.wait()

  assert info.value is boom
  # The result stays faulted: a second wait() re-raises again.
  with pytest.raises(RuntimeError):
    aresult.wait()


def test_task_without_aresult_swallows_exception():
  """Without an aresult the wrapped error is discarded (logged only)."""
  t = xe.Task(lambda: 1 / 0)

  assert t() is None


def test_aresult_set_wait_roundtrip():
  """set() wakes waiters; wait() returns the result value."""
  ar = xe.AsyncResult()
  threading.Thread(target=lambda: (time.sleep(0.02), ar.set(42))).start()

  assert ar.wait() == 42


def test_aresult_unset_wait_returns_void_sentinel():
  """An unset AsyncResult surfaces the VOID sentinel on timeout.

  Report-only finding: the wait() docstring promises TimeoutError on
  expiry, but the code returns the VOID sentinel - callers cannot
  distinguish 'not yet available' from a stored None result.
  """
  ar = xe.AsyncResult()

  assert ar.wait(timeout=0.01) is xe.VOID


def test_queue_pending_delivery_while_stopped():
  """A stopped queue still returns pending items; stop gates the wait."""
  q = xe.Queue()

  assert len(q) == 0

  q.put('a')
  q.put('b')

  assert len(q) == 2

  q.stop()

  assert q.get() == 'a'
  assert q.get() == 'b'
  assert len(q) == 0
  # Stopped AND empty: get() returns None immediately.
  assert q.get() is None


def test_queue_stop_wakes_blocked_get():
  """stop() wakes a get() blocked on an empty queue."""
  q = xe.Queue()
  got = {}

  def getter():
    got['res'] = q.get()

  th = threading.Thread(target=getter)
  th.start()
  time.sleep(0.05)
  q.stop()
  th.join(timeout=5)

  assert got['res'] is None


def test_queue_start_resumes_blocking_delivery():
  """After start(), a get() on an empty queue blocks again (times out)."""
  q = xe.Queue()
  q.stop()
  q.start()

  assert q.get(timeout=0.05) is None


def test_executor_runs_tasks_and_reports_results():
  """Executor.submit_result futures resolve with the task results."""
  ex = xe.Executor(min_threads=1, max_threads=2, idle_timeout=0.2)

  try:
    ares = [ex.submit_result(lambda x: x * x, i) for i in range(5)]

    assert [ar.wait(timeout=5) for ar in ares] == [0, 1, 4, 9, 16]
  finally:
    _shutdown(ex)


def test_executor_caps_concurrency_at_max_threads():
  """In-flight task count never exceeds max_threads."""
  ex = xe.Executor(min_threads=1, max_threads=2, idle_timeout=0.2)
  state = {'inflight': 0, 'peak': 0}
  lock = threading.Lock()

  def tracked(dur):
    with lock:
      state['inflight'] += 1
      state['peak'] = max(state['peak'], state['inflight'])
    time.sleep(dur)
    with lock:
      state['inflight'] -= 1

  try:
    for _ in range(6):
      ex.submit(tracked, 0.05)

    assert ex.wait_for_idle(timeout=5)
    assert state['peak'] <= 2
  finally:
    _shutdown(ex)


def test_executor_future_re_raises_task_exception():
  """A task exception re-raises from the future's wait()."""
  ex = xe.Executor(min_threads=1, max_threads=2, idle_timeout=0.2)

  try:
    ar = ex.submit_result(lambda: 1 / 0)

    with pytest.raises(ZeroDivisionError):
      ar.wait(timeout=5)
  finally:
    _shutdown(ex)


def test_executor_wait_for_idle_true_when_empty():
  """wait_for_idle() on a task-free executor returns True quickly."""
  ex = xe.Executor(min_threads=1, max_threads=2, idle_timeout=0.2)

  try:
    assert ex.wait_for_idle(timeout=2)
  finally:
    _shutdown(ex)


def test_executor_wait_for_idle_false_with_pending_task():
  """wait_for_idle() returns False on timeout while a task blocks."""
  ex = xe.Executor(min_threads=1, max_threads=2, idle_timeout=0.2)
  release = threading.Event()

  try:
    ex.submit(release.wait, 5)
    time.sleep(0.1)

    assert not ex.wait_for_idle(timeout=0.2)
  finally:
    release.set()
    _shutdown(ex)


def test_common_executor_singleton_identity():
  """common_executor() returns the same instance across calls.

  The executor is a process-wide singleton (the scheduler uses it), so
  the test only observes the identity contract - it must never patch,
  replace or shut down the global instance.
  """
  assert xe.common_executor() is xe.common_executor()
