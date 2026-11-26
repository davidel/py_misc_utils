"""Tests for py_misc_utils.run_once.

Pins the observed at-most-once contract: the first call executes and
its result is returned to the caller, but the result is NOT cached -
every subsequent call returns None without re-executing the function,
even with different arguments. The module docstring promises caching,
but the wrapper has no result slot; the wrapper docstring matches the
actual behavior ("on subsequent calls it returns None").
"""

import threading

import pytest

from py_misc_utils import run_once as ro


def test_first_call_executes_and_returns_result():
  calls = []

  @ro.run_once
  def fn():
    calls.append(1)

    return 'result'

  assert fn() == 'result'
  assert len(calls) == 1


def test_subsequent_calls_return_none_without_reexecuting():
  calls = []

  @ro.run_once
  def fn():
    calls.append(1)

    return 'result'

  assert fn() == 'result'
  assert fn() is None
  assert fn() is None

  assert len(calls) == 1


def test_later_calls_ignore_arguments():
  seen = []

  @ro.run_once
  def fn(a, b=0):
    seen.append((a, b))

    return a + b

  assert fn(1, b=2) == 3
  # Different arguments do not re-execute the function.
  assert fn(100, b=200) is None
  assert fn(0) is None

  assert seen == [(1, 2)]


def test_first_call_exception_propagates_and_is_not_retried():
  calls = []

  @ro.run_once
  def boom():
    calls.append('b')

    raise ValueError('first call fails')

  with pytest.raises(ValueError):
    boom()

  # The exception is not cached, but the function is marked as run:
  # a second call returns None instead of re-executing.
  assert boom() is None
  assert len(calls) == 1


def test_thread_safety_single_execution():
  calls = []
  lock = threading.Lock()

  @ro.run_once
  def fn():
    with lock:
      calls.append(1)

    return 42

  results = []
  rlock = threading.Lock()
  barrier = threading.Barrier(32)

  def worker():
    barrier.wait()

    r = fn()

    with rlock:
      results.append(r)

  threads = [threading.Thread(target=worker) for _ in range(32)]
  for t in threads:
    t.start()
  for t in threads:
    t.join()

  assert len(calls) == 1
  # The wrapper does not cache the result: exactly one worker (the one
  # which executed the body) receives the return value, the others get
  # None - all without re-executing the function.
  assert results.count(42) == 1
  assert results.count(None) == 31
