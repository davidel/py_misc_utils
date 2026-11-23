"""Decorator to ensure a function executes only once.

This module provides the run_once decorator which guarantees that the decorated
function is called at most once, with thread-safe synchronization. Subsequent
calls return the result from the first invocation.
"""

import functools
import threading


def run_once(fn):
  """Decorator that ensures the decorated function runs at most once.

  The first call to the wrapped function executes normally and caches its
  return value. All subsequent calls return the cached result without
  re-invoking the function. Thread-safe via a per-wrapper lock.

  Args:
    fn: The function to wrap. Its metadata is preserved via functools.wraps.

  Returns:
    A wrapper function that, when called, executes fn at most once. On the first
    call it returns fn's result; on subsequent calls it returns None. The
    wrapper exposes two attributes: _lock (a threading.Lock) and _has_run (a
    bool indicating whether fn has been called).
  """

  @functools.wraps(fn)
  def wrapper(*args, **kwargs):
    """Wrapper that invokes fn at most once and caches the result.

    Args:
      *args: Positional arguments passed to fn on first call.
      **kwargs: Keyword arguments passed to fn on first call.

    Returns:
      The return value of fn from the first invocation, cached for all
      subsequent calls.
    """
    with wrapper._lock:
      if not wrapper._has_run:
        wrapper._has_run = True

        return fn(*args, **kwargs)

  wrapper._lock = threading.Lock()
  wrapper._has_run = False

  return wrapper
