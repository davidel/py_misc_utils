"""Decorator to ensure a function executes only once.

This module provides the run_once decorator which guarantees that the decorated
function is called at most once, with thread-safe synchronization. Subsequent
calls return None: the first invocation's result is NOT cached.

Example:
  @run_once.run_once
  def setup():
    print('initialized')

  setup()  # executes setup(), returns None
  setup()  # returns None again - setup() is not executed again
"""

import functools
import threading


def run_once(fn):
  """Decorator that ensures the decorated function runs at most once.

  The first call executes fn and returns its result; every subsequent
  call returns None WITHOUT re-invoking fn - the return value is not
  cached. Thread-safe: in concurrent calls, exactly one caller executes
  fn and gets its result, all the others get None.

  Args:
    fn: The function to wrap. Its metadata is preserved via functools.wraps.

  Returns:
    A wrapper function that, when called, executes fn at most once. On the first
    call it returns fn's result; on subsequent calls it returns None. The
    wrapper exposes two attributes: _lock (a threading.Lock) and _has_run (a
    bool indicating whether fn has been called).

  Example:
    @run_once.run_once
    def setup():
      print('initialized')

    setup()  # executes setup(), returns None
    setup()  # returns None again - setup() is not executed again
  """

  @functools.wraps(fn)
  def wrapper(*args, **kwargs):
    """Wrapper that invokes fn at most once.

    Args:
      *args: Positional arguments passed to fn on first call.
      **kwargs: Keyword arguments passed to fn on first call.

    Returns:
      The return value of fn on the first invocation, None on subsequent
      ones.
    """
    with wrapper._lock:
      if not wrapper._has_run:
        wrapper._has_run = True

        return fn(*args, **kwargs)

  wrapper._lock = threading.Lock()
  wrapper._has_run = False

  return wrapper
