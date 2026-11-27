"""Call limiting utilities for controlling function invocation frequency.

This module provides functions to limit how many times a call site can invoke
a function, based on the caller's filename and line number. Useful for rate
limiting debug output or expensive operations called from specific locations.

The call site is identified by the (filename, line number) of the first
stack frame outside the given file - so each distinct source line that
invokes limit_call() gets its own independent counter.

Example:
  from py_misc_utils import call_limiter

  outs = []

  # The next call site runs at most once - further calls return None.
  for _ in range(3):
    if call_limiter.limit_call(1, lambda: outs.append('x')) is not None:
      pass

  assert outs == ['x']
"""

import threading

from . import traceback as tb

_LOCK = threading.Lock()
# Global lock protecting access to the _TB call-count dictionary.

_TB = dict()
# Dictionary mapping (filename, line_number) tuples to invocation counts.


def trigger(filename, count):
  """Determine whether a call at the given location should be allowed.

  Looks up the caller's frame after the given filename, tracks how many times
  that call site (filename, line number) has been triggered, and returns True
  if the count is below the given threshold.

  Args:
    filename: The source filename to search for in the call stack.
    count: The maximum number of allowed triggers before returning False.

  Returns:
    True if the call site has been triggered fewer than `count` times (or if
    no relevant frame is found), False otherwise.
  """
  frame = tb.get_frame_after(filename)
  if frame is not None:
    site = frame.f_code.co_filename, frame.f_lineno
    with _LOCK:
      c = _TB.get(site, 0)
      _TB[site] = c + 1

      return count > c

  return True


def limit_call(count, fn, *args, _filename=None, **kwargs):
  """Call fn only if the call site has not exceeded the trigger count.

  Uses trigger() to check whether the call site (defaulting to this file) is
  within its allowed invocation limit. If so, calls fn with the given args
  and returns its result. Otherwise returns None.

  Args:
    count: Maximum number of allowed invocations for this call site.
    fn: The function to conditionally call.
    *args: Positional arguments passed to fn.
    _filename: Optional filename override for the call site check. Defaults
      to the current module's __file__.
    **kwargs: Keyword arguments passed to fn.

  Returns:
    The return value of fn if the call is within the limit, otherwise None.
  """
  if trigger(_filename or __file__, count):
    return fn(*args, **kwargs)
