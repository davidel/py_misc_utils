"""Exception-safe function execution utilities.

This module provides functions that wrap other functions to catch and handle
exceptions, returning the exception object instead of propagating it. The
logging module is used directly (rather than alog) to keep this a low-level
module with minimal local dependencies.
"""

# Using logging module directly here to avoid import the alog module, since this
# is supposed to be a lower level module with minimal/no local dependencies.
import logging
import traceback


def no_except(fn, *args, **kwargs):
  """Call fn with the given args, catching and returning any exception.

    Executes fn(*args, **kwargs). If an exception occurs, logs it at WARNING
    level and returns the exception object instead of propagating it.

  Args:
    fn: The callable to execute.
    *args: Positional arguments to pass to fn.
    **kwargs: Keyword arguments to pass to fn.

  Returns:
    The return value of fn on success, or the Exception object if an
    exception was raised.
  """
  try:
    return fn(*args, **kwargs)
  except Exception as ex:  # noqa: BLE001 -- API contract: return any exception raised by the wrapped callable
    # The logging.exception() API does emit in DEBUG level, and here we want something
    # a bit more higher level.
    tb = traceback.format_exc()
    logging.warning(f'Exception while running function: {ex}\n{tb}')

    return ex


def qno_except(fn, *args, **kwargs):
  """Call fn with the given args, quietly returning any exception.

  Like no_except() but without logging. If fn raises, the exception object
  is returned directly.

  Args:
    fn: The callable to execute.
    *args: Positional arguments to pass to fn.
    **kwargs: Keyword arguments to pass to fn.

  Returns:
    The return value of fn on success, or the Exception object if an
    exception was raised.
  """
  try:
    return fn(*args, **kwargs)
  except Exception as ex:  # noqa: BLE001 -- API contract: return any exception raised by the wrapped callable
    return ex


def xwrap_fn(fn, *args, **kwargs):
  """Wrap fn so it can be called later with no arguments, catching exceptions.

  Returns a zero-argument callable that, when invoked, calls no_except(fn,
  *args, **kwargs). The returned callable never raises; it returns either
  fn's result or the exception object.

  Args:
    fn: The callable to wrap.
    *args: Positional arguments to pass to fn.
    **kwargs: Keyword arguments to pass to fn.

  Returns:
    A zero-argument callable fwrap such that fwrap() == no_except(fn,
    *args, **kwargs).
  """

  def fwrap():
    """Call the wrapped function, catching its exceptions.

    Returns:
      The result of fn, or the raised exception object.
    """
    return no_except(fn, *args, **kwargs)

  return fwrap
