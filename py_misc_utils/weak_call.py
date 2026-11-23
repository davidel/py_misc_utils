"""Weakly-referenced callables.

This module provides WeakCall, a callable which holds a weak reference to
its target function - typically a bound method of an object - so that
holding a WeakCall does not prevent the target object from being garbage
collected.
"""
import weakref


class _Gone:
  """Sentinel type returned by WeakCall when its target is gone."""
  pass


GONE = _Gone()


class WeakCall:
  """A callable holding a weak reference to a function or bound method.

  Calling a WeakCall invokes its target function with the arguments - both
  positional and keyword - given at construction time, plus the ones given
  at call time. Bound methods whose object has been garbage collected are
  not invoked: the GONE sentinel is returned instead, so that callers can
  detect the situation.
  """

  def __init__(self, fn, *args, **kwargs):
    """Creates a weak call.

    Args:
      fn: The target function - or weak reference to it. Bound methods are
        held via weakref.WeakMethod, plain functions via a lambda closure.
      *args: Positional arguments to be passed to the target function.
      **kwargs: Keyword arguments to be passed to the target function.
    """
    if isinstance(fn, weakref.WeakMethod):
      self._fn = fn
    elif hasattr(fn, '__self__'):
      self._fn = weakref.WeakMethod(fn)
    else:
      self._fn = lambda: fn

    self._args = args
    self._kwargs = kwargs

  def __call__(self, *args, **kwargs):
    """Invokes the target function - if still alive.

    Args:
      *args: Positional arguments appended to the ones given at
        construction time.
      **kwargs: Keyword arguments merged over the ones given at
        construction time.

    Returns:
      The result of the target function call, or the GONE sentinel if the
      target bound method object has been garbage collected.
    """
    fn = self._fn()
    if fn is not None:
      cargs = self._args + args
      ckwargs = self._kwargs.copy()
      ckwargs.update(kwargs)

      return fn(*cargs, **ckwargs)
    else:
      return GONE
