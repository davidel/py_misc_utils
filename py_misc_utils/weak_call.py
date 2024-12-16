import functools
import weakref


class _Gone:
  pass

GONE = _Gone()


class WeakMethod:

  def __init__(self, fn):
    if isinstance(fn, weakref.WeakMethod):
      self._fn = fn
    else:
      if hasattr(fn, '__self__'):
        self._fn = weakref.WeakMethod(fn)
      else:
        self._fn = lambda: fn

  def __call__(self):
    return self._fn()


def weak_caller(wmeth, *args, **kwargs):
  fn = wmeth()

  return fn(*args, **kwargs) if fn is not None else GONE


def weak_call(fn, *args, **kwargs):
  wmeth = WeakMethod(fn)

  return functools.partial(weak_caller, wmeth, *args, **kwargs)

