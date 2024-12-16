import weakref


class _Gone:
  pass

GONE = _Gone()


class WeakCall:

  def __init__(self, fn, *args, **kwargs):
    if isinstance(fn, weakref.WeakMethod):
      self._fn = fn
    else:
      if hasattr(fn, '__self__'):
        self._fn = weakref.WeakMethod(fn)
      else:
        self._fn = lambda: fn

    self._args = args
    self._kwargs = kwargs

  def __call__(self):
    fn = self._fn()

    return fn(*self._args, **self._kwargs) if fn is not None else GONE

