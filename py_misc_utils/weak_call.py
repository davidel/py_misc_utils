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

  def __call__(self, *args, **kwargs):
    fn = self._fn()
    if fn is not None:
      cargs = self._args + args
      ckwargs = self._kwargs.copy()
      ckwargs.update(kwargs)

      return fn(*cargs, **ckwargs)
    else:
      return GONE

