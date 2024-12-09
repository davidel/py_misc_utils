import weakref


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

