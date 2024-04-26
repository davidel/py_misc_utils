from . import abs_timeout as abst
from . import timegen as tg


class CondWaiter:

  def __init__(self, timeout=None, timegen=None):
    self._timegen = tg.TimeGen() if timegen is None else timegen
    self._timeo = abst.AbsTimeout(timeout, timefn=self._timegen.now)

  def wait(self, cond):
    return self._timegen.wait(cond, timeout=self._timeo.get())

