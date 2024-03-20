import time


class AbsTimeout:

  def __init__(self, timeout, timefn=time.time):
    self._timefn = timefn
    self._expires = timefn() + timeout if timeout is not None else None

  def get(self):
    return max(0, self._expires - self._timefn()) if self._expires is not None else None

