import time


class AbsTimeout:

  def __init__(self, timeout):
    self._expires = time.time() + timeout if timeout is not None else None

  def get(self):
    return max(0, self._expires - time.time()) if self._expires is not None else None

