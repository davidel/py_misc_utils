import time


class AbsTimeout:

  def __init__(self, timeout):
    self.expires = time.time() + timeout if timeout is not None else None

  def get(self):
    return max(0, self.expires - time.time()) if self.expires is not None else None

