import time


class TimeGen:

  def now(self):
    return time.time()

  def wait(self, cond, timeout=None):
    return cond.wait(timeout=timeout)

