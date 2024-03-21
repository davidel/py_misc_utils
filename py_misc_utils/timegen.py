import time

from . import alog


class TimeGen:

  def now(self):
    return time.time()

  def wait(self, cond, timeout=None):
    cond.wait(timeout=timeout)

  def set_time(self, current_time):
    alog.xraise(NotImplementedError, f'API not implemented: set_time()')

