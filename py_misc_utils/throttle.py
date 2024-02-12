import threading
import time


class Throttle(object):
  
  def __init__(self, xsec_limit):
    self._secsx = 1.0 / xsec_limit if xsec_limit > 0 else None
    self._last = None
    self._lock = threading.Lock()

  def _wait_time(self):
    if self._secsx is None:
      return 0
    with self._lock:
      now = time.time()
      if self._last is None:
        self._last = now - self._secsx
      horizon = self._last + self._secsx
      self._last = max(horizon, now)

      return horizon - now

  def trigger(self):
    wt = self._wait_time()
    if wt > 0:
      time.sleep(wt)

    return self._lock

