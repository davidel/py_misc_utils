import threading


class MultiWait:

  def __init__(self, count):
    self._count = count
    self._sigcount = 0
    self._lock = threading.Lock()
    self._cond = threading.Condition(lock=self._lock)

  def signal(self, n=1):
    with self._lock:
      self._sigcount = min(self._sigcount + n, self._count)
      if self._sigcount == self._count:
        self._cond.notify_all()

  def wait(self, timeout=None):
    with self._lock:
      if self._count > self._sigcount:
        return self._cond.wait(timeout=timeout)

    return True

