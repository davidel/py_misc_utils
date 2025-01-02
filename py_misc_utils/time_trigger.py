import time


class TimeTrigger:

  def __init__(self, interval):
    self._interval = interval
    self.next = time.time() + interval

  def __bool__(self):
    if self._interval:
      if (now := time.time()) >= self.next:
        self.next = now + self._interval

        return True

    return False

