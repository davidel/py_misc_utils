import time


class TimeTrigger:

  def __init__(self, trig):
    self.trig = trig
    self.last = time.time()

  def __call__(self):
    now = time.time()
    if now >= self.last + self.trig:
      self.last = now
      return True

    return False

