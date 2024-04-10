import signal
import threading

from . import fin_wrap as fw
from . import signal as sgn


_LOCK = threading.Lock()
_HANDLERS = set()


def _handler(sig, ctx):
  with _LOCK:
    for h in _HANDLERS:
      h.trigger()

  return 0


class BreakControl:

  def __init__(self):
    self._hit = False

  def open(self):
    with _LOCK:
      if not _HANDLERS:
        sgn.signal(signal.SIGINT, _handler)
      _HANDLERS.add(self)

    return self

  def close(self):
    with _LOCK:
      _HANDLERS.remove(self)
      if not _HANDLERS:
        sgn.unsignal(signal.SIGINT, _handler)

  def __enter__(self):
    return self.open()

  def __exit__(self, type, value, traceback):
    self.close()

    return False

  def trigger(self):
    self._hit = True

  def hit(self):
    return self._hit


def create():
  bc = BreakControl()

  return fw.fin_wrap_np(bc.open(), bc.close())

