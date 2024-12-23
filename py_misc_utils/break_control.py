import signal
import threading

from . import fin_wrap as fw
from . import signal as sgn


_LOCK = threading.Lock()
_HANDLERS = set()


def _handler(sig, frame):
  with _LOCK:
    for h in _HANDLERS:
      h.trigger(frame)

  return sgn.HANDLED


class BreakControl:

  def __init__(self):
    self._hit = False
    self._frame = None

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

  def __exit__(self, *exc):
    self.close()

    return False

  def trigger(self, frame):
    self._hit = True
    self._frame = frame

  def hit(self):
    return self._hit

  def frame(self):
    return self._frame


def create():
  bc = BreakControl()

  return fw.fin_wrap_np(bc.open(), bc.close)

