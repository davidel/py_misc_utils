import signal
import threading


_LOCK = threading.Lock()
_HANDLERS = set()
_PREV_HANDLER = None


def _handler(sig, ctx):
  with _LOCK:
    for h in _HANDLERS:
      h.trigger()


class BreakControl(object):

  def __init__(self):
    self._hit = False

  def __enter__(self):
    global _PREV_HANDLER
    with _LOCK:
      if not _HANDLERS:
        _PREV_HANDLER = signal.signal(signal.SIGINT, _handler)
      _HANDLERS.add(self)

    return self

  def __exit__(self, type, value, traceback):
    global _PREV_HANDLER
    with _LOCK:
      _HANDLERS.remove(self)
      if not _HANDLERS:
        signal.signal(signal.SIGINT, _PREV_HANDLER)
        _PREV_HANDLER = None

    return False

  def trigger(self):
    self._hit = True

  def hit(self):
    return self._hit

