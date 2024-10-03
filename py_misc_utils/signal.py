import signal as sgn
import threading

from . import traceback as tb


_LOCK = threading.Lock()
_HANDLERS = dict()
_PREV_HANDLERS = dict()

MAX_PRIO = 0
MIN_PRIO = 99
STD_PRIO = MIN_PRIO
CALL_NEXT = 0
HANDLED = 1


def _handler(sig, frame):
  with _LOCK:
    handlers = _HANDLERS.get(sig, ())
    prev_handler = _PREV_HANDLERS.get(sig)

  for prio, handler in handlers:
    hres = handler(sig, frame)
    if hres == HANDLED:
      return

  if callable(prev_handler):
    prev_handler(sig, frame)
  else:
    handler = sgn.getsignal(sig)
    if callable(handler):
      handler(sig, frame)


def trigger(sig, frame=None):
  _handler(sig, frame or tb.get_frame(n=1))


def signal(sig, handler, prio=STD_PRIO):
  with _LOCK:
    handlers = _HANDLERS.get(sig, ())
    _HANDLERS[sig] = tuple(sorted(handlers + ((prio, handler),), key=lambda h: h[0]))

    if sig not in _PREV_HANDLERS:
      _PREV_HANDLERS[sig] = sgn.signal(sig, _handler)


def unsignal(sig, uhandler):
  with _LOCK:
    handlers = []
    for prio, handler in _HANDLERS.get(sig, ()):
      if handler != uhandler:
        handlers.append((prio, handler))

    _HANDLERS[sig] = tuple(handlers)

    if not handlers:
      sgn.signal(sig, _PREV_HANDLERS.pop(sig))


class Signals:

  def __init__(self, sig, handler):
    if not isinstance(sig, (list, tuple)):
      sig = [sig]
    if not isinstance(handler, (list, tuple)):
      handler = [handler] * len(sig)

    self._sigs = tuple(zip(sig, handler))

  def __enter__(self):
    for sig, handler in self._sigs:
      signal(sig, handler)

    return self

  def __exit__(self, *exc):
    for sig, handler in self._sigs:
      unsignal(sig, handler)

    return True

