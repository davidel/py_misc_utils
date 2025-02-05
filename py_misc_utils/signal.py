import collections
import signal as sgn
import threading

from . import core_utils as cu
from . import traceback as tb


_Handler = collections.namedtuple('Handler', 'handler, prio')

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

  for handler in handlers:
    hres = handler.handler(sig, frame)
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


def signal(sig, handler, prio=None):
  prio = STD_PRIO if prio is None else prio

  with _LOCK:
    handlers = _HANDLERS.get(sig, ())
    handlers += (_Handler(handler, prio),)
    _HANDLERS[sig] = tuple(sorted(handlers, key=lambda h: h.prio))

    if sig not in _PREV_HANDLERS:
      _PREV_HANDLERS[sig] = sgn.signal(sig, _handler)


def unsignal(sig, uhandler):
  handlers, dropped = [], 0
  with _LOCK:
    for handler in _HANDLERS.get(sig, ()):
      if handler.handler != uhandler:
        handlers.append(handler)
      else:
        dropped += 1

    if dropped:
      _HANDLERS[sig] = tuple(handlers)
      if not handlers:
        sgn.signal(sig, _PREV_HANDLERS.pop(sig))

  return dropped


class Signals:

  def __init__(self, sig, handler, prio=None):
    if isinstance(sig, str):
      sig = [getattr(sgn, f'SIG{s.upper()}') for s in cu.splitstrip(sig, ',')]
    elif not isinstance(sig, (list, tuple)):
      sig = [sig]
    if not isinstance(handler, (list, tuple)):
      handler = [handler] * len(sig)

    self._sigs = tuple(zip(sig, handler))
    self._prio = prio

  def __enter__(self):
    for sig, handler in self._sigs:
      signal(sig, handler, prio=self._prio)

    return self

  def __exit__(self, *exc):
    for sig, handler in self._sigs:
      unsignal(sig, handler)

    return True

