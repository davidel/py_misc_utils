import collections
import signal as sgn
import threading

from . import core_utils as cu
from . import global_namespace as gns
from . import traceback as tb


_Handler = collections.namedtuple('Handler', 'handler, prio')

MAX_PRIO = 0
MIN_PRIO = 99
STD_PRIO = MIN_PRIO
CALL_NEXT = 0
HANDLED = 1


class _SignalRegistry:

  def __init__(self):
    self.lock = threading.Lock()
    self.handlers = dict()
    self.prev_handlers = dict()

  def signal(self, sig, handler, prio=None):
    prio = STD_PRIO if prio is None else prio

    with self.lock:
      handlers = self.handlers.get(sig, ())
      handlers += (_Handler(handler, prio),)
      self.handlers[sig] = tuple(sorted(handlers, key=lambda h: h.prio))

      if sig not in self.prev_handlers:
        self.prev_handlers[sig] = sgn.signal(sig, _handler)

  def unsignal(self, sig, uhandler):
    handlers, dropped = [], 0
    with self.lock:
      for handler in self.handlers.get(sig, ()):
        if handler.handler != uhandler:
          handlers.append(handler)
        else:
          dropped += 1

      if dropped:
        self.handlers[sig] = tuple(handlers)
        if not handlers:
          sgn.signal(sig, self.prev_handlers.pop(sig))

    return dropped

  def sig_handler(self, sig, frame):
    with self.lock:
      handlers = self.handlers.get(sig, ())
      prev_handler = self.prev_handlers.get(sig)

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


def _parent_fn(sreg):
  return sreg.prev_handlers


def _child_fn(prev_handlers):
  for sig, prev_handler in prev_handlers.items():
    sgn.signal(sig, prev_handler)

  return _SignalRegistry()


def _create_fn():
  return _SignalRegistry()


_SIGREG = gns.Var(f'{__name__}.SIGREG',
                  parent_fn=_parent_fn,
                  child_fn=_child_fn,
                  defval=_create_fn)

def _sig_registry():
  return gns.get(_SIGREG)


def _handler(sig, frame):
  sreg = _sig_registry()

  sreg.sig_handler(sig, frame)


def trigger(sig, frame=None):
  _handler(sig, frame or tb.get_frame(n=1))


def signal(sig, handler, prio=None):
  sreg = _sig_registry()

  sreg.signal(sig, handler, prio=prio)


def unsignal(sig, uhandler):
  sreg = _sig_registry()

  return sreg.unsignal(sig, uhandler)


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

