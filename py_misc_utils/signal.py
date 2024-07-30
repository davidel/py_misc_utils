import signal as sgn
import threading


_LOCK = threading.Lock()
_HANDLERS = dict()
_PREV_HANDLERS = dict()


def _handler(sig, ctx):
  with _LOCK:
    handlers = _HANDLERS.get(sig, ())
    prev_handler = _PREV_HANDLERS.get(sig)

  mhres = -1
  for prio, handler in handlers:
    hres = handler(sig, ctx)
    if hres > 0:
      return

    mhres = max(hres, mhres)

  if mhres < 0 and callable(prev_handler):
    prev_handler(sig, ctx)


def signal(sig, handler, prio=99):
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

