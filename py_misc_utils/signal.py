import signal as sgn
import threading


_LOCK = threading.Lock()
_HANDLERS = dict()
_PREV_HANDLERS = dict()


def _handler(sig, ctx):
  with _LOCK:
    handlers = _HANDLERS.get(sig, ())

  for handler in handlers:
    if handler(sig, ctx):
      return

  with _LOCK:
    prev_handler = _PREV_HANDLERS.get(sig, None)

  if prev_handler is not None:
    prev_handler(sig, ctx)


def signal(sig, handler):
  with _LOCK:
    handlers = _HANDLERS.get(sig, ())
    _HANDLERS[sig] = handlers + (handler,)

    if sig not in _PREV_HANDLERS:
      _PREV_HANDLERS[sig] = sgn.signal(sig, _handler)


def unsignal(sig, handler):
  with _LOCK:
    handlers = list(_HANDLERS.get(sig, ()))
    handlers.remove(handler)
    _HANDLERS[sig] = tuple(handlers)

    if not handlers:
      sgn.signal(sig, _PREV_HANDLERS.pop(sig))

