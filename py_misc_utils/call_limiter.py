import threading

from . import traceback as tb


_LOCK = threading.Lock()
_TB = dict()


def trigger(n, count):
  f = tb.get_frame(n + 1)
  if f is not None:
    tb = f.f_code.co_filename, f.f_lineno
    with _LOCK:
      c = _TB.get(tb, 0)
      _TB[tb] = c + 1

      return count > c

  return True


def limit_call(count, fn, n=0):
  if trigger(n + 1, count):
    return fn()

