import traceback
import threading


_LOCK = threading.Lock()
_TB = dict()


def _get_tb(n):
  for f, _ in traceback.walk_stack(None):
    if n == 0:
      return f.f_code.co_filename, f.f_lineno
    n -= 1


def trigger(n, count):
  tb = _get_tb(n + 1)
  if tb is not None:
    with _LOCK:
      c = _TB.get(tb, 0)
      _TB[tb] = c + 1

      return count > c

  return True


def limit_call(count, fn, n=0):
  if trigger(n + 1, count):
    return fn()

