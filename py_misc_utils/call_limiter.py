import threading

from . import traceback as tb


_LOCK = threading.Lock()
_TB = dict()


def trigger(filename, count):
  frame = tb.get_frame_after(filename)
  if frame is not None:
    tb = frame.f_code.co_filename, frame.f_lineno
    with _LOCK:
      c = _TB.get(tb, 0)
      _TB[tb] = c + 1

      return count > c

  return True


def limit_call(count, fn, *args, _filename=None, **kwargs):
  if trigger(_filename or __file__, count):
    return fn(*args, **kwargs)

