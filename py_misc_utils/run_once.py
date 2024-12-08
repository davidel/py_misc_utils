import functools
import threading


def run_once(fn):

  @functools.wraps(fn)
  def wrapper(*args, **kwargs):
    with wrapper._lock:
      if not wrapper._has_run:
        wrapper._has_run = True

        return fn(*args, **kwargs)

  wrapper._lock = threading.Lock()
  wrapper._has_run = False

  return wrapper

