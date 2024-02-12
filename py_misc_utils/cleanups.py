# Not using atexit module as it limits what cleanups callbacks can do.
# These are called as 'finally' clause from the app_main.py module, when
# everything is still up and running.

import threading
import traceback

from . import alog


_LOCK = threading.Lock()
_NEXT_ID = 0
_CLEANUPS = dict()


def register(fn, *args, **kwargs):
  global _NEXT_ID

  with _LOCK:
    fdata = _CLEANUPS.get(fn, None)
    if fdata is not None:
      _CLEANUPS[fn] = (fdata[0], args, kwargs)
    else:
      _CLEANUPS[fn] = (_NEXT_ID, args, kwargs)
      _NEXT_ID += 1

  return fn


def unregister(fn):
  with _LOCK:
    _CLEANUPS.pop(fn, None)


def run():
  with _LOCK:
    # Sort by reverse ID, which is reverse register order.
    fns = sorted(_CLEANUPS.keys(), key=lambda x: _CLEANUPS[x][0], reverse=True)
    fdata = [_CLEANUPS[fn] for fn in fns]
    _CLEANUPS.clear()

  for fn, data in zip(fns, fdata):
    try:
      fn(*data[1], **data[2])
    except Exception as e:
      tb = traceback.format_exc()
      alog.error(f'Exception while running cleanups: {e}\n{tb}')

