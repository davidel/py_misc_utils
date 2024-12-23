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
    cid = _NEXT_ID
    _CLEANUPS[cid] = (fn, args, kwargs)
    _NEXT_ID += 1

  return cid


# Decorator style registration.
def reg(fn):
  register(fn)

  return fn


def unregister(cid, run=False):
  with _LOCK:
    cfdata = _CLEANUPS.pop(cid, None)

  if run and cfdata is not None:
    fn, args, kwargs = cfdata

    fn(*args, **kwargs)

  return cdata


def run():
  with _LOCK:
    # Sort by reverse ID, which is reverse register order.
    cids = sorted(_CLEANUPS.keys(), reverse=True)
    cfdata = [_CLEANUPS[cid] for cid in cids]
    _CLEANUPS.clear()

  for fn, args, kwargs in cfdata:
    try:
      fn(*args, **kwargs)
    except Exception as e:
      tb = traceback.format_exc()
      alog.error(f'Exception while running cleanups: {e}\n{tb}')

