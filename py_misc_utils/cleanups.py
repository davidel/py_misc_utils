import atexit
import threading
import traceback

from . import alog
from . import init_variables as ivar


class _Cleanups(ivar.VarBase):

  def __init__(self):
    self._lock = threading.Lock()
    self._nextid = 0
    self._funcs = dict()

    # The run() API is called from a "finally" clause of the app_main module, which
    # is the preferred path since we know eveything is up at that time. But we also
    # register an atexit callback for cases (like child prcesses) which do not end
    # up going out the app_main path.
    atexit.register(self.run)

  def cleanup(self):
    atexit.unregister(self.run)

  def register(self, fn, *args, **kwargs):
    with self._lock:
      cid = self._nextid
      self._funcs[cid] = (fn, args, kwargs)
      self._nextid += 1

    return cid

  def unregister(self, cid, run=None):
    with self._lock:
      cfdata = self._funcs.pop(cid, None)

    if run is True and cfdata is not None:
      fn, args, kwargs = cfdata

      fn(*args, **kwargs)

    return cfdata

  def run(self):
    with self._lock:
      funcs = self._funcs
      self._funcs = dict()

    # Sort by reverse ID, which is reverse register order.
    cids = sorted(funcs.keys(), reverse=True)
    cfdata = [funcs[cid] for cid in cids]

    for fn, args, kwargs in cfdata:
      try:
        fn(*args, **kwargs)
      except Exception as e:
        tb = traceback.format_exc()
        alog.error(f'Exception while running cleanups: {e}\n{tb}')


_VARID = ivar.varid(__file__, 'cleanups')

def _cleanups():
  return ivar.get(_VARID, _Cleanups)


def register(fn, *args, **kwargs):
  return _cleanups().register(fn, *args, **kwargs)


# Decorator style registration.
def reg(fn):
  register(fn)

  return fn


def unregister(cid, run=None):
  return _cleanups().unregister(cid, run=run)


def run():
  _cleanups().run()

