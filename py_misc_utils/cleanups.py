import atexit
import collections
import threading

from . import alog
from . import global_namespace as gns


_Cleaner = collections.namedtuple('Cleaner', 'fn, args, kwargs')

class _Cleanups:

  def __init__(self):
    self._lock = threading.Lock()
    self._nextid = 0
    self._cleaners = dict()

    # The run() API is called from a "finally" clause of the multiprocessing module,
    # which is the preferred path since we know eveything is up at that time. But we
    # also register an atexit callback for cases (like child prcesses) which do not
    # end up going out the multiprocessing path (although every child process using
    # this library should be created with the multiprocessing.create_process() API).
    atexit.register(self.run)

  def register(self, fn, *args, **kwargs):
    with self._lock:
      cid = self._nextid
      self._cleaners[cid] = _Cleaner(fn=fn, args=args, kwargs=kwargs)
      self._nextid += 1

    return cid

  def unregister(self, cid, run=False):
    with self._lock:
      cleaner = self._cleaners.pop(cid, None)

    if cleaner is not None and run:
      self._run_cleaner(fn, args, kwargs)

    return cleaner

  def _run_cleaner(self, cleaner):
    try:
      cleaner.fn(*cleaner.args, **cleaner.kwargs)
    except Exception as ex:
      alog.exception(ex, exmsg=f'Exception while running cleanups')

  def run(self):
    with self._lock:
      cleaners = self._cleaners
      self._cleaners = dict()

    # Sort by reverse ID, which is reverse register order.
    cids = sorted(cleaners.keys(), reverse=True)

    for cleaner in (cleaners[cid] for cid in cids):
      self._run_cleaner(cleaner)


_CLEANUPS = gns.Var(f'{__name__}.CLEANUPS',
                    fork_init=True,
                    defval=lambda: _Cleanups())

def _cleanups():
  return gns.get(_CLEANUPS)


def register(fn, *args, **kwargs):
  return _cleanups().register(fn, *args, **kwargs)


# Decorator style registration.
def reg(fn):
  register(fn)

  return fn


def unregister(cid, run=False):
  return _cleanups().unregister(cid, run=run)


def run():
  _cleanups().run()

