"""Process exit cleanup registration and execution utilities.

This module provides a mechanism for registering cleanup functions that are
automatically executed at process exit (via atexit) or on demand. Cleanups are
stored in a global namespace variable so they survive across fork boundaries
in multiprocessing scenarios.
"""

import atexit
import collections
import threading

from . import alog
from . import global_namespace as gns

_Cleaner = collections.namedtuple('Cleaner', 'fn, args, kwargs')
# Named tuple representing a registered cleanup: (callable, args, kwargs).


class _Cleanups:
  """Manages a set of registered cleanup functions with unique IDs.

  Cleanups can be registered with optional arguments and keyword arguments.
  Each registration receives a unique integer ID. Cleanups can be unregistered
  by ID, optionally executed before removal. All registered cleanups can be
  executed at once via run().
  """

  def __init__(self):
    """Initialize a new _Cleanups instance with an empty cleaner set."""
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
    """Register a cleanup function to be called at process exit or on run().

    Args:
      fn: The callable to invoke during cleanup.
      *args: Positional arguments to pass to fn.
      **kwargs: Keyword arguments to pass to fn.

    Returns:
      An integer ID that can be used to unregister this cleanup later.
    """
    with self._lock:
      cid = self._nextid
      self._cleaners[cid] = _Cleaner(fn=fn, args=args, kwargs=kwargs)
      self._nextid += 1

    return cid

  def unregister(self, cid, run=False):
    """Remove a registered cleanup by ID, optionally executing it first.

    Args:
      cid: The integer ID returned by register().
      run: If True, execute the cleanup function before removing it.
        Defaults to False.

    Returns:
      The _Cleaner namedtuple that was removed, or None if cid was not found.
    """
    with self._lock:
      cleaner = self._cleaners.pop(cid, None)

    if cleaner is not None and run:
      self._run_cleaner(cleaner)

    return cleaner

  def _run_cleaner(self, cleaner):
    """Execute a single cleanup function, logging any exceptions.

    Args:
      cleaner: A _Cleaner namedtuple containing fn, args, and kwargs.
    """
    try:
      cleaner.fn(*cleaner.args, **cleaner.kwargs)
    except Exception as ex:  # noqa: BLE001 -- isolation boundary: user-registered cleanups must not abort remaining cleanups
      alog.exception(ex, exmsg='Exception while running cleanups')

  def run(self):
    """Execute all registered cleanups in reverse registration order.

    Clears the internal dictionary before executing, so cleanups are
    run at most once. Exceptions in individual cleanups are logged
    but do not prevent other cleanups from running.
    """
    with self._lock:
      cleaners = self._cleaners
      self._cleaners = dict()

    # Sort by reverse ID, which is reverse register order.
    cids = sorted(cleaners.keys(), reverse=True)

    for cleaner in (cleaners[cid] for cid in cids):
      self._run_cleaner(cleaner)


_CLEANUPS = gns.Var(f'{__name__}.CLEANUPS', fork_init=True, defval=lambda: _Cleanups())
# Global _Cleanups instance stored in the global namespace, initialized
# on fork so child processes get their own clean state.


def _cleanups():
  """Return the global _Cleanups instance.

  Returns:
    The _Cleanups object stored in the global namespace.
  """
  return gns.get(_CLEANUPS)


def register(fn, *args, **kwargs):
  """Register a cleanup function on the global _Cleanups instance.

    See _Cleanups.register() for details.

  Args:
    fn: The callable to invoke during cleanup.
    *args: Positional arguments to pass to fn.
    **kwargs: Keyword arguments to pass to fn.

  Returns:
    An integer ID that can be used to unregister this cleanup later.
  """
  return _cleanups().register(fn, *args, **kwargs)


# Decorator style registration.
def reg(fn):
  """Decorator that registers a function as a cleanup and returns it.

  Args:
    fn: The function to register as a cleanup.

  Returns:
    The same function object that was passed in.
  """
  register(fn)

  return fn


def unregister(cid, run=False):
  """Unregister a cleanup by ID on the global _Cleanups instance.

  See _Cleanups.unregister() for details.

  Args:
    cid: The integer ID returned by register().
    run: If True, execute the cleanup before removing it. Defaults to False.

  Returns:
    The removed _Cleaner namedtuple, or None if not found.
  """
  return _cleanups().unregister(cid, run=run)


def run():
  """Execute all registered cleanups on the global _Cleanups instance.

  Delegates to _Cleanups.run().
  """
  _cleanups().run()
