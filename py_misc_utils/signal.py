"""Process signal handling with prioritized handler chains.

This module provides utilities for registering multiple signal handlers per
signal, ordered by priority. Handlers may return HANDLED to stop chain
propagation or CALL_NEXT to continue. The :class:`Signals` context manager
provides scoped handler registration, and state is preserved across process
forks via the global namespace module.
"""

import collections
import signal as sgn
import threading

from . import core_utils as cu
from . import global_namespace as gns
from . import traceback as tb

_Handler = collections.namedtuple('Handler', 'handler, prio')

MAX_PRIO = 0
MIN_PRIO = 99
STD_PRIO = MIN_PRIO
CALL_NEXT = 0
HANDLED = 1


class _SignalRegistry:
  """Internal registry of signal handlers and previous (OS) handlers.

  Attributes:
    lock: Protects the handler registries.
    handlers: Mapping of signal number to tuple of _Handler instances sorted
      by priority.
    prev_handlers: Mapping of signal number to the OS-level handler active
      before the first registration for that signal.
  """

  def __init__(self):
    """Initialize an empty signal registry."""
    self.lock = threading.Lock()
    self.handlers = dict()
    self.prev_handlers = dict()

  def signal(self, sig, handler, prio=None):
    """Register a handler for a signal at the given priority.

    The first registration installs the OS-level dispatcher handler and
    records the previous handler for later restoration.

    Args:
      sig: The signal number.
      handler: The callable to invoke when the signal is received.
      prio: Optional priority (lower runs first); defaults to STD_PRIO.
    """
    prio = STD_PRIO if prio is None else prio

    with self.lock:
      handlers = self.handlers.get(sig, ())
      handlers += (_Handler(handler, prio),)
      self.handlers[sig] = tuple(sorted(handlers, key=lambda h: h.prio))

      if sig not in self.prev_handlers:
        self.prev_handlers[sig] = sgn.signal(sig, _handler)

  def unsignal(self, sig, uhandler):
    """Remove a previously registered handler for a signal.

    If no handlers remain, the previous OS-level handler is restored.

    Args:
      sig: The signal number.
      uhandler: The handler callable to remove.

    Returns:
      The number of matching handlers removed.
    """
    handlers, dropped = [], 0
    with self.lock:
      for handler in self.handlers.get(sig, ()):
        if handler.handler != uhandler:
          handlers.append(handler)
        else:
          dropped += 1

      if dropped:
        self.handlers[sig] = tuple(handlers)
        if not handlers:
          sgn.signal(sig, self.prev_handlers.pop(sig))

    return dropped

  def sig_handler(self, sig, frame):
    """Dispatch a received signal through the registered handler chain.

    Handlers run in priority order until one returns HANDLED. If all handlers
    return CALL_NEXT, the previous OS-level handler (if callable) is invoked.

    Args:
      sig: The signal number received.
      frame: The current stack frame.
    """
    with self.lock:
      handlers = self.handlers.get(sig, ())
      prev_handler = self.prev_handlers.get(sig)

    for handler in handlers:
      hres = handler.handler(sig, frame)
      if hres == HANDLED:
        return

    if callable(prev_handler):
      prev_handler(sig, frame)
    else:
      handler = sgn.getsignal(sig)
      if callable(handler):
        handler(sig, frame)


def _parent_fn(sreg):
  """Get the state to propagate to forked children.

  Args:
    sreg: The parent signal registry.

  Returns:
    The previous OS-level handlers, to be restored in the child.
  """
  return sreg.prev_handlers


def _child_fn(prev_handlers):
  """Create the child registry, restoring the OS-level handlers.

  Args:
    prev_handlers: Previous OS-level handlers from the parent process.

  Returns:
    A fresh _SignalRegistry for the child process.
  """
  for sig, prev_handler in prev_handlers.items():
    sgn.signal(sig, prev_handler)

  return _SignalRegistry()


def _create_fn():
  """Create a fresh signal registry."""
  return _SignalRegistry()


_SIGREG = gns.Var(f'{__name__}.SIGREG',
                  parent_fn=_parent_fn,
                  child_fn=_child_fn,
                  defval=_create_fn)


def _sig_registry():
  """Return the current thread's signal registry.

  Returns:
    The _SignalRegistry instance from the global namespace variable.
  """
  return gns.get(_SIGREG)


def _handler(sig, frame):
  """Internal signal dispatch entry point.

  Args:
    sig: The signal number received.
    frame: The current stack frame.
  """
  sreg = _sig_registry()

  sreg.sig_handler(sig, frame)


def trigger(sig, frame=None):
  """Manually invoke signal handlers for a signal as if it were received.

  Args:
    sig: The signal number to trigger.
    frame: Optional stack frame to pass; defaults to the caller's frame.
  """
  _handler(sig, frame or tb.get_frame(n=1))


def signal(sig, handler, prio=None):
  """Register a handler for a signal.

  Args:
    sig: The signal number.
    handler: The callable to invoke when the signal is received.
    prio: Optional priority (lower runs first); defaults to STD_PRIO.
  """
  sreg = _sig_registry()

  sreg.signal(sig, handler, prio=prio)


def unsignal(sig, uhandler):
  """Remove a handler for a signal.

  Args:
    sig: The signal number.
    uhandler: The handler callable to remove.

  Returns:
    The number of matching handlers removed.
  """
  sreg = _sig_registry()

  return sreg.unsignal(sig, uhandler)


class Signals:
  """Context manager for scoped signal handler registration.

  Registers handlers for one or more signals on __enter__ and removes them
  on __exit__.

  Attributes:
    _sigs: Tuple of (sig, handler) pairs.
    _prio: The priority used for registration.
  """

  def __init__(self, sig, handler, prio=None):
    """Normalize the signals and handlers into aligned tuples.

    Args:
      sig: A signal name ('int'), a signal number, or a list of them.
      handler: A callable or a list of callables aligned with sig.
      prio: Optional priority (lower runs first).
    """
    if isinstance(sig, str):
      sig = [getattr(sgn, f'SIG{s.upper()}') for s in cu.splitstrip(sig, ',')]
    elif not isinstance(sig, (list, tuple)):
      sig = [sig]
    if not isinstance(handler, (list, tuple)):
      handler = [handler] * len(sig)

    self._sigs = tuple(zip(sig, handler))
    self._prio = prio

  def __enter__(self):
    """Register all signal handlers.

    Returns:
      The Signals instance itself.
    """
    for sig, handler in self._sigs:
      signal(sig, handler, prio=self._prio)

    return self

  def __exit__(self, *exc):
    """Remove all registered signal handlers.

    Args:
      *exc: Exception information if an exception occurred in the context.

    Returns:
      True (exceptions are suppressed).
    """
    for sig, handler in self._sigs:
      unsignal(sig, handler)

    return True
