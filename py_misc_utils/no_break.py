"""Signal handling utilities to suppress break signals.

This module provides NoBreak, a context manager that temporarily suppresses
SIGINT and SIGTERM signals, recording them for later processing. Useful for
protecting critical sections from interruption.
"""

import signal

from . import core_utils as cu
from . import signal as sgn


class NoBreak:
  """Context manager that suppresses specified signals during its lifetime.

  On entry, installs handlers for the configured signals (default SIGINT and
  SIGTERM) that record received signals instead of acting on them. On exit,
  restores previous handlers and optionally re-triggers the received signals.

  Attributes:
    SIGMAP: Dict mapping signal name strings to signal module constants.
  """

  SIGMAP = {  # noqa: RUF012 -- read-only class constant
      'INT': signal.SIGINT,
      'TERM': signal.SIGTERM,
  }

  # Mapping of signal name strings to signal numbers.

  def __init__(self, sigs=None, exit_trigger=False):
    """Resolve the signals to intercept.

    Args:
      sigs: Signal names - as a comma-separated string or a sequence -
        or None to intercept the default termination signals.
      exit_trigger: If True, a received signal also triggers a system
        exit (default: False).
    """
    if sigs is None:
      self._signals = tuple(self.SIGMAP.values())
    else:
      if isinstance(sigs, str):
        sigs = cu.splitstrip(sigs, ',')

      self._signals = tuple(self.SIGMAP[sig] for sig in sigs)

    self._exit_trigger = exit_trigger

  def __enter__(self):
    """Install signal handlers for all configured signals.

    Records an empty list for received signals, then installs self._handler
    for each signal using sgn.signal() with standard priority.

    Returns:
      self, for use as a context manager.
    """
    self._signal_received = []
    for sig in self._signals:
      sgn.signal(sig, self._handler, prio=sgn.STD_PRIO)

    return self

  def _handler(self, sig, frame):
    """Signal handler that records the signal and frame instead of acting on it.

    Args:
      sig: The signal number received.
      frame: The current stack frame.

    Returns:
      sgn.HANDLED to indicate the signal was handled.
    """
    self._signal_received.append((sig, frame))

    return sgn.HANDLED

  def __exit__(self, *exc):
    """Remove signal handlers and optionally re-trigger received signals.

    Uninstalls handlers for all configured signals. If _exit_trigger is True,
    re-triggers each received signal via sgn.trigger().

    Args:
      *exc: Exception info (unused).

    Returns:
      False, to not suppress exceptions.
    """
    for sig in self._signals:
      sgn.unsignal(sig, self._handler)

    if self._exit_trigger:
      for sig, frame in self._signal_received:
        sgn.trigger(sig, frame)

    return False
