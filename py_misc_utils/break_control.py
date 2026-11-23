"""Break control utilities for handling SIGINT signals.

This module provides BreakControl, a context manager that registers a global
SIGINT handler and tracks whether a break has been received. Multiple
BreakControl instances can be active simultaneously; the first to open
installs the handler, and the last to close removes it.
"""

import signal
import threading

from . import fin_wrap as fw
from . import signal as sgn

_LOCK = threading.Lock()
# Global lock protecting access to _HANDLERS.

_HANDLERS = set()
# Set of active BreakControl instances.


def _handler(sig, frame):
  """Trigger every registered BreakControl handler for the signal."""
  with _LOCK:
    for h in _HANDLERS:
      h.trigger(frame)

  return sgn.HANDLED


class BreakControl:
  """Context manager for handling break (SIGINT) signals.

  Registers itself with a global SIGINT handler on open() and unregisters on
  close(). Tracks whether a break signal has been received and the frame at
  which it occurred. Multiple instances can be active at once.

  Attributes:
    _hit: bool, True if a break signal has been received.
    _frame: The stack frame where the break was received, or None.
  """

  def __init__(self):
    """Initialize a BreakControl with no hit and no frame."""
    self._hit = False
    self._frame = None

  def open(self):
    """Register this BreakControl with the global SIGINT handler.

    If this is the first handler, installs the global signal handler.

    Returns:
      self, for use as a context manager.
    """
    with _LOCK:
      if not _HANDLERS:
        sgn.signal(signal.SIGINT, _handler)
      _HANDLERS.add(self)

    return self

  def close(self):
    """Unregister this BreakControl from the global SIGINT handler.

    If this is the last handler, removes the global signal handler.
    """
    with _LOCK:
      _HANDLERS.remove(self)
      if not _HANDLERS:
        sgn.unsignal(signal.SIGINT, _handler)

  def __enter__(self):
    """Enter the context manager by opening this BreakControl.

    Returns:
      self.
    """
    return self.open()

  def __exit__(self, *exc):
    """Exit the context manager by closing this BreakControl.

    Args:
      *exc: Exception info (unused).

    Returns:
      False, to not suppress exceptions.
    """
    self.close()

    return False

  def trigger(self, frame):
    """Mark this BreakControl as hit and record the frame.

    Called by the global signal handler when SIGINT is received.

    Args:
      frame: The stack frame at the time of the signal.
    """
    self._hit = True
    self._frame = frame

  def hit(self):
    """Check whether a break signal has been received.

    Returns:
      True if a break has been received, False otherwise.
    """
    return self._hit

  def frame(self):
    """Return the stack frame where the break was received.

    Returns:
      The frame object, or None if no break has been received.
    """
    return self._frame


def create():
  """Create a BreakControl wrapped with fin_wrap for automatic cleanup.

  Creates a BreakControl, opens it, and registers its close() as a finalizer
  via fin_wrap_np so it is automatically closed when the returned wrapper is
  garbage collected.

  Returns:
    A fin_wrap wrapper that will call bc.close() on cleanup.
  """
  bc = BreakControl()

  return fw.fin_wrap_np(bc.open(), bc.close)
