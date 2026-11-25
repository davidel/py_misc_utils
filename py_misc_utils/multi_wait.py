"""Multi-threaded synchronization barrier with configurable signal count.

This module provides :class:`MultiWait`, a threading primitive that blocks
until a specified number of signals have been received. It is useful for
coordinating multiple worker threads that must all reach a checkpoint before
proceeding.
"""

import threading


class MultiWait:
  """A barrier that waits for a configurable number of signals before releasing.
  """

  def __init__(self, count):
    """Initialize the barrier with the required signal count.

    Args:
      count: The number of signals required before waiting threads are released.
    """
    self._count = count
    self._sigcount = 0
    self._lock = threading.Lock()
    self._cond = threading.Condition(lock=self._lock)

  def signal(self, n=1):
    """Signal that n waiters have reached the barrier.

    When the signal count reaches the configured count, all waiting threads
    are released.

    Args:
      n: The number of signals to add (default: 1).
    """
    with self._lock:
      self._sigcount = min(self._sigcount + n, self._count)
      if self._sigcount == self._count:
        self._cond.notify_all()

  def wait(self, timeout=None):
    """Block until the signal count reaches the configured count.

    Args:
      timeout: Optional maximum time in seconds to wait. If None, wait
        indefinitely.

    Returns:
      True if the barrier was released (signal count reached), False if the
      timeout elapsed first.
    """
    with self._lock:
      if self._count > self._sigcount:
        return self._cond.wait(timeout=timeout)

    return True
