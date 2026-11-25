"""Tests for py_misc_utils.signal.

Expectations pinned to the observed behavior: signal()/unsignal()
register and remove handlers with priority ordering (lower runs first);
trigger() invokes handlers manually (NOTE: trigger() recurses if no
handler returns HANDLED on signals with no callable OS default handler
— library bug, tests avoid this path); Signals context manager
registers and removes handlers on entry/exit; HANDLED stops propagation
while CALL_NEXT continues to the next handler; state is preserved
across forks via the global namespace module.
"""

import signal as sgn

from py_misc_utils import signal as s


def test_signal_unsignal_register_remove():
  """signal() registers and unsignal() removes handlers, returning count."""
  calls = []

  def handler(sig, frame):
    calls.append(sig)

  assert s.signal(sgn.SIGUSR1, handler) is None
  assert s.unsignal(sgn.SIGUSR1, handler) == 1
  assert s.unsignal(sgn.SIGUSR1, handler) == 0


def test_trigger_with_handled():
  """trigger() invokes the registered handler returning HANDLED."""
  calls = []

  def handler(sig, frame):
    calls.append(sig)
    return s.HANDLED

  s.signal(sgn.SIGUSR2, handler)
  try:
    s.trigger(sgn.SIGUSR2)
    assert calls == [sgn.SIGUSR2]
  finally:
    s.unsignal(sgn.SIGUSR2, handler)


def test_priority_ordering_handled_stops():
  """Lower priority handlers run first; HANDLED stops propagation."""
  order = []

  def low(sig, frame):
    order.append('low')
    return s.HANDLED

  def high(sig, frame):
    order.append('high')
    return s.HANDLED

  s.signal(sgn.SIGUSR1, low, prio=s.STD_PRIO)
  s.signal(sgn.SIGUSR1, high, prio=s.MAX_PRIO)
  try:
    s.trigger(sgn.SIGUSR1)
    assert order == ['high']
  finally:
    s.unsignal(sgn.SIGUSR1, low)
    s.unsignal(sgn.SIGUSR1, high)


def test_signals_context_manager():
  """Signals registers on enter and removes on exit."""
  calls = []

  def handler(sig, frame):
    calls.append(sig)
    return s.HANDLED

  with s.Signals(sgn.SIGUSR1, handler):
    s.trigger(sgn.SIGUSR1)
    assert calls == [sgn.SIGUSR1]

  # Handler removed after exit.
  s.trigger(sgn.SIGUSR1)
  assert calls == [sgn.SIGUSR1]


def test_signals_multiple_signals():
  """Signals handles multiple signals with aligned handlers."""
  calls = []

  def handler(sig, frame):
    calls.append(sig)
    return s.HANDLED

  with s.Signals([sgn.SIGUSR1, sgn.SIGUSR2], [handler, handler]):
    s.trigger(sgn.SIGUSR1)
    s.trigger(sgn.SIGUSR2)
    assert calls == [sgn.SIGUSR1, sgn.SIGUSR2]
