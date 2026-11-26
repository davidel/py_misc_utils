"""Tests for the py_misc_utils.no_break signal suppression context.

Expectations follow the documented API contract: on entry NoBreak installs
handlers for the configured signals and on exit removes exactly those
handlers; received signals are recorded in arrival order with their frame;
exit_trigger controls whether the deferred signals are re-triggered on
OS signal delivery itself is not exercised - the handler captured from
the public signal spy is invoked directly, keeping tests deterministic.
"""

import signal

import pytest

from py_misc_utils import no_break as pnb
from py_misc_utils import signal as psgn


class _Frame:
  """Minimal stand-in for the stack frame passed to signal handlers."""

  def __init__(self, tag):
    self.tag = tag


@pytest.fixture
def spy_sgn(monkeypatch):
  """Replace the py_misc_utils.signal entry points with call spies."""
  calls = {'signal': [], 'unsignal': [], 'trigger': []}

  def mk_spy(attr):

    def spy(*args, **kwargs):
      calls[attr].append((args, kwargs))

    return spy

  for attr in ('signal', 'unsignal', 'trigger'):
    monkeypatch.setattr(psgn, attr, mk_spy(attr))

  return calls


def test_enter_installs_handlers_for_default_signals(spy_sgn):
  """Default NoBreak installs its handler for SIGINT and SIGTERM."""
  with pnb.NoBreak():
    installed = [(args[0], args[1]) for args, _kw in spy_sgn['signal']]

    assert {sig for sig, _h in installed} == {signal.SIGINT, signal.SIGTERM}
    # Handlers are installed at standard priority.
    assert all(kw == {'prio': psgn.STD_PRIO} for _args, kw in spy_sgn['signal'])

  assert spy_sgn['unsignal'] == [
      ((sig, handler), {}) for (sig, handler), _kw in spy_sgn['signal']
  ]
  assert all(kw == {} for _args, kw in spy_sgn['unsignal'])


def test_custom_signals_resolved_from_names(spy_sgn):
  """Signal names - string or sequence - map to the configured signals."""
  with pnb.NoBreak('INT'):
    assert {args[0] for args, _kw in spy_sgn['signal']} == {signal.SIGINT}

  spy_sgn['signal'].clear()

  with pnb.NoBreak(['INT', 'TERM']):
    assert {args[0] for args, _kw in spy_sgn['signal']
           } == {signal.SIGINT, signal.SIGTERM}

  spy_sgn['signal'].clear()

  with pnb.NoBreak(()):
    assert not spy_sgn['signal']


def test_empty_string_signal_names_crashes_report_only(spy_sgn):
  """NoBreak('') raises KeyError instead of installing zero handlers.

  Report-only finding: an empty signal-name string is split into a
  single empty part, which is then looked up in SIGMAP and raises
  KeyError rather than being treated as 'no signals'.
  """
  with pytest.raises(KeyError):
    pnb.NoBreak('')


def test_handler_records_signals_in_order(spy_sgn):
  """Received signals are appended with their frame in arrival order."""
  frame1, frame2 = _Frame('f1'), _Frame('f2')

  with pnb.NoBreak('INT'):
    # The handler NoBreak registered, captured from the public signal spy.
    handler = spy_sgn['signal'][0][0][1]
    handler(signal.SIGINT, frame1)
    handler(signal.SIGINT, frame2)

    assert spy_sgn['trigger'] == []

  # Nothing is re-triggered by default (exit_trigger=False).
  assert spy_sgn['trigger'] == []


def test_exit_trigger_replays_signals_on_exit(spy_sgn):
  """With exit_trigger, recorded signals are re-triggered on __exit__."""
  frame = _Frame('f')

  with pnb.NoBreak('TERM', exit_trigger=True):
    handler = spy_sgn['signal'][0][0][1]
    handler(signal.SIGTERM, frame)

  assert [args for args, _kw in spy_sgn['trigger']] == [(signal.SIGTERM, frame)]


def test_exit_trigger_replays_multiple_signals_in_order(spy_sgn):
  """Multiple recorded signals are replayed in record order on exit."""
  f1, f2 = _Frame('f1'), _Frame('f2')

  with pnb.NoBreak('INT,TERM', exit_trigger=True):
    handler = spy_sgn['signal'][0][0][1]
    handler(signal.SIGINT, f1)
    handler(signal.SIGTERM, f2)

  assert [args for args, _kw in spy_sgn['trigger']] == [(signal.SIGINT, f1),
                                                        (signal.SIGTERM, f2)]


def test_exit_trigger_with_no_signals_received(spy_sgn):
  """Exit with exit_trigger and no recorded signals does not trigger."""
  with pnb.NoBreak(exit_trigger=True):
    pass

  assert spy_sgn['trigger'] == []
