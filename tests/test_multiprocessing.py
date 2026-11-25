"""Tests for py_misc_utils.multiprocessing (create_process and helpers).

All tests marked slow: they spawn real child processes.
"""

import multiprocessing
import os
import signal
import time

import pytest

from py_misc_utils import multiprocessing as mp_utils


@pytest.mark.slow
def test_create_process_returns_unstarted():
  p = mp_utils.create_process(lambda: None)
  assert isinstance(p, multiprocessing.Process)
  assert p.pid is None
  assert not p.is_alive()
  p.start()
  p.join(timeout=10.0)
  assert p.exitcode == 0


@pytest.mark.slow
def test_create_process_daemon_flag():
  p = mp_utils.create_process(lambda: None, daemon=True)
  assert p.daemon is True
  p.start()
  p.join(timeout=10.0)
  assert p.exitcode == 0


@pytest.mark.slow
def test_create_process_args_kwargs():
  q = multiprocessing.get_context('fork').Queue()

  def fn(a, b, c=None):
    q.put((a, b, c))

  p = mp_utils.create_process(fn, args=(1, 2), kwargs={'c': 3})
  p.start()
  got = q.get(timeout=10.0)
  p.join(timeout=10.0)
  assert p.exitcode == 0
  assert got == (1, 2, 3)


@pytest.mark.slow
def test_procfn_normal_return_exitcode_zero():
  p = mp_utils.create_process(lambda: 42)
  p.start()
  p.join(timeout=10.0)
  assert p.exitcode == 0


@pytest.mark.slow
def test_termination_error_exitcode_one():

  def fn():
    raise mp_utils.TerminationError()

  p = mp_utils.create_process(fn)
  p.start()
  p.join(timeout=10.0)
  assert p.exitcode == 1


@pytest.mark.slow
def test_keyboard_interrupt_exitcode_one():

  def fn():
    raise KeyboardInterrupt()

  p = mp_utils.create_process(fn)
  p.start()
  p.join(timeout=10.0)
  assert p.exitcode == 1


@pytest.mark.slow
def test_generic_exception_nonzero_exit():

  def fn():
    raise ValueError('kaboom')

  p = mp_utils.create_process(fn)
  p.start()
  p.join(timeout=10.0)
  assert p.exitcode != 0


@pytest.mark.slow
def test_create_process_sigterm_exits_cleanly():
  """create_process installs signal handlers in child processes.

  SIGTERM is converted to TerminationError by the handler, causing clean
  exit; SIGINT maps to KeyboardInterrupt: both pinned via the public
  create_process() child exit codes instead of calling _sig_handler().
  """
  def fn_sigterm(q):
    q.put('ready-term')
    time.sleep(30)

  def fn_sigint(q):
    q.put('ready-int')
    time.sleep(30)

  q1 = multiprocessing.get_context('fork').Queue()
  p1 = mp_utils.create_process(fn_sigterm, args=(q1,))
  p1.start()
  assert q1.get(timeout=10.0) == 'ready-term'
  os.kill(p1.pid, signal.SIGTERM)
  p1.join(timeout=10.0)
  assert p1.exitcode == 1

  q2 = multiprocessing.get_context('fork').Queue()
  p2 = mp_utils.create_process(fn_sigint, args=(q2,))
  p2.start()
  assert q2.get(timeout=10.0) == 'ready-int'
  os.kill(p2.pid, signal.SIGINT)
  p2.join(timeout=10.0)
  assert p2.exitcode == 1


def test_termination_error_public_exception():
  assert issubclass(mp_utils.TerminationError, Exception)
  try:
    raise mp_utils.TerminationError('boom')
  except mp_utils.TerminationError as exc:
    assert str(exc) == 'boom'


def test_context_dict_propagates_to_child():
  """create_process(context=dict) propagates the context to the child.

  The parent context capture/apply machinery is exercised via its public
  outcome: a process created with an explicit context dict runs the
  target and exits cleanly.
  """
  method = 'fork' if 'fork' in multiprocessing.get_all_start_methods() else 'spawn'
  q = multiprocessing.get_context(method).Queue()
  p = mp_utils.create_process(lambda: q.put('ctx-dict'),
                              context=multiprocessing.get_context(method))
  p.start()
  got = q.get(timeout=10.0)
  p.join(timeout=10.0)
  assert p.exitcode == 0
  assert got == 'ctx-dict'


@pytest.mark.slow
def test_context_string_creates_process():
  if 'fork' not in multiprocessing.get_all_start_methods():
    pytest.skip('fork not available')
  q = multiprocessing.get_context('fork').Queue()
  p = mp_utils.create_process(lambda: q.put('ctx-str'), context='fork')
  p.start()
  got = q.get(timeout=10.0)
  p.join(timeout=10.0)
  assert p.exitcode == 0
  assert got == 'ctx-str'
