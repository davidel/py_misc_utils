"""Tests for py_misc_utils.multiprocessing (create_process and helpers).

All tests marked slow: they spawn real child processes.
"""

import multiprocessing
import signal

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


def test_sig_handler_sigint():
  with pytest.raises(KeyboardInterrupt):
    mp_utils._sig_handler(signal.SIGINT, None)


def test_sig_handler_sigterm():
  with pytest.raises(mp_utils.TerminationError):
    mp_utils._sig_handler(signal.SIGTERM, None)


def test_sig_handler_other_signal_noop():
  # No exception for unrelated signals.
  mp_utils._sig_handler(signal.SIGUSR1, None)


def test_sig_handler_registered_in_setup():
  # _process_setup installs handlers for INT and TERM.
  import py_misc_utils.multiprocessing as m
  saved_int = signal.getsignal(signal.SIGINT)
  saved_term = signal.getsignal(signal.SIGTERM)
  try:
    m._process_setup()
    assert signal.getsignal(signal.SIGINT) is m._sig_handler
    assert signal.getsignal(signal.SIGTERM) is m._sig_handler
  finally:
    signal.signal(signal.SIGINT, saved_int)
    signal.signal(signal.SIGTERM, saved_term)


def test_process_cleanup_ignores_signals():
  import py_misc_utils.multiprocessing as m
  saved_int = signal.getsignal(signal.SIGINT)
  saved_term = signal.getsignal(signal.SIGTERM)
  try:
    m._process_cleanup()
    assert signal.getsignal(signal.SIGINT) == signal.SIG_IGN
    assert signal.getsignal(signal.SIGTERM) == signal.SIG_IGN
  finally:
    signal.signal(signal.SIGINT, saved_int)
    signal.signal(signal.SIGTERM, saved_term)


def test_capture_parent_context_injects_kwargs():
  method = 'fork' if 'fork' in multiprocessing.get_all_start_methods() else 'spawn'
  kwargs = {}
  out = mp_utils._capture_parent_context(method, kwargs)
  assert '_parent_context' in out
  assert isinstance(out['_parent_context'], dict)


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
