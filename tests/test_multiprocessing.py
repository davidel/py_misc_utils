"""Tests for py_misc_utils.multiprocessing (create_process and helpers).

All tests marked slow: they spawn real child processes.

Process-boundary contract: everything crossing a process boundary must
be picklable - target callables, their args, and GNS Var data/defval for
fork_init=False variables. Targets are module-level functions; lambdas
are forbidden under spawn/forkserver (and functools.partial wrapping
does not rescue them, since the pickle walker descends into .args).
"""

import multiprocessing
import os
import signal
import time

import pytest

from py_misc_utils import multiprocessing as mp_utils

# The default start method for the suite: everything crossing a process
# boundary is picklable, so any method works; spawn is the most portable.
DEFAULT_METHOD = 'spawn'


def _noop():
  pass


def _put_ctx_str(q):
  q.put('ctx-str')


def _put_ctx_dict(q):
  q.put('ctx-dict')


def _fn_args_kwargs(q, a, b, c=None):
  q.put((a, b, c))


def _fn_raise_termination():
  raise mp_utils.TerminationError()


def _fn_raise_interrupt():
  raise KeyboardInterrupt()


def _fn_raise_value_error():
  raise ValueError('kaboom')


def _fn_sigterm(q):
  q.put('ready-term')
  time.sleep(30)


def _fn_sigint(q):
  q.put('ready-int')
  time.sleep(30)


def _queue(method):
  return multiprocessing.get_context(method).Queue()


def _start_method_available(method):
  return method in multiprocessing.get_all_start_methods()


@pytest.mark.slow
def test_create_process_returns_unstarted():
  q = _queue(DEFAULT_METHOD)
  p = mp_utils.create_process(_put_ctx_str, args=(q,), context=DEFAULT_METHOD)
  assert isinstance(p, multiprocessing.process.BaseProcess)
  assert p.pid is None
  assert not p.is_alive()
  p.start()
  assert q.get(timeout=10.0) == 'ctx-str'
  p.join(timeout=10.0)
  assert p.exitcode == 0


@pytest.mark.slow
def test_create_process_daemon_flag():
  p = mp_utils.create_process(_noop, daemon=True, context=DEFAULT_METHOD)
  assert p.daemon is True
  p.start()
  p.join(timeout=10.0)
  assert p.exitcode == 0


@pytest.mark.slow
def test_create_process_args_kwargs():
  q = _queue(DEFAULT_METHOD)
  p = mp_utils.create_process(_fn_args_kwargs,
                              args=(q, 1, 2),
                              kwargs={'c': 3},
                              context=DEFAULT_METHOD)
  p.start()
  got = q.get(timeout=10.0)
  p.join(timeout=10.0)
  assert p.exitcode == 0
  assert got == (1, 2, 3)


@pytest.mark.slow
def test_procfn_normal_return_exitcode_zero():
  p = mp_utils.create_process(_noop, context=DEFAULT_METHOD)
  p.start()
  p.join(timeout=10.0)
  assert p.exitcode == 0


@pytest.mark.slow
def test_termination_error_exitcode_one():
  p = mp_utils.create_process(_fn_raise_termination, context=DEFAULT_METHOD)
  p.start()
  p.join(timeout=10.0)
  assert p.exitcode == 1


@pytest.mark.slow
def test_keyboard_interrupt_exitcode_one():
  p = mp_utils.create_process(_fn_raise_interrupt, context=DEFAULT_METHOD)
  p.start()
  p.join(timeout=10.0)
  assert p.exitcode == 1


@pytest.mark.slow
def test_generic_exception_nonzero_exit():
  p = mp_utils.create_process(_fn_raise_value_error, context=DEFAULT_METHOD)
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
  q1 = _queue(DEFAULT_METHOD)
  p1 = mp_utils.create_process(_fn_sigterm, args=(q1,), context=DEFAULT_METHOD)
  p1.start()
  assert q1.get(timeout=10.0) == 'ready-term'
  os.kill(p1.pid, signal.SIGTERM)
  p1.join(timeout=10.0)
  assert p1.exitcode == 1

  q2 = _queue(DEFAULT_METHOD)
  p2 = mp_utils.create_process(_fn_sigint, args=(q2,), context=DEFAULT_METHOD)
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
  q = _queue(DEFAULT_METHOD)
  ctx = multiprocessing.get_context(DEFAULT_METHOD)
  p = mp_utils.create_process(_put_ctx_dict, args=(q,), context=ctx)
  p.start()
  got = q.get(timeout=10.0)
  p.join(timeout=10.0)
  assert p.exitcode == 0
  assert got == 'ctx-dict'


@pytest.mark.legacy_fork
@pytest.mark.slow
def test_context_string_creates_process_fork():
  """A context passed as a start-method string is honored."""
  if not _start_method_available('fork'):
    pytest.skip('fork not available')
  q = _queue('fork')
  p = mp_utils.create_process(_put_ctx_str, args=(q,), context='fork')
  p.start()
  got = q.get(timeout=10.0)
  p.join(timeout=10.0)
  assert p.exitcode == 0
  assert got == 'ctx-str'


@pytest.mark.slow
def test_context_string_creates_process_spawn():
  """A context passed as a start-method string is honored."""
  q = _queue('spawn')
  p = mp_utils.create_process(_put_ctx_str, args=(q,), context='spawn')
  p.start()
  got = q.get(timeout=10.0)
  p.join(timeout=10.0)
  assert p.exitcode == 0
  assert got == 'ctx-str'
