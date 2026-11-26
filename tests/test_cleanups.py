"""Tests for py_misc_utils.cleanups.

Cleanup registration targets a process-global registry, and run() has
process-wide side effects - most notably it executes the tempdir root
rmtree registration, which is only supposed to happen at process exit.
Therefore the run()-related semantics are exercised in a subprocess,
isolating the global registry. In-process tests only use API paths with
no global side effects: register/unregister mechanics and the reg()
decorator. Note that unregister(cid, run=True) executes only the removed
cleaner, so it is side-effect safe.
"""

import os
import subprocess
import sys

from py_misc_utils import cleanups

_TOP = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Child script exercising run(): registration order, argument forwarding,
# reverse execution order, exception containment, and the drain-on-run
# semantic (a second run() must be a no-op).
_RUN_SCRIPT = '''
import sys
sys.path.insert(0, '%TOP%')

from py_misc_utils import cleanups


def record(label):
  def _fn(*args, **kwargs):
    print('RESULT ' + label + ' args=' + repr(args) +
          ' kwargs=' + repr(kwargs), flush=True)

  return _fn


def boom():
  raise RuntimeError('cleanup failure')


cleanups.register(record('A'))
cleanups.register(record('B'), 1, 2, k=3)
cleanups.register(boom)
cleanups.register(record('C'))

cleanups.run()

# The registry is drained on run(): a second run() must be a no-op.
cleanups.run()
'''

# Child script verifying the reg() decorator registers its function.
_REG_SCRIPT = '''
import sys
sys.path.insert(0, '%TOP%')

from py_misc_utils import cleanups


@cleanups.reg
def cleanup():
  print('RESULT DECORATED', flush=True)


cleanups.run()

print('AFTER-RUN', flush=True)
'''

# Child script checking that a failing cleanup does not abort the run.
_EXC_SCRIPT = '''
import sys
sys.path.insert(0, '%TOP%')

from py_misc_utils import cleanups


def boom():
  raise RuntimeError('cleanup failure')


def record():
  print('RESULT AFTER-BOOM', flush=True)


cleanups.register(boom)
cleanups.register(record)

cleanups.run()

print('EXIT-OK', flush=True)
'''


def _run_child(script):
  """Runs a child script and returns the completed process."""
  return subprocess.run(
      [sys.executable, '-c', script.replace('%TOP%', _TOP)],
      capture_output=True,
      check=False,
      text=True)


def test_register_returns_int_id():
  cid = cleanups.register(lambda: None)

  try:
    assert isinstance(cid, int)
  finally:
    cleanups.unregister(cid)


def test_unregister_returns_cleaner():
  calls = []
  cid = cleanups.register(lambda a, k=None: calls.append((a, k)), 1, k=2)

  cleaner = cleanups.unregister(cid)

  assert cleaner is not None
  assert cleaner.fn(1, k=2) is None  # The fn returns None; the call recorded.
  assert calls == [(1, 2)]
  assert cleaner.args == (1,)
  assert cleaner.kwargs == {'k': 2}

  # The cleanup was removed: it cannot be unregistered again.
  assert cleanups.unregister(cid) is None


def test_unregister_unknown_id_returns_none():
  assert cleanups.unregister(424242) is None


def test_unregister_run_true_executes_own_cleanup_once():
  calls = []
  cid = cleanups.register(lambda: calls.append('ran'))

  try:
    cleaner = cleanups.unregister(cid, run=True)

    assert cleaner is not None
    assert calls == ['ran']
  finally:
    # Already removed by the run=True unregister; nothing left to clean.
    assert cleanups.unregister(cid) is None


def test_reg_decorator_registers():
  proc = _run_child(_REG_SCRIPT)

  assert proc.returncode == 0, proc.stderr
  assert 'RESULT DECORATED' in proc.stdout
  assert 'AFTER-RUN' in proc.stdout


def test_run_order_args_and_drain():
  proc = _run_child(_RUN_SCRIPT)

  assert proc.returncode == 0, proc.stderr
  results = [line for line in proc.stdout.splitlines() if line.startswith('RESULT ')]
  # Reverse registration order; the failing cleanup did not stop the run.
  assert results == [
      "RESULT C args=() kwargs={}",
      "RESULT B args=(1, 2) kwargs={'k': 3}",
      "RESULT A args=() kwargs={}",
  ]


def test_run_contains_exceptions():
  proc = _run_child(_EXC_SCRIPT)

  assert proc.returncode == 0, proc.stderr
  assert 'RESULT AFTER-BOOM' in proc.stdout
  assert 'EXIT-OK' in proc.stdout
  assert 'Exception while running cleanups' in proc.stderr
