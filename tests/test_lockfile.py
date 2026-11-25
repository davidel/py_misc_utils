"""Tests for the py_misc_utils.lockfile file-based mutual exclusion.

Everything here goes through the public API: a LockFile is acquired, released,
or used as a context manager. Lock files live in whatever directory the library
chooses and are named after a hash of the lock name, so no test inspects or
writes the metadata document. Cross-process behaviour is exercised with real
child processes that take the lock and either hold it or die while holding it.

Every lock name is unique to the current run, which keeps tests independent of
each other, of concurrently running test processes and of lock files left
behind by earlier runs, without patching any module state.

Acquire is always paired with a release (directly or as a context manager), so
the tests leave the lock directory as they found it.
"""

import subprocess
import sys
import time
import uuid

import pytest

from py_misc_utils import lockfile as plock

# A child process that takes the lock and holds it until its parent lets go.
_HOLD_LOCK = """
import sys
from py_misc_utils import lockfile as plock

lf = plock.LockFile({name!r}, acquire_timeout=0.01, check_timeout=0.05)
if not lf.acquire(timeout=5):
  sys.exit(2)

print('HELD', flush=True)
sys.stdin.readline()
print('RELEASED' if lf.release() else 'RELEASE-FAILED', flush=True)
"""

# A child process that takes the lock and exits without releasing it, leaving a
# lock file owned by a process that no longer exists.
_DIE_WITH_LOCK = """
import sys
from py_misc_utils import lockfile as plock

lf = plock.LockFile({name!r}, acquire_timeout=0.01, check_timeout=0.05)
if not lf.acquire(timeout=5):
  sys.exit(2)

print('HELD', flush=True)
"""


def _lock(name):
  """Create a LockFile for name with short, test-friendly timeouts."""
  return plock.LockFile(name, acquire_timeout=0.01, check_timeout=0.05)


def _hold(name):
  """Start a child process holding the lock on name and return it."""
  proc = subprocess.Popen(
      [sys.executable, '-c', _HOLD_LOCK.format(name=name)],
      stdin=subprocess.PIPE,
      stdout=subprocess.PIPE,
      text=True)
  assert proc.stdout.readline().strip() == 'HELD'

  return proc


def _stop(proc):
  """Let the holding child release its lock, then reap it."""
  proc.stdin.write('\n')
  proc.stdin.flush()

  assert proc.stdout.readline().strip() == 'RELEASED'
  assert proc.wait() == 0


@pytest.fixture
def lockname():
  """A factory for lock names that are unique to the current test run.

  The library derives the lock file name from a hash of the lock name and keeps
  lock files in one machine-wide directory, so a unique name is enough to make
  a test independent of everything else using that directory.
  """
  return lambda base: f'{base}-{uuid.uuid4().hex}'


def test_acquire_and_release(lockname):
  """acquire() takes the lock and release() gives it back."""
  lf = _lock(lockname('basic'))

  assert lf.acquire(timeout=2)
  assert lf.release()


def test_release_when_not_held_reports_failure(lockname):
  """release() on a lock that is not held reports failure."""
  assert not _lock(lockname('never-acquired')).release()


def test_lock_can_be_reacquired_after_release(lockname):
  """A released lock can be taken again by the same LockFile."""
  lf = _lock(lockname('reacquire'))

  assert lf.acquire(timeout=2)
  assert lf.release()
  assert lf.acquire(timeout=2)
  assert lf.release()


def test_second_instance_cannot_acquire_held_lock(lockname):
  """While the lock is held, another LockFile times out instead of taking it."""
  name = lockname('mutex')
  first = _lock(name)
  second = _lock(name)

  assert first.acquire(timeout=2)
  try:
    assert not second.acquire(timeout=0.3)
  finally:
    assert first.release()


def test_release_by_other_process_is_rejected(lockname):
  """A process that does not own the lock cannot release it."""
  name = lockname('owner')
  proc = _hold(name)
  try:
    assert not _lock(name).release()
  finally:
    _stop(proc)


def test_lock_held_by_live_process_blocks_until_timeout(lockname):
  """A lock held by a live process is not taken before the timeout elapses."""
  name = lockname('live-holder')
  proc = _hold(name)
  try:
    started = time.time()
    assert not _lock(name).acquire(timeout=0.3)
    assert time.time() - started >= 0.3
  finally:
    _stop(proc)


def test_dead_holder_lock_is_overridden(lockname):
  """A lock left behind by a dead process is taken over on acquire()."""
  name = lockname('dead-holder')

  proc = subprocess.Popen(
      [sys.executable, '-c', _DIE_WITH_LOCK.format(name=name)],
      stdout=subprocess.PIPE,
      text=True)
  assert proc.stdout.readline().strip() == 'HELD'
  assert proc.wait() == 0

  lf = _lock(name)
  assert lf.acquire(timeout=5)
  assert lf.release()


def test_context_manager_acquires_and_releases(lockname):
  """Entering the context takes the lock; leaving it gives the lock back."""
  name = lockname('context')

  with _lock(name) as lf:
    assert lf is not None
    assert not _lock(name).acquire(timeout=0.3)

  after = _lock(name)
  assert after.acquire(timeout=2)
  assert after.release()
