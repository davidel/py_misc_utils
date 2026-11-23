"""Tests for the py_misc_utils.lockfile file-based mutual exclusion.

Expectations follow the documented API contract: acquire() creates the
lock file with YAML metadata (pid, cmdline, time), a lock held by a live
process blocks other acquirers until timeout, release() removes the lock
only for its owner, a dead holder's lock is forcefully overridden after
the check timeout, and the context manager acquires on enter and releases
on exit. The locks directory is redirected to a temporary folder so the
tests never touch the global lock namespace.
"""

import os
import subprocess
import sys
import time

import psutil
import pytest
import yaml

import py_misc_utils.lockfile as plock


@pytest.fixture
def lockdir(tmp_path, monkeypatch):
  """Redirect the module lock directory into a temporary folder."""
  path = str(tmp_path / 'locks')
  os.makedirs(path)
  monkeypatch.setattr(plock, '_LOCKDIR', path)

  return path


def _lockpath(lockdir, name):
  import hashlib
  return os.path.join(lockdir, hashlib.sha1(name.encode()).hexdigest())


def _write_meta(lpath, pid, mtime=None):
  """Write lock metadata in the exact on-disk format _mkmeta() produces."""
  tag = {
      'pid': pid,
      'cmdline': plock._CMDLINE,
      'time': mtime if mtime is not None else time.time(),
  }
  with open(lpath, 'wb') as fd:
    fd.write(yaml.dump(tag, default_flow_style=False).encode())


def test_acquire_writes_parseable_meta(lockdir):
  """acquire() creates the lock file and stores owner metadata in it."""
  lf = plock.LockFile('meta-lock', acquire_timeout=0.01, check_timeout=0.05)

  assert lf.acquire(timeout=2)

  lpath = _lockpath(lockdir, 'meta-lock')
  assert os.path.exists(lpath)

  meta = plock.LockFile('meta-lock')._locking_meta()

  assert meta.pid == os.getpid()
  assert meta.cmdline == plock._CMDLINE
  assert meta.time <= time.time()


def test_release_removes_lock_and_allows_reacquire(lockdir):
  """release() removes the lock file; the lock can be taken again."""
  lf = plock.LockFile('relock', acquire_timeout=0.01, check_timeout=0.05)

  assert lf.acquire(timeout=2)
  assert lf.release()

  assert not os.path.exists(_lockpath(lockdir, 'relock'))
  assert lf.acquire(timeout=2)


def test_release_by_other_process_is_rejected(lockdir):
  """A release from a different owner leaves the lock file in place."""
  lf = plock.LockFile('owner-lock', acquire_timeout=0.01, check_timeout=0.05)

  assert lf.acquire(timeout=2)

  # Simulate a second process holding the lock by rewriting its meta.
  lpath = _lockpath(lockdir, 'owner-lock')
  _write_meta(lpath, pid=os.getpid() + 100000)

  assert not plock.LockFile('owner-lock').release()
  assert os.path.exists(lpath)


def test_acquire_times_out_while_live_process_holds(lockdir):
  """A held-by-live-process lock makes a bounded acquire return False."""
  lf = plock.LockFile('busy-lock', acquire_timeout=0.01, check_timeout=0.05)

  assert lf.acquire(timeout=2)

  lf2 = plock.LockFile('busy-lock', acquire_timeout=0.01, check_timeout=50)

  assert not lf2.acquire(timeout=0.1)

  assert lf.release()


def test_context_manager_acquires_and_releases(lockdir):
  """The with-statement form holds the lock for the block duration."""
  name = 'ctx-lock'

  with plock.LockFile(name, acquire_timeout=0.01, check_timeout=0.05) as lf:
    assert os.path.exists(_lockpath(lockdir, name))
    assert lf._locking_meta().pid == os.getpid()

  assert not os.path.exists(_lockpath(lockdir, name))


def test_stale_lock_from_dead_process_is_overridden(lockdir):
  """A lock whose holder is gone is taken over after the check timeout."""
  name = 'stale-lock'
  lpath = _lockpath(lockdir, name)
  dead_pid = None

  # Spawn a short-lived process and use its (now dead) pid.
  proc = subprocess.Popen([sys.executable, '-c', 'pass'])
  proc.wait()
  dead_pid = proc.pid

  assert not psutil.pid_exists(dead_pid)

  lf = plock.LockFile(name, acquire_timeout=0.01, check_timeout=0.05)
  _write_meta(lpath, pid=dead_pid, mtime=time.time() - 10)

  assert lf.acquire(timeout=5)

  # The lock file now describes the current process.
  assert lf._locking_meta().pid == os.getpid()


def test_alive_pid_rejects_dead_and_foreign_cmdline(lockdir):
  """_alive_pid() is False for dead pids or changed command lines."""
  lf = plock.LockFile('alive-check')
  proc = subprocess.Popen([sys.executable, '-c', 'pass'])
  proc.wait()

  dead_meta = plock.Meta(pid=proc.pid, cmdline=plock._CMDLINE, time=time.time())

  assert not lf._alive_pid(dead_meta)

  live_meta = plock.Meta(pid=os.getpid(),
                         cmdline=['no', 'such', 'cmdline'],
                         time=time.time())

  assert not lf._alive_pid(live_meta)


def test_parse_meta_invalid_data_returns_none():
  """_parse_meta() returns None for empty or unparseable payloads."""
  lf = plock.LockFile('parse-check')

  assert lf._parse_meta(b'') is None
  assert lf._parse_meta(b'\x00\x01not-yaml') is None
