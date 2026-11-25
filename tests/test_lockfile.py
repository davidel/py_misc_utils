"""Tests for the py_misc_utils.lockfile file-based mutual exclusion.

Expectations follow the documented API contract: acquire() creates the
lock file with YAML metadata (pid, cmdline, time), a lock held by a live
process blocks other acquirers until timeout, release() removes the lock
only for its owner, a dead holder's lock is forcefully overridden after
the check timeout, and the context manager acquires on enter and releases
on exit.

The lock directory is a per-process temporary folder owned by the library
(tempdir.fastfs_dir(name='.locks')), so the lock files are located through
the public filesystem helpers rather than by patching module state.
"""

import hashlib
import os
import subprocess
import sys
import time

import psutil
import pytest
import yaml

import py_misc_utils.lockfile as plock
import py_misc_utils.tempdir as tmpd
from py_misc_utils import fs_utils as fsu


@pytest.fixture
def lockdir():
  """The library lock directory, as created by the module itself."""
  return tmpd.fastfs_dir(name='.locks')


def _lockpath(lockdir, name):
  return os.path.join(lockdir, hashlib.sha1(name.encode()).hexdigest())


def _write_meta(lpath, pid, mtime=None, cmdline=None):
  """Write lock metadata in the on-disk format the library produces."""
  tag = {
      'pid': pid,
      'cmdline': cmdline if cmdline is not None else list(psutil.Process().cmdline()),
      'time': mtime if mtime is not None else time.time(),
  }
  fsu.write_content(lpath, yaml.dump(tag, default_flow_style=False).encode())


def test_acquire_writes_parseable_meta(lockdir):
  """acquire() creates the lock file and stores owner metadata in it."""
  lf = plock.LockFile('meta-lock', acquire_timeout=0.01, check_timeout=0.05)

  assert lf.acquire(timeout=2)

  lpath = _lockpath(lockdir, 'meta-lock')
  assert os.path.exists(lpath)

  # The lock file holds YAML metadata with pid/cmdline/time: verify via
  # the on-disk format through the public filesystem helpers.
  tag = yaml.safe_load(fsu.read_content(lpath))

  assert tag['pid'] == os.getpid()
  assert isinstance(tag['cmdline'], list)
  assert tag['time'] <= time.time()


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

  with plock.LockFile(name, acquire_timeout=0.01, check_timeout=0.05):
    assert os.path.exists(_lockpath(lockdir, name))
    tag = yaml.safe_load(fsu.read_content(_lockpath(lockdir, name)))
    assert tag['pid'] == os.getpid()

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
  tag = yaml.safe_load(fsu.read_content(lpath))
  assert tag['pid'] == os.getpid()


def test_stale_lock_with_foreign_cmdline_is_overridden(lockdir):
  """A lock whose holder cmdline changed gets forcefully overridden.

  The metadata is rewritten with a bogus cmdline, so acquire() detects
  the stale holder and takes the lock over after the check timeout.
  """
  name = 'foreign-cmdline-lock'
  lpath = _lockpath(lockdir, name)

  _write_meta(lpath,
              pid=os.getpid(),
              mtime=time.time() - 10,
              cmdline=['no', 'such', 'cmdline'])

  lf = plock.LockFile(name, acquire_timeout=0.01, check_timeout=0.05)
  assert lf.acquire(timeout=5)

  tag = yaml.safe_load(fsu.read_content(lpath))
  assert tag['pid'] == os.getpid()
  assert tag['cmdline'] != ['no', 'such', 'cmdline']
