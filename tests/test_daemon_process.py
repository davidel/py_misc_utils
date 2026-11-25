"""Tests for py_misc_utils.daemon_process.

Structural tests only — no automated double-fork/pidfile tests (per plan).
Verifies Daemon alias resolves and start/stop/control methods exist.
"""

import os

from py_misc_utils import daemon_process as dp


def test_daemon_alias_exists():
  """Daemon is set to DaemonCompat or DaemonPosix."""
  assert dp.Daemon in (dp.DaemonCompat, dp.DaemonPosix)


def test_daemon_has_start_stop_getpid():
  """Daemon instances expose start(), stop(), getpid()."""
  for meth in ('start', 'stop', 'getpid'):
    assert hasattr(dp.Daemon, meth)


def test_daemon_posix_has_pid_methods():
  """DaemonPosix instances expose PID file methods."""
  assert hasattr(dp.DaemonPosix, 'getpid')
  assert hasattr(dp.DaemonPosix, 'stop')


def test_pids_dir_uses_temp():
  """_get_pids_dir returns a temp-derived directory."""
  d = dp._get_pids_dir()
  assert os.path.isdir(d)
