"""Tests for py_misc_utils.daemon_process.

Expectations pinned to the observed behavior (verified by probe scripts
before writing): the Daemon alias resolves to DaemonCompat when the
multiprocessing child bookkeeping is a set (CPython) - the documented
'multiprocessing API where available' contract. start() runs the target
in a background daemon process and returns its PID without terminating
the caller; a second start() while running returns the same PID;
stop() terminates the daemon (SIGTERM, then SIGKILL after kill_timeout)
and removes the PID file, so getpid() goes back to None and a further
stop() is a no-op returning False.

DaemonCompat is exercised in-process: its daemon is a multiprocessing
process and the caller survives start(). DaemonPosix is exercised in a
dedicated subprocess with a hard timeout: its daemonization redirects
the standard streams (impossible under the pytest capture pseudofiles)
and its raw os.fork() copy of the caller cannot be unwound safely
inside the test process. Either way only the public API is used.
"""

import os
import subprocess
import sys
import tempfile
import time
import uuid

import pytest

from py_misc_utils import daemon_process as dp

# How long the daemon target stays alive before exiting on its own -
# an upper bound only: tests stop() the daemon well before this.
TARGET_LIFETIME = 30.0
# Grace period handed to stop(): long enough for SIGTERM to be honored,
# short enough to keep the suite fast (SIGKILL follows).
KILL_TIMEOUT = 2.0
# The daemon writes its marker file quickly; allow scheduling slack.
MARKER_TIMEOUT = 5.0
# Hard timeout for the whole DaemonPosix lifecycle subprocess: a hang
# fails as a timeout instead of hanging the suite.
POSIX_SUBPROCESS_TIMEOUT = 30.0


class _Target:
  """A daemon target factory.

  DaemonCompat.start() sends the target through multiprocessing, so it
  must be picklable - a module-level class with a module-level callable
  shape keeps that contract without hidden state. When run, it records
  its own PID into a marker file and stays alive until stopped.
  """

  def __init__(self, marker_path):
    self._marker_path = marker_path

  def __call__(self):
    with open(self._marker_path, 'w') as fpt:
      fpt.write(str(os.getpid()))

    time.sleep(TARGET_LIFETIME)


def _marker_path(name):
  return os.path.join(tempfile.gettempdir(), f'{name}.marker')


def _wait_for_marker(path):
  """Waits until the marker file shows up, returning its content."""
  deadline = time.monotonic() + MARKER_TIMEOUT
  while time.monotonic() < deadline:
    try:
      with open(path) as fpt:
        return fpt.read()
    except OSError:
      time.sleep(0.05)

  raise AssertionError(f'marker file never appeared: {path}')


@pytest.fixture
def daemon_name():
  """A unique daemon name, so PID files never collide across tests."""
  return f'test-daemon-{uuid.uuid4().hex}'


@pytest.fixture
def compat_daemon(daemon_name):
  """A DaemonCompat manager over a unique name.

  Any daemon left running by a test is stopped on teardown.
  """
  dae = dp.DaemonCompat(daemon_name)
  yield dae
  dae.stop(kill_timeout=KILL_TIMEOUT)
  try:
    os.remove(_marker_path(daemon_name))
  except OSError:
    pass


@pytest.fixture
def posix_cleanup(daemon_name):
  """Cleans up after the DaemonPosix lifecycle subprocess.

  If the subprocess died mid-run, its daemon may still be alive under
  the test name: stopping it through the public API is idempotent and
  covers both the running and the already-stopped cases.
  """
  yield
  dp.DaemonPosix(daemon_name).stop(kill_timeout=KILL_TIMEOUT)
  try:
    os.remove(_marker_path(daemon_name))
  except OSError:
    pass


def test_start_returns_pid_and_runs_target(compat_daemon, daemon_name):
  """start() returns the daemon PID and runs the target in it."""
  target = _Target(_marker_path(daemon_name))

  pid = compat_daemon.start(target)

  assert isinstance(pid, int)
  assert pid == compat_daemon.getpid()
  assert _wait_for_marker(_marker_path(daemon_name)) == str(pid)


def test_start_is_idempotent_when_running(compat_daemon, daemon_name):
  """A second start() while running returns the same PID."""
  target = _Target(_marker_path(daemon_name))
  pid1 = compat_daemon.start(target)

  pid2 = compat_daemon.start(target)

  assert pid2 == pid1


def test_stop_terminates_and_clears_pidfile(compat_daemon, daemon_name):
  """stop() terminates the daemon and resets the manager state."""
  target = _Target(_marker_path(daemon_name))
  pid = compat_daemon.start(target)
  assert _wait_for_marker(_marker_path(daemon_name)) == str(pid)

  assert compat_daemon.stop(kill_timeout=KILL_TIMEOUT) is True
  assert compat_daemon.getpid() is None
  assert compat_daemon.stop(kill_timeout=KILL_TIMEOUT) is False


def test_getpid_none_before_start(daemon_name):
  """getpid() reports no daemon before anything is started."""
  dae = dp.DaemonCompat(daemon_name)

  assert dae.getpid() is None


def test_daemon_alias_selects_compat_on_cpython():
  """On CPython bookkeeping, the default alias is DaemonCompat."""
  if not dp.HAS_MP_CHILDREN:
    pytest.skip('requires multiprocessing child bookkeeping (CPython)')

  assert dp.Daemon is dp.DaemonCompat


# The DaemonPosix lifecycle, run verbatim inside a subprocess with real
# stdio. The script talks only to the public API; it prints one
# KEY=VALUE line per check and exits 0 only if all of them hold.
POSIX_SCRIPT = """
import os
import sys
import time

from py_misc_utils import daemon_process as dp

name, marker_path, target_lifetime, kill_timeout = (
    sys.argv[1], sys.argv[2], float(sys.argv[3]), float(sys.argv[4]))


def target():
  with open(marker_path, 'w') as fpt:
    fpt.write(str(os.getpid()))

  time.sleep(target_lifetime)


dae = dp.DaemonPosix(name)
pid = dae.start(target)
print(f'PID={pid}')

with open(marker_path) as fpt:
  marker_pid = fpt.read()
print(f'MARKER_MATCHES={marker_pid == str(pid)}')
print(f'GETPID={dae.getpid()}')
print(f'SECOND_START={dae.start(target)}')

print(f'STOPPED={dae.stop(kill_timeout=kill_timeout)}')
print(f'GETPID_AFTER_STOP={dae.getpid()}')
print(f'STOP_AGAIN={dae.stop(kill_timeout=kill_timeout)}')
"""


@pytest.mark.slow
def test_daemon_posix_lifecycle(posix_cleanup, daemon_name):
  """DaemonPosix: full start/run/stop lifecycle in a subprocess."""
  marker_path = _marker_path(daemon_name)
  script_dir = os.path.dirname(os.path.abspath(__file__))
  env = dict(os.environ, PYTHONPATH=os.path.dirname(script_dir))

  try:
    proc = subprocess.run([
        sys.executable, '-c', POSIX_SCRIPT, daemon_name, marker_path,
        str(TARGET_LIFETIME),
        str(KILL_TIMEOUT)
    ],
                          capture_output=True,
                          text=True,
                          timeout=POSIX_SUBPROCESS_TIMEOUT,
                          env=env,
                          check=True)
  except subprocess.TimeoutExpired:
    pytest.fail(f'DaemonPosix lifecycle hung for {POSIX_SUBPROCESS_TIMEOUT}s')

  report = dict(line.split('=', 1) for line in proc.stdout.splitlines())
  pid = report['PID']

  assert report['MARKER_MATCHES'] == 'True'
  assert report['GETPID'] == pid
  assert report['SECOND_START'] == pid
  assert report['STOPPED'] == 'True'
  assert report['GETPID_AFTER_STOP'] == 'None'
  assert report['STOP_AGAIN'] == 'False'
