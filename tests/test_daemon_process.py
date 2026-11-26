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

DaemonCompat is exercised in-process under spawn (its daemon is a
multiprocessing process and the caller survives start()). The fork ways
- DaemonPosix, which double-forks with raw os.fork(), and DaemonCompat
with the DAEMON_MP_CONTEXT=fork env knob - run in dedicated subprocesses
with a hard timeout, marked legacy_fork: their daemons detach the
standard streams (impossible under the pytest capture pseudofiles),
their fork copies of the caller cannot be unwound safely inside the
test process, and a hang must fail as a timeout instead of stalling the
suite. Either way only the public API is used.
"""

import os
import subprocess
import sys
import tempfile
import time
import uuid

import pytest

from py_misc_utils import daemon_process as dp

# DaemonCompat reads the start method for its daemon process from this
# public env knob at start() time; the suite runs it as spawn so no
# fork start (and no multi-threaded-fork deprecation) ever happens.
os.environ.setdefault('DAEMON_MP_CONTEXT', 'spawn')

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


# The fork ways of doing daemons, run verbatim inside a subprocess with
# real stdio: DaemonPosix (raw os.fork() double-fork) and DaemonCompat
# driven through the DAEMON_MP_CONTEXT=fork public env knob. The script
# talks only to the public API; it prints one KEY=VALUE line per check
# and exits 0 only if all of them hold.
LIFECYCLE_SCRIPT = """
import os
import sys
import time

from py_misc_utils import daemon_process as dp

daemon_class_name = sys.argv[1]
name, marker_path, target_lifetime, kill_timeout = (
    sys.argv[2], sys.argv[3], float(sys.argv[4]), float(sys.argv[5]))

if daemon_class_name == 'compat-fork':
  os.environ['DAEMON_MP_CONTEXT'] = 'fork'
  daemon_class = dp.DaemonCompat
else:
  daemon_class = dp.DaemonPosix


def target():
  with open(marker_path, 'w') as fpt:
    fpt.write(str(os.getpid()))

  time.sleep(target_lifetime)


dae = daemon_class(name)
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

# (daemon_class_name, description): the two fork ways of making daemons.
FORK_MODES = [
    ('posix', 'DaemonPosix raw os.fork() double-fork'),
    ('compat-fork', 'DaemonCompat with DAEMON_MP_CONTEXT=fork'),
]


@pytest.mark.slow
@pytest.mark.legacy_fork
@pytest.mark.parametrize('fork_mode', FORK_MODES, ids=lambda m: m[0])
def test_daemon_fork_lifecycle(posix_cleanup, fork_mode, daemon_name):
  """Fork-based daemons: full start/run/stop lifecycle in a subprocess.

  Parametrized over the two fork ways - raw double-fork (DaemonPosix)
  and the multiprocessing API forced to fork via the public env knob.
  """
  daemon_class_name, why = fork_mode
  marker_path = _marker_path(daemon_name)
  script_dir = os.path.dirname(os.path.abspath(__file__))
  env = dict(os.environ, PYTHONPATH=os.path.dirname(script_dir))

  try:
    proc = subprocess.run([
        sys.executable, '-c', LIFECYCLE_SCRIPT, daemon_class_name, daemon_name,
        marker_path,
        str(TARGET_LIFETIME),
        str(KILL_TIMEOUT)
    ],
                          capture_output=True,
                          text=True,
                          timeout=POSIX_SUBPROCESS_TIMEOUT,
                          env=env,
                          check=True)
  except subprocess.TimeoutExpired:
    pytest.fail(f'{why}: lifecycle hung for {POSIX_SUBPROCESS_TIMEOUT}s')

  report = dict(line.split('=', 1) for line in proc.stdout.splitlines())
  pid = report['PID']

  assert report['MARKER_MATCHES'] == 'True'
  assert report['GETPID'] == pid
  assert report['SECOND_START'] == pid
  assert report['STOPPED'] == 'True'
  assert report['GETPID_AFTER_STOP'] == 'None'
  assert report['STOP_AGAIN'] == 'False'
