"""Daemon process management.

This module provides the machinery to start and stop long-running
background processes (daemons): a process is identified by name, holding
a PID file - protected by a lock file - and its start can be performed
either by double-forking (DaemonPosix) or via the multiprocessing API
(DaemonCompat).

The Daemon alias selects the implementation - defaulting to the
multiprocessing one where available - and can be forced to the posix
one via the DAEMON_MODE environment variable.

Example:
  import time
  import py_misc_utils.daemon_process as dp

  d = dp.Daemon('/tmp/demo_b5_daemon')

  def body():
    time.sleep(2)

  pid = d.start(body)
  assert d.getpid() == pid
  assert d.stop() is True
  assert d.getpid() is None
  # Stopping an already-stopped daemon is not an error: returns False.
  assert d.stop() is False
"""
import atexit
import collections
import functools
import multiprocessing
import os
import pickle
import signal
import sys
import tempfile
import time

import psutil

from . import fs_utils as fsu
from . import lockfile as lockf
from . import osfd
from . import packet as pkt

DaemonResult = collections.namedtuple(
    'DaemonResult',
    'pid, msg, exclass',
    defaults=(-1, None, None),
)


def _get_pids_dir():
  """Returns the folder holding the daemon PID files."""
  pidsdir = os.path.join(tempfile.gettempdir(), '.pids')
  os.makedirs(pidsdir, exist_ok=True)

  return pidsdir


_PIDS_DIR = _get_pids_dir()


def _get_pidfile(name):
  """Returns the PID file path of a named daemon."""
  return os.path.join(_PIDS_DIR, f'{name}.pid')


def _term_handler(sig, frame):
  """Terminates the daemon process upon SIGINT/SIGTERM delivery."""
  sys.exit(sig)


class DaemonBase:
  """A base class implementing the daemon PID file management.

  The PID file - locked via a lock file - holds the PID of the running
  daemon, and it is removed when the daemon stops.
  """

  def __init__(self, name):
    """Creates a daemon manager.

    Args:
      name: The daemon name - identifying its PID file.
    """
    self._name = name
    self._pidfile = _get_pidfile(name)

  def _write_result(self, wpipe, **kwargs):
    """Writes a daemon-start result over a pipe."""
    dres = DaemonResult(**kwargs)
    pkt.write_packet(wpipe, pickle.dumps(dres))

  def _read_result(self, rpipe):
    """Reads a daemon-start result from a pipe."""
    return pickle.loads(pkt.read_packet(rpipe))

  def _delpid(self, pid=None):
    """Removes the daemon PID file, if the recorded PID matches.

    Args:
      pid: An optional PID the PID file must hold to be removed.

    Returns:
      True if the PID file has been removed.
    """
    with self._lockfile():
      if pid is None or self._readpid() == pid:
        try:
          os.remove(self._pidfile)

          return True
        except OSError:
          pass

      return False

  def _writepid(self, pid):
    """Records the daemon PID into the PID file.

    Args:
      pid: The daemon PID.

    Raises:
      FileExistsError: If a running daemon is already recorded in the
        PID file.
    """
    with self._lockfile():
      xpid = self._readpid()
      if xpid is not None:
        if self._runnning_pid(xpid):
          raise FileExistsError(f'Daemon already running with PID {xpid}')

        os.remove(self._pidfile)

      # Use mode=0o660 to make sure only allowed users can access the PID file.
      with osfd.OsFd(self._pidfile, os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                     mode=0o660) as fd:
        os.write(fd, f'{pid}\n'.encode())

  def _readpid(self):
    """Reads the daemon PID recorded in the PID file - if any."""
    try:
      with osfd.OsFd(self._pidfile, os.O_RDONLY) as fd:
        return int(fsu.readall(fd).strip())
    except OSError:
      pass

  def getpid(self):
    """Returns the PID of the running daemon - if any.

    Returns:
      The daemon PID, or None if the daemon is not running.
    """
    with self._lockfile():
      pid = self._readpid()

      return pid if pid is None or self._runnning_pid(pid) else None

  def _lockfile(self):
    """Returns the lock file protecting the PID file."""
    return lockf.LockFile(self._pidfile)

  def _runnning_pid(self, pid):
    """Checks whether a PID belongs to a live - non-zombie - process."""
    try:
      proc = psutil.Process(pid)

      return proc.status() not in {psutil.STATUS_DEAD, psutil.STATUS_ZOMBIE}
    except psutil.NoSuchProcess:
      return False

  def _killpid(self, pid, kill_timeout=None):
    """Terminates a process, killing it if it does not exit in time.

    Args:
      pid: The PID of the process to terminate.
      kill_timeout: An optional number of seconds to wait between the
        terminate() and kill() calls - 1 second by default.
    """
    try:
      proc = psutil.Process(pid)
      proc.terminate()
      time.sleep(kill_timeout or 1.0)
      proc.kill()
    except psutil.NoSuchProcess:
      pass

  def _setup_daemon(self, wpipe):
    """Sets up the daemon process context - and records its PID.

    Detaches the process from the controlling terminal, redirects
    stdin/stdout/stderr to /dev/null, registers the PID file cleanup
    upon exit, and reports the daemon PID back over the pipe.

    Args:
      wpipe: The pipe descriptor the daemon-start result is written to.
    """
    # This 2nd os.setsid() makes the daemon a process group, so with can kill the
    # whole group, if required. We do this only on systems that supports it.
    if os.name == 'posix':
      os.setsid()

    sys.stdout.flush()
    sys.stderr.flush()

    infd = os.open(os.devnull, os.O_RDONLY)
    outfd = os.open(os.devnull, os.O_WRONLY | os.O_APPEND)
    errfd = os.open(os.devnull, os.O_WRONLY | os.O_APPEND)

    os.dup2(infd, sys.stdin.fileno())
    os.dup2(outfd, sys.stdout.fileno())
    os.dup2(errfd, sys.stderr.fileno())

    pid = os.getpid()
    self._writepid(pid)

    # Register the signal handlers otherwise atexit callbacks will not get
    # called in case a signal terminates the daemon process.
    signal.signal(signal.SIGINT, _term_handler)
    signal.signal(signal.SIGTERM, _term_handler)
    atexit.register(functools.partial(self._delpid, pid=pid))

    self._write_result(wpipe, pid=pid)

  def stop(self, kill_timeout=None):
    """Stops the running daemon - if any.

    Args:
      kill_timeout: An optional number of seconds passed to _killpid().

    Returns:
      True if a running daemon has been stopped, False when no daemon was
      running (in which case calling stop() again is not an error).
    """
    with self._lockfile():
      pid = self._readpid()
      if pid is not None:
        self._killpid(pid, kill_timeout=kill_timeout)

    self._delpid(pid=pid)

    return pid is not None


class DaemonPosix(DaemonBase):
  """A daemon manager based on the classic posix double-fork approach."""

  def _daemonize(self):
    """Double-forks the process to create the daemon.

    The parent waits for the daemonization result over a pipe, raising
    an error if the daemon setup failed.

    Returns:
      The daemon PID in the parent process, 0 in the daemon process.
    """
    rpipe, wpipe = os.pipe()
    os.set_inheritable(wpipe, True)

    pid = os.fork()
    if pid > 0:
      dres = self._read_result(rpipe)
      if dres.pid < 0:
        raise dres.exclass(dres.msg)

      return dres.pid

    try:
      os.chdir('/')
      os.setsid()
      os.umask(0)

      pid = os.fork()
      if pid > 0:
        sys.exit(0)

      self._setup_daemon(wpipe)

      return 0
    except Exception as ex:  # noqa: BLE001 -- daemonize error tunnel: child reports any failure to parent via wpipe before exiting
      self._write_result(wpipe, exclass=ex.__class__, msg=f'Daemonize failed: {ex}')
      sys.exit(1)

  def start(self, target):
    """Starts the daemon, running a target function.

    Args:
      target: The function to be executed by the daemon process.

    Returns:
      The daemon PID in the parent process, 0 in the daemon process - in
      which case target() is executed before exiting.
    """
    pid = self.getpid()
    if pid is None and (pid := self._daemonize()) == 0:
      target()
      sys.exit(0)

    return pid


class DaemonCompat(DaemonBase):
  """A daemon manager based on the multiprocessing API.

  Used on systems where the multiprocessing process bookkeeping can be
  altered to prevent the daemon from being killed at parent exit - and
  therefore needs no double-fork.
  """

  def _write_result(self, wpipe, **kwargs):
    """Writes a daemon-start result over a multiprocessing pipe."""
    dres = DaemonResult(**kwargs)
    wpipe.send(pickle.dumps(dres))

  def _read_result(self, rpipe):
    """Reads a daemon-start result from a multiprocessing pipe."""
    return pickle.loads(rpipe.recv())

  def _boostrap(self, target, wpipe):
    """The daemon process entry point.

    Sets up the daemon context and runs the target function, exiting the
    process upon completion.

    Args:
      target: The function to be executed by the daemon process.
      wpipe: The pipe descriptor the daemon-start result is written to.
    """
    try:
      os.chdir('/')
      if os.name == 'posix':
        os.umask(0)

      self._setup_daemon(wpipe)
    except Exception as ex:  # noqa: BLE001 -- daemonize error tunnel: child reports any failure to parent via wpipe before exiting
      self._write_result(wpipe, exclass=ex.__class__, msg=f'Daemonize failed: {ex}')
      sys.exit(1)

    target()
    sys.exit(0)

  def _start_daemon(self, target):
    """Starts the daemon process via the multiprocessing API.

    The daemon process is removed from the parent multiprocessing child
    bookkeeping, so that it is not killed at parent exit.

    Args:
      target: The function to be executed by the daemon process.

    Returns:
      The daemon PID.
    """
    mps = multiprocessing.get_context(method=os.getenv('DAEMON_MP_CONTEXT'))
    rpipe, wpipe = mps.Pipe()
    proc = mps.Process(target=self._boostrap, args=(target, wpipe))
    proc.start()

    dres = self._read_result(rpipe)
    if dres.pid < 0:
      raise dres.exclass(dres.msg)

    assert dres.pid == proc.pid

    # HACK!
    # This removes the daemon process from the list of known child processes, preventing
    # it to be forcibly killed at exit.
    multiprocessing.process._children.discard(proc)

    return dres.pid

  def start(self, target):
    """Starts the daemon, running a target function.

    If a daemon is already running for this name - per the PID file - the
    existing one is left alone and its PID is returned.

    Args:
      target: The function to be executed by the daemon process.

    Returns:
      The daemon PID.
    """
    pid = self.getpid()
    if pid is None:
      pid = self._start_daemon(target)

    return pid


# Probe of a private multiprocessing internal - shape varies across Python
# versions, so any surprise must fall back to the conservative DaemonPosix.
try:
  HAS_MP_CHILDREN = isinstance(multiprocessing.process._children, set)
except:  # noqa: E722
  HAS_MP_CHILDREN = False

if HAS_MP_CHILDREN and os.getenv('DAEMON_MODE') != 'posix':
  Daemon = DaemonCompat
else:
  Daemon = DaemonPosix
