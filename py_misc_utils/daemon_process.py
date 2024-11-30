import atexit
import collections
import logging
import os
import pickle
import signal
import sys
import tempfile
import time

from . import fs_utils as fsu
from . import lockfile as lockf
from . import osfd


DaemonResult = collections.namedtuple(
  'DaemonResult',
  'pid, msg, exclass',
  defaults=(-1, None, None),
)

def _get_pids_dir():
  pidsdir = os.path.join(tempfile.gettempdir(), '.pids')
  os.makedirs(pidsdir, exist_ok=True)

  return pidsdir


_PIDS_DIR = _get_pids_dir()

def _get_pidfile(name):
  return os.path.join(_PIDS_DIR, f'{name}.pid')


def _term_handler(sig, frame):
  sys.exit(sig)


class Daemon:

  def __init__(self, name):
    self._name = name
    self._pidfile = _get_pidfile(name)

  def _write_result(self, wpipe, **kwargs):
    dres = DaemonResult(**kwargs)
    os.write(wpipe, pickle.dumps(dres))

  def _read_result(self, rpipe):
    return pickle.loads(fsu.readall(rpipe))

  def _daemonize(self, refpid=None):
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
      self._writepid(pid, refpid=refpid)

      # Register the signal handlers otherwise atexit callbacks will not get
      # called in case a signal terminates the daemon process.
      signal.signal(signal.SIGINT, _term_handler)
      signal.signal(signal.SIGTERM, _term_handler)
      atexit.register(self._delpid)

      self._write_result(wpipe, pid=pid)

      return 0
    except Exception as ex:
      self._write_result(wpipe, exclass=ex.__class__, msg=f'Daemonize failed: {ex}')
      sys.exit(1)

  def _delpid(self):
    try:
      os.remove(self._pidfile)

      return True
    except OSError:
      return False

  def _writepid(self, pid, refpid=None):
    with self._lockfile():
      flags = os.O_WRONLY | os.O_CREAT
      if refpid is not None:
        pid = self._readpid()
        if pid is not None and pid != refpid:
          raise ProcessLookupError(f'Found existing {pid} PID different from ' \
                                   f'reference PID {refpid}')
      else:
        flags |= os.O_EXCL

      # Use mode=0o660 to make sure only allowed users can access the PID file.
      with osfd.OsFd(self._pidfile, flags, mode=0o660) as fd:
        os.write(fd, f'{pid}\n'.encode())

  def _readpid(self):
    try:
      with osfd.OsFd(self._pidfile, os.O_RDONLY) as fd:
        return int(fsu.readall(fd).strip())
    except IOError:
      pass

  def getpid(self):
    with self._lockfile():
      return self._readpid()

  def _lockfile(self):
    return lockf.LockFile(self._pidfile)

  def _runnning_pid(self, pid):
    try:
      os.killpg(pid, 0)

      return True
    except ProcessLookupError:
      return False

  def is_running(self):
    pid = self.getpid()

    return pid is not None and self._runnning_pid(pid)

  def start(self, target, args=None, kwargs=None):
    pid = self.getpid()
    if pid is None or not self._runnning_pid(pid):
      if (pid := self._daemonize(refpid=pid)) == 0:
        target(*(args or ()), **(kwargs or dict()))
        sys.exit(0)

    return pid

  def stop(self, kill_timeout=None):
    if (pid := self.getpid()) is not None:
      try:
        os.killpg(pid, signal.SIGTERM)
        time.sleep(kill_timeout or 1.0)
        os.killpg(pid, signal.SIGKILL)
      except ProcessLookupError:
        pass

      self._delpid()

      return True

    return False

