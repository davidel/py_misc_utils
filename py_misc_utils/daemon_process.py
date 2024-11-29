import atexit
import logging
import os
import signal
import sys
import tempfile
import time


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

  def __init__(self, name, target, args=None, kwargs=None):
    self._name = name
    self._target = target
    self._args = tuple(args) if args else ()
    self._kwargs = kwargs.copy() if kwargs else dict()
    self._pidfile = _get_pidfile(name)

  def _write_result(self, wpipe, pid, msg):
    result = f'{pid}\n{msg}\n'
    os.write(wpipe, result.encode())

  def _read_result(self, rpipe):
    result = os.read(rpipe, 16 * 1024)
    lines = result.decode().split('\n')

    return int(lines[0]), '\n'.join(lines[1:])

  def _daemonize(self):
    rpipe, wpipe = os.pipe()
    os.set_inheritable(wpipe, True)

    pid = os.fork()
    if pid > 0:
      dpid, exmsg = self._read_result(rpipe)
      if dpid < 0:
        raise RuntimeError(exmsg)

      return dpid

    try:
      os.set_inheritable(wpipe, True)

      os.chdir('/')
      os.setsid()
      os.umask(0)

      pid = os.fork()
      if pid > 0:
        sys.exit(0)

      os.setsid()

      sys.stdout.flush()
      sys.stderr.flush()
      si = open(os.devnull, mode='r')
      so = open(os.devnull, mode='a+')
      se = open(os.devnull, mode='a+')

      os.dup2(si.fileno(), sys.stdin.fileno())
      os.dup2(so.fileno(), sys.stdout.fileno())
      os.dup2(se.fileno(), sys.stderr.fileno())

      # Register the signal handlers otherwise atexit callbacks will not get
      # called in case a signal terminates the daemon process.
      signal.signal(signal.SIGINT, _term_handler)
      signal.signal(signal.SIGTERM, _term_handler)
      atexit.register(self._delpid)

      # Use mode=0o660 to make sure only allowed users can access the PID file.
      pid = os.getpid()
      fd = os.open(self._pidfile, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode=0o660)
      os.write(fd, f'{pid}\n'.encode())
      os.close(fd)

      self._write_result(wpipe, pid, f'OK')

      return 0
    except Exception as ex:
      self._write_result(wpipe, -1, f'Daemonize failed: {ex}')
      sys.exit(1)

  def _delpid(self):
    try:
      os.remove(self._pidfile)

      return True
    except OSError:
      return False

  def getpid(self):
    try:
      with open(self._pidfile, mode='rb') as fd:
        return int(fd.read().strip())
    except IOError:
      pass

  def _runnning_pid(self, pid):
    try:
      os.killpg(pid, 0)

      return True
    except ProcessLookupError:
      return False

  def is_running(self):
    if (pid := self.getpid()) is not None:
      return self._runnning_pid(pid)

    return False

  def start(self):
    if (pid := self.getpid()) is not None and self._runnning_pid(pid):
      raise RuntimeError(f'Daemon "{self._name}" ({pid}) already exist. Already running?')

    if (pid := self._daemonize()) == 0:
      self._target(*self._args, **self._kwargs)
      sys.exit(0)

    return pid

  def stop(self):
    if (pid := self.getpid()) is not None:
      try:
        while True:
          os.killpg(pid, signal.SIGTERM)
          time.sleep(0.2)
      except ProcessLookupError:
        pass

      self._delpid()

      return True

    return False

  def restart(self):
    self.stop()
    self.start()

