import hashlib
import os
import psutil
import random
import tempfile
import time
import yaml

from . import alog
from . import assert_checks as tas
from . import fs_utils as fsu
from . import obj
from . import osfd


def _try_lockdir(path):
  if os.path.isdir(path):
    lockdir = os.path.join(path, '.locks')
    try:
      os.makedirs(lockdir, exist_ok=True)

      return lockdir
    except:
      pass


def _find_lockdir():
  path = os.getenv('LOCKSDIR', None)
  if path is not None and (lockdir := _try_lockdir(path)) is not None:
    return lockdir

  if os.name == 'posix':
    # Try known tmpfs/ramfs places in case on Unix.
    for path in ('/dev/shm', '/run/lock'):
      if (lockdir := _try_lockdir(path)) is not None:
        return lockdir

  lockdir = os.path.join(tempfile.gettempdir(), '.locks')
  os.makedirs(lockdir, exist_ok=True)

  return lockdir


_LOCKDIR = _find_lockdir()

def _lockfile(name):
  lhash = hashlib.sha1(name.encode()).hexdigest()

  return os.path.join(_LOCKDIR, lhash)


class Meta(obj.Obj):
  pass


_CMDLINE = list(psutil.Process().cmdline())
_ACQUIRE_TIMEOUT = float(os.getenv('LOCKF_AQTIMEO', 0.5))
_CHECK_TIMEOUT = float(os.getenv('LOCKF_CKTIMEO', 5.0))

class LockFile:

  def __init__(self, name, acquire_timeout=None, check_timeout=None):
    acquire_timeout = _ACQUIRE_TIMEOUT if acquire_timeout is None else acquire_timeout
    check_timeout = _CHECK_TIMEOUT if check_timeout is None else check_timeout

    self._name = name
    self._lockfile = _lockfile(name)
    self._acquire_timeout = random.gauss(mu=acquire_timeout, sigma=0.2)
    self._check_timeout = random.gauss(mu=check_timeout, sigma=0.2)

  def _mkmeta(self):
    tag = dict(pid=os.getpid(), cmdline=_CMDLINE, time=time.time())
    stag = yaml.dump(tag, default_flow_style=False)

    return stag.encode()

  def _parse_meta(self, data):
    if data:
      try:
        tag = yaml.safe_load(data.decode())

        return Meta(**tag)
      except:
        pass

  def _alive_pid(self, meta):
    is_alive = False
    if psutil.pid_exists(meta.pid):
      try:
        proc = psutil.Process(meta.pid)
        cmdline = list(proc.cmdline())

        is_alive = cmdline == meta.cmdline and proc.create_time() <= meta.time
      except psutil.NoSuchProcess:
        pass

    if not is_alive:
      alog.warning(f'Process {meta.pid} ({meta.cmdline}) holding lock on ' \
                   f'{self._name} seems vanished')

    return is_alive

  def acquire(self, timeout=None):
    quit_time = timeout + time.time() if timeout is not None else None
    check_time = time.time() + self._check_timeout
    while True:
      try:
        with osfd.OsFd(self._lockfile, os.O_WRONLY | os.O_CREAT | os.O_EXCL) as fd:
          os.write(fd, self._mkmeta())

        return True
      except OSError:
        time.sleep(self._acquire_timeout)

        now = time.time()
        if now >= check_time:
          check_time = now + self._check_timeout
          meta = self._locking_meta()
          if meta is None or not self._alive_pid(meta):
            if self._try_force_lock(meta):
              return True

        if quit_time is not None and now >= quit_time:
          alog.debug(f'Giving up on lock {self._name} after {timeout} seconds')
          return False

  def release(self):
    meta = self._locking_meta()
    if meta is not None:
      if meta.pid == os.getpid() and meta.cmdline == _CMDLINE:
        try:
          os.remove(self._lockfile)
          return True
        except OSError:
          pass
      else:
        alog.warning(f'Trying to release lock on {self._name} by pid {os.getpid()} but ' \
                     f'it was held by {meta.pid} ({meta.cmdline})')
    else:
      alog.warning(f'Trying to release lock on {self._name} from pid {os.getpid()} but ' \
                   f'no lock metadata was found')

    return False

  def _try_force_lock(self, meta):
    if meta is not None:
      alog.warning(f'Trying to override gone process {meta.pid} on {self._name}')

    upath = f'{self._lockfile}.ENFORCER'
    created = result = False
    try:
      with osfd.OsFd(upath, os.O_WRONLY | os.O_CREAT | os.O_EXCL) as fd:
        created = True

      with osfd.OsFd(self._lockfile, os.O_RDWR) as fd:
        data = fsu.readall(fd)

        lmeta = self._parse_meta(data)
        if lmeta is None or (meta is not None and lmeta == meta):
          os.lseek(fd, 0, os.SEEK_SET)
          os.truncate(fd, 0)
          os.write(fd, self._mkmeta())
          result = True

          alog.info(f'Successfull override on {self._name}')
    except OSError:
      pass
    finally:
      if created:
        os.remove(upath)

    return result

  def _locking_meta(self):
    try:
      with osfd.OsFd(self._lockfile, os.O_RDONLY) as fd:
        data = fsu.readall(fd)

      return self._parse_meta(data)
    except OSError:
      pass

  def __enter__(self):
    self.acquire()

    return self

  def __exit__(self, *exc):
    self.release()

    return False

