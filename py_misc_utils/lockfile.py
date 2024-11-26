import os
import psutil
import random
import socket
import time
import yaml

from . import alog
from . import assert_checks as tas
from . import obj
from . import osfd


_CMDLINE = list(psutil.Process().cmdline())

class LockFile:

  MAX_META_SIZE = 128 * 1024

  def __init__(self, path, acquire_timeout=0.5, check_timeout=2.5):
    self._path = path
    self._acquire_timeout = random.gauss(mu=acquire_timeout, sigma=0.2)
    self._check_timeout = random.gauss(mu=check_timeout, sigma=0.2)

  def _tag(self):
    tag = dict(pid=os.getpid(),
               cmdline=_CMDLINE,
               time=time.time(),
               hostname=socket.gethostname())
    stag = yaml.dump(tag, default_flow_style=False)

    return stag.encode()

  def _untag(self, data):
    if data:
      try:
        tag = yaml.safe_load(data.decode())

        return obj.Obj(**tag)
      except:
        pass

  def _alive_pid(self, meta):
    if psutil.pid_exists(meta.pid):
      try:
        proc = psutil.Process(meta.pid)
        cmdline = list(proc.cmdline())

        is_alive = (cmdline == meta.cmdline and proc.create_time() <= meta.time and
                    socket.gethostname() == meta.hostname)
      except:
        is_alive = False

    if not is_alive:
      alog.warning(f'Process {meta.pid} ({meta.cmdline}) holding lock on ' \
                   f'{self._path} seems vanished')

    return is_alive

  def acquire(self, timeout=None):
    quit_time = timeout + time.time() if timeout is not None else None
    check_time = time.time() + self._check_timeout
    while True:
      try:
        with osfd.OsFd(self._path, os.O_WRONLY | os.O_CREAT | os.O_EXCL) as fd:
          os.write(fd, self._tag())

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
          alog.debug(f'Giving up on lock {self._path} after {timeout} seconds')
          return False

  def release(self):
    meta = self._locking_meta()
    if meta is None or (meta.pid == os.getpid() and meta.cmdline == _CMDLINE):
      try:
        os.remove(self._path)
        return True
      except OSError:
        pass
    else:
      alog.warning(f'Trying to release lock on {self._path} by pid {os.getpid()} but ' \
                   f'it was held by {meta.pid}')

    return False

  def _try_force_lock(self, meta):
    alog.warning(f'Trying to override gone process {meta.pid if meta else "??"} on {self._path}')

    upath = f'{self._path}.REMOVER'
    created = result = False
    try:
      with osfd.OsFd(upath, os.O_WRONLY | os.O_CREAT | os.O_EXCL) as fd:
        created = True

      with osfd.OsFd(self._path, os.O_RDWR) as fd:
        data = os.read(fd, self.MAX_META_SIZE)

        lmeta = self._untag(data)
        if lmeta is None or (meta is not None and lmeta.pid == meta.pid):
          os.lseek(fd, 0, os.SEEK_SET)
          os.truncate(fd, 0)
          os.write(fd, self._tag())
          result = True

          alog.info(f'Successfull override on {self._path}')
    except OSError:
      pass
    finally:
      if created:
        os.remove(upath)

    return result

  def _locking_meta(self):
    try:
      with osfd.OsFd(self._path, os.O_RDONLY) as fd:
        data = os.read(fd, self.MAX_META_SIZE)

      return self._untag(data)
    except OSError:
      pass

  def __enter__(self):
    self.acquire()

    return self

  def __exit__(self, *exc):
    self.release()

    return False

