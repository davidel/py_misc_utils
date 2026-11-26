"""File-based inter-process locking with timeout and force-release support.

This module provides the :class:`LockFile` class which implements a cross-process
mutex using filesystem lock files. It supports configurable acquire and check
timeouts, automatic stale-lock detection (via process liveness checks), and
forceful override of locks held by dead processes. The lock metadata includes
the PID, command line, and acquisition timestamp.
"""

import hashlib
import os
import time

import psutil
import yaml

from . import alog, obj, osfd
from . import fs_utils as fsu
from . import rnd_utils as rngu
from . import tempdir as tmpd

_LOCKDIR = tmpd.fastfs_dir(name='.locks')


def _lockfile(name):
  """Compute the filesystem path for a lock file given a logical lock name.

  Args:
    name: The logical name of the lock.

  Returns:
    The full path to the lock file inside the locks directory.
  """
  lhash = hashlib.sha1(name.encode()).hexdigest()

  return os.path.join(_LOCKDIR, lhash)


class Meta(obj.Obj):
  """Simple metadata container holding lock owner information.

  Attributes are populated from a YAML document and typically include pid,
  cmdline, and time fields describing the process that holds the lock.
  """


_CMDLINE = list(psutil.Process().cmdline())
_ACQUIRE_TIMEOUT = float(os.getenv('LOCKF_AQTIMEO', 0.5))
_CHECK_TIMEOUT = float(os.getenv('LOCKF_CKTIMEO', 5.0))


class LockFile:
  """A file-based mutual exclusion lock with timeout and stale-lock recovery.

  Uses O_CREAT | O_EXCL to atomically create a lock file. If the lock is held
  by a process that no longer exists or whose command line has changed, the
  lock can be forcefully overridden after a check timeout interval.
  """

  def __init__(self, name, acquire_timeout=None, check_timeout=None):
    """Initialize a LockFile instance.

    Args:
      name: The logical name of the lock.
      acquire_timeout: Optional base sleep interval between acquire attempts.
        Defaults to the LOCKF_AQTIMEO environment variable or 0.5 seconds.
      check_timeout: Optional interval between stale-lock checks.
        Defaults to the LOCKF_CKTIMEO environment variable or 5.0 seconds.
    """
    acquire_timeout = _ACQUIRE_TIMEOUT if acquire_timeout is None else acquire_timeout
    check_timeout = _CHECK_TIMEOUT if check_timeout is None else check_timeout

    self._name = name
    self._lockfile = _lockfile(name)
    self._acquire_timeout = rngu.uniform(acquire_timeout, pct=0.2)
    self._check_timeout = rngu.uniform(check_timeout, pct=0.2)

  def _mkmeta(self):
    """Serialize the current process metadata into a YAML-encoded byte string.

    Returns:
      A YAML-encoded byte string containing pid, cmdline, and acquisition time.
    """
    tag = dict(pid=os.getpid(), cmdline=_CMDLINE, time=time.time())
    stag = yaml.dump(tag, default_flow_style=False)

    return stag.encode()

  def _parse_meta(self, data):
    """Parse YAML-encoded lock metadata into a :class:`Meta` object.

    Args:
      data: Raw byte string containing YAML-encoded metadata.

    Returns:
      A :class:`Meta` instance if parsing succeeds, or None if data is empty
      or invalid.
    """
    if data:
      try:
        tag = yaml.safe_load(data.decode())

        return Meta(**tag)
      except:
        pass

  def _alive_pid(self, meta):
    """Check whether the process recorded in meta is still alive and unchanged.

    Args:
      meta: A :class:`Meta` instance describing the lock holder.

    Returns:
      True if the process exists, has the same command line, and started before
      the lock acquisition time; False otherwise.
    """
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
    """Attempt to acquire the lock, waiting and retrying as needed.

    Args:
      timeout: Maximum time in seconds to wait for the lock. If None, wait
        indefinitely.

    Returns:
      True if the lock was acquired, False if the timeout elapsed without
      acquiring it.
    """
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
          if (meta is None or not self._alive_pid(meta)) and \
              self._try_force_lock(meta):
            return True

        if quit_time is not None and now >= quit_time:
          alog.debug(f'Giving up on lock {self._name} after {timeout} seconds')
          return False

  def release(self):
    """Release the lock if held by the current process.

    Returns:
      True if the lock was released, False otherwise.
    """
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
    """Attempt to forcefully override a lock held by a dead or stale process.

    Creates an enforcer marker file, then rewrites the lock file with the
    current process metadata if the existing metadata matches the expected
    stale meta.

    Args:
      meta: A :class:`Meta` instance describing the current lock holder, or None.

    Returns:
      True if the force override succeeded, False otherwise.
    """
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
    """Read and parse the metadata of the current lock holder.

    Returns:
      A :class:`Meta` instance describing the lock holder, or None if the lock
      file does not exist or cannot be read.
    """
    try:
      with osfd.OsFd(self._lockfile, os.O_RDONLY) as fd:
        data = fsu.readall(fd)

      return self._parse_meta(data)
    except OSError:
      pass

  def __enter__(self):
    """Acquire the lock when entering a context manager block.

    Returns:
      The :class:`LockFile` instance itself.
    """
    self.acquire()

    return self

  def __exit__(self, *exc):
    """Release the lock when exiting a context manager block.

    Args:
      *exc: Exception information if an exception occurred in the block.

    Returns:
      False (exceptions are not suppressed).
    """
    self.release()

    return False
