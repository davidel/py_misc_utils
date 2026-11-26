"""Temporary directory management with fork safety and fast filesystem support.

This module provides utilities for creating and managing temporary
directories, including a per-process root temp dir (safely re-initialized
after fork), and :func:`fastfs_dir` which locates the fastest available
filesystem (tmpfs/ramfs) for high-throughput temporary files.
"""

import os
import shutil
import tempfile

from . import cleanups
from . import global_namespace as gns
from . import rnd_utils as rngu


class _RootDir:
  """Per-process root temporary directory cleaned up on exit.
  """

  def __init__(self):
    """Create the root temporary directory and register cleanup."""
    self._path = tempfile.mkdtemp()
    self._cid = cleanups.register(shutil.rmtree, self._path, ignore_errors=True)

  def create(self):
    """Create a new temporary directory under the root.

    Returns:
      The path of the newly created directory.
    """
    return tempfile.mkdtemp(dir=self._path)

  def root(self):
    """Return the root temporary directory path.

    Returns:
      The root directory path.
    """
    return self._path


_ROOTDIR = gns.Var(f'{__name__}.ROOTDIR', fork_init=True, defval=lambda: _RootDir())


def _root_dir():
  """Return the per-process root directory manager.

  Returns:
    The _RootDir instance from the global namespace variable.
  """
  return gns.get(_ROOTDIR)


def create():
  """Create a new temporary directory under the process root temp dir.

  Returns:
    The path of the newly created directory.
  """
  return _root_dir().create()


def get_temp_root():
  """Return the process root temporary directory path.

  Returns:
    The root temporary directory path.
  """
  return _root_dir().root()


def _try_fastfs_dir(path):
  """Attempt to create a 'fastfs' subdirectory under path.

  Args:
    path: The candidate directory path.

  Returns:
    The fastfs directory path if creation succeeded, else None.
  """
  if os.path.isdir(path):
    fastfs_dir = os.path.join(path, 'fastfs')
    try:
      os.makedirs(fastfs_dir, exist_ok=True)

      return fastfs_dir
    except OSError:
      pass


def _find_fastfs_dir():
  """Find the fastest available directory for temporary files.

  Checks FASTFS_DIR env var, then known tmpfs/ramfs locations on Linux,
  then the default temp dir and cwd.

  Returns:
    The first usable fastfs directory path.
  """
  fastfs_dirs = []
  if (path := os.getenv('FASTFS_DIR')) is not None:
    fastfs_dirs.append(path)

  if os.name == 'posix':
    # Try known tmpfs/ramfs places in case on Linux.
    fastfs_dirs.append(f'/run/user/{os.getuid()}')
    fastfs_dirs.append('/dev/shm')

  fastfs_dirs.append(tempfile.gettempdir())
  fastfs_dirs.append(os.getcwd())

  for path in fastfs_dirs:
    if (fastfs_dir := _try_fastfs_dir(path)) is not None:
      return fastfs_dir


_FASTFS_DIR = _find_fastfs_dir()
_NAMELEN = int(os.getenv('FASTFS_NAMELEN', '12'))


def fastfs_dir(name=None, namelen=_NAMELEN):
  """Create (or retrieve) a subdirectory in the fast filesystem area.

  Args:
    name: Optional directory name; defaults to a random name of namelen chars.
    namelen: Length of the random name when name is not provided.

  Returns:
    The path of the created directory.
  """
  name = name or rngu.rand_string(namelen)

  path = os.path.join(_FASTFS_DIR, name)
  os.makedirs(path, exist_ok=True)

  return path
