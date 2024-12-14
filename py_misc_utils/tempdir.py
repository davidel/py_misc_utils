import os
import shutil
import tempfile

from . import cleanups
from . import rnd_utils as rngu
from . import run_once as ro


_TEMPDIR = None

@ro.run_once
def _create_tempdir():
  global _TEMPDIR

  _TEMPDIR = tempfile.mkdtemp()

  cleanups.register(shutil.rmtree, _TEMPDIR, ignore_errors=True)


def get_root():
  _create_tempdir()

  return _TEMPDIR


def create():
  _create_tempdir()

  return tempfile.mkdtemp(dir=_TEMPDIR)


def _try_fastfs_dir(path):
  if os.path.isdir(path):
    fastfs_dir = os.path.join(path, 'fastfs')
    try:
      os.makedirs(fastfs_dir, exist_ok=True)

      return fastfs_dir
    except:
      pass


def _find_fastfs_dir():
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
_NAMELEN = int(os.getenv('FASTFS_NAMELEN', 12))

def fastfs_dir(name=None, namelen=None):
  name = name or rngu.rand_string(namelen or _NAMELEN)

  path = os.path.join(_FASTFS_DIR, name)
  os.makedirs(path, exist_ok=True)

  return path

