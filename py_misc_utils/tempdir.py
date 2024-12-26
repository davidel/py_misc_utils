import os
import tempfile

from . import cleanups
from . import gfs
from . import init_variables as ivar
from . import rnd_utils as rngu


class _RootDir(ivar.VarBase):

  def __init__(self):
    self.path = tempfile.mkdtemp()
    self.cid = cleanups.register(gfs.rmtree, self.path, ignore_errors=True)

  def cleanup(self):
    cleanups.unregister(self.cid, run=True)


_VARID = ivar.varid(__name__, 'tmproot')

def _tmproot():
  return ivar.get(_VARID, _RootDir).path


def create():
  return tempfile.mkdtemp(dir=_tmproot())


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

