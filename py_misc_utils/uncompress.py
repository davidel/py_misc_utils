import os
import shutil
import tempfile

from . import alog
from . import utils as ut


class Uncompress:

  def __init__(self, path):
    self._path = path
    self._tempdir = None

  def __enter__(self):
    self._tempdir = tempfile.mkdtemp()

    bpath, ext = os.path.splitext(self._path)
    if ext in {'.gz', '.bz2', '.bzip'}:
      rpath = os.path.join(self._tempdir, os.path.basename(bpath))

      alog.debug(f'Uncompressing "{self._path}" to "{rpath}"')

      if ext == '.gz':
        ut.fgunzip(self._path, rpath)
      else:
        ut.fbunzip2(self._path, rpath)

      shutil.copystat(self._path, rpath)
    else:
      rpath = self._path

    return rpath

  def __exit__(self, *exc):
    if self._tempdir is not None:
      shutil.rmtree(self._tempdir, ignore_errors=True)

    return False

