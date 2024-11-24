import os
import shutil
import tempfile

from . import alog
from . import compression as comp


class Uncompress:

  def __init__(self, path):
    self._path = path
    self._tempdir = None

  def __enter__(self):
    bpath, ext = os.path.splitext(self._path)

    decomp = comp.decompressor(ext)
    if decomp is not None:
      self._tempdir = tempfile.mkdtemp()
      rpath = os.path.join(self._tempdir, os.path.basename(bpath))

      decomp(self._path, rpath)
      shutil.copystat(self._path, rpath)
    else:
      rpath = self._path

    return rpath

  def __exit__(self, *exc):
    if self._tempdir is not None:
      shutil.rmtree(self._tempdir, ignore_errors=True)

    return False

