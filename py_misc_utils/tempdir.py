import shutil
import tempfile

from . import cleanups
from . import run_once as ro


_TEMPDIR = None

@ro.run_once
def _create_tempdir():
  global _TEMPDIR

  _TEMPDIR = tempfile.mkdtemp()

  cleanups.register(shutil.rmtree, cfpath, ignore_errors=True)


def create():
  _create_tempdir()

  return tempfile.mkdtemp(dir=_TEMPDIR)

