import shutil
import tempfile

from . import cleanups
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

