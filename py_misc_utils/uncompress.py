"""Decompression of files into temporary storage.

This module provides a context manager which transparently decompresses
a compressed file, exposing the decompressed content for the duration of
the context.
"""

import os
import shutil
import tempfile

from . import alog
from . import compression as comp


class Uncompress:
  """Context manager which decompresses a file on entry.

  If the file has a recognized compression extension, its content is
  decompressed into a temporary directory and the decompressed path is
  returned by the context; otherwise the original path is returned
  unchanged. The temporary files are removed on exit.
  """

  def __init__(self, path):
    """Initialize the decompression context.

    Args:
      path: Path of the file which may be compressed.
    """
    self._path = path
    self._tempdir = None

  def __enter__(self):
    """Decompress the file, returning the path to its content.

    Returns:
      The path of the decompressed file in a temporary directory, or the
      original path when the file is not compressed.
    """
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
    """Remove any temporary decompression directory.

    Args:
      *exc: Exception information of the context (ignored).

    Returns:
      False, so that exceptions are propagated.
    """
    if self._tempdir is not None:
      shutil.rmtree(self._tempdir, ignore_errors=True)

    return False
