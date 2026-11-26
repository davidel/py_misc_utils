"""Atomic file overwrite via temporary file and rename.

The FileOverwrite class writes to a temporary file in the same directory as
the destination, then atomically replaces the destination on context exit.
This ensures that readers never see a partially-written file.
"""

from . import gfs


class FileOverwrite:
  """Context manager for atomic file overwrite via temp file + rename.

  Writes to a temporary file in the destination directory, then replaces the
  destination file on exit. If an exception occurs, the temp file is cleaned
  up and the original file is left untouched.
  """

  def __init__(self, dest, mode='w', **kwargs):
    """Initializes a FileOverwrite context manager.

    Args:
      dest: The destination file path to write to.
      mode: The file open mode (default 'w').
      **kwargs: Additional keyword arguments forwarded to gfs.TempFile.
    """
    self._dest = dest
    self._path = gfs.path_of(dest)
    self._mode = mode
    self._kwargs = kwargs
    self._tmpfile = None

  def __enter__(self):
    """Creates the temp file and returns a writable file handle.

    Returns:
      A file-like object writable at the temp location, or ``self._dest``
      if no filesystem path is available.
    """
    if self._path is not None:
      self._tmpfile = gfs.TempFile(nspath=self._path, mode=self._mode, **self._kwargs)

      return self._tmpfile.open()
    else:
      return self._dest

  def __exit__(self, *exc):
    """Replaces the destination with the temp file contents.

    If no exception occurred, the temp file is renamed over the destination.
    The temp file handle is always closed.

    Returns:
      False (exceptions are not suppressed).
    """
    if self._tmpfile is not None:
      try:
        if exc[0] is None:
          self._tmpfile.replace(self._path)
      finally:
        self._tmpfile.close()

    return False
