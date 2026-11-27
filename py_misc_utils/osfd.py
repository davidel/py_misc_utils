"""File descriptor context manager for safe file open/close.

This module provides :class:`OsFd`, a context manager that wraps os.open()
and os.close() to ensure file descriptors are properly closed, with optional
automatic file removal on error.
"""

import os


class OsFd:
  """Context manager for safely opening and closing file descriptors.

  Wraps os.open() and os.close() to ensure the file descriptor is always
  closed when exiting the context. Optionally removes the file if an error
  occurs and the file did not previously exist.

  Example:
    import os
    import tempfile
    from py_misc_utils import osfd

    tdir = tempfile.TemporaryDirectory()
    path = os.path.join(tdir.name, 'out.bin')

    with osfd.OsFd(path, os.O_WRONLY | os.O_CREAT, mode=0o600) as fd:
      os.write(fd, b'hello')

    with open(path, 'rb') as f:
      assert f.read() == b'hello'

    tdir.cleanup()
  """

  def __init__(self, path, flags, remove_on_error=None, **kwargs):
    """Initialize the OsFd context manager.

    Args:
      path: Filesystem path to open.
      flags: Flags passed to os.open (e.g. os.O_RDONLY, os.O_CREAT).
      remove_on_error: If True, remove the file on error. If None, defaults
        to removing the file only if it did not exist before opening.
      **kwargs: Additional keyword arguments passed to os.open (e.g. mode).
    """
    self._path = path
    self._flags = flags
    self._kwargs = kwargs
    self._remove_on_error = (remove_on_error if remove_on_error is not None else
                             not os.path.exists(path))

  def __enter__(self):
    """Open the file descriptor and return it.

    Returns:
      The integer file descriptor.
    """
    self._fd = os.open(self._path, self._flags, **self._kwargs)

    return self._fd

  def __exit__(self, *exc):
    """Close the file descriptor and optionally remove the file on error.

    Args:
      *exc: Exception information if an exception occurred in the context.

    Returns:
      False (exceptions are not suppressed).
    """
    os.close(self._fd)
    if any(ex is not None for ex in exc) and self._remove_on_error:
      try:
        os.remove(self._path)
      except OSError:
        pass

    return False
