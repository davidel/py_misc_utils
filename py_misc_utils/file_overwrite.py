from . import gfs


class FileOverwrite:

  def __init__(self, dest, mode='w', **kwargs):
    self._dest = dest
    self._path = gfs.path_of(dest)
    self._mode = mode
    self._kwargs = kwargs
    self._tmpfile = None

  def __enter__(self):
    if self._path is not None:
      self._tmpfile = gfs.TempFile(nspath=self._path, mode=self._mode, **self._kwargs)

      return self._tmpfile.open()
    else:
      return self._dest

  def __exit__(self, *exc):
    if self._tmpfile is not None:
      try:
        self._tmpfile.replace(self._path)
      finally:
        self._tmpfile.close()

    return False

