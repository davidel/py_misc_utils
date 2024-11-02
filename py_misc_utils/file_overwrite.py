from . import gen_fs as gfs


class FileOverwrite:

  def __init__(self, dest, mode='w', **kwargs):
    self._dest = dest
    self._mode = mode
    self._kwargs = kwargs
    self._temp = None

  def __enter__(self):
    if gfs.is_path_like(self._dest):
      self._temp = gfs.TempFile(nspath=self._dest, mode=self._mode, **self._kwargs)

      return self._temp.open()
    else:
      return self._dest

  def __exit__(self, *exc):
    if self._temp is not None:
      try:
        self._temp.replace(self._dest)
      finally:
        self._temp.close()

    return False

