from . import gen_fs as gfs


class FileOverwrite:

  def __init__(self, path, mode='w', **kwargs):
    self._path = path
    self._mode = mode
    self._kwargs = kwargs
    self._temp = None

  def __enter__(self):
    self._temp = gfs.TempFile(nspath=self._path, mode=self._mode, **self._kwargs)

    return self._temp.open()

  def __exit__(self, *exc):
    try:
      self._temp.replace(self._path)
    finally:
      self._temp.close()

    return False

