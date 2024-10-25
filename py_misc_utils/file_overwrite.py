from . import gen_fs as gfs


class FileOverwrite:

  def __init__(self, path, mode='w'):
    self._path = path
    self._mode = mode
    self._temp = None

  def __enter__(self):
    self._temp = gfs.TempFile(ref_path=self._path, mode=self._mode)

    return self._temp.open()

  def __exit__(self, *exc):
    self._temp.replace(self._path)
    self._temp.close()

    return False

