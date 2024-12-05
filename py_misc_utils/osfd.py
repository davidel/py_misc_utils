import os


class OsFd:

  def __init__(self, path, flags, remove_on_error=None, **kwargs):
    self._path = path
    self._flags = flags
    self._kwargs = kwargs
    self._remove_on_error = remove_on_error or False

  def __enter__(self):
    self._fd = os.open(self._path, self._flags, **self._kwargs)

    return self._fd

  def __exit__(self, *exc):
    os.close(self._fd)
    if any(ex is not None for ex in exc) and self._remove_on_error:
      try:
        os.remove(self._path)
      except OSError:
        pass

    return False

