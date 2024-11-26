import os


class OsFd:

  def __init__(self, path, *args, **kwargs):
    self._path = path
    self._args = args
    self._kwargs = kwargs

  def __enter__(self):
    self._fd = os.open(self._path, *self._args, **self._kwargs)

    return self._fd

  def __exit__(self, *exc):
    os.close(self._fd)
    return False

