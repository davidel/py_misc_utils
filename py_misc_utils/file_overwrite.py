import os
import tempfile


class FileOverwrite(object):

  def __init__(self, path, mode='w'):
    self._path = path
    self._mode = mode
    self._fd = None

  def __enter__(self):
    self._fd = tempfile.NamedTemporaryFile(mode=self._mode, dir=os.path.dirname(self._path),
                                           delete=False)

    return self._fd

  def __exit__(self, *exc):
    self._fd.close()

    os.replace(self._fd.name, self._path)

    return False

