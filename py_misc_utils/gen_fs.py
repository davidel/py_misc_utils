import os
import sys
import tempfile
import uuid

import fsspec

from . import context_managers as cm


_STD_FILES = {
  'STDIN': sys.stdin,
  'STDOUT': sys.stdout,
  'STDERR': sys.stderr,
}

def open(path, *args, **kwargs):
  sfd = _STD_FILES.get(path)
  if sfd is not None:
    return cm.NoOpCtxManager(sfd)

  return fsspec.open(path, *args, **kwargs)


def maybe_open(path, *args, **kwargs):
  fs, fpath = fsspec.core.url_to_fs(path)
  if fs.isfile(fpath):
    return fs.open(fpath, *args, **kwargs)


class TempFile:

  def __init__(self, dir=None, **kwargs):
    print(f'DIR is {dir}')

    dir = dir or tempfile.gettempdir()

    print(f'DIR is {dir}')

    path = os.path.join(dir, str(uuid.uuid4()))

    print(f'PATH is {path}')

    self._fs, self._path = fsspec.core.url_to_fs(path)
    self._dir, self._kwargs = dir, kwargs
    self._fd, self._delete = None, True

  def open(self):
    self._fd = self._fs.open(self._path, **self._kwargs)

    return self._fd

  def close(self):
    if self._fd is not None:
      self._fd.close()
      self._fd = None
    if self._delete:
      self._fs.rm(self._path)
      self._delete = False

  def replace(self, path):
    fs, fpath = fsspec.core.url_to_fs(path)

    self._delete = False
    self.close()

    tmp_path = None
    try:
      if isinstance(fs, fsspec.implementations.local.LocalFileSystem):
        os.replace(self._path, fpath)
      else:
        # File systems should really have replace() ...
        tmp_path = os.path.join(self._dir, str(uuid.uuid4()))
        self._fs.mv(fpath, tmp_path)
        self._fs.mv(self._path, fpath)
        self._fs.rm(tmp_path)
    except:
      if tmp_path is not None:
        self._fs.mv(tmp_path, fpath)
      self._delete = True
      raise

  def __enter__(self):
    return self.open()

  def __exit__(self, *exc):
    self.close()

    return False

