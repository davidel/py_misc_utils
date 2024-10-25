import os
import shutil
import string
import sys
import tempfile
import random

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


def rand_name(n=10):
  rng = random.SystemRandom()

  return ''.join(rng.choices(string.ascii_lowercase + string.digits, k=n))


def replace(src_path, dest_path):
  src_fs, src_fpath = fsspec.core.url_to_fs(src_path)
  dest_fs, dest_fpath = fsspec.core.url_to_fs(dest_path)

  if src_fs is dest_fs and isinstance(src_fs, fsspec.implementations.local.LocalFileSystem):
    os.replace(src_path, dest_path)
  else:
    tmp_path = f'{dest_fpath}.{rand_name()}'
    dest_fs.mv(dest_fpath, tmp_path)

    try:
      if src_fs is dest_fs:
        src_fs.mv(src_fpath, dest_fpath)
      else:
        with src_fs.open(src_fpath, mode='rb') as src_fd:
          with dest_fs.open(dest_fpath, mode='wb') as dest_fd:
            shutil.copyfileobj(src_fd, dest_fd)

      dest_fs.rm(tmp_path)
    except:
      dest_fs.mv(tmp_path, dest_fpath)
      raise


class TempFile:

  def __init__(self, dir=None, ref_path=None, **kwargs):
    if ref_path is not None:
      path = f'{ref_fpath}.{rand_name()}'
    else:
      dir = tempfile.gettempdir() if dir is None else dir
      path = os.path.join(dir, rand_name())

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
    self._delete = False
    self.close()

    try:
      replace(self._path, path)
    except:
      self._delete = True
      raise

  def __enter__(self):
    return self.open()

  def __exit__(self, *exc):
    self.close()

    return False

