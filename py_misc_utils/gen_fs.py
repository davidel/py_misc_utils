import os
import re
import random
import shutil
import string
import sys
import tempfile

import fsspec

from . import context_managers as cm


class TempFile:

  def __init__(self, dir=None, ref_path=None, **kwargs):
    if ref_path is not None:
      path = f'{ref_path}.{rand_name()}'
    else:
      dir = tempfile.gettempdir() if dir is None else dir
      path = os.path.join(dir, rand_name())

    self._fs, self._path = fsspec.core.url_to_fs(path)
    self._kwargs = kwargs
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


_STD_FILES = {
  'STDIN': sys.stdin,
  'STDOUT': sys.stdout,
  'STDERR': sys.stderr,
}

def open(path, **kwargs):
  sfd = _STD_FILES.get(path)
  if sfd is not None:
    return cm.NoOpCtxManager(sfd)

  return core_open(path, **kwargs)


def maybe_open(path, **kwargs):
  return core_open(path, **kwargs) if is_file(path) else None


def core_open(path, **kwargs):
  local_open = kwargs.pop('local_open', False)
  if local_open:
    fs, fpath = fsspec.core.url_to_fs(path)
    if is_localfs(fs):
      return fs.open(fpath, **kwargs)

    mode = kwargs.get('mode', 'rb')
    cache_storage = os.path.join(cache_dir(), 'py_misc_utils', 'gfs_cache')
    if 'r' in mode:
      path = fsspec.open_local(f'filecache::{path}',
                               mode=mode,
                               cache_storage=cache_storage)
    else:
      path = fsspec.open_local(f'simplecache::{path}',
                               mode=mode,
                               cache_storage=cache_storage)

  return fsspec.open(path, **kwargs)


def rand_name(n=10):
  rng = random.SystemRandom()

  return ''.join(rng.choices(string.ascii_lowercase + string.digits, k=n))


def is_file(path):
  fs, fpath = fsspec.core.url_to_fs(path)

  return fs.isfile(fpath)


def is_localfs(fs):
  return isinstance(fs, fsspec.implementations.local.LocalFileSystem)


def replace(src_path, dest_path):
  src_fs, src_fpath = fsspec.core.url_to_fs(src_path)
  dest_fs, dest_fpath = fsspec.core.url_to_fs(dest_path)

  # If not on the same file system, copy over since cross-fs renames are not allowed.
  if src_fs is not dest_fs:
    dsrc_path = f'{dest_fpath}.{rand_name()}'
    with src_fs.open(src_fpath, mode='rb') as src_fd:
      with dest_fs.open(dsrc_path, mode='wb') as dest_fd:
        shutil.copyfileobj(src_fd, dest_fd)

    src_fs, src_fpath = dest_fs, dsrc_path
  else:
    dsrc_path = None

  try:
    if is_localfs(src_fs):
      os.replace(src_path, dest_path)
    else:
      tmp_path = f'{dest_fpath}.{rand_name()}'
      dest_fs.mv(dest_fpath, tmp_path)
      try:
        dest_fs.mv(src_fpath, dest_fpath)
      except:
        dest_fs.mv(tmp_path, dest_fpath)
        raise
  except:
    if dsrc_path is not None:
      try:
        dest_fs.rm(dsrc_path)
      except:
        pass

    raise


def enumerate_files(path, matcher, fullpath=False):
  fs, fpath = fsspec.core.url_to_fs(path)
  for epath in fs.find(fpath, maxdepth=1, withdirs=True):
    fname = os.path.basename(epath)
    if matcher(fname):
      yield epath if fullpath else fname


def re_enumerate_files(path, rex, fullpath=False):
  fs, fpath = fsspec.core.url_to_fs(path)
  for epath in fs.find(fpath, maxdepth=1, withdirs=True):
    fname = os.path.basename(epath)
    m = re.match(rex, fname)
    if m:
      mpath = epath if fullpath else fname
      yield mpath, m


def normpath(path):
  path = os.path.expandvars(path)

  _, fpath = fsspec.core.url_to_fs(path)

  return fpath


def cache_dir(path=None):
  if path is None:
    path = os.getenv('CACHE_DIR', None)
    if path is None:
      path = os.path.join(os.getenv('HOME', '.'), '.cache')

  return normpath(path)

