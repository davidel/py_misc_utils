import os
import re
import shutil
import string
import sys
import tempfile

import fsspec

from . import context_managers as cm
from . import no_except as nex
from . import rnd_utils as rngu


class TempFile:

  def __init__(self, dir=None, ref_path=None, **kwargs):
    path = temp_path(ref_path=ref_path, dir=dir)

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


def open_local(path, **kwargs):
  fs, fpath = fsspec.core.url_to_fs(path)
  if is_localfs(fs):
    return fs.open(fpath, **kwargs)

  mode = kwargs.pop('mode', 'r')
  cache_storage = kwargs.pop('cache_storage', None)
  if cache_storage is None:
    cache_storage = os.path.join(cache_dir(), 'py_misc_utils', 'gfs_cache')
  if 'r' in mode:
    return fsspec.open(f'filecache::{path}',
                       mode=mode,
                       cache_storage=cache_storage,
                       **kwargs)
  else:
    return fsspec.open(f'simplecache::{path}',
                       mode=mode,
                       cache_storage=cache_storage,
                       **kwargs)


def core_open(path, **kwargs):
  local_open = kwargs.pop('local_open', False)

  return open_local(path, **kwargs) if local_open else fsspec.open(path, **kwargs)


def temp_path(ref_path=None, dir=None, rng_len=10):
  if ref_path is not None:
    return f'{ref_path}.{rngu.rand_string(rng_len)}'

  dir = tempfile.gettempdir() if dir is None else dir

  return os.path.join(dir, rngu.rand_string(rng_len))


def is_file(path):
  fs, fpath = fsspec.core.url_to_fs(path)

  return fs.isfile(fpath)


def exists(path):
  fs, fpath = fsspec.core.url_to_fs(path)

  return fs.exists(fpath)


def is_localfs(fs):
  return isinstance(fs, fsspec.implementations.local.LocalFileSystem)


def copy(src_path, dest_path, src_fs=None, dest_fs=None):
  if src_fs is None:
    src_fs, src_path = fsspec.core.url_to_fs(src_path)
  if dest_fs is None:
    dest_fs, dest_path = fsspec.core.url_to_fs(dest_path)

  with src_fs.open(src_path, mode='rb') as src_fd:
    with dest_fs.open(dest_path, mode='wb') as dest_fd:
      shutil.copyfileobj(src_fd, dest_fd)


def replace(src_path, dest_path):
  src_fs, src_fpath = fsspec.core.url_to_fs(src_path)
  dest_fs, dest_fpath = fsspec.core.url_to_fs(dest_path)

  # If not on the same file system, copy over since cross-fs renames are not allowed.
  dsrc_path = None
  if src_fs is not dest_fs:
    dsrc_path = temp_path(ref_path=dest_fpath)
    copy(src_fpath, dsrc_path, src_fs=src_fs, dest_fs=dest_fs)
    src_fs, src_fpath = dest_fs, dsrc_path

  try:
    if is_localfs(src_fs):
      os.replace(src_path, dest_path)
    else:
      # This is not atomic, sigh! File systems should really have a replace-like
      # atomic operation.
      if dest_fs.exists(dest_fpath):
        tmp_path = temp_path(ref_path=dest_fpath)
        dest_fs.mv(dest_fpath, tmp_path)
        try:
          dest_fs.mv(src_fpath, dest_fpath)
          dest_fs.rm(tmp_path)
        except:
          dest_fs.mv(tmp_path, dest_fpath)
          raise
      else:
        dest_fs.mv(src_fpath, dest_fpath)
  except:
    if dsrc_path is not None:
      nex.no_except(dest_fs.rm, dsrc_path)

    raise


def mkdir(path, create_parents=False):
  fs, fpath = fsspec.core.url_to_fs(path)
  fs.mkdir(fpath, create_parents=create_parents)


def rmdir(path):
  fs, fpath = fsspec.core.url_to_fs(path)
  fs.rmdir(fpath)


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

  fs, fpath = fsspec.core.url_to_fs(path)

  return fpath if is_localfs(fs) else fs.unstrip_protocol(fpath)


def cache_dir(path=None):
  if path is None:
    path = os.getenv('CACHE_DIR', None)
    if path is None:
      path = os.path.join(os.getenv('HOME', '.'), '.cache')

  return normpath(path)

