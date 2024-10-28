import contextlib
import datetime
import os
import re
import shutil
import stat as st
import string
import sys
import tempfile

import fsspec

from . import context_managers as cm
from . import no_except as nex
from . import obj
from . import rnd_utils as rngu


class TempFile:

  def __init__(self, nsdir=None, nspath=None, **kwargs):
    path = temp_path(nspath=nspath, nsdir=nsdir)

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
    return contextlib.nullcontext(sfd)

  return core_open(path, **kwargs)


def maybe_open(path, **kwargs):
  return core_open(path, **kwargs) if is_file(path) else None


_LOCAL_ROFS = os.getenv('LOCAL_ROFS', 'filecache')
_LOCAL_RWFS = os.getenv('LOCAL_RWFS', 'simplecache')

def open_local(path, **kwargs):
  fs, fpath = fsspec.core.url_to_fs(path)
  if is_localfs(fs):
    return fs.open(fpath, **kwargs)

  mode = kwargs.pop('mode', 'r')
  cache_storage = kwargs.pop('cache_storage', None)
  if cache_storage is None:
    cache_storage = os.path.join(cache_dir(), 'py_misc_utils', 'gfs_cache')
  if 'r' in mode:
    return fsspec.open(f'{_LOCAL_ROFS}::{path}',
                       mode=mode,
                       cache_storage=cache_storage,
                       **kwargs)
  else:
    return fsspec.open(f'{_LOCAL_RWFS}::{path}',
                       mode=mode,
                       cache_storage=cache_storage,
                       **kwargs)


def core_open(path, **kwargs):
  local_open = kwargs.pop('local_open', False)

  return open_local(path, **kwargs) if local_open else fsspec.open(path, **kwargs)


def temp_path(nspath=None, nsdir=None, rndsize=10):
  if nspath is not None:
    return f'{nspath}.{rngu.rand_string(rndsize)}'

  nsdir = tempfile.gettempdir() if nsdir is None else nsdir

  return os.path.join(nsdir, f'{rngu.rand_string(rndsize)}.tmp')


def is_file(path):
  fs, fpath = fsspec.core.url_to_fs(path)

  return fs.isfile(fpath)


def exists(path):
  fs, fpath = fsspec.core.url_to_fs(path)

  return fs.exists(fpath)


_LOCALFS_PROTOS = ('file', 'local')

def is_localfs(fs):
  if isinstance(fs.protocol, (list, tuple)):
    return any(p in _LOCALFS_PROTOS for p in fs.protocol)

  return fs.protocol in _LOCALFS_PROTOS


def is_localpath(path):
  fs, fpath = fsspec.core.url_to_fs(path)

  return is_localfs(fs)


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
    dsrc_path = temp_path(nspath=dest_fpath)
    copy(src_fpath, dsrc_path, src_fs=src_fs, dest_fs=dest_fs)
    src_fpath = dsrc_path

  try:
    if is_localfs(dest_fs):
      os.replace(src_fpath, dest_fpath)
    else:
      # This is not atomic, sigh! File systems should really have a replace-like
      # atomic operation, since the move operations fail if the target exists.
      if dest_fs.exists(dest_fpath):
        tmp_path = temp_path(nspath=dest_fpath)
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


def mkdir(path, **kwargs):
  fs, fpath = fsspec.core.url_to_fs(path)
  fs.mkdir(fpath, **kwargs)


def makedirs(path, **kwargs):
  fs, fpath = fsspec.core.url_to_fs(path)
  fs.makedirs(fpath, **kwargs)


def rmdir(path):
  fs, fpath = fsspec.core.url_to_fs(path)
  fs.rmdir(fpath)


class StatResult(obj.Obj):
  FIELDS = (
    'st_mode', 'st_ino', 'st_dev', 'st_nlink', 'st_uid', 'st_gid', 'st_size',
    'st_atime', 'st_mtime', 'st_ctime',
  )

def stat(path):
  fs, fpath = fsspec.core.url_to_fs(path)
  info = fs.info(fpath)

  sinfo = StatResult(**{k: None for k in StatResult.FIELDS})
  for k, v in info.items():
    sfield = f'st_{k}'
    if hasattr(sinfo, sfield):
      setattr(sinfo, sfield, v)

  if sinfo.st_mode is None:
    sinfo.st_mode = 0
  if info['type'] == 'file':
    sinfo.st_mode |= st.S_IFREG
  elif info['type'] == 'directory':
    sinfo.st_mode |= st.S_IFDIR
  elif info['type'] == 'link' or info.get('islink', False):
    sinfo.st_mode |= st.S_IFLNK

  if sinfo.st_size is None:
    sinfo.st_size = info.get('size')
  if sinfo.st_ctime is None:
    sinfo.st_ctime = info.get('created')
  # Some FS populates datetime.datetime for time fields, while Python stat()
  # standard requires timestamps (EPOCH seconds).
  for k in ('st_atime', 'st_mtime', 'st_ctime'):
    v = getattr(sinfo, k, None)
    if isinstance(v, datetime.datetime):
      setattr(sinfo, k, v.timestamp())

  return sinfo


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


_CACHE_DIR = os.getenv(
  'CACHE_DIR',
  os.path.join(os.getenv('HOME', os.getcwd()), '.cache')
)

def cache_dir(path=None):
  return normpath(path) if path is not None else _CACHE_DIR

