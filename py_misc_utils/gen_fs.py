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
    tmp_path = temp_path(nspath=nspath, nsdir=nsdir)

    fs, tmp_path = fsspec.core.url_to_fs(tmp_path)
    if not is_localfs(fs):
      fs, tmp_path = fsspec.core.url_to_fs(temp_path())

    self._fs, self._path = fs, tmp_path
    self._kwargs = kwargs
    self._fd, self._delete = None, False

  def open(self):
    self._fd = self._fs.open(self._path, **self._kwargs)
    self._delete = True

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


def is_dir(path):
  fs, fpath = fsspec.core.url_to_fs(path)

  return fs.isdir(fpath)


def exists(path):
  fs, fpath = fsspec.core.url_to_fs(path)

  return fs.exists(fpath)


def fs_proto(fs):
  proto = getattr(fs, 'protocol', None)

  return getattr(fs, 'fsid', None) if proto is None else proto


def is_same_fs(*args):
  protos = [fs_proto(fs) for fs in args]

  return all(p is not None and protos[0] == p for p in protos)


_LOCALFS_PROTOS = ('file', 'local')

def is_localfs(fs):
  proto = fs_proto(fs)
  if isinstance(proto, (list, tuple)):
    return any(p in _LOCALFS_PROTOS for p in proto)

  return proto in _LOCALFS_PROTOS


def is_localpath(path):
  fs, fpath = fsspec.core.url_to_fs(path)

  return is_localfs(fs)


def copy(src_path, dest_path, src_fs=None, dest_fs=None):
  if src_fs is None:
    src_fs, src_path = fsspec.core.url_to_fs(src_path)
  if dest_fs is None:
    dest_fs, dest_path = fsspec.core.url_to_fs(dest_path)

  try:
    with src_fs.open(src_path, mode='rb') as src_fd:
      with dest_fs.open(dest_path, mode='wb') as dest_fd:
        shutil.copyfileobj(src_fd, dest_fd)
  except NotImplementedError:
    # Slow path. Likely the destination file system do not support files opened
    # in write mode, so we use the more widely available get_file+put_file APIs.
    try:
      if is_localfs(src_fs):
        local_path = src_path
      else:
        local_path = temp_path()
        src_fs.get_file(src_path, local_path)

      dest_fs.put_file(local_path, dest_path)
    finally:
      if local_path != src_path:
        nex.no_except(os.remove, local_path)


def replace(src_path, dest_path):
  src_fs, src_fpath = fsspec.core.url_to_fs(src_path)
  dest_fs, dest_fpath = fsspec.core.url_to_fs(dest_path)

  if is_localfs(src_fs):
    if is_localfs(dest_fs):
      if localfs_mount(src_fpath) == localfs_mount(dest_fpath):
        os.replace(src_fpath, dest_fpath)
      else:
        shutil.move(src_fpath, dest_fpath)
    else:
      dest_fs.put_file(src_fpath, dest_fpath)
      src_fs.rm(src_fpath)
  else:
    # Quite a few file systems do not support move operations, so we try that before,
    # and if that fails, we use the more widely available get_file+put_file APIs.
    replaced = False
    try:
      if is_same_fs(src_fs, dest_fs):
        dest_fs.mv(src_fpath, dest_fpath)
        replaced = True
    except NotImplementedError:
      pass

    if not replaced:
      local_path = temp_path()
      try:
        src_fs.get_file(src_fpath, local_path)
        dest_fs.put_file(local_path, dest_fpath)
      finally:
        nex.no_except(os.remove, local_path)


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
    sfield = k if k.startswith('st_') else f'st_{k}'
    if hasattr(sinfo, sfield):
      setattr(sinfo, sfield, v)

  if sinfo.st_mode is None:
    sinfo.st_mode = 0
  itype = info.get('type')
  if itype == 'file':
    sinfo.st_mode |= st.S_IFREG
  elif itype == 'directory':
    sinfo.st_mode |= st.S_IFDIR
  elif itype == 'link' or info.get('islink', False):
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


def localfs_mount(path):
  while True:
    parent_path = os.path.dirname(path)
    if path == parent_path or os.path.ismount(path):
      return path
    path = parent_path


def find_mount(path):
  fs, fpath = fsspec.core.url_to_fs(path)

  return localfs_mount(fpath) if is_localfs(fs) else None

