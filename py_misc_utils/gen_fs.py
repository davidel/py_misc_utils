import collections
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


FsPath = collections.namedtuple('FsPath', 'fs, path')


class TempFile:

  def __init__(self, nsdir=None, nspath=None, **kwargs):
    is_local = ((nsdir is None or is_local_path(nsdir)) and
                (nspath is None or is_local_path(nspath)))
    if is_local:
      fs, tmp_path = fsspec.url_to_fs(temp_path(nspath=nspath, nsdir=nsdir))
    else:
      fs, tmp_path = fsspec.url_to_fs(temp_path())

    self._fs, self._path = fs, tmp_path
    self._kwargs = kwargs
    self.fd, self._delete = None, False

  def open(self):
    self.fd = self._fs.open(self._path, **self._kwargs)
    self._delete = True

    return self.fd

  def close_fd(self):
    if self.fd is not None:
      self.fd.close()
      self.fd = None

  def close(self):
    self.close_fd()
    if self._delete:
      self._fs.rm(self._path)
      self._delete = False

  def replace(self, path):
    self.close_fd()
    replace(self._path, path, src_fs=self._fs)
    self._delete = False

  def __enter__(self):
    self.open()

    return self

  def __exit__(self, *exc):
    self.close()

    return False


_STD_FILES = {
  'STDIN': sys.stdin,
  'STDOUT': sys.stdout,
  'STDERR': sys.stderr,
}

def std_open(path, **kwargs):
  sfd = _STD_FILES.get(path)
  if sfd is not None:
    return contextlib.nullcontext(sfd)

  return open(path, **kwargs)


def open(path, **kwargs):
  return fsspec.open(path, **kwargs)


def open_source(source, **kwargs):
  if path := path_of(source):
    return open(path, **kwargs)

  return contextlib.nullcontext(source)


def path_of(path):
  return os.fspath(path) if isinstance(path, (str, os.PathLike)) else None


def maybe_open(path, **kwargs):
  try:
    fs, fpath = fsspec.url_to_fs(path)

    return fs.open(fpath, **kwargs)
  except:
    pass


_LOCAL_ROFS = os.getenv('LOCAL_ROFS', 'filecache')
_LOCAL_RWFS = os.getenv('LOCAL_RWFS', 'simplecache')

def open_local(path, **kwargs):
  fs, fpath = fsspec.url_to_fs(path)
  if is_local_fs(fs):
    return fs.open(fpath, **kwargs)

  mode = kwargs.pop('mode', 'r')
  proxy_fs = _LOCAL_ROFS if 'r' in mode else _LOCAL_RWFS
  cache_storage = kwargs.pop('cache_storage', cache_dir())
  cache_storage = os.path.join(cache_storage, 'py_misc_utils', 'gfs', proxy_fs)

  return fsspec.open(f'{proxy_fs}::{path}',
                     mode=mode,
                     cache_storage=cache_storage,
                     **kwargs)


_TMPFN_RNDSIZE = int(os.getenv('TMPFN_RNDSIZE', 10))

def temp_path(nspath=None, nsdir=None, rndsize=None):
  rndsize = rndsize or _TMPFN_RNDSIZE

  if nspath is not None:
    return f'{nspath}.{rngu.rand_string(rndsize)}'

  nsdir = tempfile.gettempdir() if nsdir is None else nsdir

  return os.path.join(nsdir, f'{rngu.rand_string(rndsize)}.tmp')


def is_file(path):
  fs, fpath = fsspec.url_to_fs(path)

  return fs.isfile(fpath)


def is_dir(path):
  fs, fpath = fsspec.url_to_fs(path)

  return fs.isdir(fpath)


def exists(path):
  fs, fpath = fsspec.url_to_fs(path)

  return fs.exists(fpath)


def fs_proto(fs):
  proto = getattr(fs, 'protocol', None)

  return fs.fsid if proto is None else proto


def is_same_fs(*args):
  protos = [fs_proto(fs) for fs in args]

  return all(protos[0] == p for p in protos)


_LOCAL_PROTOS = {'file', 'local'}
_DEFAULT_LOCAL_PROTO = 'file'

def is_local_proto(proto):
  return proto in _LOCAL_PROTOS


def is_local_fs(fs):
  proto = fs_proto(fs)
  if isinstance(proto, (list, tuple)):
    return any(is_local_proto(p) for p in proto)

  return is_local_proto(proto)


def is_local_path(path):
  return is_local_proto(get_proto(path))


def get_proto(path):
  m = re.match(r'(\w+):(:|//)', path)

  return m.group(1).lower() if m else _DEFAULT_LOCAL_PROTO


def resolve_paths(*paths):
  resolved = []
  for path_arg in paths:
    if isinstance(path_arg, (list, tuple)):
      fs, path = path_arg
    else:
      fs, path = None, path_arg
    if fs is None:
      fs, path = fsspec.url_to_fs(path)
    resolved.append(FsPath(fs, path))

  return tuple(resolved)


def copy(src_path, dest_path, src_fs=None, dest_fs=None):
  src, dest = resolve_paths((src_fs, src_path), (dest_fs, dest_path))
  try:
    with src.fs.open(src.path, mode='rb') as src_fd:
      with dest.fs.open(dest.path, mode='wb') as dest_fd:
        shutil.copyfileobj(src_fd, dest_fd)
  except NotImplementedError:
    # Slow path. Likely the destination file system do not support files opened
    # in write mode, so we use the more widely available get_file+put_file APIs.
    try:
      if is_local_fs(src.fs):
        local_path = src.path
      else:
        local_path = temp_path()
        src.fs.get_file(src.path, local_path)

      dest.fs.put_file(local_path, dest.path)
    finally:
      if local_path != src.path:
        nex.no_except(os.remove, local_path)


def replace(src_path, dest_path, src_fs=None, dest_fs=None):
  src, dest = resolve_paths((src_fs, src_path), (dest_fs, dest_path))
  replaced = False
  try:
    if is_same_fs(src.fs, dest.fs):
      dest.fs.mv(src.path, dest.path)
      replaced = True
  except NotImplementedError:
    pass

  if not replaced:
    copy(src.path, dest.path, src_fs=src.fs, dest_fs=dest.fs)
    src.fs.rm(src.path)


def mkdir(path, **kwargs):
  fs, fpath = fsspec.url_to_fs(path)
  fs.mkdir(fpath, **kwargs)


def makedirs(path, **kwargs):
  fs, fpath = fsspec.url_to_fs(path)
  fs.makedirs(fpath, **kwargs)


def rmdir(path):
  fs, fpath = fsspec.url_to_fs(path)
  fs.rmdir(fpath)


def rmtree(path):
  fs, fpath = fsspec.url_to_fs(path)
  fs.rm(fpath, recursive=True)


class StatResult(obj.Obj):
  FIELDS = (
    'st_mode', 'st_ino', 'st_dev', 'st_nlink', 'st_uid', 'st_gid', 'st_size',
    'st_atime', 'st_mtime', 'st_ctime',
  )

def info_stat(info):
  sinfo = StatResult(**{k: None for k in StatResult.FIELDS})
  for k, v in info.items():
    sfield = k if k.startswith('st_') else f'st_{k}'
    if k == sfield or hasattr(sinfo, sfield):
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

  if sinfo.st_ctime is None:
    sinfo.st_ctime = info.get('created')
  # Some FS populates datetime.datetime for time fields, while Python stat()
  # standard requires timestamps (EPOCH seconds).
  for k, v in vars(sinfo).items():
    if k.endswith('time') and isinstance(v, datetime.datetime):
      setattr(sinfo, k, v.timestamp())

  return sinfo


def stat(path):
  fs, fpath = fsspec.url_to_fs(path)

  return info_stat(fs.info(fpath))


class RegexMatcher:

  def __init__(self, rex):
    self._rex = re.compile(rex)
    self.match = None

  def __call__(self, value):
    self.match = re.match(self._rex, value)

    return self.match is not None


def enumerate_files(path, matcher=None, return_stats=False):
  fs, fpath = fsspec.url_to_fs(path)
  if return_stats:
    for info in fs.ls(fpath, detail=True):
      fname = os.path.basename(info['name'])
      if matcher is None or matcher(fname):
        yield fname, info_stat(info)
  else:
    for lspath in fs.ls(fpath, detail=False):
      fname = os.path.basename(lspath)
      if matcher is None or matcher(fname):
        yield fname


def normpath(path):
  path = os.path.expandvars(path)

  fs, fpath = fsspec.url_to_fs(path)

  return fpath if is_local_fs(fs) else fs.unstrip_protocol(fpath)


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
  fs, fpath = fsspec.url_to_fs(path)

  return localfs_mount(fpath) if is_local_fs(fs) else None

