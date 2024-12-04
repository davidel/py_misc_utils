import collections
import contextlib
import datetime
import functools
import hashlib
import importlib
import os
import pkgutil
import re
import shutil
import stat as st
import sys

from . import assert_checks as tas
from . import cached_file as chf
from . import context_managers as cm
from . import fs_utils as fsu
from . import no_except as nex
from . import obj
from . import rnd_utils as rngu
from . import run_once as ro


FsPath = collections.namedtuple('FsPath', 'fs, path')


class TempFile:

  def __init__(self, nsdir=None, nspath=None, **kwargs):
    is_local = ((nsdir is None or is_local_path(nsdir)) and
                (nspath is None or is_local_path(nspath)))
    if is_local:
      fs, tmp_path = resolve_fs(rngu.temp_path(nspath=nspath, nsdir=nsdir))
    else:
      fs, tmp_path = resolve_fs(rngu.temp_path())

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
  fs, fpath = resolve_fs(path, **kwargs)

  return fs.open(fpath, **kwargs)


def open_source(source, **kwargs):
  if path := path_of(source):
    return open(path, **kwargs)

  return contextlib.nullcontext(source)


def path_of(path):
  return os.fspath(path) if isinstance(path, (str, os.PathLike)) else None


def maybe_open(path, **kwargs):
  try:
    return open(path, **kwargs)
  except:
    pass


def open_local(path, **kwargs):
  return open(path, **kwargs)


_LOCAL_ROFS = os.getenv('LOCAL_ROFS', 'filecache')
_LOCAL_RWFS = os.getenv('LOCAL_RWFS', 'simplecache')

def _local_args(**kwargs):
  mode = kwargs.pop('mode', 'rb')

  is_write_mode = any(s in mode for s in ('w', '+', 'a', 'x'))
  proxy_fs = _LOCAL_RWFS if is_write_mode else _LOCAL_ROFS
  cache_storage = kwargs.pop('cache_storage', None) or cache_dir()
  cache_storage = os.path.join(cache_storage, 'gfs', proxy_fs)

  kwargs['mode'] = mode
  kwargs['cache_storage'] = cache_storage
  kwargs['cache_mapper'] = CacheMapper(cache_storage)

  return proxy_fs, kwargs

def as_local(path, **kwargs):
  fs, fpath = fsspec.url_to_fs(path)
  if is_local_fs(fs):
    return fpath

  proxy_fs, lkwargs = _local_args(**kwargs)

  return fsspec.open_local(f'{proxy_fs}::{path}', **lkwargs)


def is_file(path):
  fs, fpath = resolve_fs(path)

  return fs.isfile(fpath)


def is_dir(path):
  fs, fpath = resolve_fs(path)

  return fs.isdir(fpath)


def exists(path):
  fs, fpath = resolve_fs(path)

  return fs.exists(fpath)


def fs_proto(fs):
  return fs.ID


def is_same_fs(*args):
  protos = [fs_proto(fs) for fs in args]

  return all(protos[0] == p for p in protos)


_DEFAULT_LOCAL_PROTO = 'file'

def is_local_proto(proto):
  return proto == _DEFAULT_LOCAL_PROTO


def is_local_fs(fs):
  return is_local_proto(fs_proto(fs))


def is_local_path(path):
  return is_local_proto(get_proto(path))


def get_proto(path):
  m = re.match(r'(\w+)://', path)

  return m.group(1).lower() if m else _DEFAULT_LOCAL_PROTO


def resolve_paths(*paths):
  resolved = []
  for path_arg in paths:
    if isinstance(path_arg, (list, tuple)):
      fs, path = path_arg
    else:
      fs, path = None, path_arg
    if fs is None:
      fs, path = resolve_fs(path)

    resolved.append(FsPath(fs, path))

  return tuple(resolved)


def copy(src_path, dest_path, src_fs=None, dest_fs=None):
  src, dest = resolve_paths((src_fs, src_path), (dest_fs, dest_path))

  src.fs.copyfile(src.path, dest.fs, dest.path)


def replace(src_path, dest_path, src_fs=None, dest_fs=None):
  src, dest = resolve_paths((src_fs, src_path), (dest_fs, dest_path))

  if is_same_fs(src.fs, dest.fs):
    dest.fs.replace(src.path, dest.path)
  else:
    copy(src.path, dest.path, src_fs=src.fs, dest_fs=dest.fs)
    src.fs.remove(src.path)


def mkdir(path, **kwargs):
  fs, fpath = resolve_fs(path)
  fs.mkdir(fpath, **kwargs)


def makedirs(path, **kwargs):
  fs, fpath = resolve_fs(path)
  fs.makedirs(fpath, **kwargs)


def rmdir(path):
  fs, fpath = resolve_fs(path)
  fs.rmdir(fpath)


def rmtree(path, **kwargs):
  fs, fpath = resolve_fs(path)
  fs.rmtree(fpath, **kwargs)


def stat(path):
  fs, fpath = resolve_fs(path)

  return fs.stat(fpath)


class RegexMatcher:

  def __init__(self, rex):
    self._rex = re.compile(rex)
    self.match = None

  def __call__(self, value):
    self.match = re.match(self._rex, value)

    return self.match is not None


def enumerate_files(path, matcher=None, return_stats=False):
  fs, fpath = resolve_fs(path)

  for de in fs.list(fpath):
    if matcher is None or matcher(de.name):
      if return_stats:
        yield de.name, de
      else:
        yield de.name


def normpath(path):
  _, fpath = resolve_fs(path)

  return fpath


def cache_dir(path=None):
  return chf.get_cache_dir(path=path)


def find_mount(path):
  fs, fpath = fsspec.url_to_fs(path)

  return fsu.localfs_mount(fpath) if is_local_fs(fs) else None


_FS_REGISTRY = dict()

def register_fs(cls):
  for fsid in cls.IDS:
    _FS_REGISTRY[fsid] = cls


def register_fs_from_path(path):
  for importer, modname, _ in pkgutil.iter_modules(path=path):
    if modname.endswith('_fs'):
      spec = importer.find_spec(modname)
      module = importlib.util.module_from_spec(spec)

      file_systems = getattr(module, 'FILE_SYSTEMS', ())
      for cls in file_systems:
        register_fs(cls)


@ro.run_once
def register_modules():
  import py_misc_utils.fs as pyfs

  register_fs_from_path(pyfs.__path__)

  gfs_path = os.getenv('GFS_PATH')
  if gfs_path:
    for path in gfs_path.split(':'):
      register_fs_from_path(path)


def get_proto_fs(proto, **kwargs):
  cls = _FS_REGISTRY.get(proto)
  tas.check_is_not_none(cls, msg=f'Protocol "{proto}" not registered')

  return cls(**kwargs)


def resolve_fs(path, **kwargs):
  proto = get_proto(path)

  cachedir = kwargs.pop('cache_dir', None)
  if cachedir is None:
    cachedir = cache_dir()
  cache_ctor = kwargs.pop('cache_ctor', None)
  if cache_ctor is None:
    cache_ctor = functools.partial(chf.create_cached_file, cache_dir=cachedir)

  fs = get_proto_fs(proto, cache_ctor=cache_ctor, cache_dir=cachedir, **kwargs)

  return fs, fs.norm_url(path)


register_modules()

