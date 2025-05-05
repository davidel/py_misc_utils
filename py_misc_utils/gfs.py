import collections
import contextlib
import importlib
import os
import pkgutil
import re
import shutil
import sys
import urllib.parse as uparse

from . import alog
from . import assert_checks as tas
from . import cached_file as chf
from . import context_managers as cm
from . import fs_utils as fsu
from . import mirror_from as mrf
from . import run_once as ro


class TempFile:

  def __init__(self, nsdir=None, nspath=None, **kwargs):
    nsdir = nsdir if nsdir is None or is_local_path(nsdir) else None
    nspath = nspath if nspath is None or is_local_path(nspath) else None

    self._fs, self._path = resolve_fs(fsu.temp_path(nspath=nspath, nsdir=nsdir), **kwargs)
    self._kwargs = kwargs
    self._fd, self._delete = None, False

  def open(self):
    self._fd = self._fs.open(self._path, **self._kwargs)
    self._delete = True
    mrf.mirror_all(self._fd, self, name='fd')

    return self

  def _close_fd(self):
    if self._fd is not None:
      self._fd.close()
      mrf.unmirror(self, name='fd')
      self._fd = None

  def close(self):
    self._close_fd()
    if self._delete:
      self._fs.remove(self._path)
      self._delete = False

  def replace(self, path):
    self._close_fd()
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
  if isinstance(path, str) and (sfd := _STD_FILES.get(path)) is not None:
    return contextlib.nullcontext(sfd)

  return open(path, **kwargs)


def open(source, **kwargs):
  if (path := path_of(source)) is not None:
    fs, fpath = resolve_fs(path, **kwargs)

    return fs.open(fpath, **kwargs)

  return contextlib.nullcontext(source)


def open_local(path, **kwargs):
  return open(path, **kwargs)


def maybe_open(path, **kwargs):
  try:
    return open(path, **kwargs)
  except:
    pass


def as_local(path, **kwargs):
  fs, fpath = resolve_fs(path, **kwargs)

  return fs.as_local(fpath, **kwargs)


def path_of(path):
  return os.fspath(path) if isinstance(path, (str, os.PathLike)) else None


PathSplit = collections.namedtuple('PathSplit', 'base, ext, purl')

def splitext(path):
  purl = uparse.urlparse(path)
  base, ext = os.path.splitext(purl.path)

  return PathSplit(base=base, ext=ext[1:], purl=purl)


def is_file(path):
  fs, fpath = resolve_fs(path)

  return fs.isfile(fpath)


def is_dir(path):
  fs, fpath = resolve_fs(path)

  return fs.isdir(fpath)


def exists(path):
  fs, fpath = resolve_fs(path)

  return fs.exists(fpath)


def is_same_fs(*args):
  specs = []
  for fspath in args:
    purl = uparse.urlparse(fspath.path)
    if purl.scheme:
      specs.append((purl.scheme, purl.netloc))
    else:
      specs.append((_DEFAULT_LOCAL_PROTO, fsu.localfs_mount(purl.path)))

  return all(specs[0] == s for s in specs[1:])


_DEFAULT_LOCAL_PROTO = 'file'

def is_local_proto(proto):
  return proto == _DEFAULT_LOCAL_PROTO


def is_local_fs(fs):
  return is_local_proto(fs.ID)


def is_local_path(path):
  return is_local_proto(get_proto(path))


def is_path(path):
  # It is a path is it contains a proto, or starts with '/', './' or '../'.
  return re.match(r'(\w+)://|/|\.\.?/', path) is not None


def get_proto(path):
  m = re.match(r'(\w+)://', path)

  return m.group(1).lower() if m else _DEFAULT_LOCAL_PROTO


FsPath = collections.namedtuple('FsPath', 'fs, path')

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

  if is_same_fs(src, dest):
    dest.fs.replace(src.path, dest.path)
  else:
    copy(src.path, dest.path, src_fs=src.fs, dest_fs=dest.fs)
    src.fs.remove(src.path)


def remove(path):
  fs, fpath = resolve_fs(path)
  fs.remove(fpath)


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


def link(src_path, dest_path):
  src, dest = resolve_paths(src_path, dest_path)

  tas.check(is_same_fs(src, dest),
            msg=f'Unable to link across file systems: {src_path} -> {dest_path}')

  src.fs.link(src.path, dest.path)


def symlink(src_path, dest_path):
  src, dest = resolve_paths(src_path, dest_path)

  tas.check(is_same_fs(src, dest),
            msg=f'Unable to symlink across file systems: {src_path} -> {dest_path}')

  src.fs.symlink(src.path, dest.path)


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


_CACHE_DIR = fsu.normpath(os.getenv('CACHE_DIR',
                                    os.path.join(fsu.home(), '.cache')))

def cache_dir(path=None):
  return fsu.normpath(path) if path else _CACHE_DIR


def set_cache_dir(path):
  global _CACHE_DIR

  _CACHE_DIR = fsu.normpath(path)


def find_mount(path):
  fs, fpath = resolve_fs(path)

  return fsu.localfs_mount(fpath) if is_local_fs(fs) else None


_FS_REGISTRY = dict()

def register_fs(cls):
  for fsid in cls.IDS:
    alog.debug(f'Registering file system: {fsid}')
    _FS_REGISTRY[fsid] = cls


def try_register(importer, modname, parent=None):
  try:
    if parent is None:
      spec = importer.find_spec(modname)
      module = importlib.util.module_from_spec(spec)
      spec.loader.exec_module(module)
    else:
      module = importlib.import_module(f'{parent}.{modname}')

    file_systems = getattr(module, 'FILE_SYSTEMS', ())
    for cls in file_systems:
      register_fs(cls)

    return module
  except ImportError as ex:
    alog.verbose(f'Unable to import file system module "{modname}": {ex}')


def register_fs_from_path(path, parent=None):
  for importer, modname, _ in pkgutil.iter_modules(path=path):
    if modname.endswith('_fs'):
      try_register(importer, modname, parent=parent)


@ro.run_once
def register_modules():
  import py_misc_utils.fs as pyfs

  register_fs_from_path(pyfs.__path__, parent='py_misc_utils.fs')

  gfs_path = os.getenv('GFS_PATH')
  if gfs_path:
    for path in gfs_path.split(':'):
      register_fs_from_path(path)


def get_proto_fs(proto, **kwargs):
  register_modules()

  cls = _FS_REGISTRY.get(proto)
  tas.check_is_not_none(cls, msg=f'Protocol "{proto}" not registered')

  return cls(**kwargs)


def resolve_fs(path, **kwargs):
  proto = get_proto(path)

  cachedir = chf.get_cache_dir(kwargs.pop('cache_dir', cache_dir()))

  cache_iface = kwargs.pop('cache_iface', None)
  if cache_iface is None:
    cache_iface = chf.CacheInterface(cachedir)

  fs = get_proto_fs(proto, cache_iface=cache_iface, cache_dir=cachedir, **kwargs)

  return fs, fs.norm_url(path)

