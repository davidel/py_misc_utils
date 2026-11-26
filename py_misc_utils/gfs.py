"""Generic filesystem API dispatching over local paths and URL schemes.

This module is the entry point of the filesystem layer: it routes
open/copy/stat/list/remove and related operations to the FileSystem
backend registered for the scheme of the path or URL (file, ftp, http,
gs, s3), backed by the fs/ subpackage. It also provides TempFile for
atomic writes, RegexMatcher to filter enumerations, and registration
helpers to extend the supported schemes with additional backends.
"""

import collections
import contextlib
import importlib
import os
import pkgutil
import re
import sys
import urllib.parse as uparse

from . import alog
from . import assert_checks as tas
from . import cached_file as chf
from . import fs_utils as fsu
from . import mirror_from as mrf
from . import run_once as ro


class TempFile:
  """Temporary file which is removed unless replaced into a final path.

  Attributes:
    fd: The file descriptor object of the file, mirrored after open().
  """

  def __init__(self, nsdir=None, nspath=None, **kwargs):
    """Create the path of the temporary file.

    Args:
      nsdir: Optional namespace directory the temp path is created in;
        non-local paths are ignored.
      nspath: Optional namespace path used to derive the temp path;
        non-local paths are ignored.
      **kwargs: Open keyword arguments used when open() is called.

    Attributes:
      fd: The file descriptor object of the file, mirrored after open().
    """
    nsdir = nsdir if nsdir is None or is_local_path(nsdir) else None
    nspath = nspath if nspath is None or is_local_path(nspath) else None

    self._fs, self._path = resolve_fs(fsu.temp_path(nspath=nspath, nsdir=nsdir),
                                      **kwargs)
    self._kwargs = kwargs
    self._fd, self._delete = None, False

  def open(self):
    """Open the file, mirroring its descriptor attributes.

    Returns:
      The TempFile instance itself.
    """
    self._fd = self._fs.open(self._path, **self._kwargs)
    self._delete = True
    mrf.mirror_all(self._fd, self, name='fd')

    return self

  def _close_fd(self):
    """Close the file descriptor, unmirroring its attributes."""
    if self._fd is not None:
      self._fd.close()
      mrf.unmirror(self, name='fd')
      self._fd = None

  def close(self):
    """Close the file and remove it, unless it was replaced."""
    self._close_fd()
    if self._delete:
      self._fs.remove(self._path)
      self._delete = False

  def replace(self, path):
    """Atomically move the file into its final destination path.

    Args:
      path: Destination path or URL.
    """
    self._close_fd()
    replace(self._path, path, src_fs=self._fs)
    self._delete = False

  def __enter__(self):
    """Open the file and return the TempFile itself."""
    self.open()

    return self

  def __exit__(self, *exc):
    """Close the file.

    Args:
      *exc: Exception information of the context, if any.

    Returns:
      False, so that exceptions are not suppressed.
    """
    self.close()

    return False


_STD_FILES = {
    'STDIN': sys.stdin,
    'STDOUT': sys.stdout,
    'STDERR': sys.stderr,
}


def std_open(path, **kwargs):
  """Open a standard stream by name or a plain local file.

  Args:
    path: Path of the file to open, or one of 'STDIN', 'STDOUT' and
      'STDERR' to open the corresponding standard stream.
    **kwargs: Open keyword arguments, ignored for the standard streams.

  Returns:
    A context manager yielding the opened file or standard stream.
  """
  if isinstance(path, str) and (sfd := _STD_FILES.get(path)) is not None:
    return contextlib.nullcontext(sfd)

  return open(path, **kwargs)


def open(source, **kwargs):
  """Open a path or URL through its FileSystem backend.

  Args:
    source: Path or URL to open, or an already opened file-like object,
      which is yielded unchanged.
    **kwargs: Open keyword arguments, including the cache settings.

  Returns:
    The opened file-like object, or a context manager yielding it.
  """
  if (path := path_of(source)) is not None:
    fs, fpath = resolve_fs(path, **kwargs)

    return fs.open(fpath, **kwargs)

  return contextlib.nullcontext(source)


def open_local(path, **kwargs):
  """Open a plain local file, bypassing the URL dispatching.

  Args:
    path: Path of the local file to open.
    **kwargs: Open keyword arguments.

  Returns:
    The opened file-like object.
  """
  return open(path, **kwargs)


def maybe_open(path, **kwargs):
  """Open a path, returning None instead of raising.

  Args:
    path: Path or URL to open.
    **kwargs: Open keyword arguments.

  Returns:
    The opened file-like object, or None when opening failed.
  """
  try:
    return open(path, **kwargs)
  # Open dispatches over all fs backends - each raises its own error types:
  # any open failure answers None, per the documented contract.
  except Exception:  # noqa: BLE001, S110
    pass


def as_local(path, **kwargs):
  """Return a local path holding the contents of a path or URL.

  Args:
    path: Path or URL to make available locally.
    **kwargs: Keyword arguments forwarded to the backend, including the
      cache settings.

  Returns:
    The local path of the file, downloaded through the cache interface
    when the contents are remote.
  """
  fs, fpath = resolve_fs(path, **kwargs)

  return fs.as_local(fpath, **kwargs)


def path_of(path):
  """Return the filesystem path of a path-like object.

  Args:
    path: A string, a path-like object, or anything else.

  Returns:
    The string path, or None when path is not path-like.
  """
  return os.fspath(path) if isinstance(path, (str, os.PathLike)) else None


PathSplit = collections.namedtuple('PathSplit', 'base, ext, purl')


def splitext(path):
  """Split a path or URL into its base and extension.

  Args:
    path: Path or URL to split.

  Returns:
    A PathSplit namedtuple with the path without the extension, the
    extension without the leading dot, and the parsed URL.
  """
  purl = uparse.urlparse(path)
  base, ext = os.path.splitext(purl.path)

  return PathSplit(base=base, ext=ext[1:], purl=purl)


def is_file(path):
  """Check if a path exists and is a regular file.

  Args:
    path: Path or URL to check.

  Returns:
    True if the path is a regular file.
  """
  fs, fpath = resolve_fs(path)

  return fs.isfile(fpath)


def is_dir(path):
  """Check if a path exists and is a directory.

  Args:
    path: Path or URL to check.

  Returns:
    True if the path is a directory.
  """
  fs, fpath = resolve_fs(path)

  return fs.isdir(fpath)


def exists(path):
  """Check if a path or URL exists.

  Args:
    path: Path or URL to check.

  Returns:
    True if the path exists.
  """
  fs, fpath = resolve_fs(path)

  return fs.exists(fpath)


def is_same_fs(*args):
  """Check if DirEntry objects belong to the same filesystem.

  Args:
    *args: DirEntry objects to compare, each holding a path attribute.

  Returns:
    True when every entry belongs to the same protocol and host.
  """
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
  """Check if a protocol refers to the local filesystem.

  Args:
    proto: Protocol name to check.

  Returns:
    True when the protocol is the default local one ('file').
  """
  return proto == _DEFAULT_LOCAL_PROTO


def is_local_fs(fs):
  """Check if a FileSystem backend operates on local paths.

  Args:
    fs: FileSystem backend to check.

  Returns:
    True when the backend id is the local protocol.
  """
  return is_local_proto(fs.ID)


def is_local_path(path):
  """Check if a path or URL refers to the local filesystem.

  Args:
    path: Path or URL to check.

  Returns:
    True when the protocol of the path is the local one.
  """
  return is_local_proto(get_proto(path))


def is_path(path):
  """Check if a string holds a path or URL.

  Args:
    path: String to check.

  Returns:
    True when the string contains a protocol, or starts with '/', './'
    or '../'.
  """
  # It is a path is it contains a proto, or starts with '/', './' or '../'.
  return has_proto(path) or re.match(r'/|\.\.?/', path) is not None


def has_proto(path):
  """Check if a string holds a protocol-qualified URL.

  Args:
    path: String to check.

  Returns:
    True when the string starts with a 'proto://' prefix.
  """
  return re.match(r'\w+://', path) is not None


def get_proto(path):
  """Return the protocol of a path or URL.

  Args:
    path: Path or URL to inspect.

  Returns:
    The lower-case protocol name, or the default local protocol for
    plain paths.
  """
  m = re.match(r'(\w+)://', path)

  return m.group(1).lower() if m else _DEFAULT_LOCAL_PROTO


FsPath = collections.namedtuple('FsPath', 'fs, path')
# A resolved path: its FileSystem backend and backend-specific path.


def resolve_paths(*paths):
  """Resolve multiple paths into FsPath entries.

  Args:
    *paths: Paths or URLs to resolve; a (fs, path) tuple is used as-is
      when its filesystem is already given.

  Returns:
    A tuple of FsPath namedtuple with the resolved backend and path.
  """
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
  """Copy a file across filesystems.

  Args:
    src_path: Source path or URL.
    dest_path: Destination path or URL.
    src_fs: Optional pre-resolved FileSystem of the source.
    dest_fs: Optional pre-resolved FileSystem of the destination.
  """
  src, dest = resolve_paths((src_fs, src_path), (dest_fs, dest_path))

  src.fs.copyfile(src.path, dest.fs, dest.path)


def replace(src_path, dest_path, src_fs=None, dest_fs=None):
  """Replace a file with another one, across filesystems.

  The rename is done in place when source and destination are on the
  same filesystem, otherwise it is implemented as a copy followed by a
  source removal.

  Args:
    src_path: Source path or URL.
    dest_path: Destination path or URL.
    src_fs: Optional pre-resolved FileSystem of the source.
    dest_fs: Optional pre-resolved FileSystem of the destination.
  """
  src, dest = resolve_paths((src_fs, src_path), (dest_fs, dest_path))

  if is_same_fs(src, dest):
    dest.fs.replace(src.path, dest.path)
  else:
    copy(src.path, dest.path, src_fs=src.fs, dest_fs=dest.fs)
    src.fs.remove(src.path)


def remove(path):
  """Remove a file.

  Args:
    path: Path or URL of the file to remove.
  """
  fs, fpath = resolve_fs(path)
  fs.remove(fpath)


def mkdir(path, **kwargs):
  """Create a directory.

  Args:
    path: Path or URL of the directory to create.
    **kwargs: Keyword arguments forwarded to the backend.
  """
  fs, fpath = resolve_fs(path)
  fs.mkdir(fpath, **kwargs)


def makedirs(path, **kwargs):
  """Create a directory and all its parents.

  Args:
    path: Path or URL of the directory to create.
    **kwargs: Keyword arguments forwarded to the backend.
  """
  fs, fpath = resolve_fs(path)
  fs.makedirs(fpath, **kwargs)


def rmdir(path):
  """Remove an empty directory.

  Args:
    path: Path or URL of the directory to remove.
  """
  fs, fpath = resolve_fs(path)
  fs.rmdir(fpath)


def rmtree(path, **kwargs):
  """Recursively remove a directory tree.

  Args:
    path: Path or URL of the directory tree to remove.
    **kwargs: Keyword arguments forwarded to the backend.
  """
  fs, fpath = resolve_fs(path)
  fs.rmtree(fpath, **kwargs)


def stat(path):
  """Stat a path or URL.

  Args:
    path: Path or URL to stat.

  Returns:
    The DirEntry of the path.
  """
  fs, fpath = resolve_fs(path)

  return fs.stat(fpath)


def link(src_path, dest_path):
  """Create a hard link within the same filesystem.

  Args:
    src_path: Path or URL of the existing file.
    dest_path: Path or URL of the link to create.
  """
  src, dest = resolve_paths(src_path, dest_path)

  tas.check(is_same_fs(src, dest),
            msg=f'Unable to link across file systems: {src_path} -> {dest_path}')

  src.fs.link(src.path, dest.path)


def symlink(src_path, dest_path):
  """Create a symbolic link within the same filesystem.

  Args:
    src_path: Path or URL of the existing file.
    dest_path: Path or URL of the link to create.
  """
  src, dest = resolve_paths(src_path, dest_path)

  tas.check(is_same_fs(src, dest),
            msg=f'Unable to symlink across file systems: {src_path} -> {dest_path}')

  src.fs.symlink(src.path, dest.path)


class RegexMatcher:
  """File name matcher enumerating the entries matching a regex."""

  def __init__(self, rex):
    """Compile the regular expression.

    Args:
      rex: Regular expression the file names must match; a None match
        indicates no filtering.
    """
    self._rex = re.compile(rex)
    self.match = None

  def __call__(self, value):
    """Match a file name, recording the match object.

    Args:
      value: File name to match against the regular expression.

    Returns:
      True when the name matches.
    """
    self.match = re.match(self._rex, value)

    return self.match is not None


def enumerate_files(path, matcher=None, return_stats=False):
  """Enumerate the file names of a directory.

  Args:
    path: Path or URL of the directory to enumerate.
    matcher: Optional matcher object filtering the yielded names.
    return_stats: If True, yield (name, DirEntry) pairs instead of
      plain names (default: False).

  Yields:
    The file names of the directory, or (name, DirEntry) pairs when
    return_stats is set.
  """
  fs, fpath = resolve_fs(path)

  for de in fs.list(fpath):
    if matcher is None or matcher(de.name):
      if return_stats:
        yield de.name, de
      else:
        yield de.name


def normpath(path):
  """Normalize a path or URL into its backend-specific path.

  Args:
    path: Path or URL to normalize.

  Returns:
    The normalized path as seen by its FileSystem backend.
  """
  _, fpath = resolve_fs(path)

  return fpath


_CACHE_DIR = fsu.normpath(os.getenv('CACHE_DIR', os.path.join(fsu.home(), '.cache')))


def cache_dir():
  """Return the current directory used to cache remote file contents."""
  return _CACHE_DIR


def set_cache_dir(path):
  """Set the directory used to cache remote file contents.

  Args:
    path: Path of the cache directory.
  """
  global _CACHE_DIR

  _CACHE_DIR = fsu.normpath(path)


def find_mount(path):
  """Find the mount point of a local path.

  Args:
    path: Path or URL to inspect.

  Returns:
    The mount point of the path for local filesystems, or None for
    remote ones.
  """
  fs, fpath = resolve_fs(path)

  return fsu.localfs_mount(fpath) if is_local_fs(fs) else None


_FS_REGISTRY = dict()


def register_fs(cls):
  """Register a FileSystem backend class for its protocols.

  Args:
    cls: The FileSystem class, exposing IDS and a cache_iface/cache_dir
      accepting constructor.
  """
  for fsid in cls.IDS:
    alog.debug(f'Registering file system: {fsid}')
    _FS_REGISTRY[fsid] = cls


def try_register(importer, modname, parent=None):
  """Import a module and register the FileSystem classes it exports.

  Args:
    importer: The package finder providing find_spec().
    modname: Name of the module to import.
    parent: Optional parent package name used to import the module as
      parent.modname.

  Returns:
    The imported module, or None when the import failed.
  """
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
  """Register the FileSystem classes of the *_fs modules of a directory.

  Args:
    path: Directory path to scan for *_fs modules.
    parent: Optional parent package name used to import the modules as
      parent.modname.
  """
  for importer, modname, _ in pkgutil.iter_modules(path=path):
    if modname.endswith('_fs'):
      try_register(importer, modname, parent=parent)


@ro.run_once
def register_modules():
  """Register the built-in and GFS_PATH FileSystem backends.

  The py_misc_utils.fs backends are registered once, plus any module
  found in the colon-separated GFS_PATH environment variable.
  """
  import py_misc_utils.fs as pyfs

  register_fs_from_path(pyfs.__path__, parent='py_misc_utils.fs')

  gfs_path = os.getenv('GFS_PATH')
  if gfs_path:
    for path in gfs_path.split(':'):
      register_fs_from_path(path)


def get_proto_fs(proto, **kwargs):
  """Create a FileSystem instance for a protocol.

  Args:
    proto: Protocol name of the backend to instantiate.
    **kwargs: Constructor keyword arguments forwarded to the backend.

  Returns:
    The instantiated FileSystem backend.

  Raises:
    ValueError: When the protocol is not registered.
  """
  register_modules()

  cls = _FS_REGISTRY.get(proto)
  tas.check_is_not_none(cls, msg=f'Protocol "{proto}" not registered')

  return cls(**kwargs)


def resolve_fs(path, **kwargs):
  """Resolve the FileSystem backend and normalized path of a path/URL.

  Args:
    path: Path or URL to resolve.
    **kwargs: Keyword arguments consumed by the cache settings
      (cache_dir, cache_iface) and forwarded to the backend constructor.

  Returns:
    A tuple with the FileSystem backend and the normalized path.
  """
  proto = get_proto(path)

  cachedir = chf.get_cache_dir(kwargs.pop('cache_dir', cache_dir()))

  cache_iface = kwargs.pop('cache_iface', None)
  if cache_iface is None:
    cache_iface = chf.CacheInterface(cachedir)

  fs = get_proto_fs(proto, cache_iface=cache_iface, cache_dir=cachedir, **kwargs)

  return fs, fs.norm_url(path)
