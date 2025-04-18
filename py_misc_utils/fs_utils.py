import contextlib
import functools
import io
import os
import pathlib
import shutil
import tempfile

from . import alog
from . import assert_checks as tas
from . import osfd
from . import rnd_utils as rngu


def home():
  return pathlib.Path.home()


def maybe_remove(path):
  if os.path.exists(path):
    os.remove(path)


def is_binary(fd):
  if (mode := getattr(fd, 'mode', None)) is not None:
    return 'b' in mode

  return not isinstance(fd, io.TextIOBase)


def link_or_copy(src_path, dest_path):
  try:
    os.link(src_path, dest_path)

    return dest_path
  except OSError as ex:
    alog.debug(f'Harklink failed from "{src_path}" to "{dest_path}", trying symlink. ' \
               f'Error was: {ex}')

  try:
    os.symlink(src_path, dest_path)

    return dest_path
  except OSError:
    alog.debug(f'Symlink failed from "{src_path}" to "{dest_path}", going to copy. ' \
               f'Error was: {ex}')

  shutil.copyfile(src_path, dest_path)
  shutil.copystat(src_path, dest_path)

  return dest_path


def is_newer_file(path, other):
  return os.stat(path).st_mtime > os.stat(other).st_mtime


def os_opener(*args, **kwargs):
  return functools.partial(os.open, *args, **kwargs)


def safe_rmtree(path, **kwargs):
  tpath = temp_path(nspath=path)
  try:
    os.rename(path, tpath)
  except FileNotFoundError:
    if not kwargs.get('ignore_errors', False):
      raise
  else:
    shutil.rmtree(tpath, **kwargs)


def readall(fd):
  if isinstance(fd, str):
    with osfd.OsFd(fd, os.O_RDONLY) as ifd:
      return readall(ifd)
  else:
    sres = os.stat(fd)
    os.lseek(fd, 0, os.SEEK_SET)

    return os.read(fd, sres.st_size)


def stat(path):
  try:
    return os.stat(path)
  except:
    pass


def path_split(path):
  path_parts = []
  while True:
    parts = os.path.split(path)
    if parts[0] == path:
      path_parts.append(parts[0])
      break
    elif parts[1] == path:
      path_parts.append(parts[1])
      break
    else:
      path = parts[0]
      path_parts.append(parts[1])

  path_parts.reverse()

  return path_parts


def drop_ext(path, exts):
  xpath, ext = os.path.splitext(path)

  if isinstance(exts, str):
    return xpath if ext == exts else path

  return xpath if ext in exts else path


def normpath(path):
  path = os.path.expanduser(path)
  path = os.path.expandvars(path)
  path = os.path.abspath(path)

  return os.path.normpath(path)


def localfs_mount(path):
  while True:
    parent_path = os.path.dirname(path)
    if path == parent_path or os.path.ismount(path):
      return path
    path = parent_path


def enum_chunks(stream, chunk_size=16 * 1024**2):
  while True:
    data = stream.read(chunk_size)
    if data:
      yield data
    if chunk_size > len(data):
      break


def du(path, follow_symlinks=None, visited=None):
  visited = set() if visited is None else visited
  follow_symlinks = follow_symlinks not in (None, False)

  size, dirs = 0, []
  if path not in visited:
    visited.add(path)
    with os.scandir(path) as sdit:
      for de in sdit:
        if de.is_file(follow_symlinks=follow_symlinks):
          sres = de.stat()
          size += sres.st_size
        elif de.is_dir(follow_symlinks=follow_symlinks):
          dirs.append(de.name)

    for dname in dirs:
      size += du(os.path.join(path, dname),
                 follow_symlinks=follow_symlinks,
                 visited=visited)

  return size


_TMPFN_RNDSIZE = int(os.getenv('TMPFN_RNDSIZE', 10))

def temp_path(nspath=None, nsdir=None, rndsize=_TMPFN_RNDSIZE):
  if nspath is not None:
    bpath, ext = os.path.splitext(nspath)

    return f'{bpath}.{rngu.rand_string(rndsize)}{ext}'

  nsdir = tempfile.gettempdir() if nsdir is None else nsdir

  return os.path.join(nsdir, f'{rngu.rand_string(rndsize)}.tmp')


# This does FileOverwrite() task (although locally limited) but here we do not
# pull that dependency to allow inlcusion in this module (which allows none).
@contextlib.contextmanager
def atomic_write(path, mode='wb', create_parents=False):
  tpath = temp_path(nspath=path)

  if create_parents:
    os.makedirs(os.path.dirname(path), exist_ok=True)

  fd = open(tpath, mode=mode)
  try:
    yield fd
    fd.close()
    fd = None
  finally:
    if fd is not None:
      fd.close()
      os.remove(tpath)
    else:
      os.replace(tpath, path)

