"""Filesystem and file-path utilities.

This module collects general purpose filesystem helpers: path
manipulation and normalization, file checks and copies, atomic file
writes, content read/write (read_content/write_content), temporary
(unique) paths, directory size computation and file search over a list
of folders.
"""
import contextlib
import functools
import io
import os
import pathlib
import shutil
import string
import tempfile

from . import alog, osfd
from . import rnd_utils as rngu


def home():
  """Returns the user home directory path."""
  return pathlib.Path.home()


def maybe_remove(path):
  """Removes a file, if it exists.

  Args:
    path: The file path to remove.
  """
  if os.path.exists(path):
    os.remove(path)


def is_binary(fd):
  """Checks whether a file object is opened in binary mode.

  Args:
    fd: The file object to check.

  Returns:
    True if the file object is opened in binary mode - either per its
    'b' mode flag, or because it does not implement the text I/O
    interface.
  """
  if (mode := getattr(fd, 'mode', None)) is not None:
    return 'b' in mode

  return not isinstance(fd, io.TextIOBase)


def link_or_copy(src_path, dest_path):
  """Links a file, falling back to symlinking and then copying.

  Creates dest_path as a hard link of src_path if possible; if the
  hard link cannot be created - e.g. because the paths are on different
  filesystems - a symbolic link is used; if also the symlink cannot be
  created - e.g. on filesystems not supporting them - the file is
  copied.

  Args:
    src_path: The source file path.
    dest_path: The destination file path.

  Returns:
    The destination file path.
  """
  try:
    os.link(src_path, dest_path)

    return dest_path
  except OSError as ex:
    alog.debug(f'Harklink failed from "{src_path}" to "{dest_path}", trying symlink. ' \
               f'Error was: {ex}')

  try:
    os.symlink(src_path, dest_path)

    return dest_path
  except OSError as ex:
    alog.debug(f'Symlink failed from "{src_path}" to "{dest_path}", going to copy. ' \
               f'Error was: {ex}')

  shutil.copyfile(src_path, dest_path)
  shutil.copystat(src_path, dest_path)

  return dest_path


def is_newer_file(path, other):
  """Checks whether a file is newer than another one.

  Args:
    path: The file path to check.
    other: The reference file path.

  Returns:
    True if the modification time of path is newer than the one of
    other.
  """
  return os.stat(path).st_mtime > os.stat(other).st_mtime


def os_opener(*args, **kwargs):
  """Builds an opener function for os.open()-like APIs.

  Args:
    *args: Positional arguments passed to os.open().
    **kwargs: Keyword arguments passed to os.open().

  Returns:
    A callable opening a file descriptor with the given arguments.
  """
  return functools.partial(os.open, *args, **kwargs)


def safe_rmtree(path, **kwargs):
  """Removes a directory tree in an atomic fashion.

  The directory is first renamed to a temporary path - so that other
  threads/processes will not see a partially-deleted tree - and then
  removed.

  Args:
    path: The directory tree to remove.
    **kwargs: Keyword arguments passed to shutil.rmtree() - like
      ignore_errors.

  Raises:
    FileNotFoundError: If the directory does not exist and
      ignore_errors is not set.
  """
  tpath = temp_path(nspath=path)
  try:
    os.rename(path, tpath)
  except FileNotFoundError:
    if not kwargs.get('ignore_errors', False):
      raise
  else:
    shutil.rmtree(tpath, **kwargs)


def readall(fd):
  """Reads the whole content of a file.

  Args:
    fd: The file to read: either a path, or a file descriptor.

  Returns:
    The whole file content, as bytes.
  """
  if isinstance(fd, str):
    with osfd.OsFd(fd, os.O_RDONLY) as ifd:
      return readall(ifd)
  else:
    sres = os.stat(fd)
    os.lseek(fd, 0, os.SEEK_SET)

    return os.read(fd, sres.st_size)


def read_content(path, binary=True):
  """Reads the whole content of a file.

  Args:
    path: The file path to read (string).
    binary: If True (default), returns the raw bytes content.
      Otherwise returns the content decoded as UTF-8 str.

  Returns:
    The whole file content, as bytes if binary=True, else as str.
  """
  if binary:
    with open(path, 'rb') as f:
      return f.read()
  else:
    with open(path, 'r', encoding='utf-8') as f:
      return f.read()


def write_content(path, data, binary=True):
  """Writes data to a file.

  Args:
    path: The file path to write to (string).
    data: The data to write. If binary=True, data is written as bytes
      (a str argument is encoded as UTF-8). If binary=False, data is
      written as str (a bytes argument is decoded as UTF-8).
    binary: If True (default), writes bytes; otherwise writes str.
  """
  if binary:
    if isinstance(data, str):
      data = data.encode('utf-8')
    with open(path, 'wb') as f:
      f.write(data)
  else:
    if isinstance(data, bytes):
      data = data.decode('utf-8')
    with open(path, 'w', encoding='utf-8') as f:
      f.write(data)


def stat(path):
  """Stats a file, returning None instead of raising on failure.

  Args:
    path: The file path to stat.

  Returns:
    The file stat result, or None if the file does not exist - or cannot
    be accessed.
  """
  try:
    return os.stat(path)
  except OSError:
    pass


def path_split(path):
  """Splits a path into its components.

  Args:
    path: The path to split.

  Returns:
    The list of path components - the first one possibly being the root
    (drive/volume) component.
  """
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
  """Removes the file extension from a path.

  Args:
    path: The file path.
    exts: The extension - or list/tuple of extensions - to remove, with
      the leading dot.

  Returns:
    The path without the matched extension, or the original path if the
    extension is not matched.
  """
  xpath, ext = os.path.splitext(path)

  if isinstance(exts, str):
    return xpath if ext == exts else path

  return xpath if ext in exts else path


def normpath(path):
  """Normalizes a path - expanding user/vars and making it absolute.

  Args:
    path: The path to normalize.

  Returns:
    The normalized absolute path.
  """
  path = os.path.expanduser(path)
  path = os.path.expandvars(path)
  path = os.path.abspath(path)

  return os.path.normpath(path)


def subst_path(path, ref_path):
  """Substitutes placeholders of a reference file path into a template.

  Args:
    path: The path template, holding $B, $D, $R, $N and $X placeholders.
    ref_path: The reference file path, whose components are substituted
      into the template.

  Returns:
    The path with the substituted placeholders - where $B is the
    reference file name, $D its directory, $R the whole reference path,
    $N the file name without extension and $X the extension.
  """
  spath = os.path.abspath(ref_path)
  dpath = os.path.dirname(spath)
  fname = os.path.basename(spath)
  name, ext = os.path.splitext(fname)

  path_template = string.Template(path)

  return path_template.substitute(B=fname,
                                  D=dpath,
                                  R=os.path.join(dpath, fname),
                                  N=name,
                                  X=ext)


def localfs_mount(path):
  """Returns the mount point of a local filesystem path.

  Args:
    path: The path whose mount point is looked up.

  Returns:
    The closest existing ancestor of the path which is a mount point -
    or the filesystem root, if none is found.
  """
  while True:
    parent_path = os.path.dirname(path)
    if path == parent_path or os.path.ismount(path):
      return path
    path = parent_path


def enum_chunks(stream, chunk_size=16 * 1024**2):
  """Enumerates the chunks of data of a stream.

  Args:
    stream: The stream to read.
    chunk_size: The maximum size of each chunk, in bytes.

  Yields:
    Chunks of data, as bytes - the last one possibly being shorter than
    the chunk size.
  """
  while True:
    data = stream.read(chunk_size)
    if data:
      yield data
    if chunk_size > len(data):
      break


def du(path, follow_symlinks=None, visited=None):
  """Computes the disk usage of a directory tree.

  Args:
    path: The directory tree root path.
    follow_symlinks: Whether symlinks are followed - in which case
      linked files/directories are accounted for as well. By default
      symlinks are not followed.
    visited: An optional set of already-visited paths, allowing the
      traversal of multiple trees sharing paths without accounting the
      shared paths twice.

  Returns:
    The total size - in bytes - of the files held by the directory tree.
  """
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


def find_path(name, paths, checkfn=os.path.exists):
  """Searches for a file name in a list of folders.

  Args:
    name: The file name to search.
    paths: The list of folder paths to search into.
    checkfn: An optional function checking whether the candidate path
      exists - os.path.exists by default.

  Returns:
    The first matching path found, or None if no folder holds the file
    name.
  """
  for path in paths:
    cpath = os.path.join(path, name)
    if checkfn(cpath):
      return cpath


_TMPFN_RNDSIZE = int(os.getenv('TMPFN_RNDSIZE', '10'))


def temp_path(nspath=None, nsdir=None, rndsize=_TMPFN_RNDSIZE):
  """Builds a unique (temporary) path.

  Args:
    nspath: An optional base path: the unique path is built by inserting
      a random suffix between its base name and its extension. If None,
      the path is built inside a namespace directory.
    nsdir: The directory where the unique path is built - when nspath is
      None. If None, the system temp directory is used.
    rndsize: The length of the random suffix.

  Returns:
    A unique path - not clashing with already-existing files.
  """
  if nspath is not None:
    bpath, ext = os.path.splitext(nspath)

    return f'{bpath}.{rngu.rand_string(rndsize)}{ext}'

  nsdir = tempfile.gettempdir() if nsdir is None else nsdir

  return os.path.join(nsdir, f'{rngu.rand_string(rndsize)}.tmp')


# This does FileOverwrite() task (although locally limited) but here we do not
# pull that dependency to allow inlcusion in this module (which allows none).
@contextlib.contextmanager
def atomic_write(path, mode='wb', create_parents=False):
  """Writes a file atomically.

  The file is first written into a temporary path, and then atomically
  renamed over the destination path - so that readers never observe a
  partially-written file. If the write fails, the destination file is
  left untouched.

  Args:
    path: The destination file path.
    mode: The open mode of the temporary file.
    create_parents: Whether the parent directory of the destination
      path is created if missing.

  Yields:
    The open file object of the temporary file.
  """
  tpath = temp_path(nspath=path)

  if create_parents:
    os.makedirs(os.path.dirname(path), exist_ok=True)

  completed = False
  try:
    with open(tpath, mode=mode) as fd:
      yield fd
    completed = True
  finally:
    if completed:
      os.replace(tpath, path)
    else:
      os.remove(tpath)


@contextlib.contextmanager
def cwd(path):
  """Changes the current working directory within a context.

  Args:
    path: The working directory path to switch to.

  Yields:
    The new working directory path.
  """
  cpath = os.getcwd()
  os.chdir(path)
  try:
    yield path
  finally:
    os.chdir(cpath)
