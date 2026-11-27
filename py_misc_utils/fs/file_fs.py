"""File-based FileSystem implementation using the local filesystem.

This module provides FileFs, the default FileSystem backend which operates
on plain local paths. Since files are already local, as_local() returns the
path unchanged and blocks can be read directly from the file.

Example:
  import os
  import tempfile

  from py_misc_utils.fs import file_fs

  root = tempfile.mkdtemp()
  path = os.path.join(root, 'f.txt')

  with open(path, 'w') as fd:
    fd.write('hello')

  fs = file_fs.FileFs()

  print(fs.stat(path).st_size)
"""

import os
import shutil

from .. import alog
from .. import cached_file as chf
from .. import fs_base as fsb
from .. import fs_utils as fsu
from .. import osfd


class FileReader:
  """Reader producing blocks of data from a local file."""

  def __init__(self, path):
    """Initialize the reader.

    Args:
      path: Local path of the file to read.
    """
    # Local path of the file to read.
    self._path = path

  @classmethod
  def tag(cls, sres):
    """Build a cache tag from a stat result.

    Args:
      sres: os.stat_result of the file.

    Returns:
      The cache tag embedding size and modification time.
    """
    return chf.make_tag(size=sres.st_size, mtime=sres.st_mtime)

  def support_blocks(self):
    """Return True since local files support direct block reads."""
    return True

  def read_block(self, bpath, offset, size):
    """Read a block of data from a file.

    Args:
      bpath: Path of the file to read from.
      offset: Offset in the file where reading starts.
      size: Number of bytes to read.

    Returns:
      The requested data as bytes.
    """
    if offset != chf.CachedBlockFile.WHOLE_OFFSET:
      with (osfd.OsFd(self._path, os.O_RDONLY) as
            rfd, osfd.OsFd(bpath, os.O_CREAT | os.O_TRUNC | os.O_WRONLY,
                           mode=0o440) as wfd):
        if os.lseek(rfd, offset, os.SEEK_SET) != offset:
          alog.xraise(RuntimeError, f'Unable to seek {self._path} at offset {offset}')
        data = os.read(rfd, size)
        os.write(wfd, data)

        return len(data)
    else:
      with open(self._path, mode='rb') as rfd:
        bfd = os.open(bpath, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, mode=0o440)
        with open(bfd, mode='wb') as wfd:
          shutil.copyfileobj(rfd, wfd)

      return os.path.getsize(bpath)


class FileFs(fsb.FsBase):
  """FileSystem implementation for local paths.

  It is the default backend and the reference implementation of the
  FsBase interface: every operation maps directly onto the os module
  calls of the same name.
  """

  ID = 'file'
  IDS = (ID,)

  def __init__(self, cache_iface=None, **kwargs):
    """Initialize the backend.

    Args:
      cache_iface: Optional cache interface used by inherited read
        operations; not needed since local files need no caching.
      **kwargs: Extra keyword arguments forwarded to FsBase.
    """
    super().__init__(cache_iface=cache_iface, **kwargs)

  def norm_url(self, url):
    """Normalize a local path.

    Args:
      url: The path to normalize.

    Returns:
      The normalized path, as per fs_utils.normpath().
    """
    return fsu.normpath(url)

  def _create_tag(self, sres):
    """Build the cache tag of a file from its stat result."""
    return FileReader.tag(sres)

  def stat(self, url):
    """Stat a path.

    Args:
      url: The path to stat.

    Returns:
      The DirEntry of the file, built from the os.stat result.
    """
    sres = os.stat(url)

    return fsb.DirEntry(name=os.path.basename(url),
                        path=url,
                        etag=self._create_tag(sres),
                        st_mode=sres.st_mode,
                        st_size=sres.st_size,
                        st_ctime=sres.st_ctime,
                        st_mtime=sres.st_mtime)

  def open(self, url, mode, **kwargs):
    """Open a local file.

    Args:
      url: The path of the file to open.
      mode: Open mode string.
      **kwargs: Extra open() keyword arguments; accepted for interface
        compatibility but not forwarded to the builtin open().

    Returns:
      The opened file object.
    """
    return open(url, mode=mode)

  def remove(self, url):
    """Remove a file.

    Args:
      url: The path of the file to remove.
    """
    os.remove(url)

  def rename(self, src_url, dest_url):
    """Rename a file or directory.

    Args:
      src_url: The source path.
      dest_url: The destination path.
    """
    os.rename(src_url, dest_url)

  def replace(self, src_url, dest_url):
    """Rename a file or directory, replacing the destination.

    Args:
      src_url: The source path.
      dest_url: The destination path.
    """
    os.replace(src_url, dest_url)

  def mkdir(self, url, mode=None):
    """Create a directory.

    Args:
      url: The path of the directory to create.
      mode: Optional permission mode (default: 0o777).
    """
    os.mkdir(url, mode=mode or 0o777)

  def makedirs(self, url, mode=None, exist_ok=None):
    """Create a directory and all its parents.

    Args:
      url: The path of the directory to create.
      mode: Optional permission mode (default: 0o777).
      exist_ok: If True, do not raise when the directory already exists
        (default: False).
    """
    os.makedirs(url, mode=mode or 0o777, exist_ok=exist_ok or False)

  def rmdir(self, url):
    """Remove an empty directory.

    Args:
      url: The path of the directory to remove.
    """
    os.rmdir(url)

  def rmtree(self, url, ignore_errors=None):
    """Recursively remove a directory tree.

    Args:
      url: The path of the directory tree to remove.
      ignore_errors: If True, errors raised while removing the tree are
        ignored (default: False).
    """
    fsu.safe_rmtree(url, ignore_errors=ignore_errors or False)

  def list(self, url):
    """Enumerate the entries of a directory.

    Args:
      url: The path of the directory to enumerate.

    Yields:
      A DirEntry for every file and subdirectory of the directory.
    """
    with os.scandir(url) as sdit:
      for de in sdit:
        sres = de.stat()

        yield fsb.DirEntry(name=de.name,
                           path=os.path.join(url, de.name),
                           etag=self._create_tag(sres),
                           st_mode=sres.st_mode,
                           st_size=sres.st_size,
                           st_ctime=sres.st_ctime,
                           st_mtime=sres.st_mtime)

  def put_file(self, url, data_gen):
    """Write data chunks into a file.

    Args:
      url: The path of the file to write.
      data_gen: Iterable of bytes chunks to write.
    """
    with open(url, mode='wb') as fd:
      fd.writelines(data_gen)

  def get_file(self, url):
    """Read the contents of a file as chunks.

    Args:
      url: The path of the file to read.

    Yields:
      The file contents as bytes chunks.
    """
    with open(url, mode='rb') as fd:
      yield from fsu.enum_chunks(fd)

  def as_local(self, url, **kwargs):
    """Return the path unchanged, since files are already local.

    Args:
      url: The path of the file.
      **kwargs: Extra keyword arguments, ignored.

    Returns:
      The url itself.
    """
    return url

  def link(self, src_url, dest_url):
    """Create a hard link.

    Args:
      src_url: The path of the existing file.
      dest_url: The path of the link to create.
    """
    os.link(src_url, dest_url)

  def symlink(self, src_url, dest_url):
    """Create a symbolic link.

    Args:
      src_url: The path of the existing file.
      dest_url: The path of the link to create.
    """
    os.symlink(src_url, dest_url)


FILE_SYSTEMS = (FileFs,)
