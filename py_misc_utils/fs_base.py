"""Base class for filesystem abstractions.

This module defines DirEntry, a namedtuple for directory listing entries, and
FsBase, an abstract base class that all filesystem implementations must
subclass. FsBase provides common path/mode utilities and declares the abstract
interface for filesystem operations.

Example:
  import os
  import tempfile

  from py_misc_utils import gfs

  root = tempfile.mkdtemp()
  path = os.path.join(root, 'f.txt')

  with open(path, 'w') as fd:
    fd.write('hello')

  fs = gfs.get_proto_fs('file')
  entry = fs.stat(path)

  print(entry.name, entry.st_size)
"""

import abc
import collections
import os
import stat as st

DirEntry = collections.namedtuple(
    'DirEntry',
    'name, st_mode, st_size, st_ctime, st_mtime, path, etag',
    defaults=(None, None))
# Named tuple representing a directory entry:
# - name: Entry name string.
# - st_mode: File mode bits (from os.stat).
# - st_size: File size in bytes.
# - st_ctime: Creation timestamp.
# - st_mtime: Modification timestamp.
# - path: Full path string (default: None).
# - etag: Entity tag for cache validation (default: None).


class FsBase(abc.ABC):
  """Abstract base class for filesystem implementations.

  Provides common mode-parsing class methods and default implementations for
  some operations, while declaring abstract methods that subclasses must
  implement: stat, open, remove, rename, mkdir, makedirs, rmdir, rmtree,
  list, put_file, get_file, as_local, link and symlink.
  """

  def __init__(self, cache_iface=None, **kwargs):
    """Initialize the filesystem base.

    Args:
      cache_iface: Optional cache interface object. Defaults to None.
      **kwargs: Additional keyword arguments stored in self._kwargs.
    """
    super().__init__()
    self._cache_iface = cache_iface
    self._kwargs = kwargs

  @classmethod
  def read_mode(cls, mode):
    """Check if the mode opens the file for reading.

    Args:
      mode: The file mode string.

    Returns:
      True if 'r' is in mode and '+' is not, False otherwise.
    """
    return 'r' in mode and not '+' in mode

  @classmethod
  def write_mode(cls, mode):
    """Check if the mode opens the file for writing.

    Args:
      mode: The file mode string.

    Returns:
      True if mode contains 'w', 'a', or '+', False otherwise.
    """
    return any(c in mode for c in 'wa+')

  @classmethod
  def truncate_mode(cls, mode):
    """Check if the mode opens the file for writing with truncation.

    Args:
      mode: The file mode string.

    Returns:
      True if 'w' is in mode and '+' is not, False otherwise.
    """
    return 'w' in mode and '+' not in mode

  @classmethod
  def append_mode(cls, mode):
    """Check if the mode opens the file for appending.

    Args:
      mode: The file mode string.

    Returns:
      True if 'a' is in mode, False otherwise.
    """
    return 'a' in mode

  @classmethod
  def text_mode(cls, mode):
    """Check if the mode opens the file in text mode.

    Args:
      mode: The file mode string.

    Returns:
      True if 'b' is not in mode (text mode), False otherwise.
    """
    return 'b' not in mode

  @classmethod
  def seek_stream(cls, mode, stream):
    """Seek the stream to the appropriate position based on mode.

    For append mode, seeks to end of file. Otherwise seeks to the beginning.

    Args:
      mode: The file mode string.
      stream: The file stream object to seek.
    """
    stream.seek(0, os.SEEK_END if cls.append_mode(mode) else os.SEEK_SET)

  def norm_url(self, url):
    """Normalize a URL for this filesystem.

    The base implementation returns the URL unchanged. Subclasses may override
    to perform filesystem-specific normalization.

    Args:
      url: The URL string to normalize.

    Returns:
      The normalized URL string.
    """
    return url

  def exists(self, url):
    """Check if a URL exists in this filesystem.

    Args:
      url: The URL string to check.

    Returns:
      True if the URL exists, False otherwise.
    """
    try:
      self.stat(url)

      return True
    # stat() backends raise backend-specific errors (and some return None on
    # missing URLs - tracked defect): any failure answers False, never raises.
    except Exception:  # noqa: BLE001
      return False

  def isdir(self, url):
    """Check if a URL is a directory.

    Args:
      url: The URL string to check.

    Returns:
      True if the URL is a directory, False otherwise.
    """
    try:
      de = self.stat(url)

      return st.S_ISDIR(de.st_mode)
    except Exception:  # noqa: BLE001
      return False

  def isfile(self, url):
    """Check if a URL is a regular file.

    Args:
      url: The URL string to check.

    Returns:
      True if the URL is a regular file, False otherwise.
    """
    try:
      de = self.stat(url)

      return st.S_ISREG(de.st_mode)
    # stat() backends raise backend-specific errors (and some return None on
    # missing URLs - tracked defect): any failure answers False, never raises.
    except Exception:  # noqa: BLE001
      return False

  def copyfile(self, url, dest_fs, dest_url):
    """Copy a file from this filesystem to another filesystem.

    Args:
      url: The source URL.
      dest_fs: The destination FsBase instance.
      dest_url: The destination URL.
    """
    dest_fs.put_file(dest_url, self.get_file(url))

  @abc.abstractmethod
  def stat(self, url):
    """Return stat information for the given URL.

    Args:
      url: The URL string to stat.

    Returns:
      A DirEntry namedtuple with stat information.
    """
    ...

  @abc.abstractmethod
  def open(self, url, mode, **kwargs):
    """Open a file at the given URL with the specified mode.

    Args:
      url: The URL string to open.
      mode: The file mode string.
      **kwargs: Additional keyword arguments.

    Returns:
      A file-like object.
    """
    ...

  @abc.abstractmethod
  def remove(self, url):
    """Remove the file at the given URL.

    Args:
      url: The URL string to remove.
    """
    ...

  @abc.abstractmethod
  def rename(self, src_url, dest_url):
    """Rename/move a file from src_url to dest_url.

    Args:
      src_url: The source URL string.
      dest_url: The destination URL string.
    """
    ...

  def replace(self, src_url, dest_url):
    """Replace the file at dest_url with the file at src_url.

    The base implementation delegates to rename(). Subclasses may override
    for atomic replace semantics.

    Args:
      src_url: The source URL string.
      dest_url: The destination URL string.
    """
    self.rename(src_url, dest_url)

  @abc.abstractmethod
  def mkdir(self, url, mode=None):
    """Create a directory at the given URL.

    Args:
      url: The URL string for the directory.
      mode: Optional permission mode. Defaults to None.
    """
    ...

  @abc.abstractmethod
  def makedirs(self, url, mode=None, exist_ok=None):
    """Create a directory and all parent directories at the given URL.

    Args:
      url: The URL string for the directory.
      mode: Optional permission mode. Defaults to None.
      exist_ok: If True, do not raise if the directory exists. Defaults to None.
    """
    ...

  @abc.abstractmethod
  def rmdir(self, url):
    """Remove an empty directory at the given URL.

    Args:
      url: The URL string of the directory to remove.
    """
    ...

  @abc.abstractmethod
  def rmtree(self, url, ignore_errors=None):
    """Recursively remove a directory tree at the given URL.

    Args:
      url: The URL string of the directory tree to remove.
      ignore_errors: If True, ignore errors during removal. Defaults to None.
    """
    ...

  @abc.abstractmethod
  def list(self, url):
    """List the contents of a directory at the given URL.

    Args:
      url: The URL string of the directory to list.

    Yields:
      A DirEntry namedtuple for every entry of the directory.
    """
    ...

  @abc.abstractmethod
  def put_file(self, url, data_gen):
    """Write data to a file at the given URL.

    Args:
      url: The URL string to write to.
      data_gen: A generator or iterable producing data chunks.
    """
    ...

  @abc.abstractmethod
  def get_file(self, url):
    """Read and return the contents of a file at the given URL.

    Args:
      url: The URL string to read from.

    Returns:
      The file contents.
    """
    ...

  @abc.abstractmethod
  def as_local(self, url):
    """Return a local filesystem path for the given URL, if available.

    Args:
      url: The URL string to resolve.

    Returns:
      A local path string, or None if not available.
    """
    ...

  @abc.abstractmethod
  def link(self, src_url, dest_url):
    """Create a hard link from src_url to dest_url.

    Args:
      src_url: The source URL string.
      dest_url: The destination URL string.
    """
    ...

  @abc.abstractmethod
  def symlink(self, src_url, dest_url):
    """Create a symbolic link from src_url to dest_url.

    Args:
      src_url: The source URL string.
      dest_url: The destination URL string.
    """
    ...
