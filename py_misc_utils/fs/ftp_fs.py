"""FTP FileSystem implementation using ftputil.

This module provides FtpFs, a FileSystem backend accessing remote files over
FTP. Connections (FTPHost objects) are pooled through the object cache and
file contents are read through the block-cache / writeback machinery since
FTP does not support random access reads.
"""

import ftplib
import functools
import io
import os
import tempfile
import urllib.parse as uparse

import ftputil

from .. import assert_checks as tas
from .. import cached_file as chf
from .. import context_managers as cm
from .. import fs_base as fsb
from .. import fs_utils as fsu
from .. import object_cache as objc
from .. import writeback_file as wbf


class CacheHandler(objc.Handler):
  """Object-cache handler creating and keeping alive ftputil.FTPHost objects."""

  def __init__(self, *args, **kwargs):
    """Initialize the handler with the FTPHost creation arguments.

    Args:
      *args: Positional arguments forwarded to FTPHost.
      **kwargs: Keyword arguments forwarded to FTPHost.
    """
    super().__init__()
    # Positional and keyword arguments used to create the FTPHost.
    self._args = args
    self._kwargs = kwargs

  def create(self):
    """Create a new FTPHost connection.

    Returns:
      A ftputil.FTPHost connected with the handler arguments.
    """
    host = ftputil.FTPHost(*self._args, **self._kwargs)

    # Explicitly assume server and client clocks to be aligned (the ftputil
    # default): starting from ftputil 6.0.0 the time shift must be set
    # explicitly for stat() to return timestamp data.
    if hasattr(host, 'set_time_shift'):
      host.set_time_shift(0)

    return host

  def is_alive(self, obj):
    """Return True if the connection still responds to keep_alive()."""
    try:
      obj.keep_alive()

      return True
    except:
      return False

  def close(self, obj):
    """Close the FTPHost connection."""
    obj.close()


class FtpReader:
  """Reader downloading whole files over an FTP connection."""

  def __init__(self, conn, path):
    """Initialize the reader.

    Args:
      conn: FTPHost connection used for the transfers.
      path: Remote path of the file to read.
    """
    # FTPHost connection used for the transfers and remote file path.
    self._conn = conn
    self._path = path

  @classmethod
  def tag(cls, sres):
    """Build a cache tag from a stat result.

    Args:
      sres: ftputil stat result of the file.

    Returns:
      The cache tag embedding size and modification time.
    """
    return chf.make_tag(size=sres.st_size, mtime=sres.st_mtime)

  def support_blocks(self):
    """Return False since FTP transfers do not support partial reads."""
    return False

  def read_block(self, bpath, offset, size):
    """Copy the whole remote file into a local cache file.

    Args:
      bpath: Local path of the cache file to write the contents to.
      offset: Must be CachedBlockFile.WHOLE_OFFSET since partial reads
        are not supported.
      size: Number of bytes to read; ignored since the whole file is read.

    Returns:
      The number of bytes written into the cache file.
    """
    tas.check_eq(offset,
                 chf.CachedBlockFile.WHOLE_OFFSET,
                 msg=f'Wrong offset for whole content read: {offset}')

    bfd = os.open(bpath, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, mode=0o440)
    with open(bfd, mode='wb') as wfd, self._conn.open(self._path, mode='rb') as rfd:
      self._conn.copyfileobj(rfd, wfd)

    return os.path.getsize(bpath)


# https://docs.python.org/3/library/ftplib.html
# https://ftputil.sschwarzer.net/
class FtpSession(ftplib.FTP):
  """FTP session connecting and logging in upon creation."""

  def __init__(self, host, userid, passwd, port):
    """Connect to an FTP server and authenticate.

    Args:
      host: Server host name or address.
      userid: User name used to login.
      passwd: Password used to login.
      port: TCP port of the FTP server.
    """
    super().__init__()
    self.connect(host, port=port)
    self.login(userid, passwd)


class FtpFs(fsb.FsBase):
  """FileSystem implementation for URLs with the ftp:// scheme.

  Connections are pooled per (host, port, user) via the object cache and
  remote contents are staged through the block-cache and writeback-file
  helpers, so repeated accesses are served from the local cache.
  """

  ID = 'ftp'
  IDS = (ID,)

  def __init__(self, cache_iface=None, **kwargs):
    """Initialize the backend.

    Args:
      cache_iface: Optional cache interface customizing the caching of
        the remote contents.
      **kwargs: Extra keyword arguments forwarded to FsBase.
    """
    super().__init__(cache_iface=cache_iface, **kwargs)

  def _get_connection(self, host, port, user, passwd):
    """Get a pooled FTP connection for the given server credentials.

    Args:
      host: Server host name or address.
      port: TCP port of the FTP server.
      user: Login user name.
      passwd: Login password.

    Returns:
      An ftputil.FTPHost from the object cache, shared per host/port/user.
    """
    handler = CacheHandler(host, user, passwd, port=port, session_factory=FtpSession)
    name = ('FTPFS', host, port, user)

    return objc.cache().get(name, handler)

  def _netloc(self, purl):
    """Return the (host, port) pair identifying the FTP server."""
    return (purl.hostname.lower(), purl.port or 21)

  def _parse_url(self, url):
    """Parse an ftp:// URL and return its pooled connection.

    Args:
      url: ftp:// URL to parse.

    Returns:
      A tuple with the FTPHost connection and the parsed URL.
    """
    purl = uparse.urlparse(url)

    host, port = self._netloc(purl)
    user = purl.username or 'anonymous'
    passwd = purl.password or ''

    conn = self._get_connection(host, port, user, passwd)

    return conn, purl

  def _make_reader(self, conn, purl):
    """Create a reader and metadata for the remote file.

    Args:
      conn: FTPHost connection.
      purl: Parsed ftp:// URL.

    Returns:
      A tuple with the FtpReader and the cache Meta describing the file.
    """
    sres = self._stat(conn, purl.path)

    tag = FtpReader.tag(sres)
    meta = chf.Meta(size=sres.st_size, mtime=sres.st_mtime, tag=tag)
    reader = FtpReader(conn, purl.path)

    return reader, meta

  def remove(self, url):
    """Remove a remote file.

    Args:
      url: ftp:// URL of the file to remove.
    """
    conn, purl = self._parse_url(url)
    conn.remove(purl.path)

  def rename(self, src_url, dest_url):
    """Rename a remote file within the same host.

    Args:
      src_url: ftp:// URL of the source file.
      dest_url: ftp:// URL of the destination file.

    Raises:
      RuntimeError: When the source and destination URLs are not on the
        same host.
    """
    src_conn, src_purl = self._parse_url(src_url)
    dest_conn, dest_purl = self._parse_url(dest_url)

    src_netloc, dest_netloc = self._netloc(src_purl), self._netloc(dest_purl)

    tas.check_eq(src_netloc, dest_netloc,
                 msg=f'Source and destination URL must be on the same host: ' \
                 f'{src_netloc} vs. {dest_netloc}')

    src_conn.rename(src_purl.path, dest_purl.path)

  def mkdir(self, url, mode=None):
    """Create a remote directory.

    Args:
      url: ftp:// URL of the directory to create.
      mode: Unused, since remote permissions are not supported.
    """
    conn, purl = self._parse_url(url)
    conn.mkdir(purl.path)

  def makedirs(self, url, mode=None, exist_ok=None):
    """Create a remote directory and all its parents.

    Args:
      url: ftp:// URL of the directory to create.
      mode: Unused, since remote permissions are not supported.
      exist_ok: If True, do not raise when the directory already exists
        (default: False).
    """
    conn, purl = self._parse_url(url)

    conn.makedirs(purl.path, exist_ok=exist_ok or False)

  def rmdir(self, url):
    """Remove an empty remote directory.

    Args:
      url: ftp:// URL of the directory to remove.
    """
    conn, purl = self._parse_url(url)
    conn.rmdir(purl.path)

  def rmtree(self, url, ignore_errors=None):
    """Recursively remove a remote directory tree.

    Args:
      url: ftp:// URL of the directory tree to remove.
      ignore_errors: If True, errors raised while removing the tree are
        ignored (default: False).
    """
    conn, purl = self._parse_url(url)

    conn.rmtree(purl.path, ignore_errors=ignore_errors or False)

  def _stat(self, conn, path):
    """Stat a remote path.

    Args:
      conn: FTPHost connection.
      path: Remote path to stat.

    Returns:
      The DirEntry built from the remote stat result.
    """
    sres = conn.stat(path)

    tag = FtpReader.tag(sres)

    return fsb.DirEntry(name=os.path.basename(path),
                        path=path,
                        etag=tag,
                        st_mode=sres.st_mode,
                        st_size=sres.st_size,
                        st_ctime=sres.st_ctime or sres.st_mtime,
                        st_mtime=sres.st_mtime)

  def stat(self, url):
    """Stat a remote path.

    Args:
      url: ftp:// URL to stat.

    Returns:
      The DirEntry of the path.
    """
    conn, purl = self._parse_url(url)

    return self._stat(conn, purl.path)

  def list(self, url):
    """Enumerate the entries of a remote directory.

    Args:
      url: ftp:// URL of the directory to enumerate.

    Yields:
      A DirEntry for every file and subdirectory of the directory.
    """
    conn, purl = self._parse_url(url)

    for name in conn.listdir(purl.path):
      path = os.path.join(purl.path, name)

      yield self._stat(conn, path)

  def open(self, url, mode, **kwargs):
    """Open a URL for reading or writing.

    Read modes are served through the block-cache interface, while write
    modes download the existing contents when truncation is not requested
    and wrap the stream into a WritebackFile uploading on close.

    Args:
      url: ftp:// URL to open.
      mode: Open mode string.
      **kwargs: Extra keyword arguments forwarded to the cache interface.

    Returns:
      A file-like object, wrapped into a TextIOWrapper in text modes.
    """
    conn, purl = self._parse_url(url)

    if self.read_mode(mode):
      reader, meta = self._make_reader(conn, purl)
      cfile = self._cache_iface.open(url, meta, reader, **kwargs)

      return io.TextIOWrapper(cfile) if self.text_mode(mode) else cfile
    else:
      writeback_fn = functools.partial(self._upload_file, url)
      if not self.truncate_mode(mode) and conn.path.exists(purl.path):
        url_file = self._download_file(url)
        self.seek_stream(mode, url_file)
      else:
        url_file = tempfile.TemporaryFile()

      wbfile = wbf.WritebackFile(url_file, writeback_fn)

      return io.TextIOWrapper(wbfile) if self.text_mode(mode) else wbfile

  def _upload_file(self, url, stream):
    """Upload the contents of a stream into a remote file.

    Args:
      url: ftp:// URL of the destination file.
      stream: Seekable file-like object with the contents to upload.
    """
    conn, purl = self._parse_url(url)

    stream.seek(0)
    with conn.open(purl.path, mode='wb') as dest_fd:
      conn.copyfileobj(stream, dest_fd)

  def _download_file(self, url):
    """Download a remote file into a temporary local file.

    Args:
      url: ftp:// URL of the file to download.

    Returns:
      A seekable temporary file with the remote contents.
    """
    conn, purl = self._parse_url(url)

    with cm.Wrapper(tempfile.TemporaryFile()) as ftmp:
      with conn.open(purl.path, mode='rb') as src_fd:
        conn.copyfileobj(src_fd, ftmp.v)

      return ftmp.detach()

  def put_file(self, url, data_gen):
    """Write data chunks into a remote file.

    Args:
      url: ftp:// URL of the file to write.
      data_gen: Iterable of bytes chunks to write.
    """
    conn, purl = self._parse_url(url)

    with conn.open(purl.path, mode='wb') as fd:
      for data in data_gen:
        fd.write(data)

  def get_file(self, url):
    """Read the contents of a remote file as chunks.

    Args:
      url: ftp:// URL of the file to read.

    Yields:
      The file contents as bytes chunks.
    """
    conn, purl = self._parse_url(url)

    with conn.open(purl.path, mode='rb') as fd:
      for data in fsu.enum_chunks(fd):
        yield data

  def as_local(self, url, **kwargs):
    """Return a local file with the contents of a URL.

    Args:
      url: ftp:// URL of the file to access.
      **kwargs: Extra keyword arguments forwarded to the cache interface.

    Returns:
      The path of the local file, downloaded through the cache interface.
    """
    conn, purl = self._parse_url(url)
    reader, meta = self._make_reader(conn, purl)

    return self._cache_iface.as_local(url, meta, reader, **kwargs)

  def link(self, src_url, dest_url):
    """Copy the file, since FTP has no hard link support.

    Args:
      src_url: ftp:// URL of the source file.
      dest_url: ftp:// URL of the destination file.
    """
    # There is not link support in FTP.
    self.copyfile(src_url, dest_url)

  def symlink(self, src_url, dest_url):
    """Copy the file, since FTP has no symbolic link support.

    Args:
      src_url: ftp:// URL of the source file.
      dest_url: ftp:// URL of the destination file.
    """
    self.link(src_url, dest_url)


FILE_SYSTEMS = (FtpFs,)
