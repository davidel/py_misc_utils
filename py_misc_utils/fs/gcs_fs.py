"""Google Cloud Storage FileSystem implementation.

This module provides GcsFs, a FileSystem backend for gs:// URLs delegating
the operations to the top-level py_misc_utils.gcs_fs module. Reads go
through the block-cache machinery (GCS supports pread) while writes are
staged locally and uploaded on close.
"""

import functools
import io
import os
import tempfile
import urllib.parse as uparse

from .. import assert_checks as tas
from .. import cached_file as chf
from .. import context_managers as cm
from .. import fs_base as fsb
from .. import fs_utils as fsu
from .. import gcs_fs as gcs
from .. import object_cache as objc
from .. import osfd
from .. import writeback_file as wbf


class CacheHandler(objc.Handler):
  """Object-cache handler creating per-bucket GcsFs instances."""

  def __init__(self, *args, **kwargs):
    """Initialize the handler with the GcsFs creation arguments.

    Args:
      *args: Positional arguments forwarded to GcsFs, such as the bucket.
      **kwargs: Keyword arguments forwarded to GcsFs.
    """
    super().__init__()
    # Positional and keyword arguments used to create the GcsFs.
    self._args = args
    self._kwargs = kwargs

  def create(self):
    """Create a new GcsFs for the bucket.

    Returns:
      A top-level GcsFs bound to the handler bucket.
    """
    return gcs.GcsFs(*self._args, **self._kwargs)


class GcsReader:
  """Reader fetching data from GCS, supporting both pread and full reads."""

  def __init__(self, fs, path, sres):
    """Initialize the reader.

    Args:
      fs: GcsFs used for the reads.
      path: Path of the object to read.
      sres: Stat result of the object.
    """
    # GcsFs used for the reads, object path and its stat result.
    self._fs = fs
    self._path = path
    self._sres = sres

  @classmethod
  def tag(cls, sres):
    """Build a cache tag from a GCS stat result.

    Args:
      sres: Stat result of the object, as returned by GcsFs.stat().

    Returns:
      The object ETag, or a tag built from size and modification time.
    """
    return sres.etag or chf.make_tag(size=sres.st_size, mtime=sres.st_mtime)

  def support_blocks(self):
    """Return True since GCS supports pread operations."""
    return True

  def read_block(self, bpath, offset, size):
    """Read data into a local cache file.

    Args:
      bpath: Local path of the cache file to write the contents to.
      offset: Offset in the object where reading starts; WHOLE_OFFSET
        streams the whole object.
      size: Number of bytes to read.

    Returns:
      The number of bytes written into the cache file.
    """
    if offset != chf.CachedBlockFile.WHOLE_OFFSET:
      size = min(size, self._sres.st_size - offset)
      data = self._fs.pread(self._path, offset, size)

      with osfd.OsFd(bpath, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, mode=0o440) as wfd:
        os.write(wfd, data)

      return len(data)
    else:
      with osfd.OsFd(bpath, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, mode=0o440) as wfd:
        for data in self._fs.download(self._path):
          os.write(wfd, data)

      return os.path.getsize(bpath)


class GcsFs(fsb.FsBase):
  """FileSystem implementation for gs:// URLs.

  GcsFs instances are pooled per bucket through the object cache. Paths are
  relative to the bucket root, so the leading slash of the URL path is
  stripped before dispatching the operations to the underlying GcsFs.
  """

  ID = 'gcs'
  IDS = (ID,)

  def __init__(self, cache_iface=None, **kwargs):
    """Initialize the backend.

    Args:
      cache_iface: Optional cache interface customizing the caching of
        the remote contents.
      **kwargs: Extra keyword arguments forwarded to FsBase.
    """
    super().__init__(cache_iface=cache_iface, **kwargs)

  def _get_fs(self, bucket):
    """Get a pooled GcsFs for a bucket.

    Args:
      bucket: Name of the GCS bucket.

    Returns:
      The GcsFs instance from the object cache, shared per bucket.
    """
    handler = CacheHandler(bucket)
    name = ('GCSFS', bucket)

    return objc.cache().get(name, handler)

  def _parse_url(self, url):
    """Parse a gs:// URL and return its bucket filesystem.

    Args:
      url: gs:// URL to parse.

    Returns:
      A tuple with the GcsFs of the URL bucket and the parsed URL with the
      leading slash of the path removed.
    """
    purl = uparse.urlparse(url)
    purl = purl._replace(path=purl.path.lstrip('/'))
    fs = self._get_fs(purl.hostname)

    return fs, purl

  def _make_reader(self, fs, purl):
    """Create a reader and metadata for the remote object.

    Args:
      fs: GcsFs of the URL bucket.
      purl: Parsed gs:// URL.

    Returns:
      A tuple with the GcsReader and the cache Meta describing the object.
    """
    sres = fs.stat(purl.path)
    tas.check_is_not_none(sres, msg=f'File does not exist: {purl.geturl()}')

    tag = GcsReader.tag(sres)
    meta = chf.Meta(size=sres.st_size, mtime=sres.st_mtime, tag=tag)
    reader = GcsReader(fs, purl.path, sres)

    return reader, meta

  def _parse_samefs(self, src_url, dest_url):
    """Parse two URLs and check they refer to the same bucket.

    Args:
      src_url: Source gs:// URL.
      dest_url: Destination gs:// URL.

    Returns:
      A tuple with the (filesystem, parsed URL) pair for the source and
      for the destination.
    """
    src_fs, src_purl = self._parse_url(src_url)
    dest_fs, dest_purl = self._parse_url(dest_url)

    tas.check_eq(src_fs.bucket, dest_fs.bucket,
                 msg=f'Source and destination URL must be on the same bucket: ' \
                 f'{src_url} vs. {dest_url}')

    return (src_fs, src_purl), (dest_fs, dest_purl)

  def _copy(self, src_url, dest_url):
    """Server-side copy an object within the same bucket."""
    (src_fs, src_purl), (_, dest_purl) = self._parse_samefs(src_url, dest_url)

    src_fs.copy(src_purl.path, dest_purl.path)

  def remove(self, url):
    """Remove an object.

    Args:
      url: gs:// URL of the object to remove.
    """
    fs, purl = self._parse_url(url)
    fs.remove(purl.path)

  def rename(self, src_url, dest_url):
    """Rename an object within the same bucket.

    Args:
      src_url: gs:// URL of the source object.
      dest_url: gs:// URL of the destination object.
    """
    (src_fs, src_purl), (_, dest_purl) = self._parse_samefs(src_url, dest_url)

    src_fs.rename(src_purl.path, dest_purl.path)

  def mkdir(self, url, mode=None):
    """No-op since GCS has no real directories."""

  def makedirs(self, url, mode=None, exist_ok=None):
    """No-op since GCS has no real directories."""

  def rmdir(self, url):
    """No-op since GCS has no real directories."""

  def rmtree(self, url, ignore_errors=None):
    """Recursively remove all the objects under a URL.

    Args:
      url: gs:// URL of the prefix to remove.
      ignore_errors: When True errors raised while removing the objects
        are logged instead of propagating.
    """
    fs, purl = self._parse_url(url)

    fs.rmtree(purl.path, ignore_errors=ignore_errors or False)

  def stat(self, url):
    """Stat an object.

    Args:
      url: gs:// URL to stat.

    Returns:
      The DirEntry of the object, or None if it does not exist.
    """
    fs, purl = self._parse_url(url)

    return fs.stat(purl.path)

  def list(self, url):
    """List the entries under a URL.

    Args:
      url: gs:// URL of the prefix to enumerate.

    Returns:
      A list of DirEntry objects for the objects under the prefix.
    """
    fs, purl = self._parse_url(url)

    return fs.listdir(purl.path)

  def open(self, url, mode, **kwargs):
    """Open a URL for reading or writing.

    Read modes are served through the block-cache interface, while write
    modes download the existing contents when truncation is not requested
    and wrap the stream into a WritebackFile uploading on close.

    Args:
      url: gs:// URL to open.
      mode: Open mode string.
      **kwargs: Extra keyword arguments forwarded to the cache interface.

    Returns:
      A file-like object, wrapped into a TextIOWrapper in text modes.
    """
    fs, purl = self._parse_url(url)

    if self.read_mode(mode):
      reader, meta = self._make_reader(fs, purl)
      cfile = self._cache_iface.open(url, meta, reader, **kwargs)

      return io.TextIOWrapper(cfile) if self.text_mode(mode) else cfile
    else:
      writeback_fn = functools.partial(self._upload_file, url)
      if not self.truncate_mode(mode) and fs.exists(purl.path):
        url_file = self._download_file(url)
        self.seek_stream(mode, url_file)
      else:
        # The WritebackFile owns the temp file's lifetime.
        url_file = tempfile.TemporaryFile()  # noqa: SIM115

      wbfile = wbf.WritebackFile(url_file, writeback_fn)

      return io.TextIOWrapper(wbfile) if self.text_mode(mode) else wbfile

  def _upload_file(self, url, stream):
    """Rewind a stream and upload its contents to the given URL.

    Args:
      url: Destination gs:// URL of the upload.
      stream: Seekable file-like object with the contents to upload.
    """
    stream.seek(0)
    self.put_file(url, fsu.enum_chunks(stream))

  def _download_file(self, url):
    """Download the contents of a URL into a temporary file.

    Args:
      url: gs:// URL to download.

    Returns:
      A seekable temporary file with the downloaded contents.
    """
    with cm.Wrapper(tempfile.TemporaryFile()) as ftmp:
      for data in self.get_file(url):
        ftmp.v.write(data)

      return ftmp.detach()

  def put_file(self, url, data_gen):
    """Upload data chunks to a URL.

    Args:
      url: Destination gs:// URL of the upload.
      data_gen: Iterable of bytes chunks to upload.
    """
    fs, purl = self._parse_url(url)

    fs.upload(purl.path, data_gen)

  def get_file(self, url):
    """Read the contents of a URL.

    Args:
      url: gs:// URL to read.

    Returns:
      A generator yielding the file contents as bytes chunks.
    """
    fs, purl = self._parse_url(url)

    return fs.download(purl.path)

  def as_local(self, url, **kwargs):
    """Return a local file with the contents of a URL.

    Args:
      url: gs:// URL of the file to access.
      **kwargs: Extra keyword arguments forwarded to the cache interface.

    Returns:
      The path of the local file, downloaded through the cache interface.
    """
    fs, purl = self._parse_url(url)
    reader, meta = self._make_reader(fs, purl)

    return self._cache_iface.as_local(url, meta, reader, **kwargs)

  def link(self, src_url, dest_url):
    """Copy an object, since GCS has no hard links.

    Args:
      src_url: gs:// URL of the source object.
      dest_url: gs:// URL of the destination object.
    """
    self._copy(src_url, dest_url)

  def symlink(self, src_url, dest_url):
    """Copy an object, since GCS has no symbolic links.

    Args:
      src_url: gs:// URL of the source object.
      dest_url: gs:// URL of the destination object.
    """
    self.link(src_url, dest_url)


FILE_SYSTEMS = (GcsFs,)
