"""HTTP(S) FileSystem implementation using requests.

This module provides HttpFs, a FileSystem backend for http:// and https://
URLs. Reads are served through the block-cache machinery using Range
requests when the server supports them, while writes are staged locally and
uploaded with a single PUT request when the file is closed. Directory
listing is parsed from the anchor links of an HTML index page.
"""

import functools
import io
import mimetypes
import os
import re
import stat as st
import tempfile

import bs4
import requests

from .. import alog
from .. import cached_file as chf
from .. import context_managers as cm
from .. import fs_base as fsb
from .. import fs_utils as fsu
from .. import http_utils as hu
from .. import osfd
from .. import writeback_file as wbf


class HttpReader:
  """Reader fetching data from an HTTP server through a requests session."""

  def __init__(self, url, session=None, head=None, req_kwargs=None, chunk_size=None):
    """Resolve the request parameters and detect Range-request support.

    Args:
      url: URL of the file to read.
      session: requests.Session used for the requests; a new session is
        created when missing.
      head: Already fetched HEAD response; when missing a HEAD request is
        issued to populate it.
      req_kwargs: Keyword arguments for every request, such as headers
        and authentication.
      chunk_size: Size of the chunks read when streaming whole files.
    """
    session = session if session is not None else requests.Session()
    req_kwargs = req_kwargs or dict()
    if head is None:
      head = hu.info(url, mod=session, **req_kwargs)

    allow_ranges = hu.support_ranges(head.headers)

    # Request parameters and detected capabilities of the remote resource.
    self._url = url
    self._session = session
    self._req_kwargs = req_kwargs
    self._chunk_size = chunk_size or 16 * 1024**2
    self._size = hu.content_length(head.headers)
    self._support_blocks = self._size is not None and allow_ranges

  @classmethod
  def tag(cls, head):
    """Build a cache tag from an HTTP HEAD response.

    The ETag header is preferred; size and last-modified time are used as
    a fallback when the server does not provide an ETag.

    Args:
      head: HEAD response for the URL.

    Returns:
      The cache tag for the URL.
    """
    tag = hu.etag(head.headers)
    if tag is None:
      mtime = hu.last_modified(head.headers)
      length = hu.content_length(head.headers)
      tag = chf.make_tag(size=length, mtime=mtime)

    return tag

  def support_blocks(self):
    """Return True if the server supports Range requests."""
    return self._support_blocks

  def read_block(self, bpath, offset, size):
    """Read data into a local cache file.

    When the server supports Range requests a single ranged GET of size
    bytes is issued, otherwise the whole file is streamed into the cache.

    Args:
      bpath: Local path of the cache file to write the contents to.
      offset: Offset in the file where reading starts.
      size: Number of bytes to read.

    Returns:
      The number of bytes written into the cache file.
    """
    if self._support_blocks and offset != chf.CachedBlockFile.WHOLE_OFFSET:
      with osfd.OsFd(bpath, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, mode=0o440) as wfd:
        size = min(size, self._size - offset)

        headers = self._req_kwargs.get('headers', dict()).copy()
        hu.add_range(headers, offset, offset + size)

        resp = self._session.get(self._url, headers=headers)
        resp.raise_for_status()
        data = hu.range_data(offset, offset + size - 1, resp.headers, resp.content)

        os.write(wfd, data)

        return len(data)
    else:
      resp = self._session.get(self._url, stream=True, **self._req_kwargs)
      resp.raise_for_status()
      with osfd.OsFd(bpath, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, mode=0o440) as wfd:
        for data in resp.iter_content(chunk_size=self._chunk_size):
          os.write(wfd, data)

      return os.path.getsize(bpath)


class HttpFs(fsb.FsBase):
  """FileSystem implementation for http:// and https:// URLs.

  A single requests session is shared by all the operations, configured
  with the constructor keyword arguments such as headers or authentication.
  Writes are staged into a temporary file and uploaded on close through a
  PUT request, so writeback semantics apply.

  See py_misc_utils.fs for the shared backend contract.

  Example:
    import http.server
    import threading

    from py_misc_utils import cached_file as chf
    from py_misc_utils.fs import http_fs as hf

    # A local HTTP server, so that the example is runnable as-is.
    class H(http.server.BaseHTTPRequestHandler):
      def log_message(self, *args):
        pass

      def do_HEAD(self):
        body = self.server.routes.get(self.path)
        if body is None:
          return self.send_error(404)
        self.send_response(200)
        self.send_header('ETag', '"t1"')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()

      def do_GET(self):
        body = self.server.routes.get(self.path)
        if body is None:
          return self.send_error(404)
        self.send_response(200)
        self.send_header('ETag', '"t1"')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    H.server_class = type('S', (http.server.ThreadingHTTPServer,), {'routes': {}})
    server = H.server_class(('127.0.0.1', 0), H)
    server.routes['/data/file.bin'] = b'0123456789' * 100
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f'http://127.0.0.1:{server.server_address[1]}'

    iface = chf.CacheInterface('/tmp/gfs-cache')
    fs = hf.HttpFs(cache_dir='/tmp/gfs-cache', cache_iface=iface)

    with fs.open(url + '/data/file.bin', mode='rb') as f:
      print(f.read(16))
    server.shutdown()
    server.server_close()
  """

  mimetypes.init()

  ID = 'http'
  IDS = (ID, 'https')

  def __init__(self, cache_iface=None, **kwargs):
    """Initialize the backend with a shared requests session.

    Args:
      cache_iface: Optional cache interface customizing the caching of
        the remote contents.
      **kwargs: Keyword arguments forwarded to the session requests,
        such as headers and authentication.
    """
    super().__init__(cache_iface=cache_iface, **kwargs)
    self._req_kwargs = hu.filter_request_args(kwargs)
    self._session = requests.Session()

  def _exists(self, url):
    """Return True if a HEAD request for the URL does not return an error."""
    try:
      hu.info(url, mod=self._session, **self._req_kwargs)

      return True
    except requests.exceptions.HTTPError:
      return False

  def _make_reader(self, url):
    """Create a reader and metadata for the remote resource.

    Args:
      url: URL of the file to access.

    Returns:
      A tuple with the HttpReader and the cache Meta describing the file.
    """
    head = hu.info(url, mod=self._session, **self._req_kwargs)

    tag = HttpReader.tag(head)
    size = hu.content_length(head.headers)
    mtime = hu.last_modified(head.headers)
    meta = chf.Meta(size=size, mtime=mtime, tag=tag)
    reader = HttpReader(url,
                        session=self._session,
                        head=head,
                        req_kwargs=self._req_kwargs)

    return reader, meta

  def stat(self, url):
    """Stat a URL from the headers of a HEAD request.

    Args:
      url: URL to stat.

    Returns:
      The DirEntry built from the content-length, last-modified and ETag
      headers.
    """
    head = hu.info(url, mod=self._session, **self._req_kwargs)

    length = hu.content_length(head.headers)
    mtime = hu.last_modified(head.headers)
    tag = hu.etag(head.headers) or chf.make_tag(size=length, mtime=mtime)

    # HTML pages have content, but can also be listed (for HREF linked from it).
    # hence the weird st.S_IFREG | st.S_IFDIR.
    return fsb.DirEntry(name=os.path.basename(url.rstrip('/')),
                        path=url,
                        etag=tag,
                        st_mode=st.S_IFREG | st.S_IFDIR,
                        st_size=length,
                        st_ctime=mtime,
                        st_mtime=mtime)

  def open(self, url, mode, **kwargs):
    """Open a URL for reading or writing.

    Read modes are served through the block-cache interface, while write
    modes stage the data into a temporary file uploaded with a PUT
    request when the stream is closed.

    Since HTTP has no append operation, append modes download the
    existing content - if any - into the staging file and upload the
    whole result on close: concurrent writers are not merged.

    Args:
      url: URL to open.
      mode: Open mode string.
      **kwargs: Extra keyword arguments forwarded to the cache interface.

    Returns:
      A file-like object, wrapped into a TextIOWrapper in text modes.
    """
    if self.read_mode(mode):
      reader, meta = self._make_reader(url)
      cfile = self._cache_iface.open(url, meta, reader, **kwargs)

      return io.TextIOWrapper(cfile) if self.text_mode(mode) else cfile
    else:
      writeback_fn = functools.partial(self._upload_file, url)
      if not self.truncate_mode(mode) and self._exists(url):
        url_file = self._download_file(url)
        self.seek_stream(mode, url_file)
      else:
        # The WritebackFile owns the temp file's lifetime.
        url_file = tempfile.TemporaryFile()  # noqa: SIM115

      wbfile = wbf.WritebackFile(url_file, writeback_fn)

      return io.TextIOWrapper(wbfile) if self.text_mode(mode) else wbfile

  def remove(self, url):
    """Remove a URL with a DELETE request.

    Args:
      url: URL of the resource to remove.
    """
    self._session.delete(url, **self._req_kwargs)

  def rename(self, src_url, dest_url):
    """Rename a URL downloading and re-uploading its contents.

    Since HTTP has no rename operation, the contents are downloaded from
    the source and uploaded to the destination, then the source is
    removed.

    Args:
      src_url: URL of the source resource.
      dest_url: URL of the destination resource.
    """
    # There is no "rename" in HTTP ...
    with self._download_file(src_url) as fd:
      self._upload_file(dest_url, fd)

    self.remove(src_url)

  def mkdir(self, url, mode=None):
    """No-op since HTTP has no directories."""

  def makedirs(self, url, mode=None, exist_ok=None):
    """No-op since HTTP has no directories."""

  def rmdir(self, url):
    """No-op since HTTP has no directories."""

  def rmtree(self, url, ignore_errors=None):
    """No-op since HTTP has no directories."""

  def list(self, url):
    """Enumerate the entries linked from an HTML directory page.

    The URL must be an HTML page holding the anchor links of its
    children - plain file URLs or pages without links enumerate nothing.

    Args:
      url: URL of the directory page to enumerate.

    Yields:
      A DirEntry for every relative anchor of the page, as returned by
      stat() on the linked URL. Absolute or unknown links are ignored and
      entries which cannot be statted are skipped with a debug log.
    """
    resp = self._session.get(url, **self._req_kwargs)
    resp.raise_for_status()

    html_parser = bs4.BeautifulSoup(resp.text, 'html.parser')

    for link in html_parser.find_all('a'):
      href = link.get('href')
      if href and not re.match(r'[a-zA-Z]+://', href):
        lurl = os.path.join(url, href)
        try:
          de = self.stat(lurl)

          yield de
        except GeneratorExit:
          raise
        except Exception as ex:  # noqa: BLE001 -- dir listing: un-stat-able entries are skipped, not fatal to the listing
          alog.debug(f'Unable to stat URL {lurl}: {ex}')

  def _upload_data_gen(self, url, data_gen):
    """Upload data with a PUT request, guessing the content type.

    Args:
      url: Destination URL of the upload.
      data_gen: Iterable of bytes chunks to upload.
    """
    ctype, cencoding = mimetypes.guess_type(url, strict=False)

    headers = self._req_kwargs.get('headers', dict()).copy()
    if ctype is not None:
      headers[hu.CONTENT_TYPE] = ctype
    if cencoding is not None:
      headers[hu.CONTENT_ENCODING] = cencoding

    self._session.put(url, headers=headers, data=data_gen)

  def _upload_file(self, url, stream):
    """Rewind a stream and upload its contents to the given URL.

    Args:
      url: Destination URL of the upload.
      stream: Seekable file-like object with the contents to upload.
    """
    stream.seek(0)
    self._upload_data_gen(url, fsu.enum_chunks(stream))

  def _iterate_chunks(self, url, chunk_size=None):
    """Stream the contents of a URL as chunks.

    Args:
      url: URL to download.
      chunk_size: Size of the chunks yielded; defaults to 16 MiB.

    Yields:
      The downloaded contents as bytes chunks.
    """
    chunk_size = chunk_size or 16 * 1024**2

    resp = self._session.get(url, stream=True, **self._req_kwargs)
    resp.raise_for_status()

    yield from resp.iter_content(chunk_size=chunk_size)

  def _download_file(self, url, chunk_size=None):
    """Download the contents of a URL into a temporary file.

    Args:
      url: URL to download.
      chunk_size: Size of the chunks read while downloading.

    Returns:
      A seekable temporary file with the downloaded contents.
    """
    with cm.Wrapper(tempfile.TemporaryFile()) as ftmp:
      for data in self._iterate_chunks(url, chunk_size=chunk_size):
        ftmp.v.write(data)

      return ftmp.detach()

  def put_file(self, url, data_gen):
    """Upload data chunks to a URL.

    Args:
      url: Destination URL of the upload.
      data_gen: Iterable of bytes chunks to upload.
    """
    self._upload_data_gen(url, data_gen)

  def get_file(self, url):
    """Read the contents of a URL as chunks.

    Args:
      url: URL to read.

    Yields:
      The file contents as bytes chunks.
    """
    yield from self._iterate_chunks(url)

  def as_local(self, url, **kwargs):
    """Return a local file with the contents of a URL.

    Args:
      url: URL of the file to access.
      **kwargs: Extra keyword arguments forwarded to the cache interface.

    Returns:
      The path of the local file, downloaded through the cache interface.
    """
    reader, meta = self._make_reader(url)

    return self._cache_iface.as_local(url, meta, reader, **kwargs)

  def link(self, src_url, dest_url):
    """Copy the resource, since HTTP has no hard link support.

    Args:
      src_url: URL of the source resource.
      dest_url: URL of the destination resource.
    """
    # There is not link support in HTTP.
    self.copyfile(src_url, dest_url)

  def symlink(self, src_url, dest_url):
    """Copy the resource, since HTTP has no symbolic link support.

    Args:
      src_url: URL of the source resource.
      dest_url: URL of the destination resource.
    """
    self.link(src_url, dest_url)


FILE_SYSTEMS = (HttpFs,)
