import bs4
import functools
import hashlib
import io
import mimetypes
import os
import re
import requests
import stat as st
import tempfile

from .. import alog
from .. import assert_checks as tas
from .. import context_managers as cm
from .. import fs_base as fsb
from .. import fs_utils as fsu
from .. import cached_file as chf
from .. import http_utils as hu
from .. import osfd
from .. import writeback_file as wbf


class HttpReader:

  def __init__(self, url, session=None, head=None, headers=None, chunk_size=None):
    session = session if session is not None else requests.Session()
    if head is None:
      head = session.head(url, headers=headers)
      head.raise_for_status()

    allow_ranges = hu.support_ranges(head.headers)

    self._url = url
    self._session = session
    self._headers = headers.copy() if headers else dict()
    self._chunk_size = chunk_size or 16 * 1024**2
    self._size = hu.content_length(head.headers)
    self._support_blocks = self._size is not None and allow_ranges

  @classmethod
  def tag(cls, head):
    tag = hu.etag(head.headers)
    if tag is None:
      mtime = hu.last_modified(head.headers)
      length = hu.content_length(head.headers)
      tag = chf.make_tag(size=length, mtime=mtime)

    return tag

  def support_blocks(self):
    return self._support_blocks

  def read_block(self, bpath, offset, size):
    if self._support_blocks and offset != chf.CachedBlockFile.WHOLE_OFFSET:
      with osfd.OsFd(bpath, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, mode=0o440) as wfd:
        size = min(size, self._size - offset)

        headers = self._headers.copy()
        hu.add_range(headers, offset, offset + size - 1)

        resp = self._session.get(self._url, headers=headers)
        resp.raise_for_status()
        data = hu.range_data(offset, offset + size - 1, resp.headers, resp.content)

        os.write(wfd, data)

        return len(data)
    else:
      resp = self._session.get(self._url, headers=self._headers, stream=True)
      resp.raise_for_status()
      with osfd.OsFd(bpath, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, mode=0o440) as wfd:
        for data in resp.iter_content(chunk_size=self._chunk_size):
          os.write(wfd, data)

      return os.path.getsize(bpath)


class HttpFs(fsb.FsBase):

  mimetypes.init()

  ID = 'http'
  IDS = (ID, 'https')

  def __init__(self, headers=None, cache_iface=None, **kwargs):
    super().__init__(cache_iface=cache_iface, **kwargs)
    self._headers = headers.copy() if headers else dict()
    self._session = requests.Session()

  def _exists(self, url):
    head = self._session.head(url, headers=self._headers)

    return head.status_code == 200

  def _make_reader(self, url):
    head = self._session.head(url, headers=self._headers)
    head.raise_for_status()

    tag = HttpReader.tag(head)
    size = hu.content_length(head.headers)
    mtime = hu.last_modified(head.headers)
    meta = chf.Meta(size=size, mtime=mtime, tag=tag)
    reader = HttpReader(url, session=self._session, head=head, headers=self._headers)

    return reader, meta

  def stat(self, url):
    head = self._session.head(url, headers=self._headers)
    head.raise_for_status()

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
    if self.read_mode(mode):
      reader, meta = self._make_reader(url)
      cfile = self._cache_iface.open(url, meta, reader)

      return io.TextIOWrapper(cfile) if self.text_mode(mode) else cfile
    else:
      writeback_fn = functools.partial(self._upload_file, url)
      if not self.truncate_mode(mode) and self._exists(url):
        url_file = self._download_file(url)
        self.seek_stream(mode, url_file)
      else:
        url_file = tempfile.TemporaryFile()

      wbfile = wbf.WritebackFile(url_file, writeback_fn)

      return io.TextIOWrapper(wbfile) if self.text_mode(mode) else wbfile

  def remove(self, url):
    self._session.delete(url, headers=self._headers)

  def rename(self, src_url, dest_url):
    # There is no "rename" in HTTP ...
    with self._download_file(src_url) as fd:
      self._upload_file(dest_url, fd)

    self.remove(src_url)

  def mkdir(self, url, mode=None):
    pass

  def makedirs(self, url, mode=None, exist_ok=None):
    pass

  def rmdir(self, url):
    pass

  def rmtree(self, url, ignore_errors=None):
    pass

  def list(self, url):
    resp = self._session.get(url, headers=self._headers)
    resp.raise_for_status()

    html_parser = bs4.BeautifulSoup(resp.text, 'html.parser')

    for link in html_parser.find_all('a'):
      href = link.get('href')
      if href and not re.match(r'[a-zA-Z]+://', href):
        lurl = os.path.join(url, href)
        try:
          de = self.stat(lurl)

          yield de
        except Exception as ex:
          alog.debug(f'Unable to stat URL {lurl}: {ex}')

  def _upload_data_gen(self, url, data_gen):
    ctype, cencoding = mimetypes.guess_type(url, strict=False)

    headers = self._headers.copy()
    if ctype is not None:
      headers[hu.CONTENT_TYPE] = ctype
    if cencoding is not None:
      headers[hu.CONTENT_ENCODING] = cencoding

    self._session.put(url, headers=headers, data=data_gen)

  def _upload_file(self, url, stream):
    stream.seek(0)
    self._upload_data_gen(url, fsu.enum_chunks(stream))

  def _iterate_chunks(self, url, chunk_size=None):
    chunk_size = chunk_size or 16 * 1024**2

    resp = self._session.get(url, headers=self._headers, stream=True)
    resp.raise_for_status()

    for data in resp.iter_content(chunk_size=chunk_size):
      yield data

  def _download_file(self, url, chunk_size=None):
    with cm.Wrapper(tempfile.TemporaryFile()) as ftmp:
      for data in self._iterate_chunks(url, chunk_size=chunk_size):
        ftmp.v.write(data)

      return ftmp.detach()

  def put_file(self, url, data_gen):
    self._upload_data_gen(url, data_gen)

  def get_file(self, url):
    for data in self._iterate_chunks(url):
      yield data

  def as_local(self, url):
    reader, meta = self._make_reader(url)

    return self._cache_iface.as_local(url, meta, reader)


FILE_SYSTEMS = (HttpFs,)

