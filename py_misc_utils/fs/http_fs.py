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

from .. import alog as alog
from .. import assert_checks as tas
from .. import context_managers as cm
from .. import fs_base as fsb
from .. import fs_utils as fsu
from .. import cached_file as chf
from .. import http_utils as hu
from .. import osfd as osfd
from .. import writeback_file as wbf


class HttpReader:

  def __init__(self, url, head=None, headers=None):
    if head is None:
      head = requests.head(url, headers=headers)
      head.raise_for_status()

    allow_ranges = hu.support_ranges(head.headers)

    self._url = url
    self._headers = headers.copy() if headers else dict()
    self._size = hu.content_length(head.headers)
    self._support_blocks = self._size is not None and allow_ranges

  @classmethod
  def tag(cls, head):
    etag = hu.etag(head.headers)
    mtime = hu.last_modified(head.headers)
    size = hu.content_length(head.headers)

    return f'size={size},mtime={mtime},etag={etag}'

  def support_blocks(self):
    return self._support_blocks

  def read_block(self, bpath, offset, size):
    with osfd.OsFd(bpath, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, mode=0o660) as wfd:
      if self._support_blocks:
        size = min(size, self._size - offset)

        headers = self._headers.copy()
        hu.add_range(headers, offset, offset + size - 1)

        resp = requests.get(self._url, headers=headers)
        resp.raise_for_status()
        data = resp.content

        os.write(wfd, data)
      else:
        tas.check_eq(offset, chf.CachedBlockFile.WHOLE_OFFSET,
                     msg=f'Wrong offset for whole content read: {offset}')
        resp = requests.get(self._url, headers=self._headers)
        resp.raise_for_status()
        data = resp.content

        os.write(wfd, data)

      return len(data)


class HttpFs(fsb.FsBase):

  mimetypes.init()

  ID = 'http'
  IDS = (ID, 'https')

  def __init__(self, headers=None, cache_ctor=None, **kwargs):
    super().__init__()
    self._headers = headers.copy() if headers else dict()
    self._cache_ctor = cache_ctor

  def _exists(self, url):
    head = requests.head(url, headers=self._headers)

    return head.status_code == 200

  def stat(self, url):
    head = requests.head(url, headers=self._headers)
    head.raise_for_status()

    length = hu.content_length(head.headers)
    mtime = hu.last_modified(head.headers)
    etag = hu.etag(head.headers)
    if etag is None:
      stag = f'size={length},mtime={mtime}'
      etag = hashlib.sha1(stag.encode()).hexdigest()

    # HTML pages have content, but can also be listed (for HREF linked from it).
    # hence the weird st.S_IFREG | st.S_IFDIR.
    return fsb.DirEntry(name=os.path.basename(url.rstrip('/')),
                        path=url,
                        etag=etag,
                        st_mode=st.S_IFREG | st.S_IFDIR,
                        st_size=length,
                        st_ctime=mtime,
                        st_mtime=mtime)

  def open(self, url, mode, **kwargs):
    if self.read_mode(mode):
      head = requests.head(url, headers=self._headers)
      head.raise_for_status()

      tag = HttpReader.tag(head)
      size = hu.content_length(head.headers)
      meta = chf.Meta(size=size, tag=tag)
      reader = HttpReader(url, head=head, headers=self._headers)

      cfile = self._cache_ctor(url, meta, reader)

      return io.TextIOWrapper(cfile) if self.text_mode(mode) else cfile
    else:
      writeback_fn = functools.partial(self._upload_file, url)
      if not self.truncate_mode(mode) and self._exists(url):
        url_file = self._download_file(url)
      else:
        url_file = tempfile.TemporaryFile()

      wbfile = wbf.WritebackFile(url_file, writeback_fn)

      return io.TextIOWrapper(wbfile) if self.text_mode(mode) else wbfile

  def remove(self, url):
    requests.delete(url, headers=self._headers)

  def rename(self, src_url, dest_url):
    # There is no "rename" in HTTP ...
    with self._download_file(src_url) as fd:
      self._upload_file(dest_url, fd)

    self.remove(src_url)

  def _upload_file(self, url, stream):
    ctype, cencoding = mimetypes.guess_type(url, strict=False)

    headers = self._headers.copy()
    if ctype is not None:
      headers[hu.CONTENT_TYPE] = ctype
    if cencoding is not None:
      headers[hu.CONTENT_ENCODING] = cencoding

    stream.seek(0)

    requests.put(url, headers=headers, data=fsu.enum_chunks(stream))

  def _download_file(self, url, chunk_size=None):
    resp = requests.get(url, headers=self._headers, stream=True)
    resp.raise_for_status()

    with cm.Wrapper(tempfile.TemporaryFile()) as ftmp:
      chunk_size = chunk_size or 16 * 1024**2

      for data in resp.iter_content(chunk_size=chunk_size):
        ftmp.v.write(data)

      return ftmp.detach()

  def mkdir(self, url, mode=None):
    pass

  def makedirs(self, url, mode=None, exist_ok=None):
    pass

  def rmdir(self, url):
    pass

  def rmtree(self, url, ignore_errors=None):
    pass

  def list(self, url):
    resp = requests.get(url, headers=self._headers)
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


FILE_SYSTEMS = (HttpFs,)

