import functools
import hashlib
import io
import os
import tempfile
import urllib.parse as uparse

from .. import alog
from .. import assert_checks as tas
from .. import context_managers as cm
from .. import fs_base as fsb
from .. import fs_utils as fsu
from .. import cached_file as chf
from .. import gcs_fs as gcs
from .. import writeback_file as wbf


class GcsReader:

  def __init__(self, fs, path, dentry):
    self._fs = fs
    self._path = path
    self._dentry = dentry

  @classmethod
  def tag(cls, dentry):
    return f'size={dentry.st_size},mtime={dentry.st_mtime},etag={dentry.etag}'

  def support_blocks(self):
    return True

  def read_block(self, bpath, offset, size):
    size = min(size, self._dentry.st_size - offset)
    data = self._fs.pread(self._path, offset, size)

    with open(bpath, mode='wb') as wfd:
      wfd.write(data)

    return len(data)


class GcsFs(fsb.FsBase):

  ID = 'gcs'
  IDS = (ID,)

  def __init__(self, cache_ctor=None, **kwargs):
    super().__init__()
    self._cache_ctor = cache_ctor

  def _get_fs(self, bucket):
    return gcs.GcsFs(bucket)

  def _parse_url(self, url):
    purl = uparse.urlparse(url)
    purl = purl._replace(path=purl.path.lstrip('/'))
    fs = self._get_fs(purl.hostname)

    return fs, purl

  def remove(self, url):
    fs, purl = self._parse_url(url)
    fs.remove(purl.path)

  def rename(self, src_url, dest_url):
    src_fs, src_purl = self._parse_url(src_url)
    dest_fs, dest_purl = self._parse_url(dest_url)

    tas.check_eq(src_purl.hostname, dest_purl.hostname,
                 msg=f'Source and destination URL must be on the same bucket: ' \
                 f'{src_url} vs. {dest_url}')

    src_fs.rename(src_purl.path, dest_purl.path)

  def mkdir(self, url, mode=None):
    pass

  def makedirs(self, url, mode=None, exist_ok=None):
    pass

  def rmdir(self, url):
    pass

  def rmtree(self, url, ignore_errors=None):
    fs, purl = self._parse_url(url)

    fs.rmtree(purl.path, ignore_errors=ignore_errors or False)

  def stat(self, url):
    fs, purl = self._parse_url(url)

    return fs.stat(purl.path)

  def list(self, url):
    fs, purl = self._parse_url(url)

    for de in fs.listdir(purl.path):
      yield de

  def open(self, url, mode, **kwargs):
    fs, purl = self._parse_url(url)

    if self.read_mode(mode):
      de = fs.stat(purl.path)
      tas.check_is_not_none(de, msg=f'File does not exist: {url}')

      tag = GcsReader.tag(de)
      meta = chf.Meta(size=de.st_size, tag=tag)
      reader = GcsReader(fs, purl.path, de)

      cfile = self._cache_ctor(url, meta, reader)

      return io.TextIOWrapper(cfile) if self.text_mode(mode) else cfile
    else:
      writeback_fn = functools.partial(self._upload_file, url)
      if not self.truncate_mode(mode) and fs.exists(purl.path):
        url_file = self._download_file(url)
      else:
        url_file = tempfile.TemporaryFile()

      wbfile = wbf.WritebackFile(url_file, writeback_fn)

      return io.TextIOWrapper(wbfile) if self.text_mode(mode) else wbfile

  def _upload_file(self, url, stream):
    stream.seek(0)
    self.put_file(url, fsu.enum_chunks(stream))

  def _download_file(self, url):
    with cm.Wrapper(tempfile.TemporaryFile()) as ftmp:
      for data in self.get_file(url):
        ftmp.v.write(data)

      return ftmp.detach()

  def put_file(self, url, data_gen):
    fs, purl = self._parse_url(url)

    fs.upload(purl.path, data_gen)

  def get_file(self, url):
    fs, purl = self._parse_url(url)

    for data in fs.download(purl.path):
      yield data


FILE_SYSTEMS = (GcsFs,)

