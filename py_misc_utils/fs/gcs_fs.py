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
from .. import object_cache as objc
from .. import osfd
from .. import writeback_file as wbf


class CacheHandler(objc.Handler):

  def __init__(self, *args, **kwargs):
    super().__init__()
    self._args = args
    self._kwargs = kwargs

  def create(self):
    return gcs.GcsFs(*self._args, **self._kwargs)


class GcsReader:

  def __init__(self, fs, path, sres):
    self._fs = fs
    self._path = path
    self._sres = sres

  @classmethod
  def tag(cls, sres):
    return sres.etag or chf.make_tag(size=sres.st_size, mtime=sres.st_mtime)

  def support_blocks(self):
    return True

  def read_block(self, bpath, offset, size):
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

  ID = 'gcs'
  IDS = (ID,)

  def __init__(self, cache_iface=None, **kwargs):
    super().__init__(cache_iface=cache_iface, **kwargs)

  def _get_fs(self, bucket):
    handler = CacheHandler(bucket)
    name = ('GCSFS', bucket)

    return objc.cache().get(name, handler)

  def _parse_url(self, url):
    purl = uparse.urlparse(url)
    purl = purl._replace(path=purl.path.lstrip('/'))
    fs = self._get_fs(purl.hostname)

    return fs, purl

  def _make_reader(self, fs, purl):
    sres = fs.stat(purl.path)
    tas.check_is_not_none(sres, msg=f'File does not exist: {purl.geturl()}')

    tag = GcsReader.tag(sres)
    meta = chf.Meta(size=sres.st_size, mtime=sres.st_mtime, tag=tag)
    reader = GcsReader(fs, purl.path, sres)

    return reader, meta

  def _parse_samefs(self, src_url, dest_url):
    src_fs, src_purl = self._parse_url(src_url)
    dest_fs, dest_purl = self._parse_url(dest_url)

    tas.check_eq(src_fs.bucket, dest_fs.bucket,
                 msg=f'Source and destination URL must be on the same bucket: ' \
                 f'{src_url} vs. {dest_url}')

    return (src_fs, src_purl), (dest_fs, dest_purl)

  def _copy(self, src_url, dest_url):
    (src_fs, src_purl), (dest_fs, dest_purl) = self._parse_samefs(src_url, dest_url)

    src_fs.copy(src_purl.path, dest_purl.path)

  def remove(self, url):
    fs, purl = self._parse_url(url)
    fs.remove(purl.path)

  def rename(self, src_url, dest_url):
    (src_fs, src_purl), (dest_fs, dest_purl) = self._parse_samefs(src_url, dest_url)

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

    return fs.listdir(purl.path)

  def open(self, url, mode, **kwargs):
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

    return fs.download(purl.path)

  def as_local(self, url, **kwargs):
    fs, purl = self._parse_url(url)
    reader, meta = self._make_reader(fs, purl)

    return self._cache_iface.as_local(url, meta, reader, **kwargs)

  def link(self, src_url, dest_url):
    self._copy(src_url, dest_url)

  def symlink(self, src_url, dest_url):
    self.link(src_url, dest_url)


FILE_SYSTEMS = (GcsFs,)

