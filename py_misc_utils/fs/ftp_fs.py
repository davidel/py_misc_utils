import ftplib
import ftputil
import functools
import hashlib
import io
import os
import tempfile
import urllib.parse as uparse

from .. import alog as alog
from .. import assert_checks as tas
from .. import context_managers as cm
from .. import fs_base as fsb
from .. import fs_utils as fsu
from .. import cached_file as chf
from .. import no_except as nox
from .. import writeback_file as wbf


class FtpReader:

  def __init__(self, conn, path):
    self._conn = conn
    self._path = path

  @classmethod
  def tag(cls, dentry):
    return f'size={dentry.st_size},mtime={dentry.st_mtime},etag={dentry.etag}'

  def support_blocks(self):
    return False

  def read_block(self, bpath, offset, size):
    tas.check_eq(offset, chf.CachedBlockFile.WHOLE_OFFSET,
                 msg=f'Wrong offset for whole content read: {offset}')

    with open(bpath, mode='wb') as wfd:
      with self._conn.open(self._path, mode='rb') as rfd:
        self._conn.copyfileobj(rfd, wfd)

    return os.path.getsize(bpath)


# https://docs.python.org/3/library/ftplib.html
# https://ftputil.sschwarzer.net/
class FtpSession(ftplib.FTP):

  def __init__(self, host, userid, passwd, port):
    super().__init__()
    self.connect(host, port=port)
    self.login(userid, passwd)


class FtpFs(fsb.FsBase):

  ID = 'ftp'
  IDS = (ID,)

  def __init__(self, cache_ctor=None, **kwargs):
    super().__init__()
    self._cache_ctor = cache_ctor
    self._conn_cache = dict()

  def _get_connection(self, host, port, user, passwd):
    cid = (host, port, user)
    conn = self._conn_cache.get(cid)
    if conn is not None:
      # We have a cached connection, but is it still alive?
      try:
        conn.keep_alive()
      except:
        nox.qno_except(conn.close)
        conn = None

    if conn is None:
      conn = ftputil.FTPHost(host, user, passwd,
                             port=port,
                             session_factory=FtpSession)
      self._conn_cache[cid] = conn

    return conn

  def _netloc(self, purl):
    return (purl.hostname.lower(), purl.port or 21)

  def _parse_url(self, url):
    purl = uparse.urlparse(url)

    host, port = self._netloc(purl)
    user = purl.username or 'anonymous'
    passwd = purl.password or ''

    conn = self._get_connection(host, port, user, passwd)

    return conn, purl

  def remove(self, url):
    conn, purl = self._parse_url(url)
    conn.remove(purl.path)

  def rename(self, src_url, dest_url):
    src_conn, src_purl = self._parse_url(src_url)
    dest_conn, dest_purl = self._parse_url(dest_url)

    src_netloc, dest_netloc = self._netloc(src_purl), self._netloc(dest_purl)

    tas.check_eq(src_netloc, dest_netloc,
                 msg=f'Source and destination URL must be on the same host: ' \
                 f'{src_netloc} vs. {dest_netloc}')

    src_conn.rename(src_purl.path, dest_purl.path)

  def mkdir(self, url, mode=None):
    conn, purl = self._parse_url(url)
    conn.mkdir(purl.path)

  def makedirs(self, url, mode=None, exist_ok=None):
    conn, purl = self._parse_url(url)

    conn.makedirs(purl.path, exist_ok=exist_ok or False)

  def rmdir(self, url):
    conn, purl = self._parse_url(url)
    conn.rmdir(purl.path)

  def rmtree(self, url, ignore_errors=None):
    conn, purl = self._parse_url(url)

    conn.rmtree(purl.path, ignore_errors=ignore_errors or False)

  def _stat(self, conn, path):
    sres = conn.stat(path)

    stag = f'size={sres.st_size},mtime={sres.st_mtime}'
    etag = hashlib.sha1(stag.encode()).hexdigest()

    return fsb.DirEntry(name=os.path.basename(path),
                        path=path,
                        etag=etag,
                        st_mode=sres.st_mode,
                        st_size=sres.st_size,
                        st_ctime=sres.st_ctime or sres.st_mtime,
                        st_mtime=sres.st_mtime)

  def stat(self, url):
    conn, purl = self._parse_url(url)

    return self._stat(conn, purl.path)

  def list(self, url):
    conn, purl = self._parse_url(url)

    for name in conn.listdir(purl.path):
      path = os.path.join(purl.path, name)

      yield self._stat(conn, path)

  def open(self, url, mode, **kwargs):
    conn, purl = self._parse_url(url)

    if self.read_mode(mode):
      de = self._stat(conn, purl.path)

      tag = FtpReader.tag(de)
      meta = chf.Meta(size=de.st_size, tag=tag)
      reader = FtpReader(conn, purl.path)

      cfile = self._cache_ctor(url, meta, reader)

      return io.TextIOWrapper(cfile) if self.text_mode(mode) else cfile
    else:
      writeback_fn = functools.partial(self._upload_file, url)
      if not self.truncate_mode(mode) and conn.path.exists(purl.path):
        url_file = self._download_file(url)
      else:
        url_file = tempfile.TemporaryFile()

      wbfile = wbf.WritebackFile(url_file, writeback_fn)

      return io.TextIOWrapper(wbfile) if self.text_mode(mode) else wbfile

  def _upload_file(self, url, stream):
    conn, purl = self._parse_url(url)

    stream.seek(0)
    with conn.open(purl.path, mode='wb') as dest_fd:
      conn.copyfileobj(stream, dest_fd)

  def _download_file(self, url):
    conn, purl = self._parse_url(url)

    with cm.Wrapper(tempfile.TemporaryFile()) as ftmp:
      with conn.open(purl.path, mode='rb') as src_fd:
        conn.copyfileobj(src_fd, ftmp.v)

      return ftmp.detach()

  def put_file(self, url, data_gen):
    conn, purl = self._parse_url(url)

    with conn.open(purl.path, mode='wb') as fd:
      for data in data_gen:
        fd.write(data)

  def get_file(self, url):
    conn, purl = self._parse_url(url)

    with conn.open(purl.path, mode='rb') as fd:
      for data in fsu.enum_chunks(fd):
        yield data


FILE_SYSTEMS = (FtpFs,)

