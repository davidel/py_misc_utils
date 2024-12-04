import hashlib
import os
import shutil

from .. import alog
from .. import assert_checks as tas
from .. import cached_file as chf
from .. import fs_base as fsb
from .. import fs_utils as fsu
from .. import osfd


class FileReader:

  def __init__(self, path):
    self._path = path

  @classmethod
  def tag(cls, sres):
    return f'size={sres.st_size},mtime={sres.st_mtime}'

  def support_blocks(self):
    return True

  def read_block(self, bpath, offset, size):
    if offset == chf.CachedBlockFile.WHOLE_OFFSET:
      with (open(self._path, mode='rb') as rfd, open(bpath, mode='wb') as wfd):
        shutil.copyfileobj(rfd, wfd)

      return os.path.getsize(bpath)
    else:
      with (osfd.OsFd(self._path, os.O_RDONLY) as rfd,
            osfd.OsFd(bpath, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, mode=0o660) as wfd):
        os.lseek(rfd, offset, os.SEEK_SET)
        data = os.read(rfd, size)
        os.write(wfd, data)

        return len(data)


class FileFs(fsb.FsBase):

  ID = 'file'
  IDS = (ID,)

  def __init__(self, **kwargs):
    super().__init__()

  def norm_url(self, url):
    return os.path.normcase(url)

  def _create_etag(self, sres):
    stag = f'size={sres.st_size},mtime={sres.st_mtime}'

    return hashlib.sha1(stag.encode()).hexdigest()

  def stat(self, url):
    sres = os.stat(url)

    return fsb.DirEntry(name=os.path.basename(url),
                        path=url,
                        etag=self._create_etag(sres),
                        st_mode=sres.st_mode,
                        st_size=sres.st_size,
                        st_ctime=sres.st_ctime,
                        st_mtime=sres.st_mtime)

  def open(self, url, mode, **kwargs):
    return open(url, mode=mode)

  def remove(self, url):
    os.remove(url)

  def rename(self, src_url, dest_url):
    os.rename(src_url, dest_url)

  def replace(self, src_url, dest_url):
    os.replace(src_url, dest_url)

  def mkdir(self, url, mode=None):
    os.mkdir(url, mode=mode or 0o777)

  def makedirs(self, url, mode=None, exist_ok=None):
    os.makedirs(url, mode=mode or 0o777, exist_ok=exist_ok or False)

  def rmdir(self, url):
    os.rmdir(url)

  def rmtree(self, url, ignore_errors=None):
    shutil.rmtree(url, ignore_errors=ignore_errors or False)

  def list(self, url):
    with os.scandir(url) as sdit:
      for de in sdit:
        sres = de.stat()

        yield fsb.DirEntry(name=de.name,
                           path=os.path.join(url, de.name),
                           etag=self._create_etag(sres),
                           st_mode=sres.st_mode,
                           st_size=sres.st_size,
                           st_ctime=sres.st_ctime,
                           st_mtime=sres.st_mtime)

  def put_file(self, url, data_gen):
    with open(url, mode='wb') as fd:
      for data in data_gen:
        fd.write(data)

  def get_file(self, url):
    with open(url, mode='rb') as fd:
      for data in fsu.enum_chunks(fd):
        yield data


FILE_SYSTEMS = (FileFs,)

