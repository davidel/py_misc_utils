import collections
import hashlib
import os
import re
import shutil
import time
import yaml

from . import alog
from . import assert_checks as tas
from . import fs_utils as fsu
from . import lockfile as lockf
from . import no_except as nox
from . import obj
from . import osfd
from . import rnd_utils as rngu


_DroppedBlock = collections.namedtuple('DroppedBlock', 'name, sres, cid, offset')


class Meta(obj.Obj):
  pass


class CachedBlockFile:

  METAFILE = 'META'
  BLOCKSDIR = 'blocks'
  LINKSDIR = 'links'
  WHOLE_OFFSET = -1
  CID_SIZE = 16
  BLOCKSIZE = 32 * 1024**2

  def __init__(self, path, reader, meta=None):
    self._path = path
    self._reader = reader
    self.meta = self.load_meta(path) if meta is None else meta

  @classmethod
  def default_meta(cls):
    return Meta(url=None, size=None, block_size=cls.BLOCKSIZE)

  @classmethod
  def prepare_meta(cls, meta, **kwargs):
    cmeta = cls.default_meta()
    cmeta.update_from(meta)
    cmeta.update(**kwargs)

    cid = hashlib.sha1(cmeta.tag.encode()).hexdigest()[: cls.CID_SIZE]
    cmeta.update(cid=cid)

    return cmeta

  @classmethod
  def remove(cls, path):
    try:
      fsu.safe_rmtree(path, ignore_errors=True)

      return True
    except:
      return False

  @classmethod
  def create(cls, path, meta):
    tpath = rngu.temp_path(nspath=path)
    try:
      os.makedirs(tpath, exist_ok=True)
      os.mkdir(cls.blocks_dir(tpath))
      os.mkdir(cls.links_dir(tpath))

      cls.save_meta(tpath, meta)

      os.rename(tpath, path)
    except:
      shutil.rmtree(tpath, ignore_errors=True)
      raise

  def _block_file(self, offset):
    return self.block_file(self._path, self.meta.cid, offset)

  def _fetch_block(self, offset):
    bpath = self._block_file(offset)
    with lockf.LockFile(bpath):
      if (sres := fsu.stat(bpath)) is None:
        tpath = rngu.temp_path(nspath=bpath)
        try:
          rsize = self._reader.read_block(tpath, offset, self.meta.block_size)
          if rsize > 0:
            os.replace(tpath, bpath)
            if offset == self.WHOLE_OFFSET:
              self._make_link(bpath)
        except:
          nox.qno_except(os.remove, tpath)
          raise
      else:
        rsize = sres.st_size

    return rsize, bpath

  def _make_link(self, bpath):
    lpath = self.link_file(self._path, self.meta.cid, self.meta.url)
    if not os.path.exists(lpath):
      try:
        os.makedirs(os.path.dirname(lpath), exist_ok=True)
        os.link(bpath, lpath)
      except Exception as ex:
        alog.warning(f'Unable to create link: {bpath} -> {lpath}')

  def _try_block(self, boffset, offset):
    bpath = self._block_file(boffset)
    try:
      with osfd.OsFd(bpath, os.O_RDONLY) as fd:
        sres = os.stat(fd)
        if sres.st_size >= offset:
          os.lseek(fd, offset, os.SEEK_SET)
          size = min(self.meta.block_size, sres.st_size - offset)

          return os.read(fd, size)
    except FileNotFoundError:
      pass

  def _translate_offset(self, offset):
    has_whole_content = True
    if self._reader.support_blocks():
      # Even if the reader supports blocks, we might have cached the whole content
      # at once, so make sure we do not waste the cached whole content.
      bpath = self._block_file(self.WHOLE_OFFSET)
      has_whole_content = os.path.exists(bpath)

    if has_whole_content:
      boffset = self.WHOLE_OFFSET
    else:
      boffset, offset = offset, 0

    return boffset, offset

  def cacheall(self):
    return self._fetch_block(self.WHOLE_OFFSET)

  def read_block(self, offset):
    tas.check_eq(offset % self.meta.block_size, 0,
                 msg=f'Block offset ({offset}) must be multiple of {self.meta.block_size}')

    boffset, offset = self._translate_offset(offset)

    data = self._try_block(boffset, offset)
    if data is None:
      read_size, _ = self._fetch_block(boffset)
      if read_size > 0:
        data = self._try_block(boffset, offset)

    return data

  def size(self):
    size = self.meta.size
    if size is None:
      tas.check(not self._reader.support_blocks(),
                msg=f'Readers supporting block reads must provide a proper size ' \
                f'within the metadata')
      size, _ = self._fetch_block(self.WHOLE_OFFSET)
      meta = self.meta.clone(size=size)
      self.save_meta(self._path, meta)
      self.meta = meta

    return size

  def locked(self):
    return lockf.LockFile(self._path)

  @classmethod
  def blocks_dir(cls, path):
    return os.path.join(path, cls.BLOCKSDIR)

  @classmethod
  def links_dir(cls, path):
    return os.path.join(path, cls.LINKSDIR)

  @classmethod
  def block_file(cls, path, cid, offset):
    block_id = f'block-{cid}-{offset}' if offset >= 0 else f'block-{cid}'

    return os.path.join(cls.blocks_dir(path), block_id)

  @classmethod
  def parse_block_file(cls, fname):
    m = re.match(r'block\-([^\-]+)(\-(\d+))?$', fname)
    if m:
      offset = m.group(3)
      offset = int(offset) if offset is not None else cls.WHOLE_OFFSET

      return m.group(1), offset

  @classmethod
  def link_file(cls, path, cid, url):
    lpath = os.path.join(cls.links_dir(path), cid)

    return os.path.join(lpath, os.path.basename(url))

  @classmethod
  def purge_blocks(cls, path, max_age=None):
    meta = cls.load_meta(path)

    bpath = cls.blocks_dir(path)
    dropped = []
    with os.scandir(bpath) as sdit:
      for de in sdit:
        if de.is_file():
          pbf = cls.parse_block_file(de.name)
          if pbf is not None:
            cid, offset = pbf
            if cid != meta.cid:
              dropped.append(_DroppedBlock(name=de.name,
                                           sres=de.stat(),
                                           cid=cid,
                                           offset=offset))

    max_age = max_age or int(os.getenv('GFS_CACHE_MAXAGE', 120))
    for dblock in dropped:
      if (time.time() - dblock.sres.st_mtime) > max_age:
        try:
          alog.info(f'Removing block file {dblock.name} from {path} ({meta})')
          os.remove(os.path.join(bpath, dblock.name))
        except Exception as ex:
          alog.warning(f'Unable to purge block file from {dblock.name} from {path}: {ex}')

        lpath = cls.link_file(path, dblock.cid, meta.url)
        nox.qno_except(fsu.safe_rmtree, os.path.dirname(lpath), ignore_errors=True)

  @classmethod
  def save_meta(cls, path, meta):
    mpath = os.path.join(path, cls.METAFILE)
    tpath = rngu.temp_path(nspath=mpath)
    with open(tpath, mode='w') as fd:
      yaml.dump(meta.as_dict(), fd, default_flow_style=False)

    os.replace(tpath, mpath)

  @classmethod
  def load_meta(cls, path):
    mpath = os.path.join(path, cls.METAFILE)
    with open(mpath, mode='r') as fd:
      meta = yaml.safe_load(fd)

      return Meta(**meta)


class CachedFile:

  def __init__(self, cbf):
    self.cbf = cbf
    self._offset = 0
    self._block_start = 0
    self._block = None

  def close(self):
    self.cbf = None

  @property
  def closed(self):
    return self.cbf is None

  def seek(self, pos, whence=os.SEEK_SET):
    if whence == os.SEEK_SET:
      offset = pos
    elif whence == os.SEEK_CUR:
      offset = self._offset + pos
    elif whence == os.SEEK_END:
      offset = self.cbf.size() + pos
    else:
      alog.xraise(ValueError, f'Invalid seek mode: {whence}')

    tas.check_le(offset, self.cbf.size(), msg=f'Offset out of range')
    tas.check_ge(offset, 0, msg=f'Offset out of range')

    self._offset = offset

    return offset

  def tell(self):
    return self._offset

  def _ensure_buffer(self, offset):
    boffset = offset - self._block_start
    if self._block is None or boffset < 0 or boffset >= len(self._block):
      block_offset = (offset // self.cbf.meta.block_size) * self.cbf.meta.block_size

      self._block = memoryview(self.cbf.read_block(block_offset))
      self._block_start = block_offset
      boffset = offset - block_offset

    return boffset

  def read(self, size=-1):
    if size < 0:
      rsize = self.cbf.size() - self._offset
    else:
      rsize = min(size, self.cbf.size() - self._offset)

    parts = []
    while rsize > 0:
      boffset = self._ensure_buffer(self._offset)

      csize = min(rsize, len(self._block) - boffset)
      parts.append(self._block[boffset: boffset + csize])
      self._offset += csize
      rsize -= csize

    return b''.join(parts)

  def read1(self, size=-1):
    return self.read(size=size)

  def peek(self, size=0):
    if size > 0:
      boffset = self._ensure_buffer(self._offset)
      csize = min(size, len(self._block) - boffset)

      return self._block[boffset: boffset + csize].tobytes()

    return b''

  def flush(self):
    pass

  def readable(self):
    return self.cbf is not None

  def seekable(self):
    return self.cbf is not None

  def writable(self):
    return False

  def __enter__(self):
    return self

  def __exit__(self, *exc):
    self.close()

    return False


def _get_cache_path(cache_dir, url):
  uhash = hashlib.sha1(url.encode()).hexdigest()

  return os.path.join(cache_dir, uhash)


def cleanup_cache(cache_dir=None, max_age=None):
  cache_dir = get_cache_dir(path=cache_dir)

  if os.path.isdir(cache_dir):
    with os.scandir(cache_dir) as sdit:
      for de in sdit:
        if de.is_dir():
          cfpath = os.path.join(cache_dir, de.name)
          with lockf.LockFile(cfpath):
            try:
              CachedBlockFile.purge_blocks(cfpath, max_age=max_age)
            except Exception as ex:
              alog.warning(f'Unable to purge blocks from {cfpath}: {ex}')


def make_tag(**kwargs):
  stag = ','.join(f'{k}={v}' for k, v in kwargs.items())

  return hashlib.sha1(stag.encode()).hexdigest()


def create_cached_file(url, meta, reader, cache_dir=None):
  cache_dir = get_cache_dir(path=cache_dir)

  cfpath = _get_cache_path(cache_dir, url)
  with lockf.LockFile(cfpath):
    meta = CachedBlockFile.prepare_meta(meta, url=url)
    if not os.path.isdir(cfpath):
      CachedBlockFile.create(cfpath, meta)
    else:
      xmeta = CachedBlockFile.load_meta(cfpath)
      if xmeta.cid != meta.cid:
        alog.debug(f'Updating meta of {cfpath}: {xmeta} -> {meta}')
        CachedBlockFile.save_meta(cfpath, meta)

    return CachedFile(CachedBlockFile(cfpath, reader, meta=meta))


_CACHE_DIR = os.getenv('GFS_CACHE_DIR',
                       os.path.join(os.getenv('HOME', '.'), '.cache', 'gfs'))

def get_cache_dir(path=None):
  return fsu.normpath(path or _CACHE_DIR)

