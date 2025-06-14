import collections
import datetime
import functools
import hashlib
import os
import re
import shutil
import time
import yaml

from . import alog
from . import assert_checks as tas
from . import core_utils as cu
from . import file_overwrite as fow
from . import fin_wrap as fw
from . import fs_utils as fsu
from . import lockfile as lockf
from . import no_except as nox
from . import obj
from . import osfd
from . import tempdir as tmpd


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

  def __init__(self, path, reader, meta=None, close_fn=None):
    self._path = path
    self._reader = reader
    self._close_fn = close_fn
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
    tpath = fsu.temp_path(nspath=path)
    try:
      os.makedirs(tpath, exist_ok=True)
      os.mkdir(cls.blocks_dir(tpath))
      os.mkdir(cls.links_dir(tpath))

      cls.save_meta(tpath, meta)

      os.rename(tpath, path)
    except:
      shutil.rmtree(tpath, ignore_errors=True)
      raise

  def _fblock_path(self, offset):
    return self.fblock_path(self._path, self.meta.cid, offset)

  def _fetch_block(self, offset):
    bpath = self._fblock_path(offset)
    with lockf.LockFile(bpath):
      if (sres := fsu.stat(bpath)) is None:
        tpath = fsu.temp_path(nspath=bpath)
        try:
          rsize = self._reader.read_block(tpath, offset, self.meta.block_size)
          if rsize > 0:
            os.replace(tpath, bpath)
            if offset == self.WHOLE_OFFSET:
              self._make_link(bpath)
        except:
          fsu.maybe_remove(tpath)
          raise
      else:
        rsize = sres.st_size

    return rsize, bpath

  def _make_link(self, bpath):
    lpath = self.local_link()
    if not os.path.exists(lpath):
      try:
        os.makedirs(os.path.dirname(lpath), exist_ok=True)
        os.link(bpath, lpath)
        os.chmod(lpath, 0o444)
      except Exception as ex:
        alog.warning(f'Unable to create link: {bpath} -> {lpath}')

  def _try_block(self, boffset, offset):
    bpath = self._fblock_path(boffset)
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
      bpath = self._fblock_path(self.WHOLE_OFFSET)
      has_whole_content = os.path.exists(bpath)

    if has_whole_content:
      boffset = self.WHOLE_OFFSET
    else:
      boffset, offset = offset, 0

    return boffset, offset

  def close(self):
    if self._close_fn is not None:
      self._close_fn()
      self._close_fn = None

  def cacheall(self):
    size, bpath = self._fetch_block(self.WHOLE_OFFSET)

    return self.local_link() if size > 0 else None

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
      size, _ = self._fetch_block(self.WHOLE_OFFSET)
      meta = self.meta.clone(size=size)
      self.save_meta(self._path, meta)
      self.meta = meta

    return size

  def locked(self):
    return lockf.LockFile(self._path)

  def local_link(self):
    return self.flink_path(self._path, self.meta.cid, self.meta.url)

  @classmethod
  def blocks_dir(cls, path):
    return os.path.join(path, cls.BLOCKSDIR)

  @classmethod
  def links_dir(cls, path):
    return os.path.join(path, cls.LINKSDIR)

  @classmethod
  def fblock_path(cls, path, cid, offset):
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
  def flink_path(cls, path, cid, url):
    lpath = os.path.join(cls.links_dir(path), cid)

    return os.path.join(lpath, os.path.basename(url))

  @classmethod
  def purge_blocks(cls, path, max_age=None):
    meta = cls.load_meta(path)

    bpath = cls.blocks_dir(path)
    dropped = []
    with os.scandir(bpath) as sdit:
      for dentry in sdit:
        if dentry.is_file():
          pbf = cls.parse_block_file(dentry.name)
          if pbf is not None:
            cid, offset = pbf
            if cid != meta.cid:
              dropped.append(_DroppedBlock(name=dentry.name,
                                           sres=dentry.stat(),
                                           cid=cid,
                                           offset=offset))

    max_age = max_age or int(os.getenv('GFS_CACHE_MAXAGE', 300))
    for dblock in dropped:
      if (time.time() - dblock.sres.st_mtime) > max_age:
        try:
          alog.info(f'Removing block file {dblock.name} from {path} ({meta})')
          os.remove(os.path.join(bpath, dblock.name))
        except Exception as ex:
          alog.warning(f'Unable to purge block file from {dblock.name} from {path}: {ex}')

        lpath = cls.flink_path(path, dblock.cid, meta.url)
        nox.qno_except(fsu.safe_rmtree, os.path.dirname(lpath), ignore_errors=True)

    return meta

  @classmethod
  def fmeta_path(cls, path):
    return os.path.join(path, cls.METAFILE)

  @classmethod
  def save_meta(cls, path, meta):
    mpath = cls.fmeta_path(path)
    with fow.FileOverwrite(mpath) as fd:
      yaml.dump(meta.as_dict(), fd, default_flow_style=False)

  @classmethod
  def load_meta(cls, path):
    mpath = cls.fmeta_path(path)
    with open(mpath, mode='r') as fd:
      meta = yaml.safe_load(fd)

      return Meta(**meta)

  @classmethod
  def validate(cls, path):
    try:
      return cls.load_meta(path)
    except:
      pass


class CachedFile:

  def __init__(self, cbf, block_size=None):
    fw.fin_wrap(self, 'cbf', cbf, finfn=cbf.close)
    self._block_size = block_size or cbf.meta.block_size
    self._offset = 0
    self._block_start = 0
    self._block = None

  def close(self):
    cbf = self.cbf
    if cbf is not None:
      fw.fin_wrap(self, 'cbf', None, cleanup=True)

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
      block_offset = (offset // self._block_size) * self._block_size

      self._block = memoryview(self.cbf.read_block(block_offset))
      self._block_start = block_offset
      boffset = offset - block_offset

    return boffset

  def _max_size(self, size):
    available = self.cbf.size() - self._offset

    return available if size < 0 else min(size, available)

  def read(self, size=-1):
    rsize = self._max_size(size)

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

  def readline(self, size=-1):
    rsize = self._max_size(size)

    parts = []
    while rsize > 0:
      boffset = self._ensure_buffer(self._offset)

      csize = min(rsize, len(self._block) - boffset)
      cdata = self._block[boffset: boffset + csize]

      pos = cu.vfind(cdata, b'\n')
      if pos >= 0:
        parts.append(cdata[: pos + 1])
        self._offset += pos + 1
        break
      else:
        self._offset += csize
        rsize -= csize

    return b''.join(parts)

  def flush(self):
    pass

  def readable(self):
    return not self.closed

  def seekable(self):
    return not self.closed

  def writable(self):
    return False

  def __enter__(self):
    return self

  def __exit__(self, *exc):
    self.close()

    return False


class CacheInterface:

  def __init__(self, cache_dir):
    self._cache_dir = cache_dir

  def _open(self, cfpath, url, meta, reader, close_fn=None, **kwargs):
    with lockf.LockFile(cfpath):
      meta = CachedBlockFile.prepare_meta(meta, url=url)
      if (xmeta := CachedBlockFile.validate(cfpath)) is None:
        CachedBlockFile.create(cfpath, meta)
      else:
        if xmeta.cid != meta.cid:
          alog.debug(f'Updating meta of {cfpath}: {xmeta} -> {meta}')
          CachedBlockFile.save_meta(cfpath, meta)

      return CachedFile(CachedBlockFile(cfpath, reader, meta=meta, close_fn=close_fn))

  def open(self, url, meta, reader, **kwargs):
    uncached = kwargs.pop('uncached', False)
    if uncached:
      tmp_path = tmpd.create()
      cfpath = _get_cache_path(tmp_path, url)
      close_fn = functools.partial(fsu.safe_rmtree, tmp_path, ignore_errors=True)
    else:
      cfpath = _get_cache_path(self._cache_dir, url)
      close_fn = None

    return self._open(cfpath, url, meta, reader, close_fn=close_fn, **kwargs)

  def as_local(self, url, meta, reader, **kwargs):
    cfile = self.open(url, meta, reader, **kwargs)

    local_path = cfile.cbf.cacheall()
    tas.check_is_not_none(local_path, msg=f'Unable to materialize a local path: {url}')

    return local_path


def _get_cache_path(cache_dir, url):
  uhash = hashlib.sha1(url.encode()).hexdigest()

  return os.path.join(cache_dir, uhash)


_CacheFileStats = collections.namedtuple(
  'CacheFileStats', 'path, mtime, size, meta',
)

def cleanup_cache(cache_dir, max_age=None, max_size=None):
  alog.verbose(f'Cache cleanup running: {cache_dir}')

  if os.path.isdir(cache_dir):
    cache_files = []
    with os.scandir(cache_dir) as sdit:
      for dentry in sdit:
        if dentry.is_dir():
          cfpath = os.path.join(cache_dir, dentry.name)
          with lockf.LockFile(cfpath):
            try:
              meta = CachedBlockFile.purge_blocks(cfpath, max_age=max_age)

              cfsize = fsu.du(cfpath)
              sres = os.stat(CachedBlockFile.fmeta_path(cfpath))
              cache_files.append(_CacheFileStats(path=cfpath,
                                                 mtime=sres.st_mtime,
                                                 size=cfsize,
                                                 meta=meta))
            except Exception as ex:
              alog.warning(f'Unable to purge blocks from {cfpath}: {ex}')

    cache_files = sorted(cache_files, key=lambda cfs: cfs.mtime, reverse=True)
    max_size = max_size or int(os.getenv('GFS_CACHE_MAXSIZE', 16 * 1024**3))

    cache_size = 0
    for cfs in cache_files:
      cache_size += cfs.size
      if cache_size >= max_size:
        alog.info(f'Dropping cache for {cfs.meta.url} stored at {cfs.path}')
        with lockf.LockFile(cfs.path):
          CachedBlockFile.remove(cfs.path)

    alog.debug0(f'Cache size was {cu.size_str(cache_size)} (size will be trimmed ' \
                f'to {cu.size_str(max_size)})')


def make_tag(**kwargs):
  stag = ','.join(f'{k}={v}' for k, v in kwargs.items())

  return hashlib.sha1(stag.encode()).hexdigest()


_CLEANUP_PERIOD = int(os.getenv('GFS_CACHE_CLEANUP_PERIOD', 8 * 3600))

def _cleanup_check(path):
  lpath = os.path.join(path, '.last_cleanup')
  if (sres := fsu.stat(lpath)) is None:
    do_cleanup = os.path.isdir(path)
  else:
    do_cleanup = time.time() > sres.st_mtime + _CLEANUP_PERIOD

  if do_cleanup:
    alog.debug(f'Triggering cache cleanup: {path}')
    cleanup_cache(path)
    with open(lpath, mode='w') as fd:
      fd.write(datetime.datetime.now().isoformat(timespec='microseconds'))

  return path


def get_cache_dir(path):
  cdpath = os.path.join(fsu.normpath(path), 'gfs')

  return _cleanup_check(cdpath)

