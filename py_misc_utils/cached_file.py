"""Block-based file caching.

This module implements a cache holding remote - potentially huge - files
in local folder trees, so that they can be read with a local-filesystem
performance: files are split into blocks, fetched on demand, and
identified by the hash of their metadata (URL, size, block size), so
that blocks of equal content are shared across cache instances.

The cache is cleaned up periodically - removing the least-recently-used
blocks exceeding a configurable maximum size - and offers a CacheInterface
to be implemented by the remote file readers.
"""
import collections
import datetime
import functools
import hashlib
import os
import re
import shutil
import time

import yaml

from . import alog, obj, osfd
from . import assert_checks as tas
from . import core_utils as cu
from . import file_overwrite as fow
from . import fin_wrap as fw
from . import fs_utils as fsu
from . import lockfile as lockf
from . import no_except as nox
from . import tempdir as tmpd

_DroppedBlock = collections.namedtuple('DroppedBlock', 'name, sres, cid, offset')
# A cache block dropped by the cleanup: its name, stat result, content
# id and offset within the cached file.


class Meta(obj.Obj):
  """Metadata of a cached file.

  Attributes:
    url: The URL of the file.
    size: The size of the file.
    blocksize: The size of the cache blocks.
    cid: The content id - the hash identifying the cache entry.
  """


class CachedBlockFile:
  """A block-cached file.

  A CachedBlockFile holds the cache folder tree of a file - its META
  metadata file, its blocks/ folder holding the fetched file blocks and
  its links/ folder holding the hard links to the fetched data - and
  implements block-based random-access reads over it.

  Reads are served from the fetched blocks - fetched via the reader
  (see CacheInterface) upon a cache miss - with each block fetched under
  a per-block lock file, so that concurrent readers do not fetch the
  same block twice.
  """

  METAFILE = 'META'
  BLOCKSDIR = 'blocks'
  LINKSDIR = 'links'
  WHOLE_OFFSET = -1
  CID_SIZE = 16
  BLOCKSIZE = 32 * 1024**2

  def __init__(self, path, reader, meta=None, close_fn=None):
    """Creates a block-cached file.

    Args:
      path: The cache folder path.
      reader: The CacheInterface reader fetching the file data.
      meta: An optional metadata of the file - loaded from the META
        file if None.
      close_fn: An optional function called upon closing the file.
    """
    self._path = path
    self._reader = reader
    self._close_fn = close_fn
    self.meta = self.load_meta(path) if meta is None else meta

  @classmethod
  def default_meta(cls):
    """Returns the default metadata of a cached file."""

    return Meta(url=None, size=None, block_size=cls.BLOCKSIZE)

  @classmethod
  def prepare_meta(cls, meta, **kwargs):
    """Builds the complete metadata of a cached file.

    The content id (cid) - a hash of the metadata tag - is computed and
    added to the metadata.

    Args:
      meta: An optional base metadata to start from.
      **kwargs: Metadata overrides.

    Returns:
      The complete metadata, including its content id.
    """
    cmeta = cls.default_meta()
    cmeta.update_from(meta)
    cmeta.update(**kwargs)

    cid = hashlib.sha1(cmeta.tag.encode()).hexdigest()[:cls.CID_SIZE]
    cmeta.update(cid=cid)

    return cmeta

  @classmethod
  def remove(cls, path):
    """Removes the cache folder tree of a cached file.

    Args:
      path: The cache folder path.

    Returns:
      True if the folder tree has been removed.
    """
    try:
      fsu.safe_rmtree(path, ignore_errors=True)

      return True
    except:
      return False

  @classmethod
  def create(cls, path, meta):
    """Creates the cache folder tree of a cached file - atomically.

    The folder tree is built into a temporary path, and renamed to its
    final path upon success - so that readers never observe a partially
    built tree.

    Args:
      path: The cache folder path.
      meta: The metadata of the cached file - saved into the META file.
    """
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
    """Returns the block file path of a block offset."""
    return self.fblock_path(self._path, self.meta.cid, offset)

  def _fetch_block(self, offset):
    """Fetches a file block into the cache, if not already there.

    The block is fetched under a lock file - so that concurrent readers
    do not fetch the same block twice - written to a temporary path and
    atomically renamed. Whole-file blocks are hard-linked into the
    links/ folder as well.

    Args:
      offset: The block offset within the file, or WHOLE_OFFSET for
        whole-file caching.

    Returns:
      A (size, block path) tuple.
    """
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
    """Makes the local link to a fetched whole-file block."""
    lpath = self.local_link()
    if not os.path.exists(lpath):
      try:
        os.makedirs(os.path.dirname(lpath), exist_ok=True)
        os.link(bpath, lpath)
        os.chmod(lpath, 0o444)
      except Exception:
        alog.warning(f'Unable to create link: {bpath} -> {lpath}')

  def _try_block(self, boffset, offset):
    """Reads data from a cached block, if it is already fetched.

    Args:
      boffset: The block offset.
      offset: The read offset within the block.

    Returns:
      The read data, or None if the block is not (yet) in the cache.
    """
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
    """Returns the (block offset, offset within block) pair of a file
    offset."""
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
    """Closes the cached file - releasing its reader, if any."""
    if self._close_fn is not None:
      self._close_fn()
      self._close_fn = None

  def cacheall(self):
    """Fetches the whole file content into the cache.

    Returns:
      The local link path to the cached content, or None if the file
      could not be fetched.
    """
    size, bpath = self._fetch_block(self.WHOLE_OFFSET)

    return self.local_link() if size > 0 else None

  def read_block(self, offset):
    """Reads a block of data from the file.

    On a cache miss the block is fetched via the reader - and the read
    is retried from the freshly-fetched block.

    Args:
      offset: The offset of the block within the file: it must be a
        multiple of the block size.

    Returns:
      The block data, or None if the block could not be read - e.g. if
      the offset is beyond the end of the file.
    """
    tas.check_eq(
        offset % self.meta.block_size,
        0,
        msg=f'Block offset ({offset}) must be multiple of {self.meta.block_size}')

    boffset, offset = self._translate_offset(offset)

    data = self._try_block(boffset, offset)
    if data is None:
      read_size, _ = self._fetch_block(boffset)
      if read_size > 0:
        data = self._try_block(boffset, offset)

    return data

  def size(self):
    """Returns the size of the file, fetching it from the reader - and
    caching it into the metadata - if not already known.

    Returns:
      The file size, in bytes.
    """
    size = self.meta.size
    if size is None:
      size, _ = self._fetch_block(self.WHOLE_OFFSET)
      meta = self.meta.clone(size=size)
      self.save_meta(self._path, meta)
      self.meta = meta

    return size

  def locked(self):
    """Returns the lock file serializing the access to the cache folder
    tree of the file."""
    return lockf.LockFile(self._path)

  def local_link(self):
    """Returns the local link path of the file - the path a cached
    whole-file content can be accessed from, if fetched."""
    return self.flink_path(self._path, self.meta.cid, self.meta.url)

  @classmethod
  def blocks_dir(cls, path):
    """Returns the folder path holding the blocks of a cached file."""
    return os.path.join(path, cls.BLOCKSDIR)

  @classmethod
  def links_dir(cls, path):
    """Returns the folder path holding the links of a cached file."""
    return os.path.join(path, cls.LINKSDIR)

  @classmethod
  def fblock_path(cls, path, cid, offset):
    """Returns the block file path of a content id and offset."""
    block_id = f'block-{cid}-{offset}' if offset >= 0 else f'block-{cid}'

    return os.path.join(cls.blocks_dir(path), block_id)

  @classmethod
  def parse_block_file(cls, fname):
    """Parses a block file name into its (content id, offset) parts.

    Args:
      fname: The block file name.

    Returns:
      A (cid, offset) tuple, or None if the name does not match the
      block file name format.
    """
    m = re.match(r'block\-([^\-]+)(\-(\d+))?$', fname)
    if m:
      offset = m.group(3)
      offset = int(offset) if offset is not None else cls.WHOLE_OFFSET

      return m.group(1), offset

  @classmethod
  def flink_path(cls, path, cid, url):
    """Returns the link file path of a cached content."""
    lpath = os.path.join(cls.links_dir(path), cid)

    return os.path.join(lpath, os.path.basename(url))

  @classmethod
  def purge_blocks(cls, path, max_age=None):
    """Purges the too-old blocks of a cached file.

    Args:
      path: The cache folder path.
      max_age: The maximum age - in seconds - of the blocks, dropping
        the older ones. If None, all the blocks are kept.
    """
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
              dropped.append(
                  _DroppedBlock(name=dentry.name,
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
          alog.warning(
              f'Unable to purge block file from {dblock.name} from {path}: {ex}')

        lpath = cls.flink_path(path, dblock.cid, meta.url)
        nox.qno_except(fsu.safe_rmtree, os.path.dirname(lpath), ignore_errors=True)

    return meta

  @classmethod
  def fmeta_path(cls, path):
    """Returns the metadata file path of a cached file."""
    return os.path.join(path, cls.METAFILE)

  @classmethod
  def save_meta(cls, path, meta):
    """Saves the metadata of a cached file.

    Args:
      path: The cache folder path.
      meta: The metadata to save.
    """
    mpath = cls.fmeta_path(path)
    with fow.FileOverwrite(mpath) as fd:
      yaml.dump(meta.as_dict(), fd, default_flow_style=False)

  @classmethod
  def load_meta(cls, path):
    """Loads the metadata of a cached file.

    Args:
      path: The cache folder path.

    Returns:
      The metadata.
    """
    mpath = cls.fmeta_path(path)
    with open(mpath, mode='r') as fd:
      meta = yaml.safe_load(fd)

      return Meta(**meta)

  @classmethod
  def validate(cls, path):
    """Validates the cache folder tree of a cached file.

    Args:
      path: The cache folder path.

    Returns:
      True if the folder tree holds a consistent, complete cache entry.
    """
    try:
      return cls.load_meta(path)
    except:
      pass


class CachedFile:
  """A file-like object reading a block-cached file.

  A CachedFile implements the standard read/seek/tell file interface
  over a CachedBlockFile, serving the reads from the cached blocks -
  and holding the current block in memory.
  """

  def __init__(self, cbf, block_size=None):
    """Creates a file-like object over a block-cached file.

    Args:
      cbf: The CachedBlockFile holding the cache folder tree of the
        file.
      block_size: An optional read block size override - the metadata
        block size by default.
    """
    fw.fin_wrap(self, 'cbf', cbf, finfn=cbf.close)
    self._block_size = block_size or cbf.meta.block_size
    self._offset = 0
    self._block_start = 0
    self._block = None

  def close(self):
    """Closes the file, releasing the underlying cached file."""
    cbf = self.cbf
    if cbf is not None:
      fw.fin_wrap(self, 'cbf', None, cleanup=True)

  @property
  def closed(self):
    """Whether the file is closed."""

    return self.cbf is None

  def seek(self, pos, whence=os.SEEK_SET):
    """Moves the file position.

    Args:
      pos: The position offset - relative to whence.
      whence: The seek mode: os.SEEK_SET (absolute), os.SEEK_CUR
        (relative to the current position) or os.SEEK_END (relative to
        the end of the file).

    Returns:
      The new absolute position.

    Raises:
      ValueError: If the whence mode is invalid.
      AssertionError: If the new position falls outside the file.
    """
    if whence == os.SEEK_SET:
      offset = pos
    elif whence == os.SEEK_CUR:
      offset = self._offset + pos
    elif whence == os.SEEK_END:
      offset = self.cbf.size() + pos
    else:
      alog.xraise(ValueError, f'Invalid seek mode: {whence}')

    tas.check_le(offset, self.cbf.size(), msg='Offset out of range')
    tas.check_ge(offset, 0, msg='Offset out of range')

    self._offset = offset

    return offset

  def tell(self):
    """Returns the current file position."""

    return self._offset

  def _ensure_buffer(self, offset):
    """Ensures the current block buffer covers a file offset."""
    boffset = offset - self._block_start
    if self._block is None or boffset < 0 or boffset >= len(self._block):
      block_offset = (offset // self._block_size) * self._block_size

      self._block = memoryview(self.cbf.read_block(block_offset))
      self._block_start = block_offset
      boffset = offset - block_offset

    return boffset

  def _max_size(self, size):
    """Clamps a read size to the remaining file bytes."""
    available = self.cbf.size() - self._offset

    return available if size < 0 else min(size, available)

  def read(self, size=-1):
    """Reads data from the current file position.

    Args:
      size: The number of bytes to read - all the remaining ones if -1.

    Returns:
      The read data.
    """
    rsize = self._max_size(size)

    parts = []
    while rsize > 0:
      boffset = self._ensure_buffer(self._offset)

      csize = min(rsize, len(self._block) - boffset)
      parts.append(self._block[boffset:boffset + csize])
      self._offset += csize
      rsize -= csize

    return b''.join(parts)

  def read1(self, size=-1):
    """Reads data without crossing the current block boundary.

    Args:
      size: The number of bytes to read - all the remaining ones of the
        current block if -1.

    Returns:
      The read data.
    """
    return self.read(size=size)

  def peek(self, size=0):
    """Reads data without moving the file position.

    Args:
      size: The number of bytes to peek.

    Returns:
      The peeked data.
    """
    if size > 0:
      boffset = self._ensure_buffer(self._offset)
      csize = min(size, len(self._block) - boffset)

      return self._block[boffset:boffset + csize].tobytes()

    return b''

  def readline(self, size=-1):
    """Reads a line from the current file position.

    Args:
      size: The maximum number of bytes to read - unbounded if -1.

    Returns:
      The line data - terminated by the newline, if found.
    """
    rsize = self._max_size(size)

    parts = []
    while rsize > 0:
      boffset = self._ensure_buffer(self._offset)

      csize = min(rsize, len(self._block) - boffset)
      cdata = self._block[boffset:boffset + csize]

      pos = cu.vfind(cdata, b'\n')
      if pos >= 0:
        parts.append(cdata[:pos + 1])
        self._offset += pos + 1
        break
      else:
        self._offset += csize
        rsize -= csize

    return b''.join(parts)

  def flush(self):
    """No-op: the file is read-only."""

  def readable(self):
    """Whether the file is readable."""

    return not self.closed

  def seekable(self):
    """Whether the file is seekable."""

    return not self.closed

  def writable(self):
    """Whether the file is writable: always False."""

    return False

  def __enter__(self):
    """Enters the file context."""

    return self

  def __exit__(self, *exc):
    """Exits the file context, closing the file."""

    self.close()

    return False


class CacheInterface:
  """The interface of the remote file readers feeding the block cache.

  The reader of a cached file must implement the methods of this
  interface: reading the file size, the blocks of data - or the whole
  file content for readers not supporting block-based reads - and the
  bookkeeping of the per-reader resources.
  """

  def __init__(self, cache_dir):
    """Creates a cache interface.

    Args:
      cache_dir: The path of the folder holding the cached files.
    """
    self._cache_dir = cache_dir

  def _open(self, cfpath, url, meta, reader, close_fn=None, **kwargs):
    """Opens a cache entry - creating it, if missing or inconsistent."""
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
    """Opens a cached file, creating its cache entry - if missing.

    If the cache entry exists and validates, its metadata is reused;
    otherwise the entry is created from the given metadata - completed
    with the reader-provided size, if missing.

    Args:
      url: The URL of the file to open.
      meta: The base metadata of the file.
      reader: The CacheInterface reader fetching the file data.
      **kwargs: Metadata overrides, plus 'uncached' - a flag
        materializing the file content into a temporary folder, cleaned
        up upon close.

    Returns:
      The open CachedFile.
    """
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
    """Materializes a file into a local path.

    The file is fetched - whole - into the cache, and its local link
    path is returned.

    Args:
      url: The URL of the file to materialize.
      meta: The base metadata of the file.
      reader: The CacheInterface reader fetching the file data.
      **kwargs: Metadata overrides.

    Returns:
      The local path holding the file content.

    Raises:
      AssertionError: If the file content could not be fetched.
    """
    cfile = self.open(url, meta, reader, **kwargs)

    local_path = cfile.cbf.cacheall()
    tas.check_is_not_none(local_path, msg=f'Unable to materialize a local path: {url}')

    return local_path


def _get_cache_path(cache_dir, url):
  """Returns the cache entry path of an URL."""
  uhash = hashlib.sha1(url.encode()).hexdigest()

  return os.path.join(cache_dir, uhash)


_CacheFileStats = collections.namedtuple(
    'CacheFileStats',
    'path, mtime, size, meta',
)


def cleanup_cache(cache_dir, max_age=None, max_size=None):
  """Cleans up a file cache folder.

  Purges the too-old blocks of the cached files, then drops the whole
  cache entries - least-recently-used first - until the total cache size
  fits the maximum size.

  Args:
    cache_dir: The cache folder path.
    max_age: An optional maximum age - in seconds - of the cached
      blocks, dropping the older ones. Defaults to the period the cache
      is not accessed for (see get_cache_dir()).
    max_size: An optional maximum size of the cache. Defaults to the
      GFS_CACHE_MAXSIZE environment variable value - 16GB by default.
  """
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
              cache_files.append(
                  _CacheFileStats(path=cfpath,
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
  """Builds the metadata tag hashed into a cache entry content id.

  Args:
    **kwargs: The metadata values making up the tag.

  Returns:
    The serialized tag.
  """
  stag = ','.join(f'{k}={v}' for k, v in kwargs.items())

  return hashlib.sha1(stag.encode()).hexdigest()


_CLEANUP_PERIOD = int(os.getenv('GFS_CACHE_CLEANUP_PERIOD', 8 * 3600))


def _cleanup_check(path):
  """Triggers the cleanup of a cache folder - if due."""
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
  """Returns the cache folder of a base path, triggering its cleanup.

  The cache folder is cleaned up if the last cleanup is older than the
  cleanup period - tracked via a marker file.

  Args:
    path: The base path holding the cache folder.

  Returns:
    The cache folder path.
  """
  cdpath = os.path.join(fsu.normpath(path), 'gfs')

  return _cleanup_check(cdpath)
