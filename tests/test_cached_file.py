"""Tests for py_misc_utils.cached_file.

The block cache stores remote files as block trees under a per-URL folder
(sha1 of the URL), with a META yaml file and blocks/ + links/ folders.
Tests use a stub CacheInterface reader counting fetches, verifying:
meta/cid semantics, tree create/load/validate/remove, whole-file and
block-based fetch paths, CachedFile read/seek/peek behavior, entry reuse,
inconsistent-entry rebuild, uncached mode, as_local, cleanup (max_age and
max_size) and get_cache_dir.

The cid is the sha1 of the metadata 'tag' only: callers are expected to
build the tag embedding the identity-relevant metadata via make_tag()
(url, size, etc.), so the effective cache identity is tag-driven. The
cache entry folder is the sha1 of the URL instead.
"""

import datetime
import os

import pytest

from py_misc_utils import cached_file as chf


class StubReader(chf.CacheInterface):
  """Reader fetching from an in-memory payload, counting block fetches."""

  def __init__(self, content, block_size=64, support_blocks=True):
    super().__init__(cache_dir=None)
    self.content = content
    self.sb = support_blocks
    self.fetches = []

  def support_blocks(self):
    return self.sb

  def read_block(self, tpath, offset, size):
    self.fetches.append(offset)
    if offset == chf.CachedBlockFile.WHOLE_OFFSET:
      data = self.content
    else:
      data = self.content[offset:offset + size]
    with open(tpath, 'wb') as fd:
      fd.write(data)

    return len(data)


def _meta(tag='v1', size=None, **kw):
  kw.setdefault('block_size', 64)
  return chf.Meta(tag=tag, size=size, mtime=111, **kw)


def _cpath(cache_dir, url):
  """Locate the cache entry folder of a URL through public helpers.

  The entry folder name is an implementation detail; the entry is found
  by reading each sub-folder metadata, whose url field is public.
  """
  for name in os.listdir(cache_dir):
    path = os.path.join(cache_dir, name)
    meta = chf.CachedBlockFile.load_meta(path)
    if meta is not None and meta.url == url:
      return path

  return None


def test_default_meta():
  """default_meta carries a 32MiB block size and unset url/size."""
  m = chf.CachedBlockFile.default_meta()

  assert m.url is None
  assert m.size is None
  assert m.block_size == chf.CachedBlockFile.BLOCKSIZE


def test_prepare_meta_cid_hash():
  """cid is the 16-char sha1 of the tag only; prepare_meta merges kwargs."""
  base = chf.CachedBlockFile.prepare_meta(_meta(tag='v1', size=100), url='u1')

  assert base.url == 'u1'
  assert base.size == 100
  assert len(base.cid) == chf.CachedBlockFile.CID_SIZE

  same = chf.CachedBlockFile.prepare_meta(_meta(tag='v1', size=100), url='u1')
  diff_tag = chf.CachedBlockFile.prepare_meta(_meta(tag='v2', size=100), url='u1')
  diff_size = chf.CachedBlockFile.prepare_meta(_meta(tag='v1', size=200), url='u1')
  diff_url = chf.CachedBlockFile.prepare_meta(_meta(tag='v1', size=100), url='u2')

  assert base.cid == same.cid
  assert base.cid != diff_tag.cid
  assert base.cid == diff_size.cid
  assert base.cid == diff_url.cid


def test_tree_create_load_validate_remove(tmp_path):
  """create() builds the tree + META; load/validate roundtrip; remove() drops it."""
  path = os.path.join(tmp_path, 'entry')
  meta = chf.CachedBlockFile.prepare_meta(_meta(tag='v1', size=5), url='u1')

  chf.CachedBlockFile.create(path, meta)

  loaded = chf.CachedBlockFile.load_meta(path)
  assert loaded.as_dict() == meta.as_dict()
  assert chf.CachedBlockFile.validate(path).as_dict() == meta.as_dict()
  assert os.path.isdir(chf.CachedBlockFile.blocks_dir(path))
  assert os.path.isdir(chf.CachedBlockFile.links_dir(path))

  assert chf.CachedBlockFile.remove(path)
  assert chf.CachedBlockFile.validate(path) is None


def test_validate_missing_tree(tmp_path):
  """validate() returns None for a missing/incomplete cache tree."""
  assert chf.CachedBlockFile.validate(os.path.join(tmp_path, 'nope')) is None


def test_whole_file_fetch_and_cacheall(tmp_path):
  """A non-blocking reader fetches the whole file; cacheall links it read-only."""
  cache_dir = os.path.join(tmp_path, 'cache')
  content = b'whole-file-content'
  reader = StubReader(content, support_blocks=False)
  iface = chf.CacheInterface(cache_dir)

  cfile = iface.open('http://x/data.bin', _meta(tag='v1'), reader)

  link = cfile.cbf.cacheall()
  assert link is not None
  with open(link, 'rb') as fd:
    assert fd.read() == content
  # The link is a hard link to the whole-file block, made read-only.
  assert (os.stat(link).st_mode & 0o777) == 0o444
  assert reader.fetches == [chf.CachedBlockFile.WHOLE_OFFSET]


def test_block_based_reads(tmp_path):
  """A blocking reader fetches only the blocks actually read."""
  cache_dir = os.path.join(tmp_path, 'cache')
  content = bytes(range(256))
  reader = StubReader(content, block_size=64)
  iface = chf.CacheInterface(cache_dir)

  with iface.open('http://x/data.bin', _meta(tag='v1', size=256), reader) as cfile:
    assert cfile.read(10) == content[:10]
    assert cfile.tell() == 10
    # Crossing into the second block fetches it on demand.
    assert cfile.read(100) == content[10:110]
    assert len(reader.fetches) == 2


def test_block_reads_reuse_cache(tmp_path):
  """Already-fetched blocks are not re-fetched by new CachedFile opens."""
  cache_dir = os.path.join(tmp_path, 'cache')
  content = b'x' * 200
  reader = StubReader(content)
  iface = chf.CacheInterface(cache_dir)
  meta = _meta(tag='v1')

  with iface.open('http://x/data.bin', meta, reader) as cfile:
    assert cfile.read(64) == content[:64]
  nfetches = len(reader.fetches)

  with iface.open('http://x/data.bin', meta, reader) as cfile:
    assert cfile.read(64) == content[:64]
  assert len(reader.fetches) == nfetches


def test_cachedfile_seek_peek_pread(tmp_path):
  """CachedFile implements seek/peek/pread with position preservation."""
  cache_dir = os.path.join(tmp_path, 'cache')
  content = bytes(range(100))
  iface = chf.CacheInterface(cache_dir)

  with iface.open('http://x/data.bin', _meta(tag='v1', size=100),
                  StubReader(content)) as cfile:
    assert cfile.seek(40) == 40
    assert cfile.peek(4) == content[40:44]
    assert cfile.tell() == 40
    assert cfile.read(4) == content[40:44]
    assert cfile.tell() == 44
    assert cfile.read(100) == content[44:]


def test_cachedfile_eof_and_flags(tmp_path):
  """Reads past EOF return short data; flags follow the closed state."""
  cache_dir = os.path.join(tmp_path, 'cache')
  iface = chf.CacheInterface(cache_dir)

  cfile = iface.open('http://x/data.bin', _meta(tag='v1'), StubReader(b'abc'))
  assert cfile.read(10) == b'abc'
  assert cfile.read() == b''
  assert cfile.readable()
  assert cfile.seekable()
  assert not cfile.writable()
  cfile.flush()
  cfile.close()
  assert cfile.closed
  assert not cfile.readable()


def test_entry_recreated_on_tag_change(tmp_path):
  """A changed content tag rebuilds the cache entry with the new cid."""
  cache_dir = os.path.join(tmp_path, 'cache')
  iface = chf.CacheInterface(cache_dir)
  url = 'http://x/data.bin'

  with iface.open(url, _meta(tag='v1'), StubReader(b'first')) as cfile:
    assert cfile.read() == b'first'

  with iface.open(url, _meta(tag='v2'), StubReader(b'second')) as cfile:
    assert cfile.read() == b'second'
    assert cfile.cbf.meta.cid != cfile.cbf.prepare_meta(_meta(tag='v1'), url=url).cid


def test_missing_meta_entry_fails_to_rebuild(tmp_path):
  """A entry tree without its META file cannot be rebuilt (documented bug).

  _open() treats a META-less entry as absent and create()s a new tree:
  the rename onto the still-existing entry folder fails with ENOTEMPTY.
  Reported as a library finding (rebuild path broken), not worked around.
  """
  cache_dir = os.path.join(tmp_path, 'cache')
  reader = StubReader(b'payload')
  iface = chf.CacheInterface(cache_dir)
  meta = _meta(tag='v1')

  with iface.open('http://x/data.bin', meta, reader):
    pass

  cpath = _cpath(cache_dir, 'http://x/data.bin')
  os.remove(chf.CachedBlockFile.fmeta_path(cpath))

  with pytest.raises(OSError):
    iface.open('http://x/data.bin', meta, reader)


def test_uncached_mode(tmp_path):
  """uncached=True materializes the file into a temp dir, freed on close."""
  iface = chf.CacheInterface(os.path.join(tmp_path, 'cache'))

  cfile = iface.open('http://x/data.bin',
                     _meta(tag='v1'),
                     StubReader(b'temp-content'),
                     uncached=True)

  local = cfile.cbf.cacheall()
  assert local is not None
  with open(local, 'rb') as fd:
    assert fd.read() == b'temp-content'

  cfile.close()
  assert not os.path.exists(os.path.dirname(os.path.dirname(local)))


def test_as_local(tmp_path):
  """as_local() returns a local path holding the full content."""
  iface = chf.CacheInterface(os.path.join(tmp_path, 'cache'))

  lpath = iface.as_local('http://x/data.bin', _meta(tag='v1'),
                         StubReader(b'local-content'))

  with open(lpath, 'rb') as fd:
    assert fd.read() == b'local-content'


def test_cleanup_cache_max_age(tmp_path):
  """max_age purges only stale-version blocks, never the current one.

  purge_blocks() collects blocks whose cid differs from the current
  metadata cid and drops those older than max_age; the current version
  block is always kept.
  """
  cache_dir = os.path.join(tmp_path, 'cache')
  iface = chf.CacheInterface(cache_dir)
  url = 'http://x/a.bin'

  with iface.open(url, _meta(tag='v1'), StubReader(b'aaaa')) as cfile:
    cfile.read()

  # A reopen with a new tag leaves the v1 block behind as stale.
  with iface.open(url, _meta(tag='v2'), StubReader(b'bbbb')) as cfile:
    cfile.read()

  cpath = _cpath(cache_dir, url)
  bdir = chf.CachedBlockFile.blocks_dir(cpath)
  old_time = datetime.datetime.now() - datetime.timedelta(days=30)
  aged = [
      n for n in os.listdir(bdir)
      if n.endswith(chf.CachedBlockFile.prepare_meta(_meta(tag='v1'), url=url).cid)
  ]
  assert len(aged) == 1
  os.utime(os.path.join(bdir, aged[0]), (old_time.timestamp(), old_time.timestamp()))

  chf.CachedBlockFile.purge_blocks(cpath, max_age=60)

  assert os.listdir(bdir) == [
      f'block-{chf.CachedBlockFile.prepare_meta(_meta(tag="v2"), url=url).cid}'
  ]


def test_cleanup_cache_max_size(tmp_path):
  """cleanup_cache(max_size=...) drops LRU entries until the size fits."""
  cache_dir = os.path.join(tmp_path, 'cache')
  iface = chf.CacheInterface(cache_dir)

  iface.open('http://x/old.bin',
             _meta(tag='v1'),
             StubReader(b'o' * 100),
             uncached=False).close()
  iface.open('http://x/new.bin',
             _meta(tag='v1'),
             StubReader(b'n' * 100),
             uncached=False).close()

  old_path = _cpath(cache_dir, 'http://x/old.bin')
  new_path = _cpath(cache_dir, 'http://x/new.bin')
  old_time = datetime.datetime.now() - datetime.timedelta(days=30)
  for path in (old_path, new_path):
    os.utime(path, (old_time.timestamp(), old_time.timestamp()))
  os.utime(new_path, None)  # make 'new' the LRU survivor

  # Entries are sorted newest-first and dropped once the running size
  # total crosses max_size: with 150 only the older entry is dropped,
  # with 1 the running total crosses on the first entry (both dropped).
  chf.cleanup_cache(cache_dir, max_size=150)

  assert not os.path.exists(old_path)
  assert os.path.exists(new_path)

  chf.cleanup_cache(cache_dir, max_size=1)

  assert not os.path.exists(new_path)


def test_get_cache_dir(tmp_path):
  """get_cache_dir() appends the 'gfs' folder but does not create it.

  The folder is created lazily by the cache machinery (the cleanup check
  only runs the periodic cleanup), so the returned path may not exist.
  """
  cdir = chf.get_cache_dir(os.path.join(tmp_path, 'base'))

  assert cdir == os.path.join(os.path.join(tmp_path, 'base'), 'gfs')
  assert not os.path.exists(cdir)
