"""Tests for py_misc_utils.gfs.

The gfs facade dispatches path/URL operations to registered FileSystem
backends (file protocol by default). Tests cover std_open standard
streams, TempFile replace/discard semantics, RegexMatcher (re.match
anchoring + match recording), core dispatchers (stat, is_file/is_dir,
open/makedirs/rmtree/copy/replace/remove/symlink), enumerate_files with
matchers, is_same_fs, local-path helpers, the fs registry and cache_dir
get/set.

Note: the file backend's open() returns the raw file object (not a
context manager), so callers close it explicitly. file:// URLs are NOT
supported by the file backend (norm_url leaves the scheme in the path,
resolving to garbage) - remote protocols only; plain paths must be used
for local files (reported as a finding, not pinned here).
"""

import os
from types import SimpleNamespace

import pytest

from py_misc_utils import fs_utils as fsu
from py_misc_utils import gfs


def test_std_open_streams():
  """std_open returns nullcontexts wrapping the standard streams."""
  import sys

  for name, stream in (('STDIN', sys.stdin), ('STDOUT', sys.stdout), ('STDERR',
                                                                      sys.stderr)):
    with gfs.std_open(name) as fd:
      assert fd is stream

  # Unknown names fall through to a plain open() - requiring mode.
  with pytest.raises(TypeError):
    gfs.std_open('UNKNOWN-STREAM')


def test_std_open_local_file(tmp_path):
  """std_open falls through to gfs.open: raw file object, no mode -> error."""
  path = tmp_path / 'f.txt'
  path.write_bytes(b'data')

  with pytest.raises(TypeError):
    gfs.std_open(str(path))

  fd = gfs.std_open(str(path), mode='rb')
  try:
    assert fd.read() == b'data'
  finally:
    fd.close()


def test_open_source_passthrough(tmp_path):
  """open() yields already-opened file objects unchanged."""
  path = tmp_path / 'f.txt'
  path.write_bytes(b'raw')

  with open(str(path), 'rb') as fd, gfs.open(fd) as fd2:
    assert fd2 is fd
    assert fd2.read() == b'raw'


def test_open_requires_mode_keyword():
  """Backend open() takes mode as a keyword argument."""
  with pytest.raises(TypeError):
    gfs.open('/etc/hosts', 'rb')

  fd = gfs.open('/etc/hosts', mode='rb')
  try:
    assert fd.read(4)
  finally:
    fd.close()


def test_tempfile_discard_on_close(tmp_path):
  """A TempFile closed without replace() is removed."""
  tf = gfs.TempFile(nsdir=str(tmp_path), mode='w+b')

  with tf as fd:
    fd.write(b'temporary')
    tpath = tf._path

  assert not os.path.exists(tpath)


def test_tempfile_replace(tmp_path):
  """replace() atomically moves the temp file to its destination."""
  final = tmp_path / 'final.txt'

  with gfs.TempFile(nspath=str(final), mode='w+b') as fd:
    fd.write(b'final-content')
    fd.replace(str(final))

  assert final.read_bytes() == b'final-content'


def test_tempfile_replace_nonlocal_path_ignored(tmp_path):
  """Non-local nspath is ignored: a local temp path is used instead."""
  tf = gfs.TempFile(nspath='s3://bucket/key', mode='w+b')

  with tf as fd:
    fd.write(b'local')
    tpath = tf._path

  assert tf._fs.ID == 'file'
  assert not tpath.startswith('s3://')
  assert not os.path.exists(tpath)


def test_tempfile_replace_to_url(tmp_path):
  """replace() moves a local temp file to a gfs URL destination."""
  dst = tmp_path / 'dstdir'
  dst.mkdir()

  tf = gfs.TempFile(nsdir=str(tmp_path), mode='w+b')
  with tf as fd:
    fd.write(b'url-target')
    tf.replace(f'{dst}/out.bin')

  assert (dst / 'out.bin').read_bytes() == b'url-target'
  assert not hasattr(tf, 'closed')


def test_tempfile_mirrors_fd_attrs(tmp_path):
  """open() mirrors the fd public attributes onto the TempFile."""
  tf = gfs.TempFile(nsdir=str(tmp_path), mode='w+b')

  with tf:
    assert tf.closed is False
    assert tf.writable()

  # Mirrored attributes are removed when the fd is closed.
  assert not hasattr(tf, 'closed')


def test_regex_matcher():
  """RegexMatcher uses re.match (anchored) and records the match."""
  rm = gfs.RegexMatcher(r'py.*\.py$')

  assert rm('py_misc_utils.py')
  assert not rm('apy_x.py')
  assert not rm('py_x.txt')
  assert rm('py_x.py')
  assert rm.match.group(0) == 'py_x.py'


def test_stat_and_is_file_is_dir(tmp_path):
  """stat returns a DirEntry; is_file/is_dir classify paths."""
  base = tmp_path / 'base'
  base.mkdir()
  (base / 'f.txt').write_bytes(b'0123456789')

  de = gfs.stat(str(base / 'f.txt'))
  assert de.st_size == 10
  assert de.name == 'f.txt'

  assert gfs.is_file(str(base / 'f.txt'))
  assert gfs.is_dir(str(base))
  assert not gfs.is_file(str(base))
  assert not gfs.is_dir(str(base / 'f.txt'))


def test_exists(tmp_path):
  """exists() dispatches to the backend."""
  base = tmp_path / 'base'
  base.mkdir()
  (base / 'f.txt').write_bytes(b'x')

  assert gfs.exists(str(base))
  assert gfs.exists(str(base / 'f.txt'))
  assert not gfs.exists(str(base / 'nope'))


def test_open_read_write(tmp_path):
  """open() reads and writes through the registered backend."""
  path = tmp_path / 'io.bin'

  fd = gfs.open(str(path), mode='wb')
  fd.write(b'gfs-io')
  fd.close()

  fd = gfs.open(str(path), mode='rb')
  data = fd.read()
  fd.close()

  assert data == b'gfs-io'


def test_makedirs_rmtree(tmp_path):
  """makedirs creates nested dirs; rmtree removes them."""
  tree = tmp_path / 'a' / 'b' / 'c'

  gfs.makedirs(str(tree))
  assert tree.is_dir()

  gfs.rmtree(str(tree))
  assert not tree.exists()


def test_copy_remove(tmp_path):
  """copy() duplicates a file; remove() deletes paths."""
  src = tmp_path / 'src.txt'
  dst = tmp_path / 'dst.txt'
  src.write_bytes(b'copyme')

  gfs.copy(str(src), str(dst))
  assert dst.read_bytes() == b'copyme'

  gfs.remove(str(dst))
  assert not dst.exists()


def test_replace(tmp_path):
  """replace() moves a file across paths."""
  src = tmp_path / 'mv.txt'
  dst = tmp_path / 'moved.txt'
  src.write_bytes(b'move')

  gfs.replace(str(src), str(dst))

  assert not src.exists()
  assert dst.read_bytes() == b'move'


def test_symlink_same_fs(tmp_path):
  """symlink() creates a link within the same filesystem."""
  src = tmp_path / 'real.txt'
  lnk = tmp_path / 'link.txt'
  src.write_bytes(b'linked')

  gfs.symlink(str(src), str(lnk))

  assert lnk.read_bytes() == b'linked'


def test_link_across_fs_rejected(tmp_path):
  """link/symlink across filesystems raise an AssertionError."""
  src = tmp_path / 'f.txt'
  src.write_bytes(b'x')

  with pytest.raises(AssertionError):
    gfs.link(str(src), 's3://bucket/key')


def test_enumerate_files(tmp_path):
  """enumerate_files yields names, or (name, DirEntry) with return_stats."""
  (tmp_path / 'a.txt').write_bytes(b'a')
  (tmp_path / 'sub').mkdir()
  (tmp_path / 'sub' / 'b.txt').write_bytes(b'b')

  names = list(gfs.enumerate_files(str(tmp_path)))

  assert sorted(names) == ['a.txt', 'sub']

  pairs = dict(gfs.enumerate_files(str(tmp_path), return_stats=True))

  assert set(pairs) == {'a.txt', 'sub'}
  assert pairs['a.txt'].st_size == 1


def test_enumerate_files_regex(tmp_path):
  """The matcher argument filters entries (regex match)."""
  (tmp_path / 'keep.txt').write_bytes(b'k')
  (tmp_path / 'drop.log').write_bytes(b'd')

  entries = list(
      gfs.enumerate_files(str(tmp_path), matcher=gfs.RegexMatcher(r'.*\.txt')))

  assert entries == ['keep.txt']


def test_is_same_fs(tmp_path):
  """is_same_fs compares protocol+host of path-holding entries."""
  a = tmp_path / 'a.txt'
  b = tmp_path / 'b.txt'
  a.write_bytes(b'a')
  b.write_bytes(b'b')

  ea, eb = gfs.stat(str(a)), gfs.stat(str(b))

  assert gfs.is_same_fs(ea, eb)
  assert gfs.is_same_fs(ea, SimpleNamespace(path=str(b)))
  assert not gfs.is_same_fs(ea, SimpleNamespace(path='s3://bucket/key'))
  assert not gfs.is_same_fs(SimpleNamespace(path='s3://a'),
                            SimpleNamespace(path='gs://b'))


def test_is_local_path_and_helpers(tmp_path):
  """is_local_path/find_mount classify local vs remote paths."""
  assert gfs.is_local_path(str(tmp_path))
  assert gfs.is_local_path('file:///tmp/x')
  assert not gfs.is_local_path('s3://bucket/key')

  assert gfs.find_mount(str(tmp_path)) == fsu.localfs_mount(str(tmp_path))
  assert gfs.find_mount('s3://bucket/key') is None


def test_splitext():
  """splitext splits the URL path part into base and extension."""
  ps = gfs.splitext('http://h/x/y/file.tar.gz')

  assert ps.base == '/x/y/file.tar'
  assert ps.ext == 'gz'
  assert ps.purl.netloc == 'h'


def test_get_proto_and_is_path():
  """get_proto/is_path classify path strings and protocols."""
  assert gfs.get_proto('s3://bucket/key') == 's3'
  assert gfs.get_proto('/plain/path') == 'file'
  assert gfs.get_proto('FILE://host/x') == 'file'

  assert gfs.is_path('/abs/path')
  assert gfs.is_path('./rel')
  assert gfs.is_path('s3://bucket/key')
  assert not gfs.is_path('plainname')


def test_resolve_fs_local():
  """resolve_fs returns the file backend and the path unchanged."""
  fs, fpath = gfs.resolve_fs('/tmp/x/y')

  assert fs.ID == 'file'
  assert fpath == '/tmp/x/y'


def test_registry_and_get_proto_fs():
  """get_proto_fs instantiates registered backends by protocol."""
  fs = gfs.get_proto_fs('file')

  assert fs.ID == 'file'

  with pytest.raises(AssertionError):
    gfs.get_proto_fs('no-such-proto-xyz')


def test_cache_dir_roundtrip(tmp_path):
  """set_cache_dir/cache_dir round-trips the cache location."""
  old = gfs.cache_dir()

  gfs.set_cache_dir(str(tmp_path))
  try:
    assert gfs.cache_dir() == str(tmp_path)
  finally:
    gfs.set_cache_dir(old)
