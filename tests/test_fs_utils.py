"""Tests for py_misc_utils.fs_utils.

Filesystem helpers: path manipulation (path_split, drop_ext, normpath,
subst_path), file checks and copies (link_or_copy, is_newer_file),
atomic operations (atomic_write, safe_rmtree), directory traversal
(du with symlink/visited handling), stream chunking (enum_chunks),
file search (find_path), unique temp paths and the cwd context manager.
"""

import io
import os

import pytest

from py_misc_utils import fs_utils as fsu
from py_misc_utils import osfd


def test_maybe_remove(tmp_path):
  """maybe_remove deletes an existing file and ignores missing ones."""
  path = tmp_path / 'gone.txt'
  path.write_bytes(b'x')

  fsu.maybe_remove(str(path))
  assert not path.exists()

  fsu.maybe_remove(str(path))
  fsu.maybe_remove(str(tmp_path / 'never-existed'))


def test_is_binary():
  """is_binary detects the 'b' mode flag and non-TextIO objects."""
  assert fsu.is_binary(io.StringIO()) is False
  assert fsu.is_binary(io.BytesIO()) is True

  with open(os.devnull, 'r') as fd:
    assert fsu.is_binary(fd) is False
  with open(os.devnull, 'rb') as fd:
    assert fsu.is_binary(fd) is True


def test_link_or_copy(tmp_path):
  """link_or_copy links (hard or symlink) and reads back the content."""
  src = tmp_path / 'src.txt'
  src.write_bytes(b'linked-data')
  dst = tmp_path / 'dst.txt'

  assert fsu.link_or_copy(str(src), str(dst)) == str(dst)
  assert dst.read_bytes() == b'linked-data'


def test_is_newer_file(tmp_path):
  """is_newer_file compares modification times."""
  old = tmp_path / 'old.txt'
  new = tmp_path / 'new.txt'
  old.write_bytes(b'o')
  new.write_bytes(b'n')

  os.utime(str(old), (1000, 1000))
  os.utime(str(new), (2000, 2000))

  assert fsu.is_newer_file(str(new), str(old))
  assert not fsu.is_newer_file(str(old), str(new))


def test_readall_path_and_fd(tmp_path):
  """readall works on both paths and open file descriptors."""
  path = tmp_path / 'data.bin'
  path.write_bytes(b'all-content')

  assert fsu.readall(str(path)) == b'all-content'

  with osfd.OsFd(str(path), os.O_RDONLY) as fd:
    assert fsu.readall(fd) == b'all-content'


def test_safe_rmtree(tmp_path):
  """safe_rmtree removes a tree; missing paths respect ignore_errors."""
  tree = tmp_path / 'tree'
  (tree / 'sub').mkdir(parents=True)
  (tree / 'sub' / 'f.txt').write_bytes(b'x')

  fsu.safe_rmtree(str(tree))
  assert not tree.exists()

  with pytest.raises(FileNotFoundError):
    fsu.safe_rmtree(str(tree))

  fsu.safe_rmtree(str(tree), ignore_errors=True)


def test_stat_none_on_error(tmp_path):
  """stat() returns None instead of raising for missing paths."""
  assert fsu.stat(str(tmp_path / 'nope')) is None
  assert fsu.stat(str(tmp_path)) is not None


def test_path_split():
  """path_split returns the path components, root first."""
  assert fsu.path_split('/a/b/c.txt') == ['/', 'a', 'b', 'c.txt']
  assert fsu.path_split('relative/x') == ['relative', 'x']
  assert fsu.path_split('file.txt') == ['file.txt']


def test_drop_ext():
  """drop_ext removes matched extensions only."""
  assert fsu.drop_ext('a/b/c.txt', '.txt') == 'a/b/c'
  assert fsu.drop_ext('a/b/c.txt', ['.txt', '.md']) == 'a/b/c'
  assert fsu.drop_ext('a/b/c.dat', '.txt') == 'a/b/c.dat'
  assert fsu.drop_ext('noext', '.txt') == 'noext'


def test_normpath(tmp_path, monkeypatch):
  """normpath expands user/vars and makes the path absolute."""
  monkeypatch.setenv('BATCHB', 'subdir')
  assert fsu.normpath('$BATCHB/x') == os.path.join(str(tmp_path), 'subdir', 'x') \
      if os.getcwd() == str(tmp_path) else fsu.normpath('$BATCHB/x').endswith('subdir/x')


def test_subst_path(tmp_path):
  """subst_path substitutes $B/$D/$R/$N/$X from the reference path."""
  ref = tmp_path / 'sub' / 'name.txt'
  ref.parent.mkdir()
  ref.write_bytes(b'x')

  assert fsu.subst_path('$B', str(ref)) == 'name.txt'
  assert fsu.subst_path('$D', str(ref)) == str(ref.parent.resolve())
  assert fsu.subst_path('$R', str(ref)) == str(ref.resolve())
  assert fsu.subst_path('$N', str(ref)) == 'name'
  assert fsu.subst_path('$N.dat', str(ref)) == 'name.dat'
  assert fsu.subst_path('$D/$N.dat',
                        str(ref)) == os.path.join(str(ref.parent.resolve()), 'name.dat')
  assert fsu.subst_path('out$X', str(ref)) == 'out.txt'


def test_localfs_mount():
  """localfs_mount walks up to the closest mount point."""
  assert fsu.localfs_mount('/') == '/'
  assert fsu.localfs_mount('/nonexistent/deep/path') == '/'


def test_enum_chunks():
  """enum_chunks yields bounded chunks, with a shorter tail."""
  stream = io.BytesIO(b'x' * 250)

  chunks = list(fsu.enum_chunks(stream, chunk_size=100))

  assert [len(c) for c in chunks] == [100, 100, 50]
  assert b''.join(chunks) == b'x' * 250


def test_enum_chunks_exact_boundary():
  """A stream sized exactly n*chunk_size yields no empty extra chunk."""
  stream = io.BytesIO(b'y' * 200)

  chunks = list(fsu.enum_chunks(stream, chunk_size=100))

  assert [len(c) for c in chunks] == [100, 100]


def test_du(tmp_path):
  """du sums file sizes recursively; symlinks are skipped by default."""
  root = tmp_path / 'tree'
  (root / 'a').mkdir(parents=True)
  (root / 'b.txt').write_bytes(b'x' * 100)
  (root / 'a' / 'c.txt').write_bytes(b'y' * 50)

  assert fsu.du(str(root)) == 150
  assert fsu.du(str(root), follow_symlinks=True) == 150


def test_du_symlinks_and_visited(tmp_path):
  """du follows symlinks when asked, and visited shares the accounting."""
  root = tmp_path / 'tree'
  target = tmp_path / 'target'
  target.mkdir()
  (target / 'f.bin').write_bytes(b'z' * 40)
  root.mkdir()
  (root / 'link').symlink_to(target)

  assert fsu.du(str(root)) == 0
  assert fsu.du(str(root), follow_symlinks=True) == 40

  visited = set()
  s1 = fsu.du(str(root), follow_symlinks=True, visited=visited)
  s2 = fsu.du(str(root), follow_symlinks=True, visited=visited)
  assert s1 == 40
  assert s2 == 0


def test_find_path(tmp_path):
  """find_path returns the first folder holding the name."""
  (tmp_path / 'one').mkdir()
  (tmp_path / 'two').mkdir()
  (tmp_path / 'two' / 'tool').write_bytes(b'x')

  paths = [str(tmp_path / 'one'), str(tmp_path / 'two')]

  assert fsu.find_path('tool', paths) == str(tmp_path / 'two' / 'tool')
  assert fsu.find_path('missing', paths) is None


def test_temp_path_with_nspath():
  """temp_path keeps the base name and extension, adding a random suffix."""
  upath = fsu.temp_path(nspath='/tmp/dir/archive.tar.gz')

  base = os.path.basename(upath)
  name, ext = os.path.splitext(base)
  inner, marker = os.path.splitext(name)

  assert ext == '.gz'
  assert marker.startswith('.')
  assert 1 <= len(marker[1:]) <= 10
  assert inner == 'archive.tar'


def test_temp_path_in_nsdir():
  """temp_path builds unique '.tmp' names inside the namespace dir."""
  upath = fsu.temp_path(nsdir='/nonexistent-unused-dir')

  assert os.path.basename(upath).endswith('.tmp')
  assert upath.startswith('/nonexistent-unused-dir/')


def test_atomic_write(tmp_path):
  """atomic_write renames the temp file over the destination on success."""
  path = tmp_path / 'out.txt'

  with fsu.atomic_write(str(path)) as fd:
    fd.write(b'atomic')

  assert path.read_bytes() == b'atomic'


def test_atomic_write_failure_keeps_old(tmp_path):
  """A failed write leaves the destination untouched."""
  path = tmp_path / 'out.txt'
  path.write_bytes(b'old')

  with pytest.raises(RuntimeError), fsu.atomic_write(str(path)) as fd:
    fd.write(b'new')
    raise RuntimeError('boom')

  assert path.read_bytes() == b'old'


def test_atomic_write_create_parents(tmp_path):
  """create_parents builds the missing destination directory."""
  path = tmp_path / 'deep' / 'nested' / 'out.txt'

  with fsu.atomic_write(str(path), create_parents=True) as fd:
    fd.write(b'nested')

  assert path.read_bytes() == b'nested'


def test_cwd_context(tmp_path):
  """cwd() switches the working directory and restores it."""
  original = os.getcwd()

  with fsu.cwd(str(tmp_path)) as new_cwd:
    assert new_cwd == str(tmp_path)
    assert os.getcwd() == str(tmp_path)

  assert os.getcwd() == original


def test_cwd_restores_on_error(tmp_path):
  """cwd() restores the original directory even when the body raises."""
  original = os.getcwd()

  with pytest.raises(RuntimeError), fsu.cwd(str(tmp_path)):
    raise RuntimeError('boom')

  assert os.getcwd() == original
