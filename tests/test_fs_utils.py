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
  path = os.path.join(tmp_path, 'gone.txt')
  fsu.write_content(path, b'x')

  fsu.maybe_remove(path)
  assert not os.path.exists(path)

  fsu.maybe_remove(path)
  fsu.maybe_remove(os.path.join(tmp_path, 'never-existed'))


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
  src = os.path.join(tmp_path, 'src.txt')
  fsu.write_content(src, b'linked-data')
  dst = os.path.join(tmp_path, 'dst.txt')

  assert fsu.link_or_copy(src, dst) == dst
  with open(dst, 'rb') as f:
    assert f.read() == b'linked-data'


def test_is_newer_file(tmp_path):
  """is_newer_file compares modification times."""
  old = os.path.join(tmp_path, 'old.txt')
  new = os.path.join(tmp_path, 'new.txt')
  with open(old, 'wb') as f:
    f.write(b'o')
  with open(new, 'wb') as f:
    f.write(b'n')

  os.utime(old, (1000, 1000))
  os.utime(new, (2000, 2000))

  assert fsu.is_newer_file(new, old)
  assert not fsu.is_newer_file(old, new)


def test_readall_path_and_fd(tmp_path):
  """readall works on both paths and open file descriptors."""
  path = os.path.join(tmp_path, 'data.bin')
  with open(path, 'wb') as f:
    f.write(b'all-content')

  assert fsu.readall(path) == b'all-content'

  with osfd.OsFd(path, os.O_RDONLY) as fd:
    assert fsu.readall(fd) == b'all-content'


def test_read_content(tmp_path):
  """read_content returns bytes by default, or str when binary=False."""
  path = os.path.join(tmp_path, 'data.bin')
  fsu.write_content(path, b'hello-bytes')

  assert fsu.read_content(path) == b'hello-bytes'
  assert fsu.read_content(path, binary=True) == b'hello-bytes'

  text_path = os.path.join(tmp_path, 'data.txt')
  fsu.write_content(text_path, 'hello-text', binary=False)
  assert fsu.read_content(text_path, binary=False) == 'hello-text'


def test_write_content_binary_and_text(tmp_path):
  """write_content accepts str/bytes and normalizes based on binary flag."""
  path = os.path.join(tmp_path, 'mixed.bin')
  fsu.write_content(path, 'str-as-bytes', binary=True)
  assert fsu.read_content(path) == b'str-as-bytes'

  text_path = os.path.join(tmp_path, 'mixed.txt')
  fsu.write_content(text_path, b'bytes-as-str', binary=False)
  assert fsu.read_content(text_path, binary=False) == 'bytes-as-str'


def test_write_content_roundtrip(tmp_path):
  """Written content reads back identically in both modes."""
  for binary in (True, False):
    path = os.path.join(tmp_path, f'rt{int(binary)}')
    payload = b'\x00\x01\x02payload' if binary else 'unicode: ☃'
    fsu.write_content(path, payload, binary=binary)
    assert fsu.read_content(path, binary=binary) == payload


def test_safe_rmtree(tmp_path):
  """safe_rmtree removes a tree; missing paths respect ignore_errors."""
  tree = os.path.join(tmp_path, 'tree')
  os.makedirs(os.path.join(tree, 'sub'))
  with open(os.path.join(tree, 'sub', 'f.txt'), 'wb') as f:
    f.write(b'x')

  fsu.safe_rmtree(tree)
  assert not os.path.exists(tree)

  with pytest.raises(FileNotFoundError):
    fsu.safe_rmtree(tree)

  fsu.safe_rmtree(tree, ignore_errors=True)


def test_stat_none_on_error(temp_path):
  """stat() returns None for missing paths."""
  assert fsu.stat(os.path.join(temp_path, 'nope')) is None
  assert fsu.stat(temp_path) is not None


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


def test_normpath(temp_path, monkeypatch):
  """normpath expands user/vars and makes the path absolute."""
  monkeypatch.setenv('BATCHB', 'subdir')
  npath = fsu.normpath('$BATCHB/x')

  assert os.path.isabs(npath)
  assert npath.endswith(os.path.join('subdir', 'x'))
  if os.getcwd() == temp_path:
    assert npath == os.path.join(temp_path, 'subdir', 'x')


def test_subst_path(tmp_path):
  """subst_path substitutes $B/$D/$R/$N/$X from the reference path."""
  ref = os.path.join(tmp_path, 'sub', 'name.txt')
  os.makedirs(os.path.dirname(ref))
  with open(ref, 'wb') as f:
    f.write(b'x')

  assert fsu.subst_path('$B', ref) == 'name.txt'
  assert fsu.subst_path('$D', ref) == os.path.dirname(os.path.abspath(ref))
  assert fsu.subst_path('$R', ref) == os.path.abspath(ref)
  assert fsu.subst_path('$N', ref) == 'name'
  assert fsu.subst_path('$N.dat', ref) == 'name.dat'
  assert fsu.subst_path('$D/$N.dat',
                        ref) == os.path.join(os.path.dirname(os.path.abspath(ref)),
                                             'name.dat')
  assert fsu.subst_path('out$X', ref) == 'out.txt'


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
  root = os.path.join(tmp_path, 'tree')
  os.makedirs(os.path.join(root, 'a'))
  with open(os.path.join(root, 'b.txt'), 'wb') as f:
    f.write(b'x' * 100)
  with open(os.path.join(root, 'a', 'c.txt'), 'wb') as f:
    f.write(b'y' * 50)

  assert fsu.du(root) == 150
  assert fsu.du(root, follow_symlinks=True) == 150


def test_du_symlinks_and_visited(tmp_path):
  """du follows symlinks when asked, and visited shares the accounting."""
  root = os.path.join(tmp_path, 'tree')
  target = os.path.join(tmp_path, 'target')
  os.makedirs(target)
  with open(os.path.join(target, 'f.bin'), 'wb') as f:
    f.write(b'z' * 40)
  os.makedirs(root)
  os.symlink(target, os.path.join(root, 'link'))

  assert fsu.du(root) == 0
  assert fsu.du(root, follow_symlinks=True) == 40

  visited = set()
  s1 = fsu.du(root, follow_symlinks=True, visited=visited)
  s2 = fsu.du(root, follow_symlinks=True, visited=visited)
  assert s1 == 40
  assert s2 == 0


def test_find_path(tmp_path):
  """find_path returns the first folder holding the name."""
  os.makedirs(os.path.join(tmp_path, 'one'))
  os.makedirs(os.path.join(tmp_path, 'two'))
  with open(os.path.join(tmp_path, 'two', 'tool'), 'wb') as f:
    f.write(b'x')

  paths = [os.path.join(tmp_path, 'one'), os.path.join(tmp_path, 'two')]

  assert fsu.find_path('tool', paths) == os.path.join(tmp_path, 'two', 'tool')
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


def test_atomic_write_success(tmp_path):
  """atomic_write renames the temp file over the destination on success."""
  path = os.path.join(tmp_path, 'out.txt')

  with fsu.atomic_write(path) as fd:
    fd.write(b'atomic')

  with open(path, 'rb') as f:
    assert f.read() == b'atomic'


def test_atomic_write_failure_keeps_old(tmp_path):
  """A failed write leaves the destination untouched."""
  path = os.path.join(tmp_path, 'out.txt')
  with open(path, 'wb') as f:
    f.write(b'old')

  with pytest.raises(RuntimeError), fsu.atomic_write(path) as fd:
    fd.write(b'new')
    raise RuntimeError('boom')

  with open(path, 'rb') as f:
    assert f.read() == b'old'


def test_atomic_write_create_parents(tmp_path):
  """create_parents builds the missing destination directory."""
  path = os.path.join(tmp_path, 'deep', 'nested', 'out.txt')

  with fsu.atomic_write(path, create_parents=True) as fd:
    fd.write(b'nested')

  with open(path, 'rb') as f:
    assert f.read() == b'nested'


def test_cwd_context(temp_path):
  """cwd() switches the working directory and restores it."""
  original = os.getcwd()

  with fsu.cwd(temp_path) as new_cwd:
    assert new_cwd == temp_path
    assert os.getcwd() == temp_path

  assert os.getcwd() == original


def test_cwd_restores_on_error(temp_path):
  """cwd() restores the original directory even when the body raises."""
  original = os.getcwd()

  with pytest.raises(RuntimeError), fsu.cwd(temp_path):
    raise RuntimeError('boom')

  assert os.getcwd() == original
