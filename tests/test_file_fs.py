"""Tests for py_misc_utils.fs.file_fs.

Exercises FileFs operations on plain local paths.
"""

import os

import pytest

from py_misc_utils import fs_utils as fsu
from py_misc_utils.fs import file_fs as ff


@pytest.fixture
def fs():
  return ff.FileFs()


def test_open_read_write(fs, tmp_path):
  path = str(tmp_path / 'ff.txt')
  with fs.open(path, mode='wb') as f:
    f.write(b'file fs')
  with fs.open(path, mode='rb') as f:
    assert f.read() == b'file fs'


def test_exists(fs, tmp_path):
  path = str(tmp_path / 'ff_exists.txt')
  assert not fs.exists(path)
  with open(path, 'wb') as f:
    f.write(b'x')
  assert fs.exists(path)


def test_stat(fs, tmp_path):
  path = str(tmp_path / 'ff_stat.txt')
  with open(path, 'wb') as f:
    f.write(b'stat data')
  st = fs.stat(path)
  assert st.st_size == 9


def test_list(fs, tmp_path):
  (tmp_path / 'x.txt').write_bytes(b'x')
  (tmp_path / 'y.txt').write_bytes(b'y')
  entries = fs.list(str(tmp_path))
  names = {e.name for e in entries}
  assert names == {'x.txt', 'y.txt'}


def test_remove(fs, tmp_path):
  path = str(tmp_path / 'ff_remove.txt')
  fsu.write_content(path, b'remove me')
  fs.remove(path)
  assert not fs.exists(path)


def test_copy(fs, tmp_path):
  src = str(tmp_path / 'ff_src.txt')
  dst = str(tmp_path / 'ff_dst.txt')
  fsu.write_content(src, b'copy me')
  fs.copyfile(src, fs, dst)
  with open(dst, 'rb') as f:
    assert f.read() == b'copy me'


def test_makedirs_rmtree(fs, tmp_path):
  d = str(tmp_path / 'sub' / 'deep')
  fs.makedirs(d)
  assert os.path.isdir(d)
  fs.rmtree(str(tmp_path / 'sub'))
  assert not os.path.exists(d)


def test_link_and_symlink(fs, tmp_path):
  src = str(tmp_path / 'link_src.txt')
  dst = str(tmp_path / 'link_dst.txt')
  with open(src, 'wb') as f:
    f.write(b'link me')
  fs.link(src, dst)
  with open(dst, 'rb') as f:
    assert f.read() == b'link me'
  sl = str(tmp_path / 'link_sl.txt')
  fs.symlink(src, sl)
  assert os.path.islink(sl)


def test_as_local(fs, tmp_path):
  path = str(tmp_path / 'as_local.txt')
  assert fs.as_local(path) == path
