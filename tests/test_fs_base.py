"""Tests for py_misc_utils.fs_base.

Exercises the FsBase contract through the concrete FileFs backend from
py_misc_utils.fs.file_fs: the mode-parsing class methods, seek_stream,
the stat-driven helpers (exists/isdir/isfile), data movement
(put_file/get_file/copyfile) and rename/replace semantics, plus the
DirEntry shape. Files unreachable through the public interface of a
concrete backend are out of scope.
"""

import os

import pytest

from py_misc_utils import fs_base as fsb
from py_misc_utils.fs import file_fs


@pytest.fixture
def ffs():
  return file_fs.FileFs()


@pytest.fixture
def paths(tmp_path):
  fpath = os.path.join(str(tmp_path), 'f.txt')
  with open(fpath, 'wb') as fd:
    fd.write(b'0123456789')

  dpath = os.path.join(str(tmp_path), 'd')
  os.makedirs(dpath)

  return fpath, dpath, os.path.join(str(tmp_path), 'missing')


def test_direntry_fields():
  entry = fsb.DirEntry(name='foo',
                       st_mode=0o644,
                       st_size=10,
                       st_ctime=1.0,
                       st_mtime=2.0,
                       path='/foo',
                       etag=None)
  assert entry.name == 'foo'
  assert entry.st_mode == 0o644
  assert entry.st_size == 10
  assert entry.etag is None


def test_direntry_etag_defaults_to_none():
  entry = fsb.DirEntry(name='foo',
                       st_mode=0o644,
                       st_size=10,
                       st_ctime=1.0,
                       st_mtime=2.0,
                       path='/foo')
  assert entry.etag is None


def test_fsbase_is_abstract():
  with pytest.raises(TypeError):
    fsb.FsBase()


def test_mode_parsing():
  fm = fsb.FsBase

  assert fm.read_mode('r')
  assert fm.read_mode('rb')
  assert not fm.read_mode('r+')
  assert not fm.read_mode('w')
  assert not fm.read_mode('')

  assert fm.write_mode('w')
  assert fm.write_mode('wb')
  assert fm.write_mode('a')
  assert fm.write_mode('r+')
  assert not fm.write_mode('r')
  assert not fm.write_mode('')

  assert fm.truncate_mode('w')
  assert fm.truncate_mode('wb')
  assert not fm.truncate_mode('w+')
  assert not fm.truncate_mode('a')
  assert not fm.truncate_mode('r')

  assert fm.append_mode('a')
  assert fm.append_mode('ab')
  assert not fm.append_mode('w')
  assert not fm.append_mode('r+')

  assert fm.text_mode('r')
  assert fm.text_mode('w+')
  assert not fm.text_mode('rb')
  assert not fm.text_mode('wb')


def test_seek_stream(ffs, paths):
  fpath, _, _ = paths

  with open(fpath, 'rb') as fd:
    ffs.seek_stream('r', fd)
    assert fd.tell() == 0

    ffs.seek_stream('a', fd)
    assert fd.tell() == 10

  with open(fpath, 'a') as fd:
    ffs.seek_stream('a', fd)
    assert fd.tell() == 10

    ffs.seek_stream('r', fd)
    assert fd.tell() == 0


def test_norm_url_is_identity(ffs):
  assert ffs.norm_url('/tmp/some/path') == '/tmp/some/path'


def test_exists_isdir_isfile(ffs, paths):
  fpath, dpath, missing = paths

  assert ffs.exists(fpath)
  assert ffs.exists(dpath)
  assert ffs.exists(missing) is False

  assert ffs.isfile(fpath)
  assert not ffs.isfile(dpath)
  assert not ffs.isfile(missing)

  assert ffs.isdir(dpath)
  assert not ffs.isdir(fpath)
  assert not ffs.isdir(missing)


def test_stat_entry_shape(ffs, paths):
  fpath, _, _ = paths

  de = ffs.stat(fpath)
  assert isinstance(de, fsb.DirEntry)
  assert de.name == 'f.txt'
  assert de.path == fpath
  assert de.st_size == 10
  assert de.st_mtime > 0
  assert de.etag is not None
  assert len(de.etag) == 40


def test_put_get_file(ffs, tmp_path):
  dst = os.path.join(str(tmp_path), 'dst.bin')

  ffs.put_file(dst, [b'hello ', b'world'])
  assert b''.join(ffs.get_file(dst)) == b'hello world'


def test_copyfile(ffs, tmp_path, paths):
  fpath, _, _ = paths
  dst = os.path.join(str(tmp_path), 'copy.txt')

  ffs.copyfile(fpath, ffs, dst)
  assert b''.join(ffs.get_file(dst)) == b'0123456789'


def test_rename(ffs, tmp_path, paths):
  fpath, _, _ = paths
  dst = os.path.join(str(tmp_path), 'renamed.txt')

  ffs.rename(fpath, dst)

  assert not os.path.exists(fpath)
  assert b''.join(ffs.get_file(dst)) == b'0123456789'


def test_replace_replaces_existing_target(ffs, tmp_path, paths):
  fpath, _, _ = paths
  dst = os.path.join(str(tmp_path), 'existing.txt')
  with open(dst, 'wb') as fd:
    fd.write(b'old')

  ffs.replace(fpath, dst)

  assert not os.path.exists(fpath)
  assert b''.join(ffs.get_file(dst)) == b'0123456789'
