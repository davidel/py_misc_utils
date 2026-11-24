"""Tests for py_misc_utils.gfs.

Exercises the generic filesystem dispatch over local file:// and
http:// URLs using the ephemeral HTTP server fixture.
"""

import os

import pytest

from py_misc_utils import gfs


def test_open_read_write_local(tmp_path):
  path = str(tmp_path / 'gfs_test.txt')
  with gfs.open(path, mode='wb') as f:
    f.write(b'hello gfs')
  assert os.path.exists(path)
  with gfs.open(path, mode='rb') as f:
    assert f.read() == b'hello gfs'


def test_exists_local(tmp_path):
  path = str(tmp_path / 'exists.txt')
  assert not gfs.exists(path)
  with open(path, 'wb') as f:
    f.write(b'x')
  assert gfs.exists(path)


def test_stat_local(tmp_path):
  path = str(tmp_path / 'stat.txt')
  with open(path, 'wb') as f:
    f.write(b'stat data')
  st = gfs.stat(path)
  assert st.st_size == 9


def test_list_local(tmp_path):
  (tmp_path / 'a.txt').write_bytes(b'a')
  (tmp_path / 'b.txt').write_bytes(b'b')
  entries = list(gfs.enumerate_files(str(tmp_path)))
  assert set(entries) == {'a.txt', 'b.txt'}


def test_remove_local(tmp_path):
  path = str(tmp_path / 'remove.txt')
  with open(path, 'wb') as f:
    f.write(b'remove me')
  assert gfs.exists(path)
  gfs.remove(path)
  assert not gfs.exists(path)


def test_copy_local(tmp_path):
  src = str(tmp_path / 'src.txt')
  dst = str(tmp_path / 'dst.txt')
  with open(src, 'wb') as f:
    f.write(b'copy me')
  gfs.copy(src, dst)
  with open(dst, 'rb') as f:
    assert f.read() == b'copy me'


def test_open_http(http_server, http_routes, tmp_path):
  http_routes.add_route('/remote.txt', body=b'via http')
  with gfs.open(http_server.url + '/remote.txt', mode='rb') as f:
    assert f.read() == b'via http'


def test_exists_http(http_server, http_routes):
  http_routes.add_route('/exists.txt', body=b'e')
  assert gfs.exists(http_server.url + '/exists.txt')
  assert not gfs.exists(http_server.url + '/nope.txt')


def test_temp_file(tmp_path):
  tf = gfs.TempFile(nsdir=str(tmp_path), prefix='tf_', suffix='.tmp',
                    mode='wb')
  tf.open()
  path = tf.path
  with open(path, 'wb') as f:
    f.write(b'temp content')
  assert os.path.exists(path)
  tf.close()
  assert not os.path.exists(path)
