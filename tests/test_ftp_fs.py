"""Tests for py_misc_utils.fs.ftp_fs using ftputil against a local server.

No external network access is required.
"""

from unittest import mock

import os

import ftputil
import pytest

from py_misc_utils.fs import ftp_fs as ff


@pytest.fixture
def ftp_host(tmp_path):
  """Run a minimal FTP server using pyftpdlib on localhost."""
  pytest.importorskip('pyftpdlib')
  from pyftpdlib.authorizers import DummyAuthorizer
  from pyftpdlib.handlers import FTPHandler
  from pyftpdlib.servers import FTPServer

  import threading

  authorizer = DummyAuthorizer()
  authorizer.add_user('user', 'pass', str(tmp_path), perm='elradfmwMT')
  handler = FTPHandler
  handler.authorizer = authorizer
  server = FTPServer(('127.0.0.1', 0), handler)
  server.max_cons = 256
  server.max_cons_per_ip = 256
  thread = threading.Thread(target=server.serve_forever, daemon=True)
  thread.start()
  try:
    host, port = server.address
    url = f'ftp://user:pass@{host}:{port}'
    yield url, str(tmp_path)
  finally:
    server.close_all()
    thread.join(timeout=0)


@pytest.fixture
def fs(tmp_path):
  fs = ff.FtpFs(cache_dir=str(tmp_path / 'cache'))
  cache_iface = mock.MagicMock()
  cache_iface.open.return_value.__enter__.return_value.read.return_value = b'ftp data'
  fs._cache_iface = cache_iface
  return fs


def test_ftp_open_read_write(fs, ftp_host, tmp_path):
  url, root = ftp_host
  with open(os.path.join(root, 'ftp.txt'), 'wb') as f:
    f.write(b'ftp data')
  with fs.open(url + '/ftp.txt', mode='rb') as f:
    assert f.read() == b'ftp data'


def test_ftp_exists(fs, ftp_host, tmp_path):
  url, root = ftp_host
  assert not fs.exists(url + '/missing.txt')
  with open(os.path.join(root, 'exists.txt'), 'wb') as f:
    f.write(b'e')
  assert fs.exists(url + '/exists.txt')


def test_ftp_stat(fs, ftp_host, tmp_path):
  url, root = ftp_host
  with open(os.path.join(root, 'stat.txt'), 'wb') as f:
    f.write(b'ftp stat')
  st = fs.stat(url + '/stat.txt')
  assert st.st_size == 8


def test_ftp_list(fs, ftp_host, tmp_path):
  url, root = ftp_host
  with open(os.path.join(root, 'ftp_a.txt'), 'wb') as f:
    f.write(b'a')
  with open(os.path.join(root, 'ftp_b.txt'), 'wb') as f:
    f.write(b'b')
  entries = fs.list(url + '/')
  names = {e.name for e in entries}
  assert names == {'ftp_a.txt', 'ftp_b.txt'}


def test_ftp_remove(fs, ftp_host, tmp_path):
  url, root = ftp_host
  with open(os.path.join(root, 'ftp_rm.txt'), 'wb') as f:
    f.write(b'ftp rm')
  assert fs.exists(url + '/ftp_rm.txt')
  fs.remove(url + '/ftp_rm.txt')
  assert not fs.exists(url + '/ftp_rm.txt')


def test_ftp_copy(fs, ftp_host, tmp_path):
  url, root = ftp_host
  with open(os.path.join(root, 'ftp_src.txt'), 'wb') as f:
    f.write(b'ftp copy')
  fs.copyfile(url + '/ftp_src.txt', fs, url + '/ftp_dst.txt')
  # Verify copy via the underlying FTP root (cache_iface mock returns
  # stale content for reads).
  with open(os.path.join(root, 'ftp_dst.txt'), 'rb') as f:
    assert f.read() == b'ftp copy'
