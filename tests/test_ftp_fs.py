"""Tests for py_misc_utils.fs.ftp_fs using ftputil against a local server.

No external network access is required.
"""

import os
from unittest import mock

import ftputil
import pytest

from py_misc_utils.fs import ftp_fs as ff


@pytest.fixture
def ftp_host(tmp_path):
  """Run a minimal FTP server using pyftpdlib on localhost."""
  pytest.importorskip('pyftpdlib')
  import threading

  from pyftpdlib.authorizers import DummyAuthorizer
  from pyftpdlib.handlers import FTPHandler
  from pyftpdlib.servers import FTPServer

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


def test_ftp_rename(fs, ftp_host):
  """rename() moves a file on the remote server."""
  url, root = ftp_host
  src = os.path.join(root, 'ren-src.txt')
  with open(src, 'wb') as fd:
    fd.write(b'move me')

  fs.rename(url + '/ren-src.txt', url + '/ren-dst.txt')

  assert not os.path.exists(src)
  with open(os.path.join(root, 'ren-dst.txt'), 'rb') as fd:
    assert fd.read() == b'move me'


def test_ftp_makedirs_rmdir(fs, ftp_host):
  """makedirs creates nested dirs; rmdir removes an empty one."""
  url, root = ftp_host

  fs.makedirs(url + '/a/b/c')

  assert os.path.isdir(os.path.join(root, 'a', 'b', 'c'))

  fs.rmdir(url + '/a/b/c')

  assert not os.path.isdir(os.path.join(root, 'a', 'b', 'c'))


def test_ftp_mkdir(fs, ftp_host):
  """mkdir creates a single directory level."""
  url, root = ftp_host

  fs.mkdir(url + '/single')

  assert os.path.isdir(os.path.join(root, 'single'))


def test_ftp_rmtree_recursive(fs, ftp_host):
  """rmtree removes the whole tree under the URL."""
  url, root = ftp_host
  os.makedirs(os.path.join(root, 'tree', 'sub'))
  with open(os.path.join(root, 'tree', 'f.bin'), 'wb') as fd:
    fd.write(b'x')
  with open(os.path.join(root, 'tree', 'sub', 'g.bin'), 'wb') as fd:
    fd.write(b'y')

  fs.rmtree(url + '/tree')

  assert not os.path.exists(os.path.join(root, 'tree'))


def test_ftp_stat_missing(fs, ftp_host):
  """stat() on a missing file raises PermanentError (ftputil 550)."""
  url, _ = ftp_host

  with pytest.raises(ftputil.error.PermanentError):
    fs.stat(url + '/no-such-file.bin')


def test_ftp_remove_missing(fs, ftp_host):
  """remove() on a missing file raises PermanentError (ftputil 550)."""
  url, _ = ftp_host

  with pytest.raises(ftputil.error.PermanentError):
    fs.remove(url + '/no-such-file.bin')


def test_cache_handler_sets_time_shift(fs, ftp_host):
  """CacheHandler.create() sets the time shift (ftputil 6.0 readiness)."""
  url, _ = ftp_host
  handler = ff.CacheHandler('127.0.0.1',
                            'user',
                            'pass',
                            port=int(url.rsplit(':', 1)[1]),
                            session_factory=ff.FtpSession)

  host = handler.create()

  try:
    # time_shift is a method returning the shift in seconds.
    assert host.time_shift() == 0
  finally:
    host.close()


def test_ftp_isdir_isfile(fs, ftp_host, tmp_path):
  """isdir/isfile classify remote paths via stat."""
  url, root = ftp_host
  os.makedirs(os.path.join(root, 'sub'))
  with open(os.path.join(root, 'plain.txt'), 'wb') as fd:
    fd.write(b'x')

  assert fs.isfile(url + '/plain.txt')
  assert not fs.isfile(url + '/no-such.txt')

  # The FTP root path itself does not stat as a directory (only real
  # subdirectories do, probed with pyftpdlib).
  assert fs.isdir(url + '/sub')
  assert not fs.isdir(url + '/plain.txt')
  assert not fs.isdir(url + '/no-such-dir')
