"""Tests for py_misc_utils.fs.ftp_fs additions.

Extends the base FTP tests with: rename, makedirs/rmdir/rmtree tree
semantics, non-recursive rmtree, missing-file stat/remove errors, and
the set_time_shift behavior of the connection handler.
"""

import os
from unittest import mock

import ftputil
import pytest

from py_misc_utils.fs import ftp_fs as ff


@pytest.fixture
def fs(tmp_path):
  """FtpFs with a mocked cache interface."""
  ffs = ff.FtpFs(cache_dir=str(tmp_path / 'cache'))
  ffs._cache_iface = mock.MagicMock()
  return ffs


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
    thread.join(timeout=5)


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
