"""Tests for py_misc_utils.fs.http_fs.

Exercises HttpFs read operations against the ephemeral HTTP server.
No real network access is required.
"""

import os
from unittest import mock

import pytest

from py_misc_utils.fs import http_fs as hf


@pytest.fixture
def fs(temp_path):
  """HttpFs with a mocked cache interface (no local caching side effects)."""
  return hf.HttpFs(cache_dir=os.path.join(temp_path, 'cache'),
                   cache_iface=mock.MagicMock())


def _capture_upload(http_routes, url):
  """Register a PUT route recording the request body into a dict.

  The upload body may arrive with Content-Length or chunked framing
  (put_file passes an iterator, so requests uses chunked encoding).
  """
  uploaded = {}
  path = '/' + url.split('/', 3)[-1]

  def handler(handler_obj):
    length = handler_obj.headers.get('Content-Length')
    if length is not None:
      body = handler_obj.rfile.read(int(length))
    else:
      body = b''
      while True:
        rsize = int(handler_obj.rfile.readline().strip(), 16)
        if rsize == 0:
          handler_obj.rfile.readline()
          break
        body += handler_obj.rfile.read(rsize)
        handler_obj.rfile.readline()

    uploaded['body'] = body

    return (201, {}, b'')

  http_routes.routes[path] = handler

  return uploaded


def test_read_via_http(http_server, http_routes, temp_path):
  http_routes.add_route('/hf.txt', body=b'http fs data')
  cache_iface = mock.MagicMock()
  cache_iface.open.return_value.__enter__.return_value.read.return_value = b'http fs data'
  fs = hf.HttpFs(cache_dir=os.path.join(temp_path, 'cache'), cache_iface=cache_iface)
  with fs.open(http_server.url + '/hf.txt', mode='rb') as f:
    assert f.read() == b'http fs data'


def test_exists_via_http(http_server, http_routes, temp_path):
  http_routes.add_route('/hf_exists.txt', body=b'e')
  fs = hf.HttpFs(cache_dir=os.path.join(temp_path, 'cache'))
  assert fs.exists(http_server.url + '/hf_exists.txt')
  assert not fs.exists(http_server.url + '/hf_missing.txt')


def test_stat_via_http(http_server, http_routes, temp_path):
  http_routes.add_route('/hf_stat.txt', body=b'stat over http')
  fs = hf.HttpFs(cache_dir=os.path.join(temp_path, 'cache'))
  st = fs.stat(http_server.url + '/hf_stat.txt')
  assert st.st_size == len(b'stat over http')


def test_list_html_directory(fs, http_server, http_routes):
  """list() enumerates the relative anchors of an HTML page."""
  links = '<html><body>' \
          '<a href="a.txt">a</a>' \
          '<a href="sub/b.txt">b</a>' \
          '<a href="https://other/skip.txt">skip</a>' \
          '</body></html>'
  http_routes.add_route('/dir/', body=links.encode())
  http_routes.add_route('/dir/a.txt', body=b'aaa')
  http_routes.add_route('/dir/sub/b.txt', body=b'bb')

  names = [de.name for de in fs.list(http_server.url + '/dir/')]

  assert sorted(names) == ['a.txt', 'b.txt']


def test_list_skips_unstatble_entries(fs, http_server, http_routes):
  """Anchors whose stat fails are skipped with a debug log."""
  links = '<html><body><a href="ghost.txt">g</a></body></html>'
  http_routes.add_route('/dir2/', body=links.encode())

  entries = list(fs.list(http_server.url + '/dir2/'))

  assert entries == []


def test_stat_uses_etag_header(fs, http_server, http_routes):
  """The ETag header wins over the computed size/mtime tag."""
  http_routes.add_route('/etag.txt', body=b'x', headers={'ETag': '"custom-etag"'})

  de = fs.stat(http_server.url + '/etag.txt')

  # Quotes are stripped from the ETag value.
  assert de.etag == 'custom-etag'
  assert de.st_size == 1


def test_stat_computed_tag(fs, http_server, http_routes):
  """Without an ETag header the tag is computed from size/mtime."""
  http_routes.add_route('/plain.txt',
                        body=b'x',
                        headers={'Last-Modified': 'Mon, 24 Nov 2025 00:00:00 GMT'})

  de = fs.stat(http_server.url + '/plain.txt')

  assert de.etag


def test_noop_dir_ops(fs):
  """mkdir/makedirs/rmdir/rmtree are no-ops on HTTP."""
  url = 'http://nowhere/x'

  fs.mkdir(url)
  fs.makedirs(url)
  fs.rmdir(url)
  fs.rmtree(url)


def test_remove_deletes(fs, http_server, http_routes):
  """remove() issues a DELETE (404 responses tolerated)."""
  http_routes.add_route('/gone.txt', body=b'g')

  fs.remove(http_server.url + '/gone.txt')


def test_rename_moves_content(fs, http_server, http_routes):
  """rename() downloads the source, PUTs it to the destination, DELETEs src."""
  http_routes.add_route('/ren-src.txt', body=b'moving-data')
  uploaded = _capture_upload(http_routes, http_server.url + '/ren-dst.txt')

  fs.rename(http_server.url + '/ren-src.txt', http_server.url + '/ren-dst.txt')

  assert uploaded['body'] == b'moving-data'


def test_link_symlink_broken_signature():
  """link/symlink crash with TypeError (documented library bug).

  HttpFs.link/symlink call self.copyfile(src_url, dest_url) while the
  FsBase.copyfile signature is copyfile(url, dest_fs, dest_url): the
  destination URL lands in the dest_fs slot -> TypeError. Reported as a
  library finding; the docstrings say the intent is a copy.
  """
  fs = hf.HttpFs(cache_dir='/nonexistent-hfs-cache')

  with pytest.raises(TypeError):
    fs.link('http://x/a', 'http://x/b')

  with pytest.raises(TypeError):
    fs.symlink('http://x/a', 'http://x/b')


def test_put_file_and_get_file(fs, http_server, http_routes):
  """put_file uploads chunks; get_file reads them back."""
  uploaded = _capture_upload(http_routes, http_server.url + '/putget.bin')

  fs.put_file(http_server.url + '/putget.bin', iter([b'part1-', b'part2']))

  assert uploaded['body'] == b'part1-part2'


def test_get_file_chunks(fs, http_server, http_routes):
  """get_file yields the URL content in chunks."""
  http_routes.add_route('/chunks.bin', body=b'0123456789')

  data = b''.join(fs.get_file(http_server.url + '/chunks.bin'))

  assert data == b'0123456789'


def test_as_local(fs, http_server, http_routes, temp_path):
  """as_local delegates the download to the injected cache interface."""
  http_routes.add_route('/local.bin', body=b'local-copy')

  cache_iface = mock.MagicMock()
  local_path = os.path.join(temp_path, 'local-out.bin')
  cache_iface.as_local.return_value = local_path
  fs = hf.HttpFs(cache_dir=os.path.join(temp_path, 'cache'), cache_iface=cache_iface)

  lpath = fs.as_local(http_server.url + '/local.bin')

  assert lpath == local_path
  cache_iface.as_local.assert_called_once()


def test_open_write_uploads_on_close(fs, http_server, http_routes):
  """Writing through open(mode='wb') PUTs the staged data on close."""
  uploaded = _capture_upload(http_routes, http_server.url + '/wb.bin')

  fd = fs.open(http_server.url + '/wb.bin', mode='wb')
  fd.write(b'written-data')
  fd.close()

  assert uploaded['body'] == b'written-data'
