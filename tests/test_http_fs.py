"""Tests for py_misc_utils.fs.http_fs.

Exercises HttpFs read operations against the ephemeral HTTP server.
No real network access is required.
"""

import pytest

from py_misc_utils.fs import http_fs as hf


def test_read_via_http(http_server, http_routes, tmp_path):
  http_routes.add_route('/hf.txt', body=b'http fs data')
  fs = hf.HttpFs(cache_dir=str(tmp_path / 'cache'))
  with fs.open(http_server.url + '/hf.txt', mode='rb') as f:
    assert f.read() == b'http fs data'


def test_exists_via_http(http_server, http_routes, tmp_path):
  http_routes.add_route('/hf_exists.txt', body=b'e')
  fs = hf.HttpFs(cache_dir=str(tmp_path / 'cache'))
  assert fs.exists(http_server.url + '/hf_exists.txt')
  assert not fs.exists(http_server.url + '/hf_missing.txt')


def test_stat_via_http(http_server, http_routes, tmp_path):
  http_routes.add_route('/hf_stat.txt', body=b'stat over http')
  fs = hf.HttpFs(cache_dir=str(tmp_path / 'cache'))
  st = fs.stat(http_server.url + '/hf_stat.txt')
  assert st.st_size == len(b'stat over http')
