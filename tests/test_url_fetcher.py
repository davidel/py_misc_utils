"""Tests for py_misc_utils.url_fetcher.

Tests use local file URLs and the ephemeral HTTP server fixture so no
real network access is required.
"""

import os

import pytest

from py_misc_utils import url_fetcher as uf


def test_fetch_local_file(temp_path, make_text_file):
  path = make_text_file('in.txt', 'fetch me\n')
  work = os.path.join(temp_path, 'work')
  os.makedirs(work, exist_ok=True)
  f = uf.UrlFetcher(path=work)
  f.start()
  try:
    f.enqueue(path)
    result = f.wait(path)
    assert result == b'fetch me\n'
  finally:
    f.shutdown()


def test_fetch_http_file(http_server, http_routes, temp_path):
  http_routes.add_route('/data.bin', body=b'http payload')
  work = os.path.join(temp_path, 'work')
  os.makedirs(work, exist_ok=True)
  f = uf.UrlFetcher(path=work)
  f.start()
  try:
    url = http_server.url + '/data.bin'
    f.enqueue(url)
    result = f.wait(url)
    assert result == b'http payload'
  finally:
    f.shutdown()


def test_fetch_multiple_parallel(http_server, http_routes, temp_path):
  http_routes.add_route('/one', body=b'one\n')
  http_routes.add_route('/two', body=b'two\n')
  http_routes.add_route('/three', body=b'three\n')
  work = os.path.join(temp_path, 'work')
  os.makedirs(work, exist_ok=True)
  f = uf.UrlFetcher(path=work, num_workers=3)
  f.start()
  try:
    urls = [
        http_server.url + '/one', http_server.url + '/two', http_server.url + '/three'
    ]
    f.enqueue(*urls)
    results = dict(f.iter_results())
    assert len(results) == 3
    assert results[urls[0]] == b'one\n'
    assert results[urls[1]] == b'two\n'
    assert results[urls[2]] == b'three\n'
  finally:
    f.shutdown()


def test_fetch_max_results(http_server, http_routes, temp_path):
  http_routes.add_route('/a', body=b'a\n')
  http_routes.add_route('/b', body=b'b\n')
  http_routes.add_route('/c', body=b'c\n')
  work = os.path.join(temp_path, 'work')
  os.makedirs(work, exist_ok=True)
  f = uf.UrlFetcher(path=work, num_workers=2)
  f.start()
  try:
    urls = [http_server.url + '/a', http_server.url + '/b', http_server.url + '/c']
    f.enqueue(*urls)
    results = list(f.iter_results(max_results=2))
    assert len(results) == 2
  finally:
    f.shutdown()


def test_fetch_nonexistent_local_file_raises(temp_path):
  work = os.path.join(temp_path, 'work')
  os.makedirs(work, exist_ok=True)
  f = uf.UrlFetcher(path=work)
  f.start()
  try:
    f.enqueue(os.path.join(temp_path, 'missing.txt'))
    with pytest.raises(FileNotFoundError):
      f.wait(os.path.join(temp_path, 'missing.txt'))
  finally:
    f.shutdown()


def test_context_manager(temp_path, make_text_file):
  path = make_text_file('ctx.txt', 'ctx\n')
  work = os.path.join(temp_path, 'work')
  os.makedirs(work, exist_ok=True)
  with uf.UrlFetcher(path=work) as f:
    f.enqueue(path)
    result = f.wait(path)
    assert result == b'ctx\n'
