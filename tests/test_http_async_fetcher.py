"""Tests for py_misc_utils.http_async_fetcher.

Uses the ephemeral HTTP server fixture so no real network access is required.
"""

import asyncio
import os

import pytest

from py_misc_utils import http_async_fetcher as haf
from py_misc_utils import work_results as wres


def test_http_fetch_url_single(http_server, http_routes, temp_path):
  http_routes.add_route('/single', body=b'async single')
  work = os.path.join(temp_path, 'work')
  os.makedirs(work, exist_ok=True)
  fetcher = haf.HttpAsyncFetcher(num_workers=2, path=work)
  fetcher.start()
  try:
    fetcher.enqueue(http_server.url + '/single')
    results = list(fetcher.iter_results())
    assert len(results) == 1
    _, wdata = results[0]
    assert wdata == b'async single'
  finally:
    fetcher.shutdown()


def test_http_fetch_url_404(http_server, http_routes, temp_path):
  http_routes.clear_routes()
  work = os.path.join(temp_path, 'work')
  os.makedirs(work, exist_ok=True)
  fetcher = haf.HttpAsyncFetcher(num_workers=2, path=work)
  fetcher.start()
  try:
    fetcher.enqueue(http_server.url + '/missing')
    results = list(fetcher.iter_results())
    assert len(results) == 1
    _, wdata = results[0]
    assert isinstance(wdata, wres.WorkException)
  finally:
    fetcher.shutdown()


def test_http_async_fetcher_concurrent(http_server, http_routes, temp_path):
  http_routes.add_route('/a', body=b'a\n')
  http_routes.add_route('/b', body=b'b\n')
  http_routes.add_route('/c', body=b'c\n')
  work = os.path.join(temp_path, 'work')
  os.makedirs(work, exist_ok=True)
  fetcher = haf.HttpAsyncFetcher(num_workers=3, path=work)
  fetcher.start()
  try:
    urls = [http_server.url + '/a', http_server.url + '/b', http_server.url + '/c']
    fetcher.enqueue(*urls)
    results = list(fetcher.iter_results())
    assert len(results) == 3
    bodies = {body for _, body in results}
    assert bodies == {b'a\n', b'b\n', b'c\n'}
  finally:
    fetcher.shutdown()


def test_http_async_fetcher_context_manager(http_server, http_routes, temp_path):
  http_routes.add_route('/ctx', body=b'ctx async')
  work = os.path.join(temp_path, 'work')
  os.makedirs(work, exist_ok=True)
  with haf.HttpAsyncFetcher(num_workers=2, path=work) as fetcher:
    fetcher.enqueue(http_server.url + '/ctx')
    results = list(fetcher.iter_results())
    assert len(results) == 1
    _, wdata = results[0]
    assert wdata == b'ctx async'


def test_http_async_fetcher_max_results(http_server, http_routes, temp_path):
  http_routes.add_route('/x', body=b'x\n')
  http_routes.add_route('/y', body=b'y\n')
  http_routes.add_route('/z', body=b'z\n')
  work = os.path.join(temp_path, 'work')
  os.makedirs(work, exist_ok=True)
  fetcher = haf.HttpAsyncFetcher(num_workers=2, path=work)
  fetcher.start()
  try:
    urls = [http_server.url + '/x', http_server.url + '/y', http_server.url + '/z']
    fetcher.enqueue(*urls)
    results = list(fetcher.iter_results(max_results=2))
    assert len(results) == 2
  finally:
    fetcher.shutdown()


def test_http_async_fetcher_shutdown_idempotent(temp_path):
  work = os.path.join(temp_path, 'work')
  os.makedirs(work, exist_ok=True)
  fetcher = haf.HttpAsyncFetcher(num_workers=2, path=work)
  fetcher.start()
  fetcher.shutdown()
  fetcher.shutdown()  # Should not raise.
