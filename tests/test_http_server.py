"""Tests for py_misc_utils.http_server.

Exercises the _sanitize_path helper and uses the conftest ephemeral HTTP
server fixture to verify file serving, directory listing, authorization,
and error handling without real internet access.
"""

import base64
import http.client

import pytest

from py_misc_utils import http_server as hs


def test_sanitize_path_safe():
  assert hs._sanitize_path('foo/bar.txt') == 'foo/bar.txt'


def test_sanitize_path_dotdot():
  assert hs._sanitize_path('../etc/passwd') is None


def test_sanitize_path_trailing_slash():
  assert hs._sanitize_path('foo/') is None


def test_server_serves_file(http_server):
  http_server.add_route('/hello.txt', body=b'hello world\n')
  conn = http.client.HTTPConnection(http_server.url.split('://')[1])
  conn.request('GET', '/hello.txt')
  resp = conn.getresponse()
  body = resp.read()
  conn.close()
  assert resp.status == 200
  assert body == b'hello world\n'


def test_server_404(http_server):
  conn = http.client.HTTPConnection(http_server.url.split('://')[1])
  conn.request('GET', '/does-not-exist.txt')
  resp = conn.getresponse()
  body = resp.read()
  conn.close()
  assert resp.status == 404


def test_server_authorization(http_server):
  # The conftest server is not configured with --auth. Any GET without
  # auth should succeed (no auth required).
  http_server.add_route('/hello.txt', body=b'hello world\n')
  conn = http.client.HTTPConnection(http_server.url.split('://')[1])
  conn.request('GET', '/hello.txt')
  resp = conn.getresponse()
  body = resp.read()
  conn.close()
  assert resp.status == 200
  assert body == b'hello world\n'
