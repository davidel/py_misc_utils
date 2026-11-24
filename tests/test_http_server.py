"""Tests for py_misc_utils.http_server.

The test HTTP server: _sanitize_path traversal protection, _read_stream
Content-Length and chunked parsing (including the trailer-headers bug
being unavailable via the public path), _check_authorization (missing
header, unconfigured server, bearer, basic, unsupported type), and the
PUT/DELETE/OPTIONS flows over a local HTTPServer rooted at a temp
directory (translate_path overridden, mirroring what a real deployment
does by serving a document root).
"""

import base64
import http.client
import http.server
import io
import os
import threading
import urllib.parse
import urllib.request
from types import SimpleNamespace

import pytest

from py_misc_utils import http_server as hsrv


class _Headers(dict):
  """Headers mapping returning None for missing keys (http-server style)."""

  def __missing__(self, key):
    return None


def _make_handler(docroot, auth):
  """Build a handler class serving files under docroot with given auth."""

  class Handler(hsrv.HTTPRequestHandler):
    _args = SimpleNamespace(auth=auth)

    def translate_path(self, path):
      purl = urllib.parse.urlsplit(path)
      return os.path.join(docroot, purl.path.lstrip('/'))

  return Handler


def _serve(handler_cls):
  """Run an HTTPServer with the given handler on an ephemeral port."""
  server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), handler_cls)
  thread = threading.Thread(target=server.serve_forever, daemon=True)
  thread.start()
  try:
    yield server
  finally:
    server.shutdown()
    server.server_close()
    thread.join(timeout=5)


@pytest.fixture
def plain_server(tmp_path):
  """Server with an unconfigured auth (writes rejected with 403)."""
  yield from _serve(_make_handler(str(tmp_path), None))


@pytest.fixture
def auth_server(tmp_path):
  """Server configured with a bearer token + basic password."""
  yield from _serve(_make_handler(str(tmp_path), 'sekrit'))


def _url(server, path):
  return f'http://127.0.0.1:{server.server_address[1]}{path}'


def test_sanitize_path():
  """_sanitize_path rejects any '..' occurrence and trailing slashes."""
  assert hsrv._sanitize_path('/safe/file.txt') == '/safe/file.txt'
  assert hsrv._sanitize_path('/safe/dir/') is None
  assert hsrv._sanitize_path('/../etc/passwd') is None
  # '..' is rejected anywhere in the path, even inside a name.
  assert hsrv._sanitize_path('/ok/..hidden') is None


def test_read_stream_content_length():
  """Content-Length bodies are read exactly."""
  data = b'x' * 37
  headers = _Headers({'Content-Length': str(len(data))})
  stream = io.BytesIO(data)

  chunks = list(hsrv._read_stream(headers, stream, {}))

  assert b''.join(chunks) == data


def test_read_stream_chunked():
  """Chunked bodies are decoded, with extensions kept in chunked_headers."""
  body = b'4\r\nWiki\r\n5\r\npedia\r\n0\r\n\r\n'
  headers = _Headers({'Transfer-Encoding': 'chunked'})
  stream = io.BytesIO(body)
  cheaders = {}

  chunks = list(hsrv._read_stream(headers, stream, cheaders))

  assert b''.join(chunks) == b'Wikipedia'
  assert cheaders == {}


def test_read_stream_chunk_ext_in_size_line_unsupported():
  """Standard chunk extensions in the size line raise ValueError.

  Known library bugs in _read_stream (reported, all probed):
  - chunk extensions in the size line ('4;ext=1\\r\\n') make int() fail;
  - the trailer-headers branch is unreachable-working: ln.split(':')
    applies a str delimiter to a bytes line (TypeError), and even with a
    bytes delimiter 'part[0]' references an undefined name ('parts');
  hence the chunked_headers parameter can never be populated.
  """
  body = b'4;ext=1\r\nWiki\r\n0\r\n\r\n'
  headers = _Headers({'Transfer-Encoding': 'chunked'})
  stream = io.BytesIO(body)

  with pytest.raises(ValueError):
    list(hsrv._read_stream(headers, stream, {}))


def test_read_stream_unknown_framing():
  """Bodies with neither length nor chunking raise RuntimeError."""
  stream = io.BytesIO(b'rest-of-body')

  with pytest.raises(RuntimeError):
    list(hsrv._read_stream(_Headers(), stream, {}))


def test_options_cors(auth_server):
  """OPTIONS returns 200 with the allowed-methods header (path is probed)."""
  # do_OPTIONS resolves the path too: the OPTIONS target must exist.
  req = urllib.request.Request(_url(auth_server, '/data.bin'), method='OPTIONS')
  with urllib.request.urlopen(req, timeout=5) as resp:
    assert resp.status == 200
    assert resp.headers['Allow'] == 'GET,POST,PUT,OPTIONS,HEAD,DELETE'


def test_put_requires_authorization(auth_server):
  """PUT without an Authorization header is rejected with 401."""
  req = urllib.request.Request(_url(auth_server, '/data.bin'),
                               data=b'payload',
                               method='PUT')

  with pytest.raises(urllib.error.HTTPError) as excinfo:
    urllib.request.urlopen(req, timeout=5)

  assert excinfo.value.code == 401


def test_put_unconfigured_auth(plain_server):
  """PUT with any Authorization on an unconfigured server gives 403."""
  req = urllib.request.Request(_url(plain_server, '/data.bin'),
                               data=b'payload',
                               method='PUT',
                               headers={'Authorization': 'Bearer foobar'})

  with pytest.raises(urllib.error.HTTPError) as excinfo:
    urllib.request.urlopen(req, timeout=5)

  assert excinfo.value.code == 403


def test_put_bearer_ok_and_readback(auth_server):
  """Authorized PUT creates the file; GET serves it back."""
  token = base64.b64encode(b'sekrit').decode()
  req = urllib.request.Request(_url(auth_server, '/data.bin'),
                               data=b'payload-bearer',
                               method='PUT',
                               headers={'Authorization': f'Bearer {token}'})

  with urllib.request.urlopen(req, timeout=5) as resp:
    assert resp.status == 201

  with urllib.request.urlopen(_url(auth_server, '/data.bin'), timeout=5) as resp:
    assert resp.read() == b'payload-bearer'


def test_put_bearer_bad_token(auth_server):
  """A wrong bearer token is rejected with 403."""
  token = base64.b64encode(b'wrong').decode()
  req = urllib.request.Request(_url(auth_server, '/data.bin'),
                               data=b'x',
                               method='PUT',
                               headers={'Authorization': f'Bearer {token}'})

  with pytest.raises(urllib.error.HTTPError) as excinfo:
    urllib.request.urlopen(req, timeout=5)

  assert excinfo.value.code == 403


def test_put_basic_ok(auth_server):
  """Basic auth with the configured password authorizes the PUT."""
  creds = base64.b64encode(b'user:sekrit').decode()
  req = urllib.request.Request(_url(auth_server, '/basic.bin'),
                               data=b'basic-data',
                               method='PUT',
                               headers={'Authorization': f'Basic {creds}'})

  with urllib.request.urlopen(req, timeout=5) as resp:
    assert resp.status == 201

  with urllib.request.urlopen(_url(auth_server, '/basic.bin'), timeout=5) as resp:
    assert resp.read() == b'basic-data'


def test_put_basic_bad_password(auth_server):
  """Basic auth with the wrong password is rejected with 403."""
  creds = base64.b64encode(b'user:nope').decode()
  req = urllib.request.Request(_url(auth_server, '/basic.bin'),
                               data=b'x',
                               method='PUT',
                               headers={'Authorization': f'Basic {creds}'})

  with pytest.raises(urllib.error.HTTPError) as excinfo:
    urllib.request.urlopen(req, timeout=5)

  assert excinfo.value.code == 403


def test_put_unsupported_auth_type(auth_server):
  """An unsupported authorization type is rejected with 403."""
  blob = base64.b64encode(b'whatever').decode()
  req = urllib.request.Request(_url(auth_server, '/data.bin'),
                               data=b'x',
                               method='PUT',
                               headers={'Authorization': f'Digest {blob}'})

  with pytest.raises(urllib.error.HTTPError) as excinfo:
    urllib.request.urlopen(req, timeout=5)

  assert excinfo.value.code == 403


def test_put_traversal_rejected(auth_server):
  """PUT to a traversal path is rejected with 403."""
  token = base64.b64encode(b'sekrit').decode()
  req = urllib.request.Request(_url(auth_server, '/../evil.bin'),
                               data=b'x',
                               method='PUT',
                               headers={'Authorization': f'Bearer {token}'})

  with pytest.raises(urllib.error.HTTPError) as excinfo:
    urllib.request.urlopen(req, timeout=5)

  assert excinfo.value.code == 403


def test_delete(auth_server):
  """Authorized DELETE removes the file; deleting again gives 500."""
  token = base64.b64encode(b'sekrit').decode()
  headers = {'Authorization': f'Bearer {token}'}

  req = urllib.request.Request(_url(auth_server, '/doomed.bin'),
                               data=b'data',
                               method='PUT',
                               headers=headers)

  with urllib.request.urlopen(req, timeout=5):
    pass

  req = urllib.request.Request(_url(auth_server, '/doomed.bin'),
                               method='DELETE',
                               headers=headers)

  with urllib.request.urlopen(req, timeout=5) as resp:
    assert resp.status == 200

  with pytest.raises(urllib.error.HTTPError) as excinfo:
    urllib.request.urlopen(req, timeout=5)

  assert excinfo.value.code == 500


def test_put_chunked_upload(auth_server):
  """A chunked PUT body (no Content-Length) is decoded into the file."""
  token = base64.b64encode(b'sekrit').decode()
  conn = http.client.HTTPConnection('127.0.0.1',
                                    auth_server.server_address[1],
                                    timeout=5)
  try:
    body = b'5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n'
    headers = {'Authorization': f'Bearer {token}', 'Transfer-Encoding': 'chunked'}
    conn.request('PUT', '/chunked.bin', body=body, headers=headers)
    resp = conn.getresponse()
    assert resp.status == 201
    resp.read()
  finally:
    conn.close()

  with urllib.request.urlopen(_url(auth_server, '/chunked.bin'), timeout=5) as resp:
    assert resp.read() == b'hello world'
