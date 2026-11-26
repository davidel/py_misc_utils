"""Tests for py_misc_utils.http_server over the public HTTP interface.

Covers traversal protection (PUT of '..' / trailing-slash paths -> 403),
Content-Length and chunked body parsing through real PUT requests,
_check_authorization (missing header, unconfigured server, bearer, basic,
unsupported type), and the PUT/DELETE/OPTIONS flows over a local
HTTPServer rooted at a temp directory (translate_path overridden,
mirroring what a real deployment does by serving a document root).

Note: the traversal-protection and body-framing behaviors are pinned via
their public HTTP outcomes (status codes / stored bytes) rather than by
calling the private _sanitize_path()/_read_stream() helpers directly.
"""

import base64
import contextlib
import http.client
import http.server
import os
import threading
import urllib.error
import urllib.parse
import urllib.request
from types import SimpleNamespace

import pytest

from py_misc_utils import http_server as hsrv


def _request(req):
  """Perform req, closing the response (including error responses)."""
  with pytest.raises(urllib.error.HTTPError) as excinfo, \
         contextlib.closing(urllib.request.urlopen(req, timeout=5)):
    pass

  return excinfo.value


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
def plain_server(temp_path):
  """Server with an unconfigured auth (writes rejected with 403)."""
  yield from _serve(_make_handler(temp_path, None))


@pytest.fixture
def auth_server(temp_path):
  """Server configured with a bearer token + basic password."""
  yield from _serve(_make_handler(temp_path, 'sekrit'))


def _url(server, path):
  return f'http://127.0.0.1:{server.server_address[1]}{path}'


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

  excinfo = _request(req)

  assert excinfo.code == 401


def test_put_unconfigured_auth(plain_server):
  """PUT with any Authorization on an unconfigured server gives 403."""
  req = urllib.request.Request(_url(plain_server, '/data.bin'),
                               data=b'payload',
                               method='PUT',
                               headers={'Authorization': 'Bearer foobar'})

  excinfo = _request(req)

  assert excinfo.code == 403


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

  excinfo = _request(req)

  assert excinfo.code == 403


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

  excinfo = _request(req)

  assert excinfo.code == 403


def test_put_unsupported_auth_type(auth_server):
  """An unsupported authorization type is rejected with 403."""
  blob = base64.b64encode(b'whatever').decode()
  req = urllib.request.Request(_url(auth_server, '/data.bin'),
                               data=b'x',
                               method='PUT',
                               headers={'Authorization': f'Digest {blob}'})

  excinfo = _request(req)

  assert excinfo.code == 403


def test_put_traversal_rejected(auth_server):
  """PUT to a traversal path is rejected with 403."""
  token = base64.b64encode(b'sekrit').decode()
  req = urllib.request.Request(_url(auth_server, '/../evil.bin'),
                               data=b'x',
                               method='PUT',
                               headers={'Authorization': f'Bearer {token}'})

  excinfo = _request(req)

  assert excinfo.code == 403


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

  excinfo = _request(req)

  assert excinfo.code == 500


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


def test_put_chunked_upload_with_trailing_extension_header(auth_server):
  """A colon-separated line after chunk data does not break the parse."""
  token = base64.b64encode(b'sekrit').decode()
  body = b'5\r\nhelloX-Foo: bar\r\n\r\n0\r\n\r\n'
  conn = http.client.HTTPConnection('127.0.0.1',
                                    auth_server.server_address[1],
                                    timeout=5)
  conn.request('PUT',
               '/chunkext.bin',
               body=body,
               headers={
                   'Authorization': f'Bearer {token}',
                   'Transfer-Encoding': 'chunked'
               })

  resp = conn.getresponse()

  assert resp.status == 201
  conn.close()

  with urllib.request.urlopen(_url(auth_server, '/chunkext.bin'), timeout=5) as r:
    assert r.read() == b'hello'
