"""Shared pytest fixtures for the py_misc_utils test suite.

This conftest provides the infrastructure used across all the test
modules:

- Temporary working directories (as a Path, and as a str for the library
  APIs that still expect one) and file-content factories (plain text,
  gzip blobs, pickled objects).
- A session-scoped local HTTP server on an ephemeral port, with a
  dynamically configurable route table, so that network-dependent tests
  never need real internet access.
"""

import gzip
import http.server
import io
import os
import pickle
import threading

import pytest

from py_misc_utils import fs_utils as fsu

# Cap the idle timeout of surplus executor workers (default 5s): short
# tasks in the tests must not wait the full default before the workers
# exit, which would add dead time to every run.
os.environ.setdefault('EXECUTOR_IDLE_TIMEOUT', '1')


class _RouterHandler(http.server.BaseHTTPRequestHandler):
  """Request handler dispatching requests through the server route table."""

  protocol_version = 'HTTP/1.1'

  def _dispatch(self, send_body=True):
    """Serve the request by looking its path up in the server routes."""
    routes = self.server.routes
    entry = routes.get(self.path)
    if entry is None:
      self.send_error(http.HTTPStatus.NOT_FOUND, 'No such route')
      return

    if callable(entry):
      status, headers, body = entry(self)
    else:
      status, headers, body = entry

    self.send_response(status)
    for name, value in headers.items():
      self.send_header(name, value)
    self.send_header('Content-Length', str(len(body)))
    self.end_headers()
    if send_body:
      self.wfile.write(body)

  def do_GET(self):
    """Serve GET requests."""
    self._dispatch()

  def do_HEAD(self):
    """Serve HEAD requests like a GET, but without the response body."""
    self._dispatch(send_body=False)

  def do_PUT(self):
    """Serve PUT requests (the route callable may consume the body)."""
    self._dispatch()

  def do_DELETE(self):
    """Serve DELETE requests."""
    self._dispatch()

  def log_message(self, format, *args):
    """Keep the test output clean: do not log each served request."""
    # Silence is intentional: see Ruff PIE790 decision in the plan file.


class _QuietServer(http.server.ThreadingHTTPServer):
  """HTTP server tolerating the abrupt client disconnects seen in tests."""

  daemon_threads = True

  def handle_error(self, request, client_address):
    """Swallow per-request errors: tests assert responses, not server logs."""
    # Silence is intentional: see Ruff PIE790 decision in the plan file.


class HttpTestServer:
  """Thin wrapper around the test HTTP server, exposing its base URL."""

  # The wrapped http.server instance.
  _server = None
  # The base URL the server answers on (e.g. http://127.0.0.1:54321).
  url = None

  def __init__(self, server):
    super().__init__()
    self._server = server
    host, port = server.server_address[0], server.server_address[1]
    self.url = f'http://{host}:{port}'

  @property
  def routes(self):
    """The {path: entry} route table, where entry is either a
    (status, headers, body) tuple or a callable(handler) returning one."""
    return self._server.routes

  def add_route(self, path, body=b'', status=200, headers=None):
    """Register a route serving a static (status, headers, body) response."""
    self.routes[path] = (status, dict(headers or {}), body)

  def clear_routes(self):
    """Drop all registered routes."""
    self.routes.clear()


@pytest.fixture(scope='session')
def http_server():
  """Session-scoped local HTTP server on an OS-assigned ephemeral port."""
  server = _QuietServer(('127.0.0.1', 0), _RouterHandler)
  server.routes = {}
  thread = threading.Thread(target=server.serve_forever, daemon=True)
  thread.start()
  try:
    yield HttpTestServer(server)
  finally:
    server.shutdown()
    server.server_close()
    thread.join(timeout=5)


@pytest.fixture
def http_routes(http_server):
  """Function-scoped view of the HTTP server, with routes reset per test."""
  http_server.clear_routes()
  try:
    yield http_server
  finally:
    http_server.clear_routes()


@pytest.fixture
def temp_path(tmp_path):
  """A temporary directory path, as a str.

  Several library entry points (gfs, lockfile, ...) call str-only methods
  such as re.match() or .encode() on their path arguments, so they reject
  pathlib.Path. Tests needing those APIs request this fixture instead of
  wrapping tmp_path in str() at every call site.
  """
  return str(tmp_path)


@pytest.fixture
def tmp_cwd(tmp_path):
  """Run the test with the current working directory moved to tmp_path."""
  old_cwd = os.getcwd()
  os.chdir(tmp_path)
  try:
    yield tmp_path
  finally:
    os.chdir(old_cwd)


@pytest.fixture
def make_text_file(tmp_path):
  """Factory creating temporary text files with a known content."""

  def _make(name='file.txt', content='hello world\n'):
    path = os.path.join(tmp_path, name)
    fsu.write_content(path, content, binary=False)
    return path

  return _make


@pytest.fixture
def make_gzip_file(tmp_path):
  """Factory creating temporary gzip files compressing known bytes."""

  def _make(name='file.gz', content=b'payload bytes'):
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode='wb') as fgz:
      fgz.write(content)
    path = os.path.join(tmp_path, name)
    fsu.write_content(path, buf.getvalue())
    return path

  return _make


@pytest.fixture
def make_pickle_file(tmp_path):
  """Factory creating temporary files holding a pickled object."""

  def _make(name='file.pkl', obj=None):
    if obj is None:
      obj = {'key': [1, 2, 3]}
    path = os.path.join(tmp_path, name)
    with open(path, 'wb') as fpt:
      pickle.dump(obj, fpt)
    return path

  return _make
