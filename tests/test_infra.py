"""Tests for the shared test infrastructure defined in conftest.py.

Each test pins one contract of the fixtures: routing (static and
handler-driven), unknown-route errors, route reset, working-directory
switching and the content factories round-tripping their payloads.
"""

import gzip
import pickle
import urllib.error
import urllib.request

import pytest


def test_http_server_serves_static_routes(http_routes):
  """A registered static route is served with its status, headers and body."""
  http_routes.add_route('/hello',
                        body=b'hello world',
                        status=200,
                        headers={'X-Test': 'yes'})

  with urllib.request.urlopen(http_routes.url + '/hello') as response:
    assert response.status == 200
    assert response.headers.get('X-Test') == 'yes'
    assert response.read() == b'hello world'


def test_http_server_serves_custom_status_codes(http_routes):
  """Static routes can carry arbitrary status codes and headers."""
  http_routes.add_route('/created', body=b'ok', status=201, headers={'X-Custom': 'v1'})

  with urllib.request.urlopen(http_routes.url + '/created') as response:
    assert response.status == 201
    assert response.headers.get('X-Custom') == 'v1'
    assert response.read() == b'ok'


def test_http_server_supports_handler_driven_routes(http_routes):
  """Callable routes build responses from the incoming request."""

  def _handler(handler):
    """Echo back the X-Word request header."""
    return 200, {}, ('echo:' + (handler.headers.get('X-Word') or '')).encode()

  http_routes.routes['/echo'] = _handler

  request = urllib.request.Request(http_routes.url + '/echo',
                                   headers={'X-Word': 'alpha'})
  with urllib.request.urlopen(request) as response:
    assert response.read() == b'echo:alpha'

  request = urllib.request.Request(http_routes.url + '/echo')
  with urllib.request.urlopen(request) as response:
    assert response.read() == b'echo:'


def test_http_server_unknown_route_is_404(http_routes):
  """Requests to unregistered routes fail with HTTP 404."""
  with pytest.raises(urllib.error.HTTPError) as excinfo:
    urllib.request.urlopen(http_routes.url + '/nope')

  assert excinfo.value.code == 404


def test_clear_routes_removes_all_registered_routes(http_routes):
  """clear_routes() empties the route table, so old paths turn to 404."""
  http_routes.add_route('/one', body=b'1')
  http_routes.add_route('/two', body=b'2')
  assert set(http_routes.routes) == {'/one', '/two'}

  http_routes.clear_routes()

  assert http_routes.routes == {}
  with pytest.raises(urllib.error.HTTPError) as excinfo:
    urllib.request.urlopen(http_routes.url + '/one')

  assert excinfo.value.code == 404


def test_tmp_cwd_runs_the_test_inside_tmp_path(tmp_cwd):
  """Files created relative to the CWD land inside tmp_path."""
  with open('marker.txt', 'w') as fpt:
    fpt.write('here')

  assert (tmp_cwd / 'marker.txt').read_text() == 'here'


def test_make_text_file_writes_expected_content(make_text_file):
  """The text factory writes the requested (or default) content."""
  path = make_text_file('data.txt', content='abc\n')
  default_path = make_text_file()

  with open(path, 'r') as fpt:
    assert fpt.read() == 'abc\n'
  with open(default_path, 'r') as fpt:
    assert fpt.read() == 'hello world\n'


def test_make_gzip_file_roundtrips_payload(make_gzip_file):
  """The gzip factory produces a valid gzip archive of the payload."""
  path = make_gzip_file('data.gz', content=b'abc123')

  with open(path, 'rb') as fpt:
    assert fpt.read(2) == b'\x1f\x8b'
  with gzip.open(path, 'rb') as fpt:
    assert fpt.read() == b'abc123'
  with gzip.open(make_gzip_file(), 'rb') as fpt:
    assert fpt.read() == b'payload bytes'


def test_make_pickle_file_roundtrips_objects(make_pickle_file):
  """The pickle factory stores and restores the exact object."""
  obj = {'nested': [1, 2, 3]}
  path = make_pickle_file('data.pkl', obj=obj)

  with open(path, 'rb') as fpt:
    assert pickle.load(fpt) == obj
  with open(make_pickle_file(), 'rb') as fpt:
    assert pickle.load(fpt) == {'key': [1, 2, 3]}
