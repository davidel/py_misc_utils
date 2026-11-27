"""Tests for py_misc_utils.fs.gcs_fs against a real in-process fake server.

The GCS client honors STORAGE_EMULATOR_HOST, so the filesystem can be
exercised end-to-end over HTTP against a local server implementing the
subset of the Cloud Storage JSON API that gcs_fs uses. No GCP access or
credentials are required, and no library internals are patched.
"""

import base64
import http.server
import json
import os
import re
import struct
import threading
import urllib.parse

import pytest

from py_misc_utils.fs import gcs_fs as gf

_CRC_TABLE = []


def _crc32c(data):
  """CRC32C (Castagnoli), as the GCS API uses for object checksums."""
  if not _CRC_TABLE:
    poly = 0x82F63B78
    for i in range(256):
      crc = i
      for _ in range(8):
        crc = (crc >> 1) ^ poly if crc & 1 else crc >> 1
      _CRC_TABLE.append(crc)

  crc = 0xFFFFFFFF
  for b in data:
    crc = _CRC_TABLE[(crc ^ b) & 0xFF] ^ (crc >> 8)
  return crc ^ 0xFFFFFFFF


class FakeGcs:
  """Minimal Cloud Storage JSON API server, backed by a dict of objects."""

  def __init__(self, bucket):
    self.bucket = bucket
    self.objects = {}
    self.sessions = {}
    self.calls = []
    self.base_url = None
    self._srv = None

  def _meta(self, name, data=b''):
    crc = _crc32c(data)
    return {
        'kind': 'storage#object',
        'bucket': self.bucket,
        'name': name,
        'size': str(len(data)),
        'etag': 'e' + name,
        'crc32c': base64.b64encode(struct.pack('>I', crc)).decode(),
        'timeCreated': '2024-01-01T00:00:00.000Z',
        'updated': '2024-01-01T00:00:00.000Z',
    }

  def start(self):
    server = self

    class Handler(http.server.BaseHTTPRequestHandler):

      def log_message(self, *args):
        pass

      def _send(self, code, payload=None, ctype='application/json'):
        if payload is None:
          body = b''
        elif isinstance(payload, bytes):
          body = payload
        else:
          body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header('Content-Type', ctype)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

      def _read_body(self):
        length = int(self.headers.get('Content-Length') or 0)
        return self.rfile.read(length) if length else b''

      def _object_meta(self, name):
        return server._meta(name, server.objects[name])

      def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = urllib.parse.unquote(parsed.path)
        server.calls.append(('GET', path))

        if path.startswith('/download/storage/v1/b/') or 'alt=media' in self.path:
          name = path.split('/o/', 1)[1]
          data = server.objects.get(name, b'')
          rng = self.headers.get('Range')
          if rng:
            match = re.match(r'bytes=(\d+)-(\d+)', rng)
            start, end = int(match.group(1)), int(match.group(2))
            chunk = data[start:end + 1]
            self.send_response(206)
            self.send_header('Content-Range', f'bytes {start}-{end}/{len(data)}')
            self.send_header('Content-Length', str(len(chunk)))
            self.end_headers()
            self.wfile.write(chunk)
          else:
            self._send(200, data, 'application/octet-stream')
          return

        match = re.match(r'/storage/v1/b/([^/]+)/o/(.+)$', path)
        if match:
          name = match.group(2)
          if name not in server.objects:
            return self._send(404, {'error': {'code': 404, 'message': 'not found'}})
          return self._send(200, self._object_meta(name))

        if re.match(r'/storage/v1/b/[^/]+/o$', path):
          prefix = urllib.parse.parse_qs(parsed.query).get('prefix', [''])[0]
          items = [
              self._object_meta(n)
              for n in sorted(server.objects)
              if n.startswith(prefix)
          ]
          return self._send(200, {'kind': 'storage#objects', 'items': items})

        if re.match(r'/storage/v1/b/[^/]+$', path):
          return self._send(200, {'kind': 'storage#bucket', 'name': server.bucket})

        return self._send(404, {})

      def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        path = urllib.parse.unquote(parsed.path)
        body = self._read_body()
        server.calls.append(('POST', path))

        if 'upload/storage/v1' in path and 'uploadType=resumable' in self.path:
          # The object name rides in the metadata body of the initiate call.
          name = urllib.parse.parse_qs(parsed.query).get('name', [''])[0]
          if not name:
            try:
              name = json.loads(body or b'{}').get('name', '') or ''
            except ValueError:
              name = ''
          sid = str(len(server.sessions))
          server.sessions[sid] = name
          self.send_response(200)
          self.send_header(
              'Location', f'{server.base_url}/upload/resumable/{sid}'
              f'?name={urllib.parse.quote(name)}')
          self.send_header('Content-Length', '0')
          self.end_headers()
          return

        if path.startswith('/upload/resumable/'):
          sid = path.split('/')[-1]
          name = server.sessions.get(sid, '')
          server.objects[name] = body
          return self._send(200, self._object_meta(name))

        match = re.match(r'/storage/v1/b/[^/]+/o/(.+)/copyTo/b/[^/]+/o/(.+)$', path)
        if match:
          src, dst = match.group(1), match.group(2)
          server.objects[dst] = server.objects.get(src, b'')
          return self._send(200, self._object_meta(dst))

        return self._send(404, {})

      def do_PUT(self):
        parsed = urllib.parse.urlparse(self.path)
        path = urllib.parse.unquote(parsed.path)
        body = self._read_body()
        server.calls.append(('PUT', path))

        if path.startswith('/upload/resumable/'):
          sid = path.split('/')[-1]
          name = server.sessions.get(sid, '')
          server.objects[name] = body
          return self._send(200, self._object_meta(name))

        return self._send(404, {})

      def do_DELETE(self):
        parsed = urllib.parse.urlparse(self.path)
        path = urllib.parse.unquote(parsed.path)
        server.calls.append(('DELETE', path))
        match = re.match(r'/storage/v1/b/[^/]+/o/(.+)$', path)
        if match:
          server.objects.pop(match.group(1), None)
          return self._send(204)
        return self._send(404, {})

    self._srv = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    self._srv.daemon_threads = True
    threading.Thread(target=self._srv.serve_forever, daemon=True).start()
    self.base_url = f'http://127.0.0.1:{self._srv.server_address[1]}'

    return self.base_url

  def stop(self):
    self._srv.shutdown()
    self._srv.server_close()


@pytest.fixture(scope='module')
def gcs_env():
  """A fake GCS server shared by the module.

  gcs_fs pools one backend (and thus one GCS client) per bucket in a
  process-global object cache, so the server must outlive individual
  tests: a per-test server would leave the pooled client pointing at a
  dead port and every later request would hang on connect.
  """
  fake = FakeGcs('test-bucket')
  url = fake.start()
  old = os.environ.get('STORAGE_EMULATOR_HOST')
  os.environ['STORAGE_EMULATOR_HOST'] = url
  try:
    yield fake
  finally:
    if old is None:
      os.environ.pop('STORAGE_EMULATOR_HOST', None)
    else:
      os.environ['STORAGE_EMULATOR_HOST'] = old
    fake.stop()


@pytest.fixture
def fs(gcs_env):
  gcs_env.objects.clear()
  gcs_env.calls.clear()

  return gf.GcsFs()


def test_gcs_put_and_get_file(fs, gcs_env):
  """put_file() stores the chunks; get_file() yields the content back."""
  fs.put_file('gs://test-bucket/data.bin', [b'hello ', b'world'])

  assert gcs_env.objects == {'data.bin': b'hello world'}
  assert b''.join(fs.get_file('gs://test-bucket/data.bin')) == b'hello world'


def test_gcs_exists(fs, gcs_env):
  """exists() reflects the presence of the object in the bucket."""
  gcs_env.objects['present.bin'] = b'x'

  assert fs.exists('gs://test-bucket/present.bin')


def test_gcs_stat_missing_object_raises(fs, gcs_env):
  """stat() raises FileNotFoundError for a missing object, so exists()
  reports False."""
  with pytest.raises(FileNotFoundError):
    fs.stat('gs://test-bucket/missing.bin')

  assert fs.exists('gs://test-bucket/missing.bin') is False


def test_gcs_open_missing_object_raises(fs, gcs_env):
  """open() raises FileNotFoundError when the object does not exist."""
  with pytest.raises(FileNotFoundError):
    fs.open('gs://test-bucket/never-there.bin', mode='rb')


def test_gcs_stat(fs, gcs_env):
  """stat() reports the object size."""
  gcs_env.objects['stat.bin'] = b'0123456789'

  assert fs.stat('gs://test-bucket/stat.bin').st_size == 10


def test_gcs_list(fs, gcs_env):
  """list() enumerates the objects of the bucket."""
  gcs_env.objects.update({'a.txt': b'a', 'b.txt': b'b'})

  names = {entry.name for entry in fs.list('gs://test-bucket/')}

  assert names == {'a.txt', 'b.txt'}


def test_gcs_remove(fs, gcs_env):
  """remove() deletes the object from the bucket."""
  gcs_env.objects['rm.txt'] = b'gone'

  fs.remove('gs://test-bucket/rm.txt')

  assert 'rm.txt' not in gcs_env.objects


def test_gcs_rename(fs, gcs_env):
  """rename() copies the object and removes the original."""
  gcs_env.objects['src.txt'] = b'payload'

  fs.rename('gs://test-bucket/src.txt', 'gs://test-bucket/dst.txt')

  assert gcs_env.objects == {'dst.txt': b'payload'}


def test_gcs_copy(fs, gcs_env):
  """copyfile() streams the object through get_file/put_file."""
  gcs_env.objects['src.txt'] = b'payload'

  fs.copyfile('gs://test-bucket/src.txt', fs, 'gs://test-bucket/dst.txt')

  assert gcs_env.objects == {'src.txt': b'payload', 'dst.txt': b'payload'}


def test_gcs_rmtree(fs, gcs_env):
  """rmtree() removes every object under a prefix."""
  gcs_env.objects.update({'d/a.txt': b'a', 'd/b.txt': b'b', 'keep.txt': b'k'})

  fs.rmtree('gs://test-bucket/d')

  assert gcs_env.objects == {'keep.txt': b'k'}
