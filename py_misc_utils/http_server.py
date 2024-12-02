import argparse
import copy
import http.server
import os


def _sanitize_path(path):
  if '..' not in path and not path.endswith('/'):
    return path


def _read_stream(headers, stream, chunked_headers, chunk_size=None):
  length = headers['Content-Length']
  encoding = headers['Transfer-Encoding']
  if length is not None:
    length = int(length)
    chunk_size = chunk_size or 16 * 1024**2
    while length > 0:
      rsize = min(length, chunk_size)

      yield stream.read(rsize)

      length -= rsize

  elif encoding == 'chunked':
    while True:
      size = int(stream.readline().strip(), 16)
      if size == 0:
        break

      yield stream.read(size)

      while True:
        ln = stream.readline().strip()
        if not ln:
          break
        parts = ln.split(':', maxsplit=1)
        if len(parts) == 2:
          chunked_headers[part[0].strip()] = part[1].strip()

  else:
    raise RuntimeError(f'Unable to read data: {headers}')


class HandlerException(Exception):

  def __init__(self, code, message, explain):
    super().__init__()
    self.code = code
    self.message = message
    self.explain = explain


class HTTPRequestHandler(http.server.CGIHTTPRequestHandler):

  def _check_authorization(self, op, path):
    hauth = self.headers['Authorization']
    # Very crude auth!
    if hauth != self._args.auth:
      raise HandlerException(403, 'Forbidden', f'Action not allowed on "{self.path}"\n')

  def do_OPTIONS(self):
    self.send_response(200, 'OK')
    self.send_header('Allow', 'GET,POST,PUT,OPTIONS,HEAD,DELETE')
    self.send_header('Content-Length', '0')
    self.end_headers()

  def do_DELETE(self):
    path = _sanitize_path(self.translate_path(self.path))
    if path is None:
      self.send_error(403,
                      message='Forbidden',
                      explain=f'Action not allowed on "{self.path}"\n')
    else:
      try:
        self._check_authorization('DELETE', path)

        os.remove(path)

        self.send_response(200, 'OK')
        self.send_header('Content-Length', '0')
        self.end_headers()
      except HandlerException as ex:
        self.send_error(ex.code,
                        message=ex.message,
                        explain=ex.explain)
      except OSError as ex:
        self.send_error(500,
                        message='Internal Server Error',
                        explain=f'Internal error: {ex}\n')

  def do_PUT(self):
    path = _sanitize_path(self.translate_path(self.path))
    if path is None:
      self.send_error(403,
                      message='Forbidden',
                      explain=f'Action not allowed on "{self.path}"\n')
    else:
      try:
        self._check_authorization('PUT', path)

        os.makedirs(os.path.dirname(path), exist_ok=True)

        chunked_headers = dict()
        with open(path, 'wb') as f:
          for data in _read_stream(self.headers, self.rfile, chunked_headers):
            f.write(data)

        self.send_response(201, 'Created')
        self.end_headers()
      except HandlerException as ex:
        self.send_error(ex.code,
                        message=ex.message,
                        explain=ex.explain)
      except Exception as ex:
        self.send_error(500,
                        message='Internal Server Error',
                        explain=f'Internal error: {ex}\n')


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('--bind', default='0.0.0.0',
                      help='Specify alternate bind address')
  parser.add_argument('--port', type=int, default=8000,
                      help='Specify alternate port')
  parser.add_argument('--protocol', default='HTTP/1.1',
                      help='Conform to this HTTP version')
  parser.add_argument('--auth',
                      help='The authentication to be used when performing HTTP write operations')

  args = parser.parse_args()

  # Make a copy of the class, to allow adding the args (and also because the
  # http.server.test() API plants data inside the class global namespace.
  req_handler = copy.copy(HTTPRequestHandler)
  req_handler._args = args

  http.server.test(HandlerClass=req_handler,
                   port=args.port,
                   bind=args.bind,
                   protocol=args.protocol)

