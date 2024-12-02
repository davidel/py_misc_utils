import argparse
import http.server
import os


def _sanitize_path(path):
  if '..' not in path and not path.endswith('/'):
    return path.lstrip('/')


def _read_stream(headers, stream, chunk_size=None):
  chunk_size = chunk_size or 16 * 1024**2

  length = headers['Content-Length']
  encoding = headers['Transfer-Encoding']
  if length is not None:
    length = int(length)
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

  else:
    raise RuntimeError(f'Unable to read data: {headers}')


class HTTPRequestHandler(http.server.SimpleHTTPRequestHandler):

  def do_PUT(self):
    path = _sanitize_path(self.translate_path(self.path))
    if path is None:
      self.send_error(403,
                      message='Forbidden',
                      explain=f'PUT not allowed on "{self.path}"\n')
    else:
      try:
        os.makedirs(os.path.dirname(path), exist_ok=True)

        with open(path, 'wb') as f:
          for data in _read_stream(self.headers, self.rfile):
            f.write(data)

        self.send_response(201, 'Created')
        self.end_headers()
      except Exception as ex:
        self.send_error(500,
                        message='Internal Server Error',
                        explain=f'Internal error: {ex}\n')

        print(ex)


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('--bind', default='0.0.0.0',
                      help='Specify alternate bind address')
  parser.add_argument('--port', type=int, default=8000,
                      help='Specify alternate port')

  args = parser.parse_args()
  http.server.test(HandlerClass=HTTPRequestHandler, port=args.port, bind=args.bind)

