import argparse
import http.server
import os


def _sanitize_path(path):
  if '..' not in path and not path.endswith('/'):
    return path.lstrip('/')


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

        length = int(self.headers['Content-Length'])
        with open(path, 'wb') as f:
          f.write(self.rfile.read(length))

        self.send_response(201, 'Created')
        self.end_headers()
      except Exception as ex:
        self.send_error(500,
                        message='Internal Server Error',
                        explain=f'Internal error: {ex}\n')

        print(f'{path} : {ex}')


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('--bind', default='0.0.0.0',
                      help='Specify alternate bind address')
  parser.add_argument('--port', type=int, default=8000,
                      help='Specify alternate port')

  args = parser.parse_args()
  http.server.test(HandlerClass=HTTPRequestHandler, port=args.port, bind=args.bind)

