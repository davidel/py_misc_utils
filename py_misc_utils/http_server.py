import argparse
import base64
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


class HTTPRequestHandler(http.server.SimpleHTTPRequestHandler):

  def _check_authorization(self, op, path):
    hauth = self.headers['Authorization']
    if hauth is None:
      raise HandlerException(401, 'Unauthorized', f'Action not allowed on "{self.path}"\n')
    if self._args.auth is None:
      self.log_message(f'Client request needs authorization but server not configured ' \
                       f'for it: {op} {self.path}')
      raise HandlerException(403, 'Forbidden', f'Action not allowed on "{self.path}"\n')

    try:
      auth_type, auth_config = [x.strip() for x in hauth.split(' ', maxsplit=1)]

      auth_type = auth_type.lower()
      if auth_type == 'bearer':
        token = base64.b64decode(auth_config).decode()
        if token != self._args.auth:
          self.log_message(f'Client authorization invalid: {op} {self.path} {token}')
          raise HandlerException(403, 'Forbidden', f'Action not allowed on "{self.path}"\n')
      elif auth_type == 'basic':
        creds = base64.b64decode(auth_config).decode()
        user, passwd = creds.split(':', maxsplit=1)

        # Very crude auth!
        if passwd != self._args.auth:
          self.log_message(f'Client authorization invalid: {op} {self.path} {user}:{passwd}')
          raise HandlerException(403, 'Forbidden', f'Action not allowed on "{self.path}"\n')
      else:
        raise HandlerException(403, 'Forbidden', f'Authorization not supported: {hauth}\n')
    except HandlerException:
      raise
    except Exception as ex:
      raise HandlerException(500, 'Internal Server Error', f'Internal error: {ex}\n')

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


class ClassWrapper:

  def __init__(self, cls, **kwargs):
    self._cls = cls
    self._kwargs = kwargs

  def __call__(self, *args, **kwargs):
    obj = self._cls.__new__(self._cls, *args, **kwargs)
    obj.__dict__.update(self._kwargs)
    obj.__dict__.update({k: v for k, v in vars(self).items() if not k.startswith('_')})
    obj.__init__(*args, **kwargs)

    return obj


if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Simple HTTP Server For Testing')
  parser.add_argument('--bind', default='0.0.0.0',
                      help='Specify alternate bind address')
  parser.add_argument('--port', type=int, default=8000,
                      help='Specify alternate port')
  parser.add_argument('--protocol', default='HTTP/1.1',
                      help='Conform to this HTTP version')
  parser.add_argument('--auth',
                      help='The authentication to be used when performing HTTP write operations')

  args = parser.parse_args()

  # Wrap the handler class, to allow adding the args (and also because the
  # http.server.test() API plants data inside the class global namespace.
  req_handler = ClassWrapper(HTTPRequestHandler, _args=args)

  http.server.test(HandlerClass=req_handler,
                   port=args.port,
                   bind=args.bind,
                   protocol=args.protocol)

