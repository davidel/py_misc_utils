"""Simple HTTP server with authorization support for testing."""

import argparse
import base64
import copy
import http.server
import os


def _sanitize_path(path):
  """Sanitize a path to prevent directory traversal attacks.

  Returns None if the path contains '..' or ends with '/', otherwise returns
  the path unchanged.

  Args:
    path: The path string to sanitize.

  Returns:
    The path if safe, None otherwise.
  """
  if '..' not in path and not path.endswith('/'):
    return path


def _read_stream(headers, stream, chunked_headers, chunk_size=None):
  """Read data from an HTTP request stream based on headers.

  Supports Content-Length and chunked Transfer-Encoding. Yields chunks of
  data read from the stream.

  Args:
    headers: The HTTP request headers.
    stream: The input stream to read from.
    chunked_headers: Dict to store chunked extension headers.
    chunk_size: Optional chunk size for reading. Defaults to None.

  Yields:
    Bytes chunks read from the stream.

  Raises:
    RuntimeError: If neither Content-Length nor chunked encoding is present.
  """
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
  """Exception raised for HTTP handler errors with status code and message.

  Attributes:
    code: The HTTP status code.
    message: The error message string.
    explain: The explanation string.
  """

  def __init__(self, code, message, explain):
    """Initialize the exception with HTTP error details.

    Args:
      code: The HTTP status code (e.g. 401, 403, 500).
      message: A short error message.
      explain: A longer explanation string.
    """
    super().__init__()
    self.code = code
    self.message = message
    self.explain = explain


class HTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
  """HTTP request handler with authorization for write operations.

  Extends SimpleHTTPRequestHandler to add bearer and basic authentication
  checking for PUT and DELETE operations. OPTIONS is supported for CORS
  preflight requests.
  """

  def _check_authorization(self, op, path):
    """Check authorization for an operation on a path.

    Validates the Authorization header against the server's auth configuration.
    Supports bearer and basic auth types.

    Args:
      op: The operation string (e.g. 'PUT', 'DELETE').
      path: The resource path being accessed.

    Raises:
      HandlerException: With appropriate HTTP status code if authorization
        fails or is not configured.
    """
    hauth = self.headers['Authorization']
    if hauth is None:
      raise HandlerException(401, 'Unauthorized',
                             f'Action not allowed on "{self.path}"\n')
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
          raise HandlerException(403, 'Forbidden',
                                 f'Action not allowed on "{self.path}"\n')
      elif auth_type == 'basic':
        creds = base64.b64decode(auth_config).decode()
        user, passwd = creds.split(':', maxsplit=1)

        # Very crude auth!
        if passwd != self._args.auth:
          self.log_message(
              f'Client authorization invalid: {op} {self.path} {user}:{passwd}')
          raise HandlerException(403, 'Forbidden',
                                 f'Action not allowed on "{self.path}"\n')
      else:
        raise HandlerException(403, 'Forbidden',
                               f'Authorization not supported: {hauth}\n')
    except HandlerException:
      raise
    except Exception as ex:
      raise HandlerException(500, 'Internal Server Error', f'Internal error: {ex}\n')

  def do_OPTIONS(self):
    """Handle OPTIONS requests with CORS headers.

    Responds with 200 OK and lists allowed methods.
    """
    self.send_response(200, 'OK')
    self.send_header('Allow', 'GET,POST,PUT,OPTIONS,HEAD,DELETE')
    self.send_header('Content-Length', '0')
    self.end_headers()

  def do_DELETE(self):
    """Handle DELETE requests with authorization check.

    Deletes the file at the requested path if authorized.
    """
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
        self.send_error(ex.code, message=ex.message, explain=ex.explain)
      except OSError as ex:
        self.send_error(500,
                        message='Internal Server Error',
                        explain=f'Internal error: {ex}\n')

  def do_PUT(self):
    """Handle PUT requests with authorization check.

    Creates or overwrites the file at the requested path if authorized.
    """
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
        self.send_error(ex.code, message=ex.message, explain=ex.explain)
      except Exception as ex:
        self.send_error(500,
                        message='Internal Server Error',
                        explain=f'Internal error: {ex}\n')


class ClassWrapper:
  """Wraps a class to inject constructor keyword arguments as attributes.

  When called, creates a new instance with the wrapped class and updates its
  __dict__ with the stored kwargs before calling __init__. Also intercepts
  __setattr__ to store attribute assignments in _kwargs.

  PRIVATE_ATTRIBUTES: Set of attribute names that are not stored in _kwargs.
  """

  PRIVATE_ATTRIBUTES = {'_cls', '_kwargs'}

  # Attribute names excluded from _kwargs storage.

  def __init__(self, cls, **kwargs):
    """Initialize the wrapper with a class and keyword arguments.

    Args:
      cls: The class to wrap.
      **kwargs: Keyword arguments to inject into wrapped instances.
    """
    self._cls = cls
    self._kwargs = kwargs

  def __setattr__(self, name, value):
    """Set an attribute, storing non-private ones in _kwargs.

    Args:
      name: The attribute name.
      value: The value to set.
    """
    sdict = super().__getattribute__('__dict__')
    sdict[name] = value
    if name not in ClassWrapper.PRIVATE_ATTRIBUTES:
      sdict['_kwargs'][name] = value

  def __call__(self, *args, **kwargs):
    """Create and initialize a new instance of the wrapped class.

    The instance's __dict__ is updated with stored kwargs before __init__
    is called, allowing http.server.test() to inject data.

    Args:
      *args: Positional arguments for the wrapped class __init__.
      **kwargs: Keyword arguments for the wrapped class __init__.

    Returns:
      A new instance of the wrapped class.
    """
    obj = self._cls.__new__(self._cls, *args, **kwargs)
    # Add ctor supplied data and data planted by the http.server.test() API
    # (talking about bad API design).
    obj.__dict__.update(self._kwargs)
    obj.__init__(*args, **kwargs)

    return obj


if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Simple HTTP Server For Testing')
  parser.add_argument('--bind',
                      default='0.0.0.0',
                      help='Specify alternate bind address')
  parser.add_argument('--port', type=int, default=8000, help='Specify alternate port')
  parser.add_argument('--protocol',
                      default='HTTP/1.1',
                      help='Conform to this HTTP version')
  parser.add_argument(
      '--auth',
      help='The authentication to be used when performing HTTP write operations')

  args = parser.parse_args()

  # Wrap the handler and server classes, to allow adding the args (and also because
  # the http.server.test() API plants data inside the class global namespace!
  req_handler = ClassWrapper(HTTPRequestHandler, _args=args)
  server_class = ClassWrapper(http.server.ThreadingHTTPServer)

  http.server.test(HandlerClass=req_handler,
                   ServerClass=server_class,
                   port=args.port,
                   bind=args.bind,
                   protocol=args.protocol)
