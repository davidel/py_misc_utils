import io
import requests
import sys

from . import alog


def next_chunk(resp_iter):
  try:
    return memoryview(next(resp_iter))
  except StopIteration:
    pass


class StreamUrl:

  def __init__(self, url, headers=None, auth=None, chunk_size=1024 * 128, **kwargs):
    req_headers = headers.copy() if headers else dict()
    if auth:
      req_headers['Authorization'] = auth

    alog.debug(f'Opening "{url}" with {req_headers}')

    self._response = requests.get(url, headers=req_headers, stream=True)
    self._response.raise_for_status()
    self._resp_iter = self._response.iter_content(chunk_size=chunk_size)
    self._buffer = next_chunk(self._resp_iter)

  def read(self, size=-1):
    size = size if size >= 0 else sys.maxsize
    iobuf = io.BytesIO()
    while self._buffer is not None and size > 0:
      if size >= len(self._buffer):
        iobuf.write(self._buffer)
        size -= len(self._buffer)
        self._buffer = next_chunk(self._resp_iter)
      else:
        iobuf.write(self._buffer[: size])
        self._buffer = self._buffer[size:]
        break

    return iobuf.getvalue()

