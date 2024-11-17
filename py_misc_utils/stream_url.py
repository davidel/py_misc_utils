import io
import requests
import sys

from . import alog


class StreamUrl:

  def __init__(self, url, headers=None, auth=None, chunk_size=1024 * 128, **kwargs):
    req_headers = headers.copy() if headers else dict()
    if auth:
      req_headers['Authorization'] = auth

    alog.debug(f'Opening "{url}" with {req_headers}')

    resp = requests.get(url, headers=req_headers, stream=True)
    resp.raise_for_status()

    self._url = url
    self._headers = req_headers
    self._chunk_size = chunk_size

    if ('bytes' in resp.headers.get('Accept-Ranges', '') and
        (length := resp.headers.get('Content-Length')) is not None):
      self._length = int(length)
      self._offset = 0
      self._resp_iter = None
    else:
      self._resp_iter = resp.iter_content(chunk_size=chunk_size)

    self._buffer = self._next_chunk()

  def _next_chunk(self):
    if self._resp_iter is not None:
      try:
        return memoryview(next(self._resp_iter))
      except StopIteration:
        pass
    else:
      size = min(self._chunk_size, self._length - self._offset)
      if size > 0:
        req_headers = self._headers.copy()
        req_headers['Range'] = f'bytes={self._offset}-{self._offset + size - 1}'

        resp = requests.get(self._url, headers=req_headers)
        resp.raise_for_status()

        self._offset += size

        return memoryview(resp.content)

  def read(self, size=-1):
    size = size if size >= 0 else sys.maxsize
    iobuf = io.BytesIO()
    while self._buffer is not None and size > 0:
      if size >= len(self._buffer):
        iobuf.write(self._buffer)
        size -= len(self._buffer)
        self._buffer = self._next_chunk()
      else:
        iobuf.write(self._buffer[: size])
        self._buffer = self._buffer[size:]
        break

    return iobuf.getvalue()

