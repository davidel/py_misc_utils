import requests
import sys

from . import alog
from . import assert_checks as tas
from . import http_utils as hu
from . import utils as ut


class StreamUrl:

  def __init__(self, url, headers=None, auth=None, chunk_size=1024 * 256, **kwargs):
    req_headers = headers.copy() if headers else dict()
    if auth:
      req_headers[hu.AUTHORIZATION] = auth

    alog.debug(f'Opening "{url}" with {req_headers}')

    resp = requests.get(url, headers=req_headers, stream=True)
    resp.raise_for_status()

    self._url = url
    self._headers = req_headers
    self._chunk_size = chunk_size

    if (hu.support_ranges(resp.headers) and
        (length := hu.content_length(resp.headers)) is not None):
      self._length = length
      self._offset = 0
      self._etag = resp.headers.get(hu.ETAG)
      self._resp_iter = None
    else:
      self._resp_iter = resp.iter_content(chunk_size=chunk_size)

    self._buffer = self._next_chunk()

  def _next_chunk(self, size_hint=0):
    if self._resp_iter is not None:
      try:
        return memoryview(next(self._resp_iter))
      except StopIteration:
        pass
    else:
      size = min(max(self._chunk_size, size_hint), self._length - self._offset)
      if size > 0:
        req_headers = self._headers.copy()
        hu.add_range(req_headers, self._offset, self._offset + size - 1)

        resp = requests.get(self._url, headers=req_headers)
        resp.raise_for_status()
        data = resp.content

        tas.check_eq(self._etag, resp.headers.get(hu.ETAG),
                     msg=f'Expired content at "{self._url}"')
        tas.check_eq(len(data), size,
                     msg=f'Invalid read size ({len(data)} vs. {size}) at "{self._url}"')

        self._offset += len(data)

        return memoryview(data)

  def read(self, size=-1):
    size, data = size if size >= 0 else sys.maxsize, []
    while self._buffer is not None and size > 0:
      if size >= len(self._buffer):
        data.append(self._buffer)
        size -= len(self._buffer)
        self._buffer = self._next_chunk(size_hint=size)
      else:
        data.append(self._buffer[: size])
        self._buffer = self._buffer[size:]
        break

    return b''.join(data)

