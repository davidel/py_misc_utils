import os
import requests
import sys
import tempfile
import threading

from . import alog
from . import assert_checks as tas
from . import fin_wrap as fw
from . import http_utils as hu
from . import utils as ut


class Streamer:

  def __init__(self, resp):
    self._resp = resp
    self._lock = threading.Lock()
    self._cond = threading.Condition(lock=self._lock)
    self._tempfile = tempfile.TemporaryFile()
    self._offset = 0
    self._size = 0
    self._completed = False
    self._closed = False
    self._thread = threading.Thread(target=self._stream)
    self._thread.start()

  def _stream(self):
    for data in self._resp:
      with self._lock:
        self._tempfile.seek(self._size)
        self._tempfile.write(data)
        self._size += len(data)
        self._cond.notify_all()
        if self._closed:
          break

    with self._lock:
      self._completed = True
      self._cond.notify_all()

  def close(self):
    with self._lock:
      self._closed = True
      while not self._completed:
        self._cond.wait()

    self._thread.join()

    with self._lock:
      if self._tempfile is not None:
        self._tempfile.close()
        self._tempfile = None

  def read(self, size=-1):
    with self._lock:
      while not (self._completed or self._closed or
                 (size >= 0 and self._size >= self._offset + size)):
        self._cond.wait()

      available = self._size - self._offset
      to_read = min(size, available) if size >= 0 else available
      if not self._closed and to_read > 0:
        self._tempfile.seek(self._offset)
        data = self._tempfile.read(to_read)
        self._offset += len(data)
      else:
        data = b''

      return data


class StreamUrl:

  def __init__(self, url, headers=None, auth=None, chunk_size=None, allow_ranges=None,
               **kwargs):
    chunk_size = ut.value_or(chunk_size, 16 * 1024**2)
    allow_ranges = ut.value_or(allow_ranges, True)

    req_headers = headers.copy() if headers else dict()
    if auth:
      req_headers[hu.AUTHORIZATION] = auth

    alog.debug(f'Opening "{url}" with {req_headers}')

    resp = requests.get(url, headers=req_headers, stream=True)
    resp.raise_for_status()

    self._url = url
    self._headers = req_headers
    self._chunk_size = chunk_size

    if (allow_ranges and hu.support_ranges(resp.headers) and
        (length := hu.content_length(resp.headers)) is not None):
      self._length = length
      self._offset = 0
      self._etag = hu.etag(resp.headers)
      self._streamer = None
    else:
      streamer = Streamer(resp.iter_content(chunk_size=chunk_size))
      fw.fin_wrap(self, '_streamer', streamer, finfn=streamer.close)

    self._buffer = self._next_chunk()

  def _next_chunk(self, size_hint=0):
    if self._streamer is not None:
      data = self._streamer.read(size=max(size_hint, self._chunk_size))

      return memoryview(data) if data else None
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

