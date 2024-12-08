import os
import tempfile
import threading

from . import alog
from . import assert_checks as tas
from . import fin_wrap as fw


class StreamedFile:

  def __init__(self, resp):
    self._resp = resp
    self._lock = threading.Lock()
    self._cond = threading.Condition(lock=self._lock)

    tmpfile = tempfile.TemporaryFile()
    fw.fin_wrap(self, '_tempfile', tmpfile, finfn=tmpfile.close)

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

  def _wait_completed(self):
    with self._lock:
      while not (self._completed or self._closed):
        self._cond.wait()

  def close(self):
    with self._lock:
      self._closed = True
      while not self._completed:
        self._cond.wait()

    self._thread.join()

    with self._lock:
      tempfile = self._tempfile
      if tempfile is not None:
        fw.fin_wrap(self, '_tempfile', None)

    if tempfile is not None:
      tempfile.close()

  @property
  def closed(self):
    return self._tempfile is None

  def seek(self, pos, whence=os.SEEK_SET):
    if whence == os.SEEK_SET:
      offset = pos
    elif whence == os.SEEK_CUR:
      offset = self._offset + pos
    elif whence == os.SEEK_END:
      self._wait_completed()
      offset = self._size + pos
    else:
      alog.xraise(ValueError, f'Invalid seek mode: {whence}')

    tas.check_ge(offset, 0, msg=f'Offset out of range')

    self._offset = offset

    return offset

  def tell(self):
    return self._offset

  def _read(self, offset, size, adj_offset):
    while not (self._completed or self._closed or
               (size >= 0 and self._size >= offset + size)):
      self._cond.wait()

    available = self._size - offset
    to_read = min(size, available) if size >= 0 else available
    if not self._closed and to_read > 0:
      self._tempfile.seek(offset)
      data = self._tempfile.read(to_read)
      if adj_offset:
        self._offset += len(data)
    else:
      data = b''

    return data

  def read(self, size=-1):
    with self._lock:
      return self._read(self._offset, size, True)

  def read1(self, size=-1):
    return self.read(size=size)

  def peek(self, size=0):
    with self._lock:
      return self._read(self._offset, size, False)

  def pread(self, offset, size):
    with self._lock:
      return self._read(offset, size, False)

  def flush(self):
    pass

  def readable(self):
    return not self.closed

  def seekable(self):
    return not self.closed

  def writable(self):
    return False

  def __enter__(self):
    return self

  def __exit__(self, *exc):
    self.close()

    return False

