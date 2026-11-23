"""A file-like object backed by a streaming response.

This module provides :class:`StreamedFile`, which wraps a streaming iterable
response into a seekable, readable file-like interface backed by a temporary
file. Data is consumed from the stream in a background thread, allowing reads
to proceed as data arrives, with proper blocking semantics for reads ahead of
the stream position.
"""

import os
import tempfile
import threading

from . import alog
from . import assert_checks as tas
from . import fin_wrap as fw


class StreamedFile:
  """A seekable, readable file-like object fed by a streaming response.

  The stream is consumed by a background daemon thread into a temp file.
  Reads block until the requested data is available or the stream completes
  or the file is closed.

  Attributes:
    _resp: The source iterable of data chunks.
    _lock: Protects internal state.
    _cond: Condition variable for blocking reads.
    _tempfile: The backing temporary file.
    _offset: The current read position.
    _size: The total bytes written so far.
    _completed: Whether the stream is fully consumed.
    _closed: Whether the file has been closed.
    _thread: The background streaming thread.
  """

  def __init__(self, resp):
    """Initialize the StreamedFile from a streaming response.

    Args:
      resp: An iterable yielding data chunks (e.g. a response stream).
    """
    self._resp = resp
    self._lock = threading.Lock()
    self._cond = threading.Condition(lock=self._lock)

    tmpfile = tempfile.TemporaryFile()
    fw.fin_wrap(self, '_tempfile', tmpfile, finfn=tmpfile.close)

    self._offset = 0
    self._size = 0
    self._completed = False
    self._closed = False
    self._thread = threading.Thread(target=self._stream, daemon=True)
    self._thread.start()

  def _stream(self):
    """Consume the response stream, writing data to the backing temp file."""
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
    """Block until the stream is fully consumed or the file is closed."""
    with self._lock:
      while not (self._completed or self._closed):
        self._cond.wait()

  def close(self):
    """Close the file, waiting for the streaming thread to finish."""
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
    """Whether the file is closed.

    Returns:
      True if the file has been closed.
    """
    return self._tempfile is None

  def seek(self, pos, whence=os.SEEK_SET):
    """Move the file position indicator.

    Args:
      pos: The position to seek to.
      whence: The reference point (SEEK_SET, SEEK_CUR, or SEEK_END).

    Returns:
      The new absolute offset.
    """
    if whence == os.SEEK_SET:
      offset = pos
    elif whence == os.SEEK_CUR:
      offset = self._offset + pos
    elif whence == os.SEEK_END:
      self._wait_completed()
      offset = self._size + pos
    else:
      alog.xraise(ValueError, f'Invalid seek mode: {whence}')

    if offset > 0:
      if whence != os.SEEK_END:
        self._wait_completed()
      tas.check_le(offset, self._size, msg=f'Offset out of range')

    tas.check_ge(offset, 0, msg=f'Offset out of range')

    self._offset = offset

    return offset

  def tell(self):
    """Return the current file position.

    Returns:
      The current offset.
    """
    return self._offset

  def _read(self, offset, size, adj_offset):
    """Read data at offset, blocking until the data is available.

    Args:
      offset: The absolute offset to read from.
      size: The number of bytes to read.
      adj_offset: If True, advance the current position after reading.

    Returns:
      The bytes read.
    """
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
    """Read up to size bytes from the current position.

    Args:
      size: Number of bytes to read; -1 to read all available.

    Returns:
      The bytes read.
    """
    with self._lock:
      return self._read(self._offset, size, True)

  def read1(self, size=-1):
    """Read up to size bytes (alias for read).

    Args:
      size: Number of bytes to read.

    Returns:
      The bytes read.
    """
    return self.read(size=size)

  def peek(self, size=0):
    """Return bytes from the current position without advancing it.

    Args:
      size: Maximum number of bytes to peek.

    Returns:
      The peeked bytes (may be fewer than requested).
    """
    with self._lock:
      size = min(size, max(1, self._size - self._offset))

      return self._read(self._offset, size, False) if size > 0 else b''

  def pread(self, offset, size):
    """Read bytes at an absolute offset without changing the position.

    Args:
      offset: The absolute offset to read from.
      size: Number of bytes to read.

    Returns:
      The bytes read.
    """
    with self._lock:
      return self._read(offset, size, False)

  def flush(self):
    """No-op; flushing is not applicable to this read-only file."""
    pass

  def readable(self):
    """Whether the file is readable.

    Returns:
      True if the file is not closed.
    """
    return not self.closed

  def seekable(self):
    """Whether the file is seekable.

    Returns:
      True if the file is not closed.
    """
    return not self.closed

  def writable(self):
    """Whether the file is writable.

    Returns:
      Always False for StreamedFile.
    """
    return False

  def __enter__(self):
    """Return the file itself when entering a context.

    Returns:
      The StreamedFile instance.
    """
    return self

  def __exit__(self, *exc):
    """Close the file when exiting a context.

    Args:
      *exc: Exception information if an exception occurred.

    Returns:
      False (exceptions are not suppressed).
    """
    self.close()

    return False
