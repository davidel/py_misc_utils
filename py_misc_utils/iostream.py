"""Stream wrapper for file descriptors and socket-like objects.

This module provides IOStream, a wrapper that normalizes read/write access
across different types of file-like objects (files, sockets, etc.) by
detecting available methods at construction time.
"""

import functools
import os


class IOStream:
  """Wrapper that normalizes read/write operations on file-like objects.

  Detects whether the underlying object has write/read methods (like files
  and sockets) or uses os.write/os.read (like raw file descriptors), and
  exposes a consistent .write() and .read() interface.

  Attributes:
    write: A callable for writing data.
    read: A callable for reading data.
  """

  def __init__(self, fd):
    """Initialize the IOStream wrapping a file descriptor or file-like object.

    Args:
      fd: A file descriptor (int), file-like object with write/read methods,
        or socket-like object with send/recv methods.
    """
    if (wfn := getattr(fd, 'write', None)) is not None:
      self.write = wfn
    elif (wfn := getattr(fd, 'send', None)) is not None:
      self.write = wfn
    else:
      self.write = functools.partial(os.write, fd)

    if (rfn := getattr(fd, 'read', None)) is not None:
      self.read = rfn
    elif (rfn := getattr(fd, 'recv', None)) is not None:
      self.read = rfn
    else:
      self.read = functools.partial(os.read, fd)
