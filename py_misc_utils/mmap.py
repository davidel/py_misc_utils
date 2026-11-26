"""Memory-mapped file view utilities.

This module provides :func:`file_view` which creates a read-only memory-mapped
view of a file, exposing it as a memoryview for efficient access without
loading the entire file into process memory.
"""

import mmap
import os

from . import osfd


def file_view(path):
  """Create a read-only memory-mapped view of a file.

  Args:
    path: Filesystem path to the file to memory-map.

  Returns:
    A memoryview over the memory-mapped file contents. The underlying mmap
    object is kept alive by the memoryview's buffer protocol and is released
    when the memoryview is garbage collected.
  """
  with osfd.OsFd(path, os.O_RDONLY) as fd:
    mm = mmap.mmap(fd, 0, access=mmap.ACCESS_READ)

  # We can close the fd, but we cannot close the mmap. When the memoryview
  # will be garbage collected, the buffer protocol used by memoryview will
  # decref the mmap object which will be automatically released.
  return memoryview(mm)
