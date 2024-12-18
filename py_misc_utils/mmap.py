import mmap
import os

from . import alog
from . import osfd


def file_view(path):
  with osfd.OsFd(path, os.O_RDONLY) as fd:
    mm = mmap.mmap(fd, 0, access=mmap.ACCESS_READ)

  # We can close the fd, but we cannot close the mmap. When the memoryview
  # will be garbage collected, the buffer protocol used by memoryview will
  # decref the mmap object which will be automatically released.
  return memoryview(mm)

