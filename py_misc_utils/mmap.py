import functools
import mmap
import os

from . import alog
from . import fin_wrap as fw
from . import mirror_from as mrf


class MMap:

  READ = 1
  WRITE = 1 << 1

  def __init__(self, path, mode):
    fd = None
    try:
      fd = os.open(path, os.O_RDWR if mode & self.WRITE else os.O_RDONLY)

      mm_access = 0
      if mode & self.WRITE:
        mm_access |= mmap.ACCESS_WRITE
      if mode & self.READ:
        mm_access |= mmap.ACCESS_READ
      mm = mmap.mmap(fd, 0, access=mm_access)
    except:
      if fd is not None:
        os.close(fd)
      raise

    self._path = path
    self._mode = mode
    self._fd = fd

    finfn = functools.partial(self._cleaner, fd, mm, mode & self.WRITE)
    fw.fin_wrap(self, '_mm', mm, finfn=finfn)
    mrf.mirror_all(self._mm, self, name='mm')

  @classmethod
  def _cleaner(cls, fd, mm, flush):
    if flush:
      mm.flush()
    mm.close()
    os.close(fd)

  def close(self):
    mm = self._mm
    if mm is not None:
      mrf.unmirror(self, name='mm')
      fw.fin_wrap(self, '_mm', None)
      self._cleaner(self._fd, mm, self._mode & self.WRITE)

  def view(self):
    return memoryview(self._mm)

  def __enter__(self):
    return self

  def __exit__(self, *exc):
    self.close()

    return False

