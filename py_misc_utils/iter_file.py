import bisect
import os

from . import alog
from . import assert_checks as tas


class IterFile:

  def __init__(self, data_gen):
    offset, blocks, offsets = 0, [], []
    for block in data_gen:
      blocks.append(block)
      offsets.append(offset)
      offset += len(block)

    offsets.append(offset)

    self._blocks = tuple(blocks)
    self._offsets = tuple(offsets)
    self._size = offset
    self._offset = 0
    self._block = None
    self._block_offset = 0

  def close(self):
    self._blocks = None

  @property
  def closed(self):
    return self._blocks is None

  def seek(self, pos, whence=os.SEEK_SET):
    if whence == os.SEEK_SET:
      offset = pos
    elif whence == os.SEEK_CUR:
      offset = self._offset + pos
    elif whence == os.SEEK_END:
      offset = self._size + pos
    else:
      alog.xraise(ValueError, f'Invalid seek mode: {whence}')

    tas.check_le(offset, self._size, msg=f'Offset out of range')
    tas.check_ge(offset, 0, msg=f'Offset out of range')

    self._offset = offset

    return offset

  def tell(self):
    return self._offset

  def _ensure_buffer(self, offset):
    boffset = offset - self._block_offset
    if self._block is None or boffset < 0 or boffset >= len(self._block):
      pos = bisect.bisect(self._offsets, offset) - 1
      if pos >= len(self._blocks):
        self._block_offset = self._size
        self._block = b''
      else:
        self._block_offset = self._offsets[pos]
        self._block = memoryview(self._blocks[pos])

      boffset = offset - self._block_offset

    return boffset

  def read(self, size=-1):
    if size < 0:
      rsize = self._size - self._offset
    else:
      rsize = min(size, self._size - self._offset)

    parts = []
    while rsize > 0:
      boffset = self._ensure_buffer(self._offset)

      csize = min(rsize, len(self._block) - boffset)
      parts.append(self._block[boffset: boffset + csize])
      self._offset += csize
      rsize -= csize

    return b''.join(parts)

  def read1(self, size=-1):
    return self.read(size=size)

  def peek(self, size=0):
    if size > 0:
      boffset = self._ensure_buffer(self._offset)
      csize = min(size, len(self._block) - boffset)

      return self._block[boffset: boffset + csize].tobytes()

    return b''

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


