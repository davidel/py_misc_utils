"""File-like object for iterating over data blocks with seek support.

This module provides IterFile, a read-only file-like wrapper that presents
an iterable of data blocks as a single seekable stream. It maintains an
internal index of block offsets for efficient random access.
"""

import bisect
import os

from . import alog
from . import assert_checks as tas


class IterFile:
  """Read-only file-like wrapper over an iterable of data blocks.

  Presents an iterable of data blocks as a single seekable byte stream.
  Maintains an internal index of block offsets for efficient random access
  and buffering.

  Example:
    from py_misc_utils import iter_file as itf

    blocks = iter([b'hello ', b'world'])

    with itf.IterFile(blocks) as f:
      print(f.read(8))  # b'hello wo'
      f.seek(2)
      print(f.read(3))  # b'llo'
  """

  def __init__(self, data_gen):
    """Initialize the IterFile from a data generator.

    Reads all blocks from data_gen, building an internal offset index for
    seeking. The entire dataset is held in memory as a tuple of blocks.

    Args:
      data_gen: An iterable yielding bytes-like blocks of data.
    """
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
    """Close the stream, releasing all block references."""
    self._blocks = None

  @property
  def closed(self):
    """Check if the stream is closed.

    Returns:
      True if the stream has been closed, False otherwise.
    """
    return self._blocks is None

  def seek(self, pos, whence=os.SEEK_SET):
    """Set the current read position.

    Args:
      pos: The position to seek to.
      whence: Reference point (os.SEEK_SET, os.SEEK_CUR, or os.SEEK_END).
        Defaults to os.SEEK_SET.

    Returns:
      The new absolute position.

    Raises:
      ValueError: If whence is invalid.
      AssertionError: If the resulting offset is out of range.
    """
    if whence == os.SEEK_SET:
      offset = pos
    elif whence == os.SEEK_CUR:
      offset = self._offset + pos
    elif whence == os.SEEK_END:
      offset = self._size + pos
    else:
      alog.xraise(ValueError, f'Invalid seek mode: {whence}')

    tas.check_le(offset, self._size, msg='Offset out of range')
    tas.check_ge(offset, 0, msg='Offset out of range')

    self._offset = offset

    return offset

  def tell(self):
    """Return the current read position.

    Returns:
      The current byte offset.
    """
    return self._offset

  def _ensure_buffer(self, offset):
    """Ensure the internal block buffer covers the given offset.

    Loads the block containing the offset into self._block if not already
    buffered.

    Args:
      offset: The byte offset to ensure is buffered.

    Returns:
      The byte offset within the current block.
    """
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
    """Read up to size bytes from the current position.

    Args:
      size: Number of bytes to read. If negative, read to end of stream.
        Defaults to -1.

    Returns:
      The bytes read (may be fewer than requested at EOF).
    """
    if size < 0:
      rsize = self._size - self._offset
    else:
      rsize = min(size, self._size - self._offset)

    parts = []
    while rsize > 0:
      boffset = self._ensure_buffer(self._offset)

      csize = min(rsize, len(self._block) - boffset)
      parts.append(self._block[boffset:boffset + csize])
      self._offset += csize
      rsize -= csize

    return b''.join(parts)

  def read1(self, size=-1):
    """Read up to size bytes (same as read()).

    Args:
      size: Number of bytes to read. Defaults to -1.

    Returns:
      The bytes read.
    """
    return self.read(size=size)

  def peek(self, size=0):
    """Peek at up to size bytes without advancing the position.

    Args:
      size: Number of bytes to peek. Defaults to 0.

    Returns:
      The peeked bytes, or b'' if size is 0.
    """
    if size > 0:
      boffset = self._ensure_buffer(self._offset)
      csize = min(size, len(self._block) - boffset)

      return self._block[boffset:boffset + csize].tobytes()

    return b''

  def flush(self):
    """No-op flush (read-only stream)."""

  def readable(self):
    """Check if the stream is readable.

    Returns:
      True if the stream is not closed, False otherwise.
    """
    return not self.closed

  def seekable(self):
    """Check if the stream supports seeking.

    Returns:
      True if the stream is not closed, False otherwise.
    """
    return not self.closed

  def writable(self):
    """Check if the stream supports writing.

    Returns:
      False (read-only stream).
    """
    return False

  def __enter__(self):
    """Enter the context manager.

    Returns:
      self.
    """
    return self

  def __exit__(self, *exc):
    """Exit the context manager, closing the stream.

    Args:
      *exc: Exception info (unused).

    Returns:
      False, to not suppress exceptions.
    """
    self.close()

    return False
