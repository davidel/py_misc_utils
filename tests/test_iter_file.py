"""Tests for py_misc_utils.iter_file.

IterFile presents an iterable of bytes blocks as a read-only, seekable
file-like object; the iterable is fully consumed at construction and an
offset index allows random access. Tests cover reads across block
boundaries, seeking (SET/CUR/END, out-of-range errors), peek/read1,
zero-length blocks (skipped via the bisect index), close semantics and
flag behavior.
"""

import os

import pytest

from py_misc_utils import iter_file as itf


def _make(blocks):
  """Create an IterFile from a list of blocks."""
  return itf.IterFile(iter(blocks))


def test_read_all():
  """read(-1) concatenates every block."""
  with _make([b'hello ', b'world']) as f:
    assert f.read() == b'hello world'


def test_read_sizes_across_blocks():
  """read(n) spans block boundaries and advances tell() accordingly."""
  with _make([b'ab', b'cdef', b'gh']) as f:
    assert f.read(3) == b'abc'
    assert f.tell() == 3
    assert f.read(4) == b'defg'
    assert f.tell() == 7
    assert f.read(10) == b'h'
    assert f.read(1) == b''


def test_read_exact_until_eof():
  """read() with no size returns everything and stays at EOF."""
  with _make([b'abc']) as f:
    assert f.read() == b'abc'
    assert f.tell() == 3
    assert f.read() == b''


def test_seek_and_reread():
  """seek(SET/CUR/END) then read returns data from the new position."""
  with _make([b'0123456789']) as f:
    assert f.seek(4) == 4
    assert f.read(2) == b'45'
    assert f.seek(-3, os.SEEK_CUR) == 3
    assert f.read(2) == b'34'
    assert f.seek(-2, os.SEEK_END) == 8
    assert f.read() == b'89'
    # Seek back to zero and re-read the full content.
    assert f.seek(0) == 0
    assert f.read() == b'0123456789'


def test_seek_out_of_range():
  """seek beyond EOF or before zero raises an AssertionError."""
  with _make([b'0123456789']) as f:
    with pytest.raises(AssertionError):
      f.seek(11)
    with pytest.raises(AssertionError):
      f.seek(-1)
    with pytest.raises(ValueError):
      f.seek(0, 42)


def test_peek_does_not_advance():
  """peek returns data from the current position without moving it."""
  with _make([b'abcdef']) as f:
    assert f.peek(3) == b'abc'
    assert f.tell() == 0
    assert f.read(3) == b'abc'
    assert f.peek(10) == b'def'
    assert f.peek(0) == b''


def test_read1_matches_read():
  """read1 behaves like read (buffered single-block read)."""
  with _make([b'abcd', b'ef']) as f:
    assert f.read1(2) == b'ab'
    assert f.read1(10) == b'cdef'
    assert f.read1(1) == b''


def test_zero_length_blocks_skipped():
  """Zero-length blocks are transparent to reads and seeks.

  The offset index gets duplicate offsets for empty blocks; bisect-right
  lookup lands on the next non-empty block, so reads spanning them work
  (probed: SCRATCH/tools/probe_iter_file_zero_block.py).
  """
  with _make([b'ab', b'', b'cd', b'']) as f:
    assert f.read(3) == b'abc'
    assert f.seek(2) == 2
    assert f.read() == b'cd'
    assert f.seek(0, os.SEEK_END) == 4


def test_writable_false():
  """The stream is read-only: writable() is False."""
  with _make([b'data']) as f:
    assert not f.writable()


def test_flags_and_close():
  """readable/seekable follow close state; closed flips after close()."""
  f = _make([b'data'])
  assert f.readable()
  assert f.seekable()
  f.close()
  assert f.closed
  assert not f.readable()
  assert not f.seekable()
  # close() is idempotent (setting _blocks to None again).
  f.close()


def test_read_after_close():
  """read() after close() raises TypeError (blocks were released)."""
  f = _make([b'data'])
  f.close()
  with pytest.raises(TypeError):
    f.read()


def test_empty_stream():
  """An empty iterable yields an empty, seekable stream."""
  with _make([]) as f:
    assert f.read() == b''
    assert f.seek(0, os.SEEK_END) == 0
    assert f.tell() == 0
