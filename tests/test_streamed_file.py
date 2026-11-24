"""Tests for py_misc_utils.streamed_file.

StreamedFile wraps a streaming iterable into a seekable, readable file-like
object backed by a temp file, with a feeder thread. Tests cover basic reads,
partial reads, peek/pread (position-preserving), seeking (SET/CUR/END and
out-of-range errors), close semantics, and the blocking behavior of reads
that run ahead of the stream.
"""

import os
import threading

import pytest

from py_misc_utils import streamed_file as sf


def _make(chunks):
  """Create a StreamedFile over a list of chunks."""
  return sf.StreamedFile(iter(chunks))


def test_read_all():
  """read(-1) returns the full stream content once it is available."""
  with _make([b'hello ', b'world']) as f:
    assert f.read() == b'hello world'


def test_read_sizes():
  """read(n) returns at most n bytes and advances the position."""
  with _make([b'abc', b'def', b'ghi']) as f:
    assert f.read(4) == b'abcd'
    assert f.tell() == 4
    assert f.read(2) == b'ef'
    assert f.tell() == 6
    assert f.read(10) == b'ghi'
    assert f.read(10) == b''


def test_read_blocks_until_data_available():
  """A read running ahead of the stream blocks until data arrives."""
  gate = threading.Event()
  released = threading.Event()

  def gen():
    yield b'first'
    gate.wait(timeout=5)
    yield b'second'

  f = sf.StreamedFile(gen())
  try:
    assert f.read(5) == b'first'

    result = {}

    def reader():
      result['data'] = f.read(6)

    t = threading.Thread(target=reader)
    t.start()
    # The reader must stay blocked until the second chunk is released.
    t.join(timeout=0.3)
    assert t.is_alive()
    gate.set()
    t.join(timeout=5)
    assert not t.is_alive()
    assert result['data'] == b'second'
    released.set()
  finally:
    gate.set()
    if not released.is_set():
      f.close()


def test_peek_does_not_advance():
  """peek returns data from the current position without moving it."""
  with _make([b'abcdef']) as f:
    assert f.peek(3) == b'abc'
    assert f.tell() == 0
    assert f.read(3) == b'abc'
    assert f.peek(0) == b''


def test_pread_absolute():
  """pread reads at an absolute offset without changing the position."""
  with _make([b'0123456789']) as f:
    assert f.pread(4, 3) == b'456'
    assert f.tell() == 0
    assert f.read(2) == b'01'
    assert f.pread(8, 100) == b'89'


def test_seek_modes():
  """seek supports SEEK_SET, SEEK_CUR and SEEK_END."""
  with _make([b'0123456789']) as f:
    assert f.seek(4) == 4
    assert f.seek(2, os.SEEK_CUR) == 6
    assert f.seek(-3, os.SEEK_END) == 7
    assert f.read() == b'789'


def test_seek_out_of_range():
  """seek beyond the stream size or before zero raises an error."""
  with _make([b'0123456789']) as f:
    with pytest.raises(AssertionError):
      f.seek(11)
    with pytest.raises(AssertionError):
      f.seek(-1)


def test_seek_end_waits_for_completion():
  """SEEK_END waits for the full stream to know the final size."""
  gate = threading.Event()

  def gen():
    yield b'abc'
    gate.wait(timeout=5)
    yield b'def'

  f = sf.StreamedFile(gen())
  try:
    result = {}

    def seeker():
      result['pos'] = f.seek(0, os.SEEK_END)

    t = threading.Thread(target=seeker)
    t.start()
    t.join(timeout=0.3)
    assert t.is_alive()
    gate.set()
    t.join(timeout=5)
    assert not t.is_alive()
    assert result['pos'] == 6
  finally:
    gate.set()
    f.close()


def test_forward_seek_waits_for_completion():
  """Forward seeks wait for the whole stream, not just the seek target.

  Probed behavior (SCRATCH/tools/probe_streamed_file.py): any seek with
  offset > 0 blocks until the stream is fully consumed, even when the
  target position is already available. Close interrupts the wait.
  """
  gate = threading.Event()

  def gen():
    yield b'abc'
    gate.wait(timeout=5)
    yield b'def'

  f = sf.StreamedFile(gen())
  try:
    result = {}

    def seeker():
      result['pos'] = f.seek(2)

    t = threading.Thread(target=seeker)
    t.start()
    t.join(timeout=0.3)
    assert t.is_alive()
    gate.set()
    t.join(timeout=5)
    assert not t.is_alive()
    assert result['pos'] == 2
    assert f.read(1) == b'c'
  finally:
    gate.set()
    f.close()


def test_seek_end_beyond_size_rejected():
  """A positive SEEK_END offset beyond the stream size raises an error.

  Verified by reading seek(): the range check (check_le against the
  stream size) applies to all whence modes; END only differs in that
  it waits for completion before computing the offset.
  """
  with _make([b'0123456789']) as f:
    with pytest.raises(AssertionError):
      f.seek(2, os.SEEK_END)
    assert f.seek(-3, os.SEEK_END) == 7


def test_file_like_flags():
  """readable/seekable follow closed state; writable is always False."""
  f = _make([b'data'])
  assert f.readable()
  assert f.seekable()
  assert not f.writable()
  f.close()
  assert f.closed
  assert not f.readable()
  assert not f.seekable()


def test_flush_noop():
  """flush() is a no-op on a read-only stream."""
  with _make([b'data']) as f:
    f.flush()


def test_close_idempotent():
  """close() may be called multiple times safely."""
  f = _make([b'data'])
  f.close()
  f.close()
  assert f.closed


def test_read_empty_stream():
  """An empty stream yields empty reads and size-zero seeks."""
  with _make([]) as f:
    assert f.read() == b''
    assert f.seek(0, os.SEEK_END) == 0
    assert f.read(5) == b''
