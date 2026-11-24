"""Tests for py_misc_utils.writeback_file.WritebackFile.

Expectations pinned to the observed behavior (verified by probe scripts
before writing): WritebackFile mirrors the wrapped stream's read
attributes, invokes the writeback callback with the stream on close()
(and on clean context exit, which also closes the stream), and just
closes the stream when the writeback is skipped. detach() discards the
writeback; an exception inside the context skips the writeback.
"""

import io

from py_misc_utils import writeback_file as wbf


def test_mirror_read_methods():
  """Wrapped-stream read attributes are mirrored onto the wrapper."""
  src = io.BytesIO(b'hello')
  wfile = wbf.WritebackFile(src, lambda stream: None)

  assert wfile.read(5) == b'hello'
  wfile.close(run_writeback=False)


def test_close_runs_writeback():
  """close() invokes the writeback callback with the stream."""
  src = io.BytesIO(b'data')
  collected = []
  wfile = wbf.WritebackFile(src, lambda stream: collected.append(stream.getvalue()))

  wfile.close()

  assert collected == [b'data']


def test_close_without_writeback():
  """close(run_writeback=False) just closes the stream."""
  src = io.BytesIO(b'data')
  collected = []
  wfile = wbf.WritebackFile(src, lambda stream: collected.append(1))

  wfile.close(run_writeback=False)

  assert collected == []
  assert src.closed


def test_detach_discards_writeback():
  """detach() is an alias for close(run_writeback=False)."""
  src = io.BytesIO(b'data')
  collected = []
  wfile = wbf.WritebackFile(src, lambda stream: collected.append(1))

  wfile.detach()

  assert collected == []
  assert src.closed


def test_context_exit_runs_writeback():
  """A clean context exit runs the writeback."""
  src = io.BytesIO(b'data')
  collected = []

  with wbf.WritebackFile(src,
                         lambda stream: collected.append(stream.getvalue())) as wfile:
    wfile.read(2)

  assert collected == [b'data']


def test_exception_skips_writeback():
  """An exception inside the context skips the writeback (stream still
  closed)."""
  src = io.BytesIO(b'data')
  collected = []

  try:
    with wbf.WritebackFile(src, lambda stream: collected.append(1)):
      raise RuntimeError('boom')
  except RuntimeError:
    pass

  assert collected == []
  assert src.closed


def test_double_close_is_noop():
  """Closing twice does not re-run the writeback."""
  src = io.BytesIO(b'data')
  count = []

  wfile = wbf.WritebackFile(src, lambda stream: count.append(1))
  wfile.close()
  wfile.close()

  assert count == [1]
