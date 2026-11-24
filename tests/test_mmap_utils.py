"""Tests for py_misc_utils.mmap.

file_view() creates a read-only memory-mapped view of a file returned as
a memoryview whose backing mmap stays alive via the buffer protocol.
Tests cover content access across the view, length, read-only
 enforcement, and view validity after the function returns (the file
descriptor is already closed at that point).
"""

import pytest

from py_misc_utils import mmap as mmu


def test_file_view_contents(tmp_path):
  """The view exposes the full file content and length."""
  path = tmp_path / 'data.bin'
  payload = b'0123456789' * 100
  path.write_bytes(payload)

  view = mmu.file_view(str(path))

  assert len(view) == len(payload)
  assert bytes(view[:10]) == b'0123456789'
  assert bytes(view[-3:]) == b'789'


def test_file_view_readonly(tmp_path):
  """The view is read-only: writes raise."""
  path = tmp_path / 'data.bin'
  path.write_bytes(b'hello')

  view = mmu.file_view(str(path))

  with pytest.raises(TypeError):
    view[0] = 65


def test_file_view_alive_after_close(tmp_path):
  """The view stays valid after file_view() returned (fd already closed)."""
  path = tmp_path / 'data.bin'
  path.write_bytes(b'abcdef')

  view = mmu.file_view(str(path))

  # The fd is closed inside file_view(); the mmap must survive via the
  # memoryview buffer protocol reference.
  assert bytes(view) == b'abcdef'


def test_file_view_empty_file(tmp_path):
  """Mapping an empty file raises ValueError (mmap restriction)."""
  path = tmp_path / 'empty.bin'
  path.write_bytes(b'')

  with pytest.raises(ValueError):
    mmu.file_view(str(path))
