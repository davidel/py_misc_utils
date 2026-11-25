"""Tests for py_misc_utils.mmap.

file_view() creates a read-only memory-mapped view of a file returned as
a memoryview whose backing mmap stays alive via the buffer protocol.
Tests cover content access across the view, length, read-only
 enforcement, and view validity after the function returns (the file
descriptor is already closed at that point).
"""

import os

import pytest

from py_misc_utils import mmap as mmu


def test_file_view_contents(tmp_path):
  """The view exposes the full file content and length."""
  path = os.path.join(tmp_path, 'data.bin')
  payload = b'0123456789' * 100
  with open(path, 'wb') as f:
    f.write(payload)

  view = mmu.file_view(path)

  assert len(view) == len(payload)
  assert bytes(view[:10]) == b'0123456789'
  assert bytes(view[-3:]) == b'789'


def test_file_view_readonly(tmp_path):
  """The view is read-only: writes raise."""
  path = os.path.join(tmp_path, 'data.bin')
  with open(path, 'wb') as f:
    f.write(b'hello')

  view = mmu.file_view(path)

  with pytest.raises(TypeError):
    view[0] = 65


def test_file_view_alive_after_close(tmp_path):
  """The view stays valid after file_view() returned (fd already closed)."""
  path = os.path.join(tmp_path, 'data.bin')
  with open(path, 'wb') as f:
    f.write(b'abcdef')

  view = mmu.file_view(path)

  # The fd is closed inside file_view(); the mmap must survive via the
  # memoryview buffer protocol reference.
  assert bytes(view) == b'abcdef'


def test_file_view_empty_file(tmp_path):
  """Mapping an empty file raises ValueError (mmap restriction)."""
  path = os.path.join(tmp_path, 'empty.bin')
  with open(path, 'wb') as f:
    f.write(b'')

  with pytest.raises(ValueError):
    mmu.file_view(path)
