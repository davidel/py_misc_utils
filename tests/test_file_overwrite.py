"""Tests for py_misc_utils.file_overwrite.

FileOverwrite writes to a temp file in the destination directory and
atomically replaces the destination on clean exit. On exception, the
temp file is discarded and the original destination is left untouched.
Tests pin: replace on clean exit, discard on exception, absence of
temp-file leftovers, mode forwarding, and the non-filesystem-object
branch (the object itself is returned, with no filesystem effects).
"""

import os

import pytest

from py_misc_utils import file_overwrite as fow


def test_write_and_replace(tmp_path):
  dest = os.path.join(tmp_path, 'dest.txt')

  with fow.FileOverwrite(dest) as fd:
    fd.write('content-A')

  with open(dest, 'r') as f:
    assert f.read() == 'content-A'


def test_overwrite_existing(tmp_path):
  dest = os.path.join(tmp_path, 'dest.txt')
  with open(dest, 'w') as f:
    f.write('old')

  with fow.FileOverwrite(dest) as fd:
    fd.write('new')

  with open(dest, 'r') as f:
    assert f.read() == 'new'


def test_exception_leaves_original_intact(tmp_path):
  dest = os.path.join(tmp_path, 'dest.txt')
  with open(dest, 'w') as f:
    f.write('orig')

  with pytest.raises(RuntimeError), fow.FileOverwrite(dest) as fd:
    fd.write('partial junk')
    raise RuntimeError('boom')

  with open(dest, 'r') as f:
    assert f.read() == 'orig'


def test_exception_no_temp_files_left_behind(tmp_path):
  dest = os.path.join(tmp_path, 'dest.txt')

  with pytest.raises(RuntimeError), fow.FileOverwrite(dest) as fd:
    fd.write('partial junk')
    raise RuntimeError('boom')

  assert os.listdir(tmp_path) == []


def test_mode_forwarding_binary(tmp_path):
  dest = os.path.join(tmp_path, 'dest.bin')

  with fow.FileOverwrite(dest, mode='wb') as fd:
    fd.write(b'\x00\x01\x02')

  with open(dest, 'rb') as f:
    assert f.read() == b'\x00\x01\x02'


def test_non_path_object():

  class Dummy:
    pass

  dummy = Dummy()

  with fow.FileOverwrite(dummy) as fd:
    assert fd is dummy

    # Exceptions are not suppressed by the no-path branch either.
    with pytest.raises(RuntimeError):
      raise RuntimeError('boom')
