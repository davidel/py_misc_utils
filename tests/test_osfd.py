"""Tests for py_misc_utils.osfd.

OsFd is a context manager wrapping os.open/os.close, optionally removing
the file when the body raises. Tests cover fd usage inside the context,
the closed fd after exit, creation with mode, and the remove-on-error
matrix: default (remove only newly created files), explicit True/False,
and files kept when no error occurred.
"""

import os

import pytest

from py_misc_utils import osfd


def test_open_read(tmp_path):
  """The fd is usable inside the context and closed after exit."""
  path = tmp_path / 'data.txt'
  path.write_bytes(b'payload')

  with osfd.OsFd(str(path), os.O_RDONLY) as fd:
    assert os.read(fd, 16) == b'payload'

  with pytest.raises(OSError):
    os.read(fd, 1)


def test_create_with_mode(tmp_path):
  """O_CREAT honors the mode kwarg (modulo umask)."""
  path = tmp_path / 'new.bin'

  with osfd.OsFd(str(path), os.O_WRONLY | os.O_CREAT, mode=0o600):
    pass

  assert (os.stat(str(path)).st_mode & 0o777) == 0o600


def test_existing_file_kept_on_error(tmp_path):
  """Default remove_on_error keeps a pre-existing file on error."""
  path = tmp_path / 'keep.txt'
  path.write_bytes(b'keep')

  with pytest.raises(RuntimeError), osfd.OsFd(str(path), os.O_RDONLY):
    raise RuntimeError('boom')

  assert path.read_bytes() == b'keep'


def test_new_file_removed_on_error(tmp_path):
  """Default remove_on_error removes a file the context created."""
  path = tmp_path / 'scratch.txt'

  with pytest.raises(RuntimeError), osfd.OsFd(str(path),
                                              os.O_WRONLY | os.O_CREAT,
                                              mode=0o600):
    raise RuntimeError('boom')

  assert not os.path.exists(str(path))


def test_remove_on_error_false_keeps_new_file(tmp_path):
  """remove_on_error=False keeps a newly created file on error."""
  path = tmp_path / 'kept.bin'

  with pytest.raises(RuntimeError), osfd.OsFd(str(path),
                                              os.O_WRONLY | os.O_CREAT,
                                              remove_on_error=False,
                                              mode=0o600):
    raise RuntimeError('boom')

  assert os.path.exists(str(path))


def test_remove_on_error_true_removes_existing(tmp_path):
  """remove_on_error=True removes even a pre-existing file on error."""
  path = tmp_path / 'existing.txt'
  path.write_bytes(b'existing')

  with pytest.raises(RuntimeError), osfd.OsFd(str(path),
                                              os.O_RDONLY,
                                              remove_on_error=True):
    raise RuntimeError('boom')

  assert not os.path.exists(str(path))


def test_new_file_kept_without_error(tmp_path):
  """A newly created file survives a clean context exit."""
  path = tmp_path / 'clean.bin'

  with osfd.OsFd(str(path), os.O_WRONLY | os.O_CREAT, mode=0o600) as fd:
    os.write(fd, b'clean')

  assert path.read_bytes() == b'clean'


def test_open_missing_file_raises(tmp_path):
  """Opening a missing file without O_CREAT raises FileNotFoundError."""
  with pytest.raises(FileNotFoundError), osfd.OsFd(str(tmp_path / 'nope.bin'),
                                                   os.O_RDONLY):
    pass
