"""Tests for py_misc_utils.fs_base.

Exercises the DirEntry namedtuple and the abstract FsBase interface.
"""

import pytest

from py_misc_utils import fs_base as fsb


def test_direntry_fields():
  entry = fsb.DirEntry(name='foo',
                       st_mode=0o644,
                       st_size=10,
                       st_ctime=1.0,
                       st_mtime=2.0,
                       path='/foo',
                       etag=None)
  assert entry.name == 'foo'
  assert entry.st_mode == 0o644
  assert entry.st_size == 10
  assert entry.etag is None


def test_fsbase_is_abstract():
  with pytest.raises(TypeError):
    fsb.FsBase()
