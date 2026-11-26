"""Tests for py_misc_utils.uncompress.

Uncompress is a context manager that passes through files with
unrecognized extensions unchanged and decompresses recognized ones
(.gz/.bz2/.xz) into a temporary directory which is removed on exit.
"""

import bz2
import gzip
import lzma
import os
import tempfile

import pytest

from py_misc_utils import uncompress


def _write(src, blob, original):
  with open(src, 'wb') as fd:
    fd.write(blob)

  return original


def test_uncompress_passthrough_unknown_extension(tmp_path):
  """Unrecognized extensions are passed through unchanged."""
  src = os.path.join(tmp_path, 'data.xyz')
  original = b'plain data'
  with open(src, 'wb') as fd:
    fd.write(original)

  with uncompress.Uncompress(src) as path:
    assert path == src
    with open(path, 'rb') as fd:
      assert fd.read() == original


def test_uncompress_passthrough_no_extension(tmp_path):
  """Files without any extension are passed through unchanged."""
  src = os.path.join(tmp_path, 'data')
  original = b'plain data'
  with open(src, 'wb') as fd:
    fd.write(original)

  with uncompress.Uncompress(src) as path:
    assert path == src
    with open(path, 'rb') as fd:
      assert fd.read() == original


def test_uncompress_tempdir_not_created_for_passthrough(tmp_path):
  """Uncompress creates no temp directory for unrecognized extensions."""
  src = os.path.join(str(tmp_path), 'data.xyz')
  original = b'plain data'
  with open(src, 'wb') as fd:
    fd.write(original)

  with uncompress.Uncompress(src) as path:
    assert path == src
    # No tempdir should be created; the path is the original file.
    assert os.path.dirname(src) == str(tmp_path)


def test_uncompress_gz(tmp_path):
  """gzip files are decompressed into a temporary directory."""
  src = os.path.join(tmp_path, 'file.gz')
  original = b'the payload'
  with open(src, 'wb') as fd:
    fd.write(gzip.compress(original))

  with uncompress.Uncompress(src) as path:
    assert path != src
    with open(path, 'rb') as fd:
      assert fd.read() == original


def test_uncompress_xz(tmp_path):
  """xz files are decompressed into a temporary directory."""
  src = os.path.join(tmp_path, 'file.xz')
  original = b'the payload'
  with open(src, 'wb') as fd:
    fd.write(lzma.compress(original))

  with uncompress.Uncompress(src) as path:
    assert path != src
    with open(path, 'rb') as fd:
      assert fd.read() == original


def test_uncompress_bz2(tmp_path):
  """bz2 files are decompressed into a temporary directory."""
  src = os.path.join(tmp_path, 'file.bz2')
  original = b'the payload'
  with open(src, 'wb') as fd:
    fd.write(bz2.compress(original))

  with uncompress.Uncompress(src) as path:
    assert path != src
    with open(path, 'rb') as fd:
      assert fd.read() == original


def test_uncompress_tempdir_removed_after_use(tmp_path):
  """The temporary decompression directory is removed on exit."""
  src = os.path.join(tmp_path, 'file.gz')
  original = b'the payload'
  with open(src, 'wb') as fd:
    fd.write(gzip.compress(original))

  seen_path = None
  with uncompress.Uncompress(src) as path:
    seen_path = path
    with open(path, 'rb') as fd:
      assert fd.read() == original

  assert not os.path.exists(seen_path)
  assert os.path.exists(src)


def test_uncompress_tempdir_removed_on_error(tmp_path):
  """The tempdir is removed even when the body raises."""
  src = os.path.join(tmp_path, 'file.gz')
  original = b'the payload'
  with open(src, 'wb') as fd:
    fd.write(gzip.compress(original))

  seen_path = None
  try:
    with uncompress.Uncompress(src) as path:
      seen_path = path
      raise RuntimeError('boom')
  except RuntimeError:
    pass

  assert not os.path.exists(seen_path)
  assert os.path.exists(src)


def test_uncompress_tempdir_removed_when_decompression_fails(tmp_path, monkeypatch):
  """The tempdir is removed even when decompression fails inside __enter__.

  A failing decompressor raises out of __enter__, so __exit__ never
  runs; __enter__ must clean up its own temporary directory (leaked
  before the fix - see the reported defect).
  """
  # tempfile.mkdtemp is a public function: wrap it to capture the exact
  # path of the directory created by __enter__ (the as-path of the
  # context manager is unavailable, since __enter__ raises before the
  # body could bind it).
  created = []
  real_mkdtemp = tempfile.mkdtemp

  def recording_mkdtemp(**kwargs):
    d = real_mkdtemp(**kwargs)
    created.append(d)
    return d

  monkeypatch.setattr(tempfile, 'mkdtemp', recording_mkdtemp)

  src = os.path.join(tmp_path, 'file.gz')
  with open(src, 'wb') as fd:
    fd.write(b'definitely not gzip data')

  with pytest.raises(gzip.BadGzipFile), uncompress.Uncompress(src):
    pass

  assert len(created) == 1
  # The failed context manager must not leave the temporary directory
  # behind: __enter__ cleans up after itself on the failure path.
  assert not os.path.exists(created[0])
  assert os.path.exists(src)
