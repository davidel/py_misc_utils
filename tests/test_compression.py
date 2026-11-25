"""Tests for py_misc_utils.compression.

Expectations pinned to the observed behavior: fgzip/fgunzip, fbzip2/fbunzip2,
fxzip/fxunzip roundtrip data through gzip/bzip2/xz; compress/decompress
dispatch by extension and copy unchanged for unsupported extensions;
compressor/decompressor return (processor, module) tuples or None;
dopen/copen transparently (de)compress streams by extension.
"""

import os

from py_misc_utils import compression as comp


def test_fgzip_fgunzip_roundtrip(tmp_path):
  """gzip compression then decompression recovers original bytes."""
  src = os.path.join(tmp_path, 'data.txt')
  original = b'hello world ' * 100
  with open(src, 'wb') as fd:
    fd.write(original)

  gz = os.path.join(tmp_path, 'data.txt.gz')
  comp.fgzip(src, gz)
  assert os.path.getsize(gz) < os.path.getsize(src)

  out = os.path.join(tmp_path, 'data.out')
  comp.fgunzip(gz, out)
  with open(out, 'rb') as fd:
    assert fd.read() == original


def test_fbzip2_fbunzip2_roundtrip(tmp_path):
  """bzip2 compression then decompression recovers original bytes."""
  src = os.path.join(tmp_path, 'data.txt')
  original = b'hello world ' * 100
  with open(src, 'wb') as fd:
    fd.write(original)

  bz2 = os.path.join(tmp_path, 'data.txt.bz2')
  comp.fbzip2(src, bz2)

  out = os.path.join(tmp_path, 'data.out')
  comp.fbunzip2(bz2, out)
  with open(out, 'rb') as fd:
    assert fd.read() == original


def test_fxzip_fxunzip_roundtrip(tmp_path):
  """xz compression then decompression recovers original bytes."""
  src = os.path.join(tmp_path, 'data.txt')
  original = b'hello world ' * 100
  with open(src, 'wb') as fd:
    fd.write(original)

  xz = os.path.join(tmp_path, 'data.txt.xz')
  comp.fxzip(src, xz)

  out = os.path.join(tmp_path, 'data.out')
  comp.fxunzip(xz, out)
  with open(out, 'rb') as fd:
    assert fd.read() == original


def test_compress_decompress_unsupported_extension_copies(tmp_path):
  """Unsupported extensions copy the file unchanged."""
  src = os.path.join(tmp_path, 'data.txt')
  original = b'hello world'
  with open(src, 'wb') as fd:
    fd.write(original)

  dest = os.path.join(tmp_path, 'data.xyz')
  comp.compress(src, dest)
  with open(dest, 'rb') as fd:
    assert fd.read() == original

  dest2 = os.path.join(tmp_path, 'data2.xyz')
  comp.decompress(src, dest2)
  with open(dest2, 'rb') as fd:
    assert fd.read() == original


def test_compressor_decompressor_known_extensions():
  """Known extensions return processor+module; unknown returns None."""
  for ext, mod in (('.gz', comp.gzip), ('.bz2', comp.bz2), ('.xz', comp.lzma)):
    proc, m = comp.compressor(ext)
    assert m is mod
    assert callable(proc)
    proc2, m2 = comp.decompressor(ext)
    assert m2 is mod
    assert callable(proc2)

  assert comp.compressor('.xyz') is None
  assert comp.decompressor('.xyz') is None


def test_dopen_copen_roundtrip(tmp_path):
  """dopen/copen transparently decompress/compress by extension."""
  path = os.path.join(tmp_path, 'data.txt.gz')

  original = b'transparent stream data'
  with comp.copen(path, mode='w') as fd:
    fd.write(original)

  with comp.dopen(path, mode='r') as fd:
    assert fd.read() == original


def test_dopen_copen_unsupported_extension_passthrough(tmp_path):
  """Unsupported extensions pass through plain files."""
  path = os.path.join(tmp_path, 'data.txt')
  original = b'plain text'
  with comp.copen(path, mode='wb') as fd:
    fd.write(original)
  with comp.dopen(path, mode='rb') as fd:
    assert fd.read() == original
