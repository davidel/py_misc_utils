"""Transparent compression/decompression of files and streams.

This module provides gzip/bzip2/xz compression helpers operating on any
filesystem supported by the gfs layer, plus dispatch helpers - keyed by
file extension - and dopen()/copen() context managers which transparently
(de)compress the data flowing through the opened streams.
"""
import bz2
import collections
import gzip
import contextlib
import lzma
import os
import shutil

from . import gfs


def fgzip(src, dest):
  """Compresses a file with gzip.

  Args:
    src: The source file path.
    dest: The destination file path.
  """
  with gfs.open(src, mode='rb') as infd, gfs.open(dest, mode='wb') as outfd:
    with gzip.open(outfd, mode='wb') as zfd:
      shutil.copyfileobj(infd, zfd)


def fgunzip(src, dest):
  """Decompresses a gzip file.

  Args:
    src: The source file path.
    dest: The destination file path.
  """
  with gfs.open(src, mode='rb') as infd, gfs.open(dest, mode='wb') as outfd:
    with gzip.open(infd, mode='rb') as zfd:
      shutil.copyfileobj(zfd, outfd)


def fbzip2(src, dest):
  """Compresses a file with bzip2.

  Args:
    src: The source file path.
    dest: The destination file path.
  """
  with gfs.open(src, mode='rb') as infd, gfs.open(dest, mode='wb') as outfd:
    with bz2.open(outfd, mode='wb') as zfd:
      shutil.copyfileobj(infd, zfd)


def fbunzip2(src, dest):
  """Decompresses a bzip2 file.

  Args:
    src: The source file path.
    dest: The destination file path.
  """
  with gfs.open(src, mode='rb') as infd, gfs.open(dest, mode='wb') as outfd:
    with bz2.open(infd, mode='rb') as zfd:
      shutil.copyfileobj(zfd, outfd)


def fxzip(src, dest):
  """Compresses a file with lzma (xz).

  Args:
    src: The source file path.
    dest: The destination file path.
  """
  with gfs.open(src, mode='rb') as infd, gfs.open(dest, mode='wb') as outfd:
    with lzma.open(outfd, mode='wb') as zfd:
      shutil.copyfileobj(infd, zfd)


def fxunzip(src, dest):
  """Decompresses an lzma (xz) file.

  Args:
    src: The source file path.
    dest: The destination file path.
  """
  with gfs.open(src, mode='rb') as infd, gfs.open(dest, mode='wb') as outfd:
    with lzma.open(infd, mode='rb') as zfd:
      shutil.copyfileobj(zfd, outfd)


_Processor = collections.namedtuple('Processor', 'processor, module')

_COMPRESSORS = {
    '.bz2': _Processor(processor=fbzip2, module=bz2),
    '.bzip': _Processor(processor=fbzip2, module=bz2),
    '.gz': _Processor(processor=fgzip, module=gzip),
    '.xz': _Processor(processor=fxzip, module=lzma),
}


def compressor(ext):
  """Returns the compressor processor of a file extension.

  Args:
    ext: The file extension - including the leading dot.

  Returns:
    A (compressor function, compression module) tuple, or None if the
    extension is not supported.
  """
  return _COMPRESSORS.get(ext)


_DECOMPRESSORS = {
    '.bz2': _Processor(processor=fbunzip2, module=bz2),
    '.bzip': _Processor(processor=fbunzip2, module=bz2),
    '.gz': _Processor(processor=fgunzip, module=gzip),
    '.xz': _Processor(processor=fxunzip, module=lzma),
}


def decompressor(ext):
  """Returns the decompressor processor of a file extension.

  Args:
    ext: The file extension - including the leading dot.

  Returns:
    A (decompressor function, compression module) tuple, or None if the
    extension is not supported.
  """
  return _DECOMPRESSORS.get(ext)


def compress(src, dest):
  """Compresses a file, chosen by the destination file extension.

  If the destination extension is not supported, the file is copied
  unchanged.

  Args:
    src: The source file path.
    dest: The destination file path.
  """
  _, ext = os.path.splitext(dest)
  comp = compressor(ext)
  if comp is not None:
    comp.processor(src, dest)
  else:
    shutil.copyfile(src, dest)


def decompress(src, dest):
  """Decompresses a file, chosen by the source file extension.

  If the source extension is not supported, the file is copied
  unchanged.

  Args:
    src: The source file path.
    dest: The destination file path.
  """
  _, ext = os.path.splitext(src)
  decomp = decompressor(ext)
  if decomp is not None:
    decomp.processor(src, dest)
  else:
    shutil.copyfile(src, dest)


@contextlib.contextmanager
def dopen(path, mode='r', **kwargs):
  """Opens a - possibly compressed - file for reading.

  If the file extension is supported, the stream is transparently
  decompressed; otherwise the plain file is opened.

  Args:
    path: The file path to open.
    mode: The open mode of the returned stream.
    **kwargs: Keyword arguments passed to gfs.open().

  Yields:
    The open file object.
  """
  _, ext = os.path.splitext(path)

  decomp = decompressor(ext)
  if decomp is None:
    with gfs.open(path, mode=mode, **kwargs) as fd:
      yield fd
  else:
    with gfs.open(path, mode='rb', **kwargs) as fd:
      with decomp.module.open(fd, mode=mode) as zfd:
        yield zfd


@contextlib.contextmanager
def copen(path, mode='w', **kwargs):
  """Opens a - possibly compressed - file for writing.

  If the file extension is supported, the written data is transparently
  compressed; otherwise the plain file is opened.

  Args:
    path: The file path to open.
    mode: The open mode of the returned stream.
    **kwargs: Keyword arguments passed to gfs.open().

  Yields:
    The open file object.
  """
  _, ext = os.path.splitext(path)

  comp = compressor(ext)
  if comp is None:
    with gfs.open(path, mode=mode, **kwargs) as fd:
      yield fd
  else:
    with gfs.open(path, mode='wb', **kwargs) as fd:
      with comp.module.open(fd, mode=mode) as zfd:
        yield zfd
