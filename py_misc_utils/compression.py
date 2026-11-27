"""Transparent compression/decompression of files and streams.

This module provides gzip/bzip2/xz compression helpers operating on any
filesystem supported by the gfs layer, plus dispatch helpers - keyed by
file extension - and dopen()/copen() context managers which transparently
(de)compress the data flowing through the opened streams.

Example:
  import os
  import tempfile

  import py_misc_utils.compression as comp


  tdir = tempfile.mkdtemp()
  src = os.path.join(tdir, 'data.csv')
  dest = os.path.join(tdir, 'data.csv.gz')

  with open(src, 'w') as fd:
    fd.write('a,b\\n1,2\\n')

  comp.compress(src, dest)
  comp.decompress(dest, os.path.join(tdir, 'data.csv'))
  assert open(src).read() == open(os.path.join(tdir, 'data.csv')).read()
"""
import bz2
import collections
import contextlib
import gzip
import lzma
import os
import shutil

from . import gfs


def fgzip(src, dest):
  """Compresses a file with gzip.

  Args:
    src: The source file path.
    dest: The destination file path.

  Example:
    import os
    import tempfile

    import py_misc_utils.compression as comp


    tdir = tempfile.mkdtemp()
    src = os.path.join(tdir, 'f.txt')
    dest = os.path.join(tdir, 'f.txt.gz')

    with open(src, 'w') as fd:
      fd.write('compress me ' * 200)

    comp.fgzip(src, dest)
    assert os.path.getsize(dest) < os.path.getsize(src)

    back = os.path.join(tdir, 'f_back.txt')
    comp.fgunzip(dest, back)
    assert open(back).read() == 'compress me ' * 200
  """
  with (gfs.open(src, mode='rb') as infd, gfs.open(dest, mode='wb') as
        outfd, gzip.open(outfd, mode='wb') as zfd):
    shutil.copyfileobj(infd, zfd)


def fgunzip(src, dest):
  """Decompresses a gzip file.

  Args:
    src: The source file path.
    dest: The destination file path.
  """
  with (gfs.open(src, mode='rb') as infd, gfs.open(dest, mode='wb') as
        outfd, gzip.open(infd, mode='rb') as zfd):
    shutil.copyfileobj(zfd, outfd)


def fbzip2(src, dest):
  """Compresses a file with bzip2.

  Args:
    src: The source file path.
    dest: The destination file path.

  Example:
    import os
    import tempfile

    import py_misc_utils.compression as comp


    tdir = tempfile.mkdtemp()
    src = os.path.join(tdir, 'f.txt')
    dest = os.path.join(tdir, 'f.txt.bz2')

    with open(src, 'w') as fd:
      fd.write('bzip2 me ' * 200)

    comp.fbzip2(src, dest)
    assert os.path.getsize(dest) < os.path.getsize(src)

    back = os.path.join(tdir, 'f_back.txt')
    comp.fbunzip2(dest, back)
    assert open(back).read() == 'bzip2 me ' * 200
  """
  with (gfs.open(src, mode='rb') as infd, gfs.open(dest, mode='wb') as
        outfd, bz2.open(outfd, mode='wb') as zfd):
    shutil.copyfileobj(infd, zfd)


def fbunzip2(src, dest):
  """Decompresses a bzip2 file.

  Args:
    src: The source file path.
    dest: The destination file path.
  """
  with (gfs.open(src, mode='rb') as infd, gfs.open(dest, mode='wb') as
        outfd, bz2.open(infd, mode='rb') as zfd):
    shutil.copyfileobj(zfd, outfd)


def fxzip(src, dest):
  """Compresses a file with lzma (xz).

  Args:
    src: The source file path.
    dest: The destination file path.

  Example:
    import os
    import tempfile

    import py_misc_utils.compression as comp


    tdir = tempfile.mkdtemp()
    src = os.path.join(tdir, 'f.txt')
    dest = os.path.join(tdir, 'f.txt.xz')

    with open(src, 'w') as fd:
      fd.write('xz me ' * 200)

    comp.fxzip(src, dest)
    assert os.path.getsize(dest) < os.path.getsize(src)

    back = os.path.join(tdir, 'f_back.txt')
    comp.fxunzip(dest, back)
    assert open(back).read() == 'xz me ' * 200
  """
  with (gfs.open(src, mode='rb') as infd, gfs.open(dest, mode='wb') as
        outfd, lzma.open(outfd, mode='wb') as zfd):
    shutil.copyfileobj(infd, zfd)


def fxunzip(src, dest):
  """Decompresses an lzma (xz) file.

  Args:
    src: The source file path.
    dest: The destination file path.
  """
  with (gfs.open(src, mode='rb') as infd, gfs.open(dest, mode='wb') as
        outfd, lzma.open(infd, mode='rb') as zfd):
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

  Example:
    import gzip

    import py_misc_utils.compression as comp

    proc, module = comp.compressor('.gz')
    assert module is gzip
    assert comp.compressor('.txt') is None
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

  Example:
    import lzma

    import py_misc_utils.compression as comp

    dproc, dmodule = comp.decompressor('.xz')
    assert dmodule is lzma
    assert comp.decompressor('.bin') is None
  """
  return _DECOMPRESSORS.get(ext)


def compress(src, dest):
  """Compresses a file, chosen by the destination file extension.

  If the destination extension is not supported, the file is copied
  unchanged.

  Args:
    src: The source file path.
    dest: The destination file path.

  Example:
    import os
    import tempfile

    import py_misc_utils.compression as comp


    tdir = tempfile.mkdtemp()
    src = os.path.join(tdir, 'a.bin')
    with open(src, 'wb') as fd:
      fd.write(b'x' * 1000)

    comp.compress(src, os.path.join(tdir, 'a.bin.bz2'))
    comp.compress(src, os.path.join(tdir, 'a.copy.bin'))  # No ext: plain copy.
    assert os.path.getsize(os.path.join(tdir, 'a.bin.bz2')) < 1000

    comp.decompress(os.path.join(tdir, 'a.bin.bz2'), os.path.join(tdir, 'a2.bin'))
    assert open(os.path.join(tdir, 'a2.bin'), 'rb').read() == b'x' * 1000
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

  Example:
    import os
    import tempfile

    import py_misc_utils.compression as comp


    tdir = tempfile.mkdtemp()
    src = os.path.join(tdir, 'a.bin')
    dest = os.path.join(tdir, 'a.bin.xz')
    with open(src, 'wb') as fd:
      fd.write(b'y' * 1000)

    comp.compress(src, dest)
    comp.decompress(dest, os.path.join(tdir, 'a2.bin'))
    assert open(os.path.join(tdir, 'a2.bin'), 'rb').read() == b'y' * 1000
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

  Example:
    import os
    import tempfile

    import py_misc_utils.compression as comp


    tdir = tempfile.mkdtemp()
    plain = os.path.join(tdir, 'log.txt')
    gz = os.path.join(tdir, 'log.txt.gz')

    with open(plain, 'w') as fd:
      fd.write('hello world')

    comp.fgzip(plain, gz)
    with comp.dopen(gz, mode='rt') as fd:
      assert fd.read() == 'hello world'

    # Uncompressed files are opened as-is.
    with comp.dopen(plain, mode='rt') as fd:
      assert fd.read() == 'hello world'
  """
  _, ext = os.path.splitext(path)

  decomp = decompressor(ext)
  if decomp is None:
    with gfs.open(path, mode=mode, **kwargs) as fd:
      yield fd
  else:
    with (gfs.open(path, mode='rb', **kwargs) as fd, decomp.module.open(fd, mode=mode)
          as zfd):
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

  Example:
    import os
    import tempfile

    import py_misc_utils.compression as comp


    tdir = tempfile.mkdtemp()
    path = os.path.join(tdir, 'out.json.gz')

    with comp.copen(path, mode='wt') as fd:
      fd.write('{"key": 1}')

    with comp.dopen(path, mode='rt') as fd:
      assert fd.read() == '{"key": 1}'
  """
  _, ext = os.path.splitext(path)

  comp = compressor(ext)
  if comp is None:
    with gfs.open(path, mode=mode, **kwargs) as fd:
      yield fd
  else:
    with (gfs.open(path, mode='wb', **kwargs) as fd, comp.module.open(fd, mode=mode) as
          zfd):
      yield zfd
