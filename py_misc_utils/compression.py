import bz2
import collections
import gzip
import contextlib
import lzma
import os
import shutil

from . import gfs


def fgzip(src, dest):
  with gfs.open(src, mode='rb') as infd, gfs.open(dest, mode='wb') as outfd:
    with gzip.open(outfd, mode='wb') as zfd:
      shutil.copyfileobj(infd, zfd)


def fgunzip(src, dest):
  with gfs.open(src, mode='rb') as infd, gfs.open(dest, mode='wb') as outfd:
    with gzip.open(infd, mode='rb') as zfd:
      shutil.copyfileobj(zfd, outfd)


def fbzip2(src, dest):
  with gfs.open(src, mode='rb') as infd, gfs.open(dest, mode='wb') as outfd:
    with bz2.open(outfd, mode='wb') as zfd:
      shutil.copyfileobj(infd, zfd)


def fbunzip2(src, dest):
  with gfs.open(src, mode='rb') as infd, gfs.open(dest, mode='wb') as outfd:
    with bz2.open(infd, mode='rb') as zfd:
      shutil.copyfileobj(zfd, outfd)


def fxzip(src, dest):
  with gfs.open(src, mode='rb') as infd, gfs.open(dest, mode='wb') as outfd:
    with lzma.open(outfd, mode='wb') as zfd:
      shutil.copyfileobj(infd, zfd)


def fxunzip(src, dest):
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
  return _COMPRESSORS.get(ext)


_DECOMPRESSORS = {
  '.bz2': _Processor(processor=fbunzip2, module=bz2),
  '.bzip': _Processor(processor=fbunzip2, module=bz2),
  '.gz': _Processor(processor=fgunzip, module=gzip),
  '.xz': _Processor(processor=fxunzip, module=lzma),
}

def decompressor(ext):
  return _DECOMPRESSORS.get(ext)


def compress(src, dest):
  _, ext = os.path.splitext(dest)
  comp = compressor(ext)
  if comp is not None:
    comp.processor(src, dest)
  else:
    shutil.copyfile(src, dest)


def decompress(src, dest):
  _, ext = os.path.splitext(src)
  decomp = decompressor(ext)
  if decomp is not None:
    decomp.processor(src, dest)
  else:
    shutil.copyfile(src, dest)


@contextlib.contextmanager
def dopen(path, mode='r', **kwargs):
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
  _, ext = os.path.splitext(path)

  comp = compressor(ext)
  if comp is None:
    with gfs.open(path, mode=mode, **kwargs) as fd:
      yield fd
  else:
    with gfs.open(path, mode='wb', **kwargs) as fd:
      with comp.module.open(fd, mode=mode) as zfd:
        yield zfd

