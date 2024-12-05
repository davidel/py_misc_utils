import bz2
import gzip
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


_COMPRESSORS = {
  '.bz2': fbzip2,
  '.bzip': fbzip2,
  '.gz': fgzip,
  '.xz': fxzip,
}

def compressor(ext):
  return _COMPRESSORS.get(ext)


_DECOMPRESSORS = {
  '.bz2': fbunzip2,
  '.bzip': fbunzip2,
  '.gz': fgunzip,
  '.xz': fxunzip,
}

def decompressor(ext):
  return _DECOMPRESSORS.get(ext)


def compress(src, dest):
  _, ext = os.path.splitext(dest)
  comp = compressor(ext)
  if comp is not None:
    comp(src, dest)
  else:
    shutil.copyfile(src, dest)


def decompress(src, dest):
  _, ext = os.path.splitext(src)
  decomp = decompressor(ext)
  if decomp is not None:
    decomp(src, dest)
  else:
    shutil.copyfile(src, dest)

