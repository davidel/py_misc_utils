import bz2
import gzip
import lzma
import shutil

from . import gen_fs as gfs


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


def flzip(src, dest):
  with gfs.open(src, mode='rb') as infd, gfs.open(dest, mode='wb') as outfd:
    with lzma.open(outfd, mode='wb') as zfd:
      shutil.copyfileobj(infd, zfd)


def flunzip(src, dest):
  with gfs.open(src, mode='rb') as infd, gfs.open(dest, mode='wb') as outfd:
    with lzma.open(infd, mode='rb') as zfd:
      shutil.copyfileobj(zfd, outfd)

