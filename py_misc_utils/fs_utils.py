import functools
import os
import shutil

from . import alog
from . import assert_checks as tas


def link_or_copy(src_path, dest_path):
  try:
    os.link(src_path, dest_path)

    return dest_path
  except OSError as ex:
    alog.debug(f'Harklink failed from "{src_path}" to "{dest_path}", trying symlink. ' \
               f'Error was: {ex}')

  try:
    os.symlink(src_path, dest_path)

    return dest_path
  except OSError:
    alog.debug(f'Symlink failed from "{src_path}" to "{dest_path}", going to copy. ' \
               f'Error was: {ex}')

  shutil.copyfile(src_path, dest_path)
  shutil.copystat(src_path, dest_path)

  return dest_path


def is_newer_file(path, other):
  return os.stat(path).st_mtime > os.stat(other).st_mtime


def os_opener(*args, **kwargs):
  return functools.partial(os.open, *args, **kwargs)


def readall(fd):
  sres = os.stat(fd)
  os.lseek(fd, 0, os.SEEK_SET)

  return os.read(fd, sres.st_size)


def stat(path):
  try:
    return os.stat(path)
  except:
    pass


def path_split(path):
  path_parts = []
  while True:
    parts = os.path.split(path)
    if parts[0] == path:
      path_parts.append(parts[0])
      break
    elif parts[1] == path:
      path_parts.append(parts[1])
      break
    else:
      path = parts[0]
      path_parts.append(parts[1])

  path_parts.reverse()

  return path_parts


def drop_ext(path, exts):
  xpath, ext = os.path.splitext(path)

  if isinstance(exts, str):
    return xpath if ext == exts else path

  return xpath if ext in exts else path


def normpath(path):
  path = os.path.expanduser(path)
  path = os.path.expandvars(path)
  path = os.path.abspath(path)

  return os.path.normpath(path)


def enum_chunks(stream, chunk_size=16 * 1024**2):
  while True:
    data = stream.read(chunk_size)
    if data:
      yield data
    if chunk_size > len(data):
      break

