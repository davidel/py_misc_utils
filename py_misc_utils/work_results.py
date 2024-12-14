import hashlib
import os
import pickle

from . import file_overwrite as fow


def url_path(path, url, dirlen=2):
  uhash = hashlib.sha1(url.encode()).hexdigest()
  udir = os.path.join(path, uhash[-dirlen:])
  os.makedirs(udir, exist_ok=True)

  return os.path.join(udir, uhash)


_ERROR_TAG = b'#!$ERROR\n\n'

def make_error(msg):
  return _ERROR_TAG + msg


def write_error(path, **kwargs):
  with fow.FileOverwrite(path, mode='wb') as fd:
    fd.write(make_error(pickle.dumps(kwargs)))


def get_error(data):
  emask = data[: len(_ERROR_TAG)]
  if emask == _ERROR_TAG:
    return pickle.loads(data[len(emask):])

