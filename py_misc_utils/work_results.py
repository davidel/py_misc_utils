import hashlib
import os
import pickle

from . import rnd_utils as rngu


class WorkException:

  def __init__(self, exception):
    # Some exceptions cannot be pickled, so the real exception cannot be serialized
    # (or it can be serialized, and failed to be deserialized for missing arguments).
    # In such cases we write a generic exception with the string representation of
    # the original one.
    self._type = type(exception)
    try:
      self._data = pickle.dumps(exception)
      pickle.loads(self._data)
    except:
      self._data = pickle.dumps(Exception(repr(exception)))

  def do_raise(self):
    raise pickle.loads(self._data)

  def is_instance(self, *types):
    return any(t == self._type for t in types)



def url_path(path, url, dirlen=2):
  uhash = hashlib.sha1(url.encode()).hexdigest()
  udir = os.path.join(path, uhash[-dirlen:])
  os.makedirs(udir, exist_ok=True)

  return os.path.join(udir, uhash)


_ERROR_TAG = b'#!$ERROR\n\n'

def make_error(msg):
  return _ERROR_TAG + msg


_EXCEPT_KEY = 'exception'

def write_error(path, exception, **kwargs):
  kwargs[_EXCEPT_KEY] = WorkException(exception)

  # This does FileOverwrite() task (locally limited) but here we do not pull that
  # dependency to minimize the ones of this module.
  tpath = rngu.temp_path(nspath=path)
  with open(tpath, mode='wb') as fd:
    fd.write(make_error(pickle.dumps(kwargs)))

  os.replace(tpath, path)


def get_error(data):
  emask = data[: len(_ERROR_TAG)]
  if emask == _ERROR_TAG:
    return pickle.loads(data[len(emask):])


def raise_on_error(data):
  error = get_error(data)
  if error is not None:
    error[_EXCEPT_KEY].do_raise()

  return data


def raise_if_error(data):
  if isinstance(data, WorkException):
    data.do_raise()

  return data

