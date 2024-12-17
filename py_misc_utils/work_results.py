import hashlib
import os
import pickle

from . import rnd_utils as rngu


class WorkException:

  def __init__(self, exception, **kwargs):
    # Some exceptions cannot be pickled, so the real exception cannot be serialized
    # (or it can be serialized, and failed to be deserialized for missing arguments).
    # In such cases we write a generic exception with the string representation of
    # the original one.
    self._ex_type = type(exception)
    try:
      self._ex_data = pickle.dumps(exception)
      pickle.loads(self._ex_data)
    except:
      self._ex_data = pickle.dumps(Exception(repr(exception)))

    for k, v in kwargs.items():
      setattr(self, k, v)

  def do_raise(self):
    raise pickle.loads(self._ex_data)

  def is_instance(self, *types):
    return any(t == self._ex_type for t in types)


def work_path(path, workid, dirlen=2):
  uhash = hashlib.sha1(workid.encode()).hexdigest()
  udir = os.path.join(path, uhash[-dirlen:])
  os.makedirs(udir, exist_ok=True)

  return os.path.join(udir, uhash)


_ERROR_TAG = b'#@$ERROR$@#\n'

def make_error(msg):
  return _ERROR_TAG + msg


def write_error(path, exception, **kwargs):
  wex = WorkException(exception, **kwargs)

  # This does FileOverwrite() task (locally limited) but here we do not pull that
  # dependency to minimize the ones of this module.
  tpath = rngu.temp_path(nspath=path)
  with open(tpath, mode='wb') as fd:
    fd.write(make_error(pickle.dumps(wex)))

  os.replace(tpath, path)


def get_error(data):
  if data.startswith(_ERROR_TAG):
    return pickle.loads(data[len(_ERROR_TAG):])


def raise_if_error(data):
  if isinstance(data, WorkException):
    data.do_raise()

  return data


def raise_on_error(data):
  raise_if_error(get_error(data))

  return data


def get_work(wpath, path=None, workid=None):
  wpath = wpath or work_path(path, workid)

  with open(wpath, mode='rb') as fd:
    data = fd.read()

  return raise_on_error(data)


def tryget_work(path, workid):
  wpath = work_path(path, workid)

  return get_work(wpath) if os.path.isfile(wpath) else None

