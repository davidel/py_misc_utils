import contextlib
import hashlib
import os
import pickle

from . import fs_utils as fsu


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
    raise self.exception()

  def is_instance(self, *types):
    return any(t == self._ex_type for t in types)

  def exception(self):
    return pickle.loads(self._ex_data)

  def __repr__(self):
    exception = pickle.loads(self._ex_data)
    xvars = {k: v for k, v in vars(self).items() if not k.startswith('_')}

    return f'{repr(exception)}: {xvars}'


def work_hash(workid):
  return hashlib.sha1(workid.encode()).hexdigest()


def work_path(path, workid):
  return os.path.join(path, work_hash(workid))


def enum_ready(path):
  with os.scandir(path) as rit:
    for dentry in rit:
      yield rit.name, rit.path


_ERROR_TAG = b'#@$ERROR$@#\n'

def make_error(msg):
  return _ERROR_TAG + msg


def write_result(path):
  return fsu.atomic_write(path)


def write_error(path, exception, **kwargs):
  wex = WorkException(exception, **kwargs)

  with write_result(path) as fd:
    fd.write(make_error(pickle.dumps(wex)))


def get_error(data):
  if data.startswith(_ERROR_TAG):
    return pickle.loads(data[len(_ERROR_TAG):])


def raise_if_error(data):
  if isinstance(data, WorkException):
    data.do_raise()

  return data


def load_work(wpath, path=None, workid=None):
  wpath = wpath or work_path(path, workid)

  with open(wpath, mode='rb') as fd:
    data = fd.read()

  error = get_error(data)

  return error or data


def get_work(wpath, path=None, workid=None):
  data = load_work(wpath, path=path, workid=workid)

  return raise_if_error(data)

