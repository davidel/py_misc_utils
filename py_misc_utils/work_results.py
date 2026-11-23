"""Work-result storage and exchange.

This module provides helpers to store and retrieve the results of
asynchronous work - via unique work ids - along with the handling of the
work failures: failed work results are marked with an error tag, holding
a pickled WorkException, so that the consumers can detect the failure and
re-raise it as if the work was executed locally.
"""
import contextlib
import hashlib
import os
import pickle

from . import fs_utils as fsu


class WorkException:
  """A serializable holder of an exception raised by a worker.

  Some exceptions cannot be pickled, so the held exception is verified
  to survive a pickle round-trip: if not, a generic Exception holding
  its string representation is stored instead.

  Arbitrary attributes - set from the kwargs - can be attached to the
  exception holder, to be transported along the exception.
  """

  def __init__(self, exception, **kwargs):
    """Creates an exception holder.

    Args:
      exception: The exception to hold.
      **kwargs: Optional attributes to attach to the holder.
    """
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
    """Raises the held exception."""

    raise self.exception()

  def is_instance(self, *types):
    """Checks the type of the held exception.

    Args:
      *types: The exception types to check.

    Returns:
      True if the type of the held exception matches any of the given
      ones.
    """
    return any(t == self._ex_type for t in types)

  def exception(self):
    """Returns the held exception.

    Returns:
      The deserialized held exception - or its generic fallback.
    """
    return pickle.loads(self._ex_data)

  def __repr__(self):
    """Returns the string representation of the exception holder."""
    exception = pickle.loads(self._ex_data)
    xvars = {k: v for k, v in vars(self).items() if not k.startswith('_')}

    return f'{repr(exception)}: {xvars}'


def work_hash(workid):
  """Returns the hash of a work id.

  Args:
    workid: The work id - typically a string holding an URL or another
      unique work identifier.

  Returns:
    The SHA-1 hash of the work id.
  """
  return hashlib.sha1(workid.encode()).hexdigest()


def work_path(path, workid):
  """Returns the work-result file path of a work id.

  Args:
    path: The folder holding the work results.
    workid: The work id.

  Returns:
    The work-result file path.
  """
  return os.path.join(path, work_hash(workid))


def enum_ready(path):
  """Enumerates the work results stored in a folder.

  Args:
    path: The folder holding the work results.

  Yields:
    (name, path) tuples - one per stored work result.
  """
  with os.scandir(path) as rit:
    for dentry in rit:
      yield rit.name, rit.path


_ERROR_TAG = b'#@$ERROR$@#\n'


def make_error(msg):
  """Tags a message as an error payload.

  Args:
    msg: The error payload - e.g. a pickled WorkException.

  Returns:
    The tagged error payload.
  """
  return _ERROR_TAG + msg


def write_result(path):
  """Returns a context manager writing a work result atomically.

  Args:
    path: The work-result file path.

  Returns:
    A context manager yielding the open file object of the work-result
    file.
  """
  return fsu.atomic_write(path)


def write_error(path, exception, **kwargs):
  """Writes an error-tagged work result.

  Args:
    path: The work-result file path.
    exception: The exception to record.
    **kwargs: Optional attributes attached to the WorkException holder.
  """
  wex = WorkException(exception, **kwargs)

  with write_result(path) as fd:
    fd.write(make_error(pickle.dumps(wex)))


def get_error(data):
  """Extracts the WorkException held by an error-tagged payload.

  Args:
    data: The work-result data to check.

  Returns:
    The deserialized WorkException, or None if the data is not
    error-tagged.
  """
  if data.startswith(_ERROR_TAG):
    return pickle.loads(data[len(_ERROR_TAG):])


def raise_if_error(data):
  """Raises the error held by a work result - if any.

  Args:
    data: The work-result data.

  Returns:
    The work-result data, if it does not hold an error.

  Raises:
    Exception: The exception held by the work result, if any.
  """
  if isinstance(data, WorkException):
    data.do_raise()

  return data


def load_work(wpath, path=None, workid=None):
  """Loads a work result - without raising its error, if any.

  Args:
    wpath: An optional work-result file path - taking precedence over
      the path/workid pair.
    path: The folder holding the work results - used if wpath is None.
    workid: The work id - used if wpath is None.

  Returns:
    The work-result data: either the payload, or the WorkException
    deserialized from an error-tagged payload.
  """
  wpath = wpath or work_path(path, workid)

  with open(wpath, mode='rb') as fd:
    data = fd.read()

  error = get_error(data)

  return error or data


def get_work(wpath, path=None, workid=None):
  """Loads a work result, raising its error - if any.

  Args:
    wpath: An optional work-result file path - taking precedence over
      the path/workid pair.
    path: The folder holding the work results - used if wpath is None.
    workid: The work id - used if wpath is None.

  Returns:
    The work-result payload.

  Raises:
    Exception: The exception held by the work result, if any.
  """
  data = load_work(wpath, path=path, workid=workid)

  return raise_if_error(data)
