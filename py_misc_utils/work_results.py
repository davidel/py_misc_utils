"""Work-result storage and exchange.

This module provides helpers to store and retrieve the results of
asynchronous work - via unique work ids - along with the handling of the
work failures: failed work results are marked with an error tag, holding
a pickled WorkException, so that the consumers can detect the failure and
re-raise it as if the work was executed locally.

Example:
  import os
  import pickle
  import tempfile

  import py_misc_utils.work_results as pwr


  tdir = tempfile.mkdtemp()
  path = pwr.work_path(tdir, 'sum-job')
  with pwr.write_result(path) as fd:
    fd.write(pickle.dumps({'sum': 42}))

  assert pickle.loads(pwr.get_work(wpath=path)) == {'sum': 42}
"""
import hashlib
import importlib
import os
import pickle

from . import fs_utils as fsu


class WorkException:
  """A serializable holder of an exception raised by a worker.

  Some exceptions cannot be pickled, so the held exception is verified
  to survive a pickle round-trip: if not, a generic Exception holding
  its string representation is stored instead.

  The type of the held exception is tracked as an importable
  (module, qualname) reference, so that the holder itself stays
  picklable even when the exception class is local or dynamically
  created. In that case the reported type degrades to Exception - the
  most precise claim which can be transported - and is_instance() stays
  consistent with the exception actually re-raised by do_raise().

  Arbitrary attributes - set from the kwargs - can be attached to the
  exception holder, to be transported along the exception.

  Example:
    import pickle

    import py_misc_utils.work_results as pwr

    wex = pwr.WorkException(ValueError('bad input'), worker='W1')
    assert wex.is_instance(ValueError)
    assert isinstance(wex.exception(), ValueError)
    assert repr(wex) == "ValueError('bad input'): {'worker': 'W1'}"

    # The holder is picklable, and keeps the kwargs after the roundtrip.
    wex2 = pickle.loads(pickle.dumps(wex))
    assert wex2.is_instance(ValueError) and wex2.worker == 'W1'
  """

  def __init__(self, exception, **kwargs):
    """Creates an exception holder.

    Args:
      exception: The exception to hold.
      **kwargs: Optional attributes to attach to the holder.

    Example:
      import py_misc_utils.work_results as pwr

      wex = pwr.WorkException(KeyError('missing-key'), worker='W9')
      assert wex.is_instance(KeyError)
      assert not wex.is_instance(ValueError)

      try:
        wex.do_raise()
      except KeyError as e:
        assert str(e) == "'missing-key'"
    """
    # Some exceptions cannot be pickled, so the real exception cannot be serialized
    # (or it can be serialized, and failed to be deserialized for missing arguments).
    # In such cases we write a generic exception with the string representation of
    # the original one.
    excls = type(exception)
    try:
      # The type reference must be transportable on its own: local or
      # dynamically created classes cannot be referenced by name, in
      # which case only the generic Exception type can be claimed.
      pickle.loads(pickle.dumps(excls))
      self._ex_type = excls
      self._ex_type_ref = (excls.__module__,
                           getattr(excls, '__qualname__', excls.__name__))
    except Exception:  # noqa: BLE001 - degrade on any pickle failure
      self._ex_type = Exception
      self._ex_type_ref = ('builtins', 'Exception')

    try:
      self._ex_data = pickle.dumps(exception)
      pickle.loads(self._ex_data)
    except Exception:  # noqa: BLE001 - degrade on any pickle failure
      self._ex_data = pickle.dumps(Exception(f'{excls.__name__}: {exception}'))

    for k, v in kwargs.items():
      setattr(self, k, v)

  def do_raise(self):
    """Raises the held exception."""

    raise self.exception()

  def _resolve_type(self):
    """Resolves the importable type reference into the actual class.

    Holders pickled before the (module, qualname) reference was
    introduced carry only the _ex_type class object: for those, the
    stored object is used as-is.

    Returns:
      The exception type the holder claims to represent.
    """
    ref = getattr(self, '_ex_type_ref', None)
    if ref is None:
      return self._ex_type

    exmod, exname = ref
    try:
      return getattr(importlib.import_module(exmod), exname)
    except (ImportError, AttributeError):
      return Exception

  def is_instance(self, *types):
    """Checks the type of the held exception.

    The check uses the resolved (module, qualname) reference, so it stays
    consistent with the exception type actually re-raised by do_raise():
    locally-defined exception classes degrade to a generic Exception.

    Args:
      *types: The exception types to check against.

    Returns:
      True if the held exception type is one of the given types.

    Example:
      import py_misc_utils.work_results as pwr

      # Exception classes which cannot be referenced by (module, qualname)
      # - like locally-defined ones - degrade to a generic Exception.
      class LocalEx(Exception):
        pass


      wex = pwr.WorkException(LocalEx('local only'))
      assert wex.is_instance(Exception)
      assert not wex.is_instance(LocalEx)
      assert str(wex.exception()) == 'LocalEx: local only'
    """
    cls = self._resolve_type()

    return any(issubclass(cls, t) for t in types)

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

    return f'{exception!r}: {xvars}'


def work_hash(workid):
  """Returns the hash of a work id.

  Args:
    workid: The work id - typically a string holding an URL or another
      unique work identifier.

  Returns:
    The SHA-1 hash of the work id.

  Example:
    import py_misc_utils.work_results as pwr

    assert len(pwr.work_hash('job-1')) == 40  # SHA-1 hex digest.
  """
  return hashlib.sha1(workid.encode()).hexdigest()


def work_path(path, workid):
  """Returns the work-result file path of a work id.

  Args:
    path: The folder holding the work results.
    workid: The work id.

  Returns:
    The work-result file path.

  Example:
    import os

    import py_misc_utils.work_results as pwr

    path = pwr.work_path('/results', 'job-1')
    assert os.path.dirname(path) == '/results'
    assert os.path.basename(path) == pwr.work_hash('job-1')
  """
  return os.path.join(path, work_hash(workid))


def enum_ready(path):
  """Enumerates the work results stored in a folder.

  Args:
    path: The folder holding the work results.

  Yields:
    (name, path) tuples - one per stored work result.

  Example:
    import pickle
    import tempfile

    import py_misc_utils.work_results as pwr


    tdir = tempfile.mkdtemp()
    for workid in ('a', 'b'):
      with pwr.write_result(pwr.work_path(tdir, workid)) as fd:
        fd.write(pickle.dumps(workid))

    names = sorted(name for name, path in pwr.enum_ready(tdir))
    assert len(names) == 2, names
  """
  with os.scandir(path) as rit:
    for dentry in rit:
      yield dentry.name, dentry.path


_ERROR_TAG = b'#@$ERROR$@#\n'


def make_error(msg):
  """Tags a message as an error payload.

  Args:
    msg: The error payload - e.g. a pickled WorkException.

  Returns:
    The tagged error payload.

  Example:
    import pickle

    import py_misc_utils.work_results as pwr

    tagged = pwr.make_error(pickle.dumps(pwr.WorkException(RuntimeError('boom'))))
    assert tagged.startswith(b'#@$ERROR$@#')
  """
  return _ERROR_TAG + msg


def write_result(path):
  """Returns a context manager writing a work result atomically.

  Args:
    path: The work-result file path.

  Returns:
    A context manager yielding the open file object of the work-result
    file.

  Example:
    import pickle
    import tempfile

    import py_misc_utils.work_results as pwr


    tdir = tempfile.mkdtemp()
    path = pwr.work_path(tdir, 'sum-job')
    with pwr.write_result(path) as fd:
      fd.write(pickle.dumps({'sum': 42}))

    assert pickle.loads(pwr.load_work(wpath=path)) == {'sum': 42}
  """
  return fsu.atomic_write(path)


def write_error(path, exception, **kwargs):
  """Writes an error-tagged work result.

  Args:
    path: The work-result file path.
    exception: The exception to record.
    **kwargs: Optional attributes attached to the WorkException holder.

  Example:
    import tempfile

    import py_misc_utils.work_results as pwr


    tdir = tempfile.mkdtemp()
    pwr.write_error(pwr.work_path(tdir, 'bad-job'), ValueError('failed'),
                    worker='W2')
    wex = pwr.load_work(wpath=pwr.work_path(tdir, 'bad-job'))
    assert wex.is_instance(ValueError) and wex.worker == 'W2'
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

  Example:
    import pickle

    import py_misc_utils.work_results as pwr

    assert pwr.get_error(pickle.dumps({'ok': 1})) is None
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

  Example:
    import pickle

    import py_misc_utils.work_results as pwr

    tagged = pwr.make_error(pickle.dumps(pwr.WorkException(RuntimeError('boom'))))
    wex = pwr.get_error(tagged)

    try:
      pwr.raise_if_error(wex)
    except RuntimeError as e:
      assert str(e) == 'boom'
  """
  if isinstance(data, WorkException):
    data.do_raise()

  return data


def load_work(wpath=None, path=None, workid=None):
  """Loads a work result - without raising its error, if any.

  Args:
    wpath: An optional work-result file path - taking precedence over
      the path/workid pair.
    path: The folder holding the work results - used if wpath is None.
    workid: The work id - used if wpath is None.

  Returns:
    The work-result data: either the payload, or the WorkException
    deserialized from an error-tagged payload.

  Example:
    import pickle
    import tempfile

    import py_misc_utils.work_results as pwr


    tdir = tempfile.mkdtemp()
    path = pwr.work_path(tdir, 'sum-job')
    with pwr.write_result(path) as fd:
      fd.write(pickle.dumps({'sum': 42}))

    assert pickle.loads(pwr.load_work(wpath=path)) == {'sum': 42}

    pwr.write_error(pwr.work_path(tdir, 'bad-job'), ValueError('failed'))
    wex = pwr.load_work(wpath=pwr.work_path(tdir, 'bad-job'))
    assert wex.is_instance(ValueError)
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

  Example:
    import pickle
    import tempfile

    import py_misc_utils.work_results as pwr


    tdir = tempfile.mkdtemp()
    path = pwr.work_path(tdir, 'sum-job')
    with pwr.write_result(path) as fd:
      fd.write(pickle.dumps({'sum': 42}))

    assert pickle.loads(pwr.get_work(wpath=path)) == {'sum': 42}

    pwr.write_error(pwr.work_path(tdir, 'bad-job'), ValueError('failed'))
    try:
      pwr.get_work(wpath=pwr.work_path(tdir, 'bad-job'))
    except ValueError as e:
      assert str(e) == 'failed'
  """
  data = load_work(wpath, path=path, workid=workid)

  return raise_if_error(data)
