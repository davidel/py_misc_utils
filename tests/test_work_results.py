"""Tests for the py_misc_utils.work_results result exchange helpers.

Expectations follow the documented API contract: work ids hash to stable
file paths, results are written and read back verbatim, error-tagged
payloads carry a pickled WorkException that get_work() re-raises as if
raised locally, WorkException survives a pickle roundtrip (falling back to
a generic Exception for unpicklable held exceptions), and enum_ready()
enumerates stored result files.
"""

import pickle

import pytest

import py_misc_utils.work_results as pwr


def test_work_hash_is_deterministic():
  """work_hash() is a stable digest of the work id."""
  h1 = pwr.work_hash('http://x/y')
  h2 = pwr.work_hash('http://x/y')

  assert h1 == h2
  assert h1 != pwr.work_hash('other')


def test_work_path_joins_folder_and_hash():
  """work_path() places the hashed id inside the given folder."""
  assert pwr.work_path('/tmp/res', 'wid') == '/tmp/res/' + pwr.work_hash('wid')


def test_write_and_load_roundtrip(tmp_path):
  """write_result()/load_work() store and return the exact payload.

  Report-only finding: load_work(path=, workid=) resolves the file as
  work_path(path, workid) - the hashed id - while write_result() writes
  to the literal path it is given, so the two call forms only pair up
  when writers go through work_path() themselves.
  """
  wpath = pwr.work_path(str(tmp_path), 'a.result')

  with pwr.write_result(wpath) as fd:
    fd.write(b'payload-bytes')

  assert pwr.load_work(wpath) == b'payload-bytes'
  assert pwr.load_work(path=str(tmp_path), workid='a.result') == \
    b'payload-bytes'


def test_get_work_returns_payload(tmp_path):
  """get_work() returns plain payloads unchanged."""
  wpath = str(tmp_path / 'b.result')

  with pwr.write_result(wpath) as fd:
    fd.write(pickle.dumps({'v': 42}))

  assert pickle.loads(pwr.get_work(wpath)) == {'v': 42}


def test_error_payload_is_detected_and_raises(tmp_path):
  """get_work() re-raises the WorkException held by an error payload."""
  wpath = str(tmp_path / 'c.result')

  pwr.write_error(wpath, ValueError('bad work'), extra='meta')

  # load_work() surfaces the WorkException without raising.
  wex = pwr.load_work(wpath)

  assert isinstance(wex, pwr.WorkException)
  assert wex.is_instance(ValueError)
  assert wex.extra == 'meta'

  # get_work() re-raises the held exception.
  with pytest.raises(ValueError, match='bad work'):
    pwr.get_work(wpath)


def test_work_exception_survives_pickle_roundtrip():
  """WorkException pickles and re-raises its held exception."""
  wex = pwr.WorkException(ValueError('boom'), tag='t1')
  wex2 = pickle.loads(pickle.dumps(wex))

  assert wex2.is_instance(ValueError)

  with pytest.raises(ValueError, match='boom'):
    wex2.do_raise()


def test_work_exception_unpicklable_falls_back_generic():
  """Holding an unpicklable exception stores a generic Exception.

  The reported type degrades to Exception, so is_instance() stays
  consistent with the exception actually raised by do_raise().
  """

  class _Unpicklable(Exception):

    def __str__(self):
      return 'no-ride'

    def __reduce__(self):
      raise TypeError('no pickle')

  wex = pwr.WorkException(_Unpicklable())

  # The claim degrades to the transportable generic type.
  assert wex.is_instance(Exception)
  assert not wex.is_instance(_Unpicklable)

  held = wex.exception()

  assert held.__class__ is Exception
  # The fallback message carries the class name and the original message.
  assert '_Unpicklable: no-ride' in str(held)


def test_holder_picklable_with_local_exception_class():
  """A holder with a locally-defined exception class stays picklable."""

  def make():

    class LocalEx(Exception):
      pass

    return pwr.WorkException(LocalEx('local boom'), stage='w1')

  wex = make()
  # The headline regression: the holder itself must serialize.
  wex2 = pickle.loads(pickle.dumps(wex))

  # The degraded-but-honest type claim survives the roundtrip.
  assert wex2.is_instance(Exception)
  assert not wex2.is_instance(ValueError)
  assert wex2.stage == 'w1'

  with pytest.raises(Exception) as excinfo:
    wex2.do_raise()

  assert 'LocalEx: local boom' in str(excinfo.value)


def test_holder_roundtrip_preserves_exact_type():
  """Importable exception classes survive with their exact type."""
  wex = pwr.WorkException(ValueError('exact boom'), code=7)
  wex2 = pickle.loads(pickle.dumps(wex))

  # is_instance() derives through the exception hierarchy.
  assert wex2.is_instance(ValueError)
  assert wex2.is_instance(Exception)
  assert wex2.code == 7

  with pytest.raises(ValueError, match='exact boom'):
    wex2.do_raise()


def test_enum_ready_lists_result_files(tmp_path):
  """enum_ready() yields (name, path) for every stored result."""
  for tag in ('w1', 'w2'):
    with pwr.write_result(str(tmp_path / tag)) as fd:
      fd.write(b'x')

  names = sorted(name for name, _path in pwr.enum_ready(str(tmp_path)))

  assert names == ['w1', 'w2']


def test_get_work_missing_file_raises_oserror(tmp_path):
  """load_work() resolves path/workid when wpath is omitted."""
  with pytest.raises(OSError):
    pwr.load_work(path=str(tmp_path), workid='nope')
