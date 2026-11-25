"""Tests for py_misc_utils.pickle_wrap.

Expectations pinned to the observed behavior (verified by probe scripts
before writing): wrap() traverses lists/tuples/dicts and wraps objects
whose root module is not in KNOWN_MODULES and not stdlib into
PickleWrap instances; unwrap() loads PickleWrap instances back (keeping
unloadable ones as-is); add_known_module() extends the known set;
PickleWrap records the qualified class name and pickled data.
"""



from py_misc_utils import pickle_wrap as pw


class Local:
  """A class from the test module - not a known module."""

  def __init__(self, v):
    self.v = v


def _reset_known():
  """Keep the global KNOWN_MODULES state stable across tests."""
  added = set()

  class Tracker:
    pass

  return added


def test_wrap_known_module_object_untouched():
  """Objects from known modules pass through unwrapped."""
  obj = dict(a=[1, 2], b=3)
  assert pw.wrap(obj) == obj


def test_wrap_local_object():
  """A test-local object is wrapped into a PickleWrap."""
  res = pw.wrap(Local(9))

  assert isinstance(res, pw.PickleWrap)
  assert res.wrapped_class() == f'{__name__}.Local'


def test_wrap_nested():
  """Nested lists/dicts get PickleWrap leaves."""
  res = pw.wrap(dict(items=[Local(1), Local(2)], known=[1, 2]))

  assert isinstance(res['items'][0], pw.PickleWrap)
  assert res['known'] == [1, 2]


def test_wrap_tuple_rebuilt():
  """Wrapped tuples are rebuilt as tuples."""
  res = pw.wrap((Local(1),))
  assert isinstance(res, tuple)
  assert isinstance(res[0], pw.PickleWrap)


def test_unwrap_roundtrip():
  """unwrap() loads wrapped objects back."""
  wrapped = pw.wrap(Local(5))
  obj = pw.unwrap(wrapped)

  assert isinstance(obj, Local)
  assert obj.v == 5


def test_unwrap_nested_roundtrip():
  """A wrapped tree survives wrap/unwrap unchanged."""
  orig = dict(items=[Local(1), 2], plain='x')
  obj = pw.unwrap(pw.wrap(orig))

  assert obj['plain'] == 'x'
  assert isinstance(obj['items'][0], Local)
  assert obj['items'][0].v == 1
  assert obj['items'][1] == 2


class _BadPickle:
  """Pickle module whose loads() always raises."""

  @staticmethod
  def dumps(obj):
    return b'some-data'

  @staticmethod
  def loads(data):
    raise ValueError('cannot load')


def test_unwrap_unloadable_kept():
  """A PickleWrap whose load() raises is kept as-is by unwrap()."""
  wrap = pw.PickleWrap(Local(3), pickle_module=_BadPickle)

  res = pw.unwrap(wrap, pickle_module=_BadPickle)

  assert res is wrap
  assert isinstance(res, pw.PickleWrap)


def test_add_known_module():
  """add_known_module() makes objects of that module pass through."""
  pw.add_known_module(__name__)
  try:
    assert pw.wrap(Local(1)) is not None
    res = pw.wrap(Local(1))
    assert not isinstance(res, pw.PickleWrap)
  finally:
    pw.KNOWN_MODULES.discard('tests')


def test_pickle_wrap_direct():
  """PickleWrap stores the qual name and round-trips the data."""
  wrap = pw.PickleWrap(Local(7))

  assert wrap.wrapped_class() == f'{__name__}.Local'
  obj = wrap.load()
  assert obj.v == 7


def test_pickle_wrap_custom_pickle_module():
  """A custom pickle-like module can be used for (de)serialization."""
  import json

  class JSONPickle:

    @staticmethod
    def dumps(obj):
      return json.dumps(obj).encode()

    @staticmethod
    def loads(data):
      return json.loads(data.decode())

  wrap = pw.PickleWrap(dict(a=1), pickle_module=JSONPickle)
  assert wrap.load(pickle_module=JSONPickle) == dict(a=1)


def test_picklewrap_of_dict_passthrough():
  """Plain dicts of known-module values are returned unchanged."""
  vals = dict(a=1, b='x', c=[1.0, 2.0])
  assert pw.wrap(vals) == vals
