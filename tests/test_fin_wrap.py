"""Tests for py_misc_utils.fin_wrap.

fin_wrap stores a wrapper on a parent attribute and registers a
weakref.finalize on the wrapper, so the cleanup function fires when
the wrapper becomes unreachable. Teardown mode (obj=None) either calls
(cleanup=True) or detaches (cleanup=False) the registered finalizer.
The internal attribute name used to store the wrapper on the parent is
a library detail and is never asserted here.
"""

import gc

import pytest

from py_misc_utils import fin_wrap as fw


class Holder:
  pass


@pytest.fixture
def fired():
  """Returns (events, finfn): the finalizer appends to the list."""
  events = []

  def finfn():
    events.append('fired')

  return events, finfn


def test_wrap_stores_object_on_parent(fired):
  _events, finfn = fired
  parent = Holder()
  obj = object()

  fw.fin_wrap(parent, 'v', obj, finfn=finfn)

  assert parent.v is obj


def test_wrap_returns_the_wrapper_instance(fired):
  _events, finfn = fired
  parent = Holder()
  obj = object()

  res = fw.fin_wrap(parent, 'v', obj, finfn=finfn)

  assert isinstance(res, fw.fin_wrap)
  assert parent.v is obj


def test_wrap_requires_finfn():
  parent = Holder()

  with pytest.raises(AssertionError):
    fw.fin_wrap(parent, 'v', object())


def test_finalizer_fires_when_parent_dies(fired):
  """The wrapper is referenced by the parent, so it dies with it."""
  events, finfn = fired
  parent = fw._Wrapper()
  fw.fin_wrap(parent, 'v', object(), finfn=finfn)

  gc.collect()
  assert events == []

  del parent
  gc.collect()

  assert events == ['fired']


def test_fin_wrap_np_finalizer_fires_on_wrapper_death(fired):
  events, finfn = fired
  obj = object()

  wrapper = fw.fin_wrap_np(obj, finfn)
  assert wrapper.v is obj

  gc.collect()
  assert events == []

  del wrapper
  gc.collect()

  assert events == ['fired']


def test_teardown_cleanup_true_calls_finalizer_and_nulls_attribute(fired):
  events, finfn = fired
  parent = Holder()
  fw.fin_wrap(parent, 'v', object(), finfn=finfn)

  fw.fin_wrap(parent, 'v', None, cleanup=True)

  assert events == ['fired']
  assert parent.v is None


def test_teardown_cleanup_true_does_not_fire_twice(fired):
  events, finfn = fired
  parent = Holder()
  fw.fin_wrap(parent, 'v', object(), finfn=finfn)

  fw.fin_wrap(parent, 'v', None, cleanup=True)

  # weakref.finalize guards against double invocation.
  del parent
  gc.collect()

  assert events == ['fired']


def test_teardown_cleanup_false_detaches_finalizer(fired):
  events, finfn = fired
  parent = Holder()
  fw.fin_wrap(parent, 'v', object(), finfn=finfn)

  fw.fin_wrap(parent, 'v', None, cleanup=False)

  # Detached finalizer never fires - not even when the parent dies.
  del parent
  gc.collect()

  assert events == []


def test_teardown_without_registration_is_noop():
  parent = Holder()

  fw.fin_wrap(parent, 'v', None, cleanup=True)
  fw.fin_wrap(parent, 'v', None, cleanup=False)


def test_teardown_with_obj_none_and_no_finfn_is_allowed():
  # obj=None routes to the teardown path even with no registered
  # finalizer; the attribute is still assigned (to None) first.
  parent = Holder()

  fw.fin_wrap(parent, 'v', None)

  assert parent.v is None


class Target:

  def __init__(self):
    self.value = 7

  def method(self, x):
    return x * 2


def test_finwrapper_delegates_read_and_call(fired):
  _events, finfn = fired
  target = Target()

  wrapper = fw.FinWrapper(target, finfn)

  assert wrapper.value == 7
  assert wrapper.method(21) == 42


def test_finwrapper_delegates_set_and_delete(fired):
  _events, finfn = fired
  target = Target()

  wrapper = fw.FinWrapper(target, finfn)
  wrapper.value = 99
  assert target.value == 99

  del wrapper.value
  assert not hasattr(target, 'value')


def test_finwrapper_missing_attribute_raises(fired):
  _events, finfn = fired
  target = Target()
  wrapper = fw.FinWrapper(target, finfn)

  with pytest.raises(AttributeError):
    _ = wrapper.nonexistent


def test_finwrapper_finalizer_fires_on_death(fired):
  events, finfn = fired
  wrapper = fw.FinWrapper(Target(), finfn)

  gc.collect()
  assert events == []

  del wrapper
  gc.collect()

  assert events == ['fired']
