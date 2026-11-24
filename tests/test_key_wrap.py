"""Tests for py_misc_utils.key_wrap.

Expectations pinned to the observed behavior (verified by probe scripts
before writing): key_wrap(cname, key_name) builds a class comparing by
the designated key attribute, extra kwargs become instance attributes,
repr shows the key, and instances hash by the key.
"""

import pytest

from py_misc_utils import key_wrap as kw


@pytest.fixture
def PKey():
  """A generated class keyed by 'id'."""
  return kw.key_wrap('PKey', 'id')


def test_stores_key_and_attrs(PKey):
  """Instances store the key plus any extra kwargs as attributes."""
  p = PKey(id=7, name='n7')
  assert p.id == 7
  assert p.name == 'n7'


def test_ordering(PKey):
  """Comparisons use the key attribute."""
  a = PKey(id=1)
  b = PKey(id=2)
  assert a < b
  assert a <= b
  assert b > a
  assert b >= a


def test_equality_and_hash(PKey):
  """Equal keys compare equal and share the same hash bucket."""
  a = PKey(id=5, name='x')
  b = PKey(id=5, name='y')
  assert a == b
  assert hash(a) == hash(b)
  assert len({a, b}) == 1


def test_inequality(PKey):
  """Different keys are unequal; eq access other.key (AttributeError on
  unrelated types is the observed behavior, not NotImplementedError)."""
  assert PKey(id=1) != PKey(id=2)


def test_eq_unrelated_type_raises(PKey):
  """__eq__ dereferences other.key, so unrelated types raise
  AttributeError (report-only behavior, no isinstance guard)."""
  with pytest.raises(AttributeError):
    bool(PKey(id=1) == 'not-a-key')


def test_repr(PKey):
  """repr shows key=value followed by the remaining attributes."""
  assert repr(PKey(id=9)) == 'id=[9] : {}'
  assert repr(PKey(id=9, name='n9')) == "id=[9] : {'name': 'n9'}"
