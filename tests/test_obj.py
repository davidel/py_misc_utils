"""Tests for py_misc_utils.obj.Obj and str_value.

Expectations are pinned to the observed behavior (verified by probe
scripts before writing): Obj stores kwargs as attributes, as_dict()
recursively converts nested Obj instances (including inside
lists/tuples and dict values) to plain dicts, clone() is a shallow copy
with overrides, equality compares attribute contents, update() copies
another object's attributes and returns self, and str_value() quotes
strings with escaped inner quotes.
"""

import copy

from py_misc_utils import obj as objm


def test_kwargs_become_attributes():
  """Constructor kwargs are plain instance attributes."""
  o = objm.Obj(a=1, b='two')
  assert o.a == 1
  assert o.b == 'two'


def test_equality_based_on_attributes():
  """Two Objs with equal attributes compare equal."""
  assert objm.Obj(a=1, b='two') == objm.Obj(a=1, b='two')
  assert objm.Obj(a=1) != objm.Obj(a=2)


def test_equality_ignores_missing_attributes_of_other():
  """Report-only: an attribute missing from other compares False."""
  o = objm.Obj(a=1, b=2)
  assert o != objm.Obj(a=1)


def test_as_dict_flattens_nested_objs():
  """Nested Obj instances convert to plain dicts recursively."""
  inner = objm.Obj(x=1)
  outer = objm.Obj(inner=inner)
  assert outer.as_dict() == {'inner': {'x': 1}}


def test_as_dict_recurses_into_lists_and_tuples():
  """Obj items inside lists/tuples convert, preserving the type."""
  inner = objm.Obj(x=1)
  outer = objm.Obj(lst=[inner, 2], tup=(inner,))
  ad = outer.as_dict()
  assert ad['lst'] == [{'x': 1}, 2]
  assert isinstance(ad['lst'], list)
  assert isinstance(ad['tup'], tuple)


def test_as_dict_recurses_into_dict_values():
  """Obj values inside dicts convert, keys are left untouched."""
  inner = objm.Obj(x=1)
  outer = objm.Obj(d={'k': inner, 'plain': 3})
  assert outer.as_dict()['d'] == {'k': {'x': 1}, 'plain': 3}


def test_as_dict_preserves_container_types():
  """Empty and non-Obj containers keep their concrete types."""
  o = objm.Obj(l=[], d={}, t=(1, 2))
  ad = o.as_dict()
  assert ad['l'] == []
  assert ad['d'] == {}
  assert ad['t'] == (1, 2)


def test_clone_is_shallow_copy_with_overrides():
  """clone() copies attributes and applies kwargs overrides."""
  o = objm.Obj(a=1, b='two')
  c = o.clone(b='three')
  assert (c.a, c.b) == (1, 'three')
  assert o.b == 'two'


def test_clone_shares_mutable_values():
  """clone() is shallow: mutable attribute values are shared."""
  shared = [1]
  o = objm.Obj(data=shared)
  c = o.clone()
  c.data.append(2)
  assert o.data == [1, 2]


def test_update_from_copies_attributes_and_returns_self():
  """update_from() merges another object's __dict__ into self."""
  o = objm.Obj(a=1)
  ret = o.update_from(objm.Obj(a=9, b=2))
  assert ret is o
  assert (o.a, o.b) == (9, 2)


def test_str_value_quotes_and_escapes_strings():
  """Strings are quoted with inner quotes escaped."""
  assert objm.str_value('say "hi"') == '"say \\"hi\\""'
  assert objm.str_value(3.5) == '3.5'


def test_repr_uses_str_value():
  """repr() formats strings quoted and scalars bare."""
  o = objm.Obj(a=1, b='x')
  assert repr(o) == 'Obj(a=1, b="x")'


def test_copy_protocol_uses_getstate():
  """copy.copy() roundtrips the attribute dict."""
  o = objm.Obj(a=1, b='two')
  c = copy.copy(o)
  assert c == o
  assert c is not o
