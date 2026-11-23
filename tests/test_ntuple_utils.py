"""Tests for py_misc_utils.ntuple_utils.extend.

Expectations are pinned to the observed behavior (verified by probe
scripts before writing): extend() appends new fields after the base
fields, base fields having defaults are reordered after the non-default
fields (collections.namedtuple requirement), new-field defaults follow
the namedtuple positional last-N semantics, and duplicate field names
are rejected with an AssertionError.
"""

import collections

import pytest

from py_misc_utils import ntuple_utils as ntu


def test_extend_appends_fields_after_base_fields():
  """New fields are appended after the preserved base fields."""
  Point = collections.namedtuple('Point', 'x y')
  Point3 = ntu.extend(Point, 'Point3', 'z')
  assert Point3._fields == ('x', 'y', 'z')


def test_extend_accepts_comma_separated_string():
  """Field specs can be comma/space separated strings."""
  Point = collections.namedtuple('Point', 'x y')
  Point3 = ntu.extend(Point, 'Point3', 'z, w')
  assert Point3._fields == ('x', 'y', 'z', 'w')


def test_extend_new_defaults_follow_namedtuple_positional_semantics():
  """defaults apply to the rightmost new fields, in order."""
  Point = collections.namedtuple('Point', 'x y')
  Point4 = ntu.extend(Point, 'Point4', 'z w', defaults=[7, 9])
  assert Point4._field_defaults == {'z': 7, 'w': 9}
  assert Point4(1, 2, 3, 4) == (1, 2, 3, 4)
  assert Point4(1, 2, z=3) == (1, 2, 3, 9)


def test_extend_preserves_base_field_defaults():
  """Base field defaults are carried over to the extended type."""
  Base = collections.namedtuple('Base', 'a b', defaults=[10])
  Ext = ntu.extend(Base, 'Ext', 'c')
  assert Ext._field_defaults == {'b': 10}


def test_extend_reorders_defaulted_base_fields_last():
  """Base fields with defaults move last so the result is constructible
  positionally; the non-defaulted base fields keep their order."""
  Base = collections.namedtuple('Base', 'a b c', defaults=[10, 20])
  Ext = ntu.extend(Base, 'Ext', 'd')
  assert Ext._fields == ('a', 'd', 'b', 'c')
  assert Ext._field_defaults == {'b': 10, 'c': 20}
  assert Ext(1, 2) == (1, 2, 10, 20)


def test_extend_rejects_duplicate_field_names():
  """A new field colliding with a base field raises AssertionError."""
  Point = collections.namedtuple('Point', 'x y')
  with pytest.raises(AssertionError):
    ntu.extend(Point, 'Bad', 'x')


def test_extend_rejects_duplicate_new_fields():
  """Two new fields with the same name raise AssertionError."""
  Point = collections.namedtuple('Point', 'x y')
  with pytest.raises(AssertionError):
    ntu.extend(Point, 'Bad', 'z z')


def test_extended_type_is_independent_from_base():
  """The extended type constructs without base defaults interference."""
  Base = collections.namedtuple('Base', 'a b', defaults=[10])
  Ext = ntu.extend(Base, 'Ext', 'c', defaults=[5])
  assert Base(1) == (1, 10)
  assert Ext(1) == (1, 10, 5)
