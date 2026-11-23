"""Tests for py_misc_utils.enum.make_enum.

Expectations are pinned to the observed behavior (verified by probe
scripts before writing): make_enum() builds a class-like object whose
fields are accessible as attributes with auto-incrementing values
starting at base, explicit values (NAME=value or (NAME, value) tuples)
pin the sequence, repr() summarizes the fields, and len() counts them.
Duplicate names are silently overwritten (report-only shortcoming).
"""

import pytest

from py_misc_utils import enum as enu


def test_auto_values_start_at_base():
  """Auto-increment starts at base (0 by default)."""
  Color = enu.make_enum('Color', 'RED, GREEN, BLUE')
  assert Color.RED == 0
  assert Color.GREEN == 1
  assert Color.BLUE == 2


def test_custom_base():
  """A nonzero base shifts every auto value."""
  Neg = enu.make_enum('Neg', 'A, B', base=-3)
  assert Neg.A == -3
  assert Neg.B == -2


def test_explicit_value_via_string_syntax():
  """NAME=value in the string pins that field's value."""
  Color = enu.make_enum('Color', 'RED, GREEN=5, BLUE')
  assert (Color.RED, Color.GREEN, Color.BLUE) == (0, 5, 6)


def test_explicit_value_via_tuple_syntax():
  """(NAME, value) tuples pin that field's value."""
  Fruit = enu.make_enum('Fruit', ['APPLE', ('BANANA', 10), 'CHERRY'])
  assert (Fruit.APPLE, Fruit.BANANA, Fruit.CHERRY) == (0, 10, 11)


def test_auto_continues_after_explicit_value():
  """After an explicit value, auto-increment resumes from it."""
  Fruit = enu.make_enum('Fruit', ['APPLE', ('BANANA', 10), 'CHERRY'])
  assert Fruit.CHERRY == 11


def test_first_and_last_track_value_span():
  """_first is the base and _last the highest assigned value."""
  Fruit = enu.make_enum('Fruit', ['APPLE', ('BANANA', 10), 'CHERRY'])
  assert Fruit._first == 0
  assert Fruit._last == 11


def test_names_mapping():
  """_values maps names to values and _names the reverse."""
  Color = enu.make_enum('Color', 'RED, GREEN=5, BLUE')
  assert Color._values == {'RED': 0, 'GREEN': 5, 'BLUE': 6}
  assert Color._names == {0: 'RED', 5: 'GREEN', 6: 'BLUE'}


def test_len_counts_fields():
  """len() is the number of declared fields."""
  assert len(enu.make_enum('Color', 'RED, GREEN, BLUE')) == 3


def test_repr_format():
  """repr() renders ClassName(NAME=value, ...)."""
  Color = enu.make_enum('Color', 'RED, GREEN')
  assert repr(Color) == 'Color(RED=0, GREEN=1)'


def test_descent_value_rejected():
  """An explicit value below the running counter raises AssertionError."""
  with pytest.raises(AssertionError):
    enu.make_enum('Desc', 'A=5, B=2')


def test_duplicate_names_silently_overwrite():
  """Report-only: duplicate names do not raise and the last assignment
  wins, leaving len() at 1."""
  Dup = enu.make_enum('Dup', 'A, A')
  assert Dup.A == 1
  assert len(Dup) == 1
