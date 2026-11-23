"""Tests for the py_misc_utils.ordered_set OrderedSet collection.

Expectations follow the documented contracts: insertion order is kept,
duplicates are no-ops reporting the original sequence number, removal
follows set semantics, and the algebra operations return new OrderedSet
instances preserving iteration order.
"""

import copy

import pytest

import py_misc_utils.ordered_set as osd


def test_init_preserves_insertion_order():
  """Iteration yields elements in insertion order, duplicates dropped."""
  oset = osd.OrderedSet([3, 1, 2, 1, 3])

  assert list(oset) == [3, 1, 2]
  assert len(oset) == 3


def test_add_assigns_increasing_sequence_numbers():
  """add() returns 0-based sequence numbers, reusing them for duplicates."""
  oset = osd.OrderedSet()

  assert oset.add('x') == 0
  assert oset.add('y') == 1
  assert oset.add('x') == 0
  assert list(oset) == ['x', 'y']


def test_contains_and_len_follow_set_semantics():
  """Membership and length ignore insertion order and duplicates."""
  oset = osd.OrderedSet(['a', 'b'])

  assert 'a' in oset
  assert 'z' not in oset
  assert len(oset) == 2


def test_remove_raises_and_discard_is_silent():
  """remove() raises KeyError for missing values, discard() does not."""
  oset = osd.OrderedSet(['a'])

  oset.discard('missing')

  with pytest.raises(KeyError):
    oset.remove('missing')

  oset.remove('a')

  assert len(oset) == 0


def test_pop_removes_the_last_element():
  """pop() removes and returns the most recently added element."""
  oset = osd.OrderedSet(['a', 'b'])

  assert oset.pop() == 'b'
  assert oset.pop() == 'a'
  assert len(oset) == 0

  with pytest.raises(KeyError):
    oset.pop()


def test_clear_empties_and_restarts_numbering():
  """clear() drops all elements and restarts sequence numbering."""
  oset = osd.OrderedSet(['a', 'b'])
  oset.clear()

  assert len(oset) == 0
  assert oset.add('c') == 0


def test_values_matches_iteration_order():
  """values() exposes the elements in insertion order (a generator)."""
  oset = osd.OrderedSet(['b', 'a', 'c'])

  assert list(oset.values()) == ['b', 'a', 'c']


def test_union_adds_missing_elements_in_order():
  """union() returns the union, this-set order first, then new elements."""
  oset = osd.OrderedSet(['a', 'b'])
  udata = oset.union(['b', 'c'], ['d'])

  assert list(udata) == ['a', 'b', 'c', 'd']
  assert type(udata) is osd.OrderedSet
  assert list(oset) == ['a', 'b']


def test_union_leaves_the_original_untouched():
  """union() builds a new set without mutating self."""
  oset = osd.OrderedSet(['a', 'b'])
  udata = oset.union(['c', 'd'])

  assert list(udata) == ['a', 'b', 'c', 'd']
  assert list(oset) == ['a', 'b']


def test_copy_is_independent_from_the_original():
  """copy() and copy.copy() return sets with their own internal state."""
  oset = osd.OrderedSet(['a', 'b'])
  cloned = oset.copy()
  cloned.add('y')
  oset.add('z')

  assert list(cloned) == ['a', 'b', 'y']
  assert list(oset) == ['a', 'b', 'z']

  shallow = copy.copy(oset)
  oset.discard('z')

  assert list(shallow) == ['a', 'b', 'z']
  assert list(oset) == ['a', 'b']


def test_intersection_keeps_only_common_elements():
  """intersection() keeps elements present in every other iterable."""
  oset = osd.OrderedSet(['a', 'b', 'c'])

  assert list(oset.intersection(['b', 'c', 'd'], ['c', 'x'])) == ['c']


def test_intersection_without_others_keeps_all_elements():
  """intersection() with no arguments yields every element, in order."""
  oset = osd.OrderedSet(['a', 'b'])

  assert list(oset.intersection()) == ['a', 'b']


def test_difference_removes_elements_found_in_others():
  """difference() keeps elements not appearing in any of the others."""
  oset = osd.OrderedSet(['a', 'b', 'c'])

  assert list(oset.difference(['b'])) == ['a', 'c']
