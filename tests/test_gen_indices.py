"""Tests for py_misc_utils.gen_indices.generate.

Expectations are pinned to the observed behavior (verified by probe
scripts before writing): generate() yields one tuple per selected
element, expanding ints to singleton ranges, slices to their index
ranges, iterables element-wise, and Ellipsis (inside tuples only) to
full ranges over the remaining dimensions. Bare Ellipsis and invalid
specifications raise, and a partial tuple specification raises
TypeError (report-only shortcoming vs numpy auto-fill semantics).
"""

import numpy as np
import pytest

from py_misc_utils import gen_indices as gin


def test_integer_index_yields_single_tuple():
  """An int index selects one element as a 1-tuple."""
  assert list(gin.generate((4,), 2)) == [(2,)]


def test_slice_index_expands_to_range():
  """A slice index expands to its index range."""
  assert list(gin.generate((4,), slice(1, 3))) == [(1,), (2,)]


def test_list_index_expands_elementwise():
  """A list index yields one tuple per listed element."""
  assert list(gin.generate((4,), [0, 2])) == [(0,), (2,)]


def test_ndarray_fancy_index_expands_elementwise():
  """A numpy fancy index yields one tuple per element."""
  result = list(gin.generate((4,), np.array([1, 3])))
  assert [t[0] for t in result] == [1, 3]


def test_tuple_of_ints_selects_single_element():
  """A tuple of ints selects exactly one element."""
  assert list(gin.generate((2, 3), (1, 2))) == [(1, 2)]


def test_tuple_with_slices_expands_cartesian_product():
  """Slice components expand over their axes."""
  assert list(gin.generate((2, 3), (slice(None), 1))) == [(0, 1), (1, 1)]


def test_tuple_with_iterable_components():
  """Iterable components expand like fancy indexing."""
  assert list(gin.generate((2, 3), ([0, 1], slice(None, 2)))) == [(0, 0), (0, 1),
                                                                  (1, 0), (1, 1)]


def test_leading_ellipsis_fills_remaining_dimensions():
  """A leading Ellipsis expands to full ranges of the middle axes."""
  assert list(gin.generate((2, 3), (Ellipsis, 0))) == [(0, 0), (1, 0)]


def test_trailing_ellipsis_fills_leading_dimensions():
  """A trailing Ellipsis expands to full ranges of the middle axes."""
  assert list(gin.generate((2, 3, 2), (1, Ellipsis, 0))) == [(1, 0, 0), (1, 1, 0),
                                                             (1, 2, 0)]


def test_bare_ellipsis_raises_type_error():
  """A bare Ellipsis specification is rejected with TypeError (it is not
  one of the supported top-level index types)."""
  with pytest.raises(TypeError):
    list(gin.generate((2, 3), Ellipsis))


def test_double_ellipsis_raises_value_error():
  """Two Ellipsis entries in one tuple raise ValueError."""
  with pytest.raises(ValueError):
    list(gin.generate((2, 3), (Ellipsis, Ellipsis)))


def test_integer_on_multi_dimensional_shape_raises():
  """An int index requires a 1-D shape (AssertionError otherwise)."""
  with pytest.raises(AssertionError):
    list(gin.generate((2, 3), 5))


def test_list_on_multi_dimensional_shape_raises():
  """A list index requires a 1-D shape (AssertionError otherwise)."""
  with pytest.raises(AssertionError):
    list(gin.generate((2, 3), [0, 1]))


def test_partial_tuple_raises_type_error():
  """A tuple shorter than the shape raises TypeError (report-only:
  numpy would auto-fill trailing dimensions with full ranges)."""
  with pytest.raises(TypeError):
    list(gin.generate((2, 3), (1,)))
