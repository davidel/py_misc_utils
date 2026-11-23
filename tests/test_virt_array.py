"""Tests for py_misc_utils.virt_array.VirtArray.

Expectations are pinned to the observed behavior (verified by probe
scripts before writing): VirtArray is a read-only Sequence view over
selected indices of an underlying array, supports slicing into new
VirtArray views, materialization to numpy, and index shuffling via a
seeded generator for deterministic order.
"""

import numpy as np

from py_misc_utils import virt_array as vir


def test_shape_and_len_reflect_selection():
  """shape is (len(indices), element-shape); len matches selection."""
  data = np.arange(10).reshape(5, 2)
  va = vir.VirtArray(data, [3, 1, 4])
  assert va.shape == (3, 2)
  assert len(va) == 3


def test_getitem_returns_selected_underlying_elements():
  """Integer access dereferences the index into the underlying data."""
  data = np.arange(10).reshape(5, 2)
  va = vir.VirtArray(data, [3, 1, 4])
  assert va[0].tolist() == [6, 7]
  assert va[2].tolist() == [8, 9]


def test_slice_returns_new_virt_array_view():
  """Slicing yields a VirtArray over the selected index range."""
  data = np.arange(10).reshape(5, 2)
  va = vir.VirtArray(data, [3, 1, 4])
  sub = va[1:3]
  assert isinstance(sub, vir.VirtArray)
  assert sub.indices.tolist() == [1, 4]
  assert sub[0].tolist() == [2, 3]


def test_to_numpy_stacks_selection_in_index_order():
  """Materialization stacks elements in index order."""
  data = np.arange(10).reshape(5, 2)
  va = vir.VirtArray(data, [3, 1, 4])
  assert va.to_numpy().tolist() == [[6, 7], [2, 3], [8, 9]]


def test_numpy_conversion_protocol():
  """np.array() materializes the view via __array__."""
  data = np.arange(10).reshape(5, 2)
  va = vir.VirtArray(data, [3, 1, 4])
  assert np.array(va).tolist() == [[6, 7], [2, 3], [8, 9]]


def test_to_numpy_dtype_argument():
  """dtype conversion happens at materialization time."""
  data = np.arange(10).reshape(5, 2)
  va = vir.VirtArray(data, [0, 1])
  assert va.to_numpy(dtype=np.float64).dtype == np.float64


def test_empty_selection():
  """An empty index list yields an empty view with shape (0,)."""
  data = np.arange(10).reshape(5, 2)
  va = vir.VirtArray(data, [])
  assert va.shape == (0,)
  assert len(va) == 0


def test_shuffle_with_seeded_rng_is_deterministic():
  """Shuffling with a seeded generator is reproducible and yields a
  true permutation of the indices."""
  data = np.arange(10).reshape(5, 2)
  va1 = vir.VirtArray(data, [0, 1, 2, 3, 4])
  va1.shuffle(np.random.default_rng(42))
  va2 = vir.VirtArray(data, [0, 1, 2, 3, 4])
  va2.shuffle(np.random.default_rng(42))
  assert va1.indices.tolist() == va2.indices.tolist()
  assert sorted(va1.indices.tolist()) == [0, 1, 2, 3, 4]


def test_shuffle_permutes_values_consistently():
  """After shuffling, the view still exposes the same elements."""
  data = np.arange(10).reshape(5, 2)
  va = vir.VirtArray(data, [0, 1, 2, 3, 4])
  before = np.array(va)
  va.shuffle(np.random.default_rng(123))
  after = np.array(va)
  assert sorted(map(tuple, before.tolist())) == sorted(map(tuple, after.tolist()))


def test_iteration_yields_all_selected_elements():
  """Iteration over the Sequence walks every selected element."""
  data = np.arange(10).reshape(5, 2)
  va = vir.VirtArray(data, [4, 0, 2])
  assert [x.tolist() for x in va] == [[8, 9], [0, 1], [4, 5]]
