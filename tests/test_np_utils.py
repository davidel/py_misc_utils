"""Tests for py_misc_utils.np_utils.

Expectations follow the documented API contract. Tests assert specific
values verified by probe scripts; where library behavior deviates from
the documented contract the test will fail, exposing a possible library
bug (see KNOWN LIBRARY BUGS in SCRATCH/REFACTORING_PLAN.md).
"""

import numpy as np
import pytest

from py_misc_utils import np_utils as npu


def test_diff_split_groups_by_mask():
  """diff_split() groups indices where mask(diff) is true."""
  data = np.array([1.0, 2.0, 5.0, 6.0, 9.0])
  splits = npu.diff_split(data, lambda d: d > 2.0)

  assert [data[s].tolist() for s in splits] == [
    [1.0, 2.0],
    [5.0, 6.0],
    [9.0],
  ]


def test_group_by_delta_splits_contiguous_groups():
  """group_by_delta() returns contiguous groups split at mask positions."""
  data = np.array([1.0, 2.0, 5.0, 6.0, 9.0])
  groups = npu.group_by_delta(data, lambda d: d > 2.0)

  assert [g.tolist() for g in groups] == [
    [1.0, 2.0],
    [5.0, 6.0],
    [9.0],
  ]


def test_group_splits_returns_split_indices():
  """group_splits() returns indices where splits should occur."""
  data = np.array([1.0, 2.0, 5.0, 6.0])
  splits = npu.group_splits(data, lambda d: d > 2.0)

  assert splits.tolist() == [1]


def test_fillna_replaces_nan_with_neighbor():
  """fillna() fills NaN runs from neighbors (following value, or preceding
  value for trailing runs), or defval when there is no neighbor at all."""
  data = np.array([1.0, np.nan, np.nan, 4.0, np.nan, np.nan])
  result = npu.fillna(data)

  assert result.tolist() == [1.0, 4.0, 4.0, 4.0, 4.0, 4.0]


def test_fillna_copy_leaves_original_unchanged():
  """fillna(copy=True) does not mutate the input array."""
  data = np.array([1.0, np.nan, 3.0])
  result = npu.fillna(data, copy=True)

  assert not np.array_equal(data, result)
  assert np.isnan(data[1])


def test_fillna_all_nan_uses_defval():
  """fillna() uses defval when the entire array is NaN."""
  data = np.array([np.nan, np.nan, np.nan])
  result = npu.fillna(data, defval=-1)

  assert result.tolist() == [-1.0, -1.0, -1.0]


def test_infer_np_dtype_promotes_to_float64():
  """infer_np_dtype() prefers float64 over float32 and integers."""
  dtype = npu.infer_np_dtype([np.float32, np.float64, np.int32])

  assert dtype == np.float64


def test_infer_np_dtype_promotes_to_float32():
  """infer_np_dtype() promotes float32 over smaller integer types."""
  dtype = npu.infer_np_dtype([np.int16, np.float32])

  assert dtype == np.float32


def test_infer_np_dtype_picks_largest_integer():
  """infer_np_dtype() picks the largest integer type when no floats."""
  dtype = npu.infer_np_dtype([np.int16, np.int64])

  assert dtype == np.int64


def test_infer_np_dtype_defaults_to_float32():
  """infer_np_dtype() returns float32 for an empty sequence."""
  dtype = npu.infer_np_dtype([])

  assert dtype == np.float32


def test_maybe_stack_slices_stacks_ndarrays():
  """maybe_stack_slices() stacks a list of ndarrays along an axis."""
  slices = [np.array([1, 2]), np.array([3, 4])]
  result = npu.maybe_stack_slices(slices)

  assert isinstance(result, np.ndarray)
  assert result.tolist() == [[1, 2], [3, 4]]


def test_maybe_stack_slices_passthrough_non_ndarrays():
  """maybe_stack_slices() returns the list unchanged when elements are not
  ndarrays."""
  slices = [[1, 2], [3, 4]]
  result = npu.maybe_stack_slices(slices)

  assert result == slices


def test_to_numpy_passthrough_ndarray():
  """to_numpy() returns ndarrays unchanged."""
  data = np.array([1, 2, 3])
  result = npu.to_numpy(data)

  assert result is data


def test_to_numpy_from_array_like():
  """to_numpy() converts list-likes to ndarrays."""
  result = npu.to_numpy([1, 2, 3])

  assert isinstance(result, np.ndarray)
  assert result.tolist() == [1, 2, 3]


def test_is_numeric_integer_floating_numpy():
  """is_numeric/is_integer/is_floating/is_numpy reflect numpy type hierarchy."""
  assert npu.is_numeric(np.int32)
  assert npu.is_numeric(np.float64)
  assert not npu.is_numeric(bool)

  assert npu.is_integer(np.int32)
  assert not npu.is_integer(np.float64)

  assert npu.is_floating(np.float64)
  assert not npu.is_floating(np.int32)

  assert npu.is_numpy(np.array([1, 2]))
  assert not npu.is_numpy([1, 2])


def test_astype_converts_numeric_columns():
  """astype() converts numeric columns to the target dtype."""
  data = np.array([1, 2, 3], dtype=np.int32)
  result = npu.astype(data, 0, np.float64)

  assert result.dtype == np.float64


def test_astype_skips_non_numeric_columns():
  """astype() leaves non-numeric columns unchanged."""
  data = np.array(['a', 'b', 'c'])
  result = npu.astype(data, 0, np.int32)

  assert result is data


def test_astype_with_dict_dtype():
  """astype() applies per-column dtypes from a dict mapping."""
  data = np.array([1, 2, 3], dtype=np.int32)
  result = npu.astype(data, 0, {0: np.float64})

  assert result.dtype == np.float64


def test_softmax_produces_valid_distribution():
  """softmax() outputs positive values summing to 1 along the last axis."""
  x = np.array([1.0, 2.0, 3.0])
  probs = npu.softmax(x)

  assert np.all(probs > 0)
  assert np.isclose(probs.sum(), 1.0)


def test_softmax_2d():
  """softmax() normalizes along the last axis for 2D inputs."""
  x = np.array([[1.0, 2.0], [3.0, 4.0]])
  probs = npu.softmax(x)

  assert np.allclose(probs.sum(axis=1), 1.0)


def test_categorical_1d_single_sample():
  """categorical() with 1D input and n=None returns a single scalar index."""
  probs = np.array([1.0, 0.0, 0.0])
  np.random.seed(42)
  result = npu.categorical(probs)

  assert result == 0


def test_categorical_1d_multiple_samples():
  """categorical() with 1D input and n>1 returns an array of valid indices."""
  probs = np.array([100.0, 0.0, 0.0])
  np.random.seed(42)
  result = npu.categorical(probs, n=10)

  assert result.shape == (10,)
  assert np.all((result >= 0) & (result < 3))


def test_categorical_2d():
  """categorical() with 2D input samples per row."""
  probs = np.array([[1.0, 0.0], [0.0, 1.0]])
  np.random.seed(42)
  result = npu.categorical(probs, n=1)

  assert result.tolist() == [[0], [1]]


def test_onehot_encodes_indices():
  """onehot() converts integer indices to one-hot vectors."""
  values = np.array([0, 2, 1])
  result = npu.onehot(values, num_categories=3)

  assert result.tolist() == [
    [1.0, 0.0, 0.0],
    [0.0, 0.0, 1.0],
    [0.0, 1.0, 0.0],
  ]


def test_onehot_infers_num_categories():
  """onehot() infers num_categories from max(values) + 1 when not given."""
  values = np.array([0, 1, 2])
  result = npu.onehot(values)

  assert result.shape == (3, 3)


def test_normalize_zero_mean_unit_std():
  """normalize() produces zero mean and unit standard deviation."""
  data = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
  result = npu.normalize(data)

  assert np.isclose(result.mean(), 0.0)
  assert np.isclose(result.std(), 1.0)


def test_normalize_handles_zero_std():
  """normalize() treats zero standard deviations as 1.0."""
  data = np.array([5.0, 5.0, 5.0])
  result = npu.normalize(data)

  assert np.allclose(result, 0.0)


def test_hasher_depends_on_dtype_shape_bytes():
  """hasher() produces a hash from dtype, shape, and raw bytes."""
  a = np.array([1, 2, 3], dtype=np.int32)
  b = np.array([1, 2, 3], dtype=np.int32)
  c = np.array([1, 2, 3], dtype=np.int64)

  assert npu.hasher(a) == npu.hasher(b)
  assert npu.hasher(a) != npu.hasher(c)


def test_moving_average():
  """moving_average() computes the rolling mean including current element."""
  data = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
  result = npu.moving_average(data, window=3)

  assert np.isclose(result[2], 2.0)
  assert np.isclose(result[3], 3.0)
  assert np.isclose(result[4], 4.0)


def test_moving_average_exclude_current():
  """moving_average(include_current=False) averages previous window elements."""
  data = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
  result = npu.moving_average(data, window=3, include_current=False)

  assert len(result) == 5


def test_rolling_window():
  """rolling_window() creates sliding windows over the last dimension."""
  a = np.array([1, 2, 3, 4, 5])
  windows = npu.rolling_window(a, window=3)

  assert windows.shape == (3, 3)
  assert windows.tolist() == [[1, 2, 3], [2, 3, 4], [3, 4, 5]]


def test_shift_positive():
  """shift(pos>0) shifts right, filling with edge values."""
  data = np.array([1, 2, 3, 4, 5])
  result = npu.shift(data, pos=2)

  assert result.tolist() == [1, 1, 1, 2, 3]


def test_shift_negative():
  """shift(pos<0) shifts left, filling with edge values."""
  data = np.array([1, 2, 3, 4, 5])
  result = npu.shift(data, pos=-2)

  assert result.tolist() == [3, 4, 5, 5, 5]


def test_shift_zero():
  """shift(pos=0) returns the array unchanged."""
  data = np.array([1, 2, 3])
  result = npu.shift(data, pos=0)

  assert np.array_equal(result, data)


def test_complement_indices():
  """complement_indices() returns indices not in the input set."""
  result = npu.complement_indices([0, 2], size=5)

  assert result.tolist() == [1, 3, 4]


def test_polyfit_std():
  """polyfit_std() returns std, fitted values, and polynomial object."""
  yv = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
  std, fyv, yfit = npu.polyfit_std(yv, deg=1)

  assert np.isclose(std, 0.0)
  assert np.allclose(fyv, yv)
  assert yfit is not None


def test_npdict_clone_copies_arrays():
  """npdict_clone() deep-copies a dict of numpy arrays."""
  npd = {'a': np.array([1, 2]), 'b': np.array([3, 4])}
  cloned = npu.npdict_clone(npd)

  assert cloned['a'].tolist() == [1, 2]
  assert cloned['b'].tolist() == [3, 4]
  cloned['a'][0] = 99
  assert npd['a'][0] == 1


def test_is_ordered_nondecreasing():
  """is_ordered() checks non-decreasing order by default."""
  assert npu.is_ordered([1, 2, 2, 3])
  assert not npu.is_ordered([1, 3, 2])


def test_is_ordered_nonincreasing():
  """is_ordered(reverse=True) checks non-increasing order."""
  assert npu.is_ordered([3, 2, 2, 1], reverse=True)
  assert not npu.is_ordered([1, 2, 3], reverse=True)


class TestRingBuffer:
  """Tests for RingBuffer fixed-capacity ring buffer."""

  def test_append_and_len(self):
    """append() adds elements and __len__ reports the count."""
    rb = npu.RingBuffer(capacity=5, dtype=np.int32, vshape=())

    rb.append(1)
    rb.append(2)
    assert len(rb) == 2

  def test_extend(self):
    """extend() adds multiple elements at once."""
    rb = npu.RingBuffer(capacity=10, dtype=np.int32, vshape=())

    rb.extend([1, 2, 3])
    assert len(rb) == 3
    assert rb.to_numpy().tolist() == [1, 2, 3]

  def test_to_numpy_chronological_order(self):
    """to_numpy() returns elements in chronological (append) order."""
    rb = npu.RingBuffer(capacity=4, dtype=np.int32, vshape=())
    rb.append(1)
    rb.append(2)
    rb.append(3)

    assert rb.to_numpy().tolist() == [1, 2, 3]

  def test_wrap_around(self):
    """RingBuffer wraps around when capacity is exceeded, keeping the
    last `capacity` elements in chronological order."""
    rb = npu.RingBuffer(capacity=3, dtype=np.int32, vshape=())
    rb.append(1)
    rb.append(2)
    rb.append(3)
    rb.append(4)
    rb.append(5)

    assert rb.to_numpy().tolist() == [3, 4, 5]

  def test_select_keeps_indices(self):
    """select() keeps only elements at specified indices."""
    rb = npu.RingBuffer(capacity=10, dtype=np.int32, vshape=())
    rb.extend([10, 20, 30, 40])
    rb.select(np.array([0, 2]))

    assert rb.to_numpy().tolist() == [10, 30]

  def test_resize(self):
    """resize() changes capacity, preserving the first `capacity` elements."""
    rb = npu.RingBuffer(capacity=5, dtype=np.int32, vshape=())
    rb.extend([1, 2, 3, 4, 5])
    rb.resize(3)

    assert len(rb) == 3
    assert rb.to_numpy().tolist() == [1, 2, 3]

  def test_multidim_elements(self):
    """RingBuffer supports multi-dimensional element shapes."""
    rb = npu.RingBuffer(capacity=3, dtype=np.float32, vshape=(2,))
    rb.append(np.array([1.0, 2.0]))
    rb.append(np.array([3.0, 4.0]))

    assert rb.to_numpy().shape == (2, 2)
    assert rb.to_numpy().tolist() == [[1.0, 2.0], [3.0, 4.0]]
