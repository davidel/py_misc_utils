"""Tests for py_misc_utils.transform_array.TransformArray.

Expectations are pinned to the observed behavior (verified by probe
scripts before writing): TransformArray applies a pipeline lazily on
element access, slices return new TransformArray views (range honored,
including step), and numpy materialization stacks the transformed
elements, with an empty data set yielding an empty array.
"""

import numpy as np

from py_misc_utils import transform_array as tra


def test_lazy_access_applies_pipeline_per_item():
  """Access applies the pipeline to the accessed element only."""
  data = [np.array([1, 2]), np.array([3, 4]), np.array([5, 6])]
  calls = []
  ta = tra.TransformArray(data, lambda x: calls.append(x) or x * 10)
  assert ta[1].tolist() == [30, 40]
  assert calls == [data[1]]


def test_len_and_shape_from_underlying_data():
  """len and shape come from the underlying data, not the pipeline."""
  data = [np.array([1, 2]), np.array([3, 4]), np.array([5, 6])]
  ta = tra.TransformArray(data, lambda x: x)
  assert len(ta) == 3
  assert ta.shape == (3, 2)


def test_slice_returns_lazy_subview():
  """Slicing yields a new TransformArray over the selected range."""
  data = [np.array([1, 2]), np.array([3, 4]), np.array([5, 6])]
  calls = []
  ta = tra.TransformArray(data, lambda x: calls.append(x) or x * 10)
  sub = ta[0:2]
  assert isinstance(sub, tra.TransformArray)
  assert len(sub) == 2
  # Slicing itself must not trigger any pipeline call.
  assert calls == []
  assert sub[1].tolist() == [30, 40]


def test_slice_with_step():
  """Sliced views honor the step."""
  data = [np.array([1, 2]), np.array([3, 4]), np.array([5, 6])]
  ta = tra.TransformArray(data, lambda x: x * 2)
  sub = ta[0:3:2]
  assert len(sub) == 2
  assert sub.to_numpy().tolist() == [[2, 4], [10, 12]]


def test_to_numpy_stacks_transformed_elements():
  """Materialization applies the pipeline to every element."""
  data = [np.array([1, 2]), np.array([3, 4]), np.array([5, 6])]
  ta = tra.TransformArray(data, lambda x: x * 10)
  assert ta.to_numpy().tolist() == [[10, 20], [30, 40], [50, 60]]


def test_to_numpy_with_dtype():
  """dtype conversion happens at materialization time."""
  data = [np.array([1, 2]), np.array([3, 4])]
  ta = tra.TransformArray(data, lambda x: x)
  assert ta.to_numpy(dtype=np.float64).dtype == np.float64


def test_scalar_pipeline_to_numpy():
  """Scalar-producing pipelines are broadcast to arrays on stacking."""
  data = [1, 2, 3]
  ta = tra.TransformArray(data, lambda x: x * 2)
  assert ta.to_numpy().tolist() == [2, 4, 6]


def test_empty_data_yields_empty_array():
  """An empty TransformArray materializes to an empty numpy array."""
  ta = tra.TransformArray([], lambda x: x)
  assert ta.shape == (0,)
  assert len(ta) == 0
  assert ta.to_numpy().shape == (0,)
