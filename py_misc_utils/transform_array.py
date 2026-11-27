"""Lazy transformation of array-like data.

This module provides a sequence wrapper which applies a transformation
pipeline to elements on access, without materializing the transformed
data upfront.
"""

import collections

import numpy as np

from . import core_utils as cu
from . import np_utils as npu


class TransformArray(collections.abc.Sequence):
  """A sequence which applies a processing pipeline lazily.

  Elements are transformed by the configured pipeline only when they are
  accessed, either individually, as slices or through conversion to a
  numpy array.

  Example:
    ta = transform_array.TransformArray([1, 2, 3], lambda x: x * 2)
    ta[0]  # 2 - the element is transformed on access.
    ta.to_numpy()  # The whole array transformed: array([2, 4, 6]).
  """

  def __init__(self, data, pipeline):
    """Initialize the transformed array.

    Args:
      data: Sequence of items to transform.
      pipeline: Callable (e.g. a Pipeline) applied to each accessed item.
    """
    super().__init__()
    self.data = data
    self._pipeline = pipeline
    self.shape = cu.compute_shape(data)

  def __getitem__(self, idx):
    """Return one item or a sub-sequence of transformed items.

    Args:
      idx: Integer index or slice of the items to access.

    Returns:
      The transformed item for an integer index, or a new TransformArray
      over the selected range for a slice.
    """
    if isinstance(idx, slice):
      start, end, step = idx.indices(len(self))
      slices = [self.data[i] for i in range(start, end, step)]

      return __class__(npu.maybe_stack_slices(slices), self._pipeline)

    return self._pipeline(self.data[idx])

  def __len__(self):
    """Return the number of items in the array."""
    return len(self.data)

  def to_numpy(self, dtype=None):
    """Stack all transformed items into a numpy array.

    Args:
      dtype: Optional numpy dtype for the result array.

    Returns:
      A numpy array with all the items transformed by the pipeline.
    """
    slices = [self[i] for i in range(len(self))]
    if not slices:
      return np.empty((0,))
    if not isinstance(slices[0], np.ndarray):
      slices = [np.array(x) for x in slices]

    npa = np.stack(slices, axis=0)

    return npa.astype(dtype) if dtype is not None else npa

  def __array__(self, dtype=None):
    """Support numpy conversion protocols by materializing the array."""
    return self.to_numpy(dtype=dtype)
