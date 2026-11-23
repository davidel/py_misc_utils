"""Numpy array views over a subset of another array.

This module provides VirtArray, a sequence which exposes a selection of
elements of an underlying array as a regular indexable array object.
"""

import collections

import numpy as np

from . import core_utils as cu


def _compute_shape(data, indices):
  """Compute the shape of a virtual array over a selection of elements.

  Args:
    data: Underlying sequence of elements.
    indices: Sequence of indices selecting the elements to expose.

  Returns:
    The (leading length, element shape) tuple describing the virtual
    array shape.
  """
  shape = [len(indices)]
  if shape[0] > 0:
    t = data[indices[0]]
    shape.extend(cu.compute_shape(t))

  return tuple(shape)


class VirtArray(collections.abc.Sequence):
  """A sequence which acts as a view over selected elements.

  The virtual array exposes the elements of the underlying data selected
  by a set of indices, supporting slicing, numpy conversion and index
  shuffling without copying the data.
  """

  def __init__(self, data, indices):
    """Initialize the virtual array.

    Args:
      data: Sequence providing the elements.
      indices: Indices of the elements to expose, as a numpy array or any
        indexable sequence.
    """
    super().__init__()
    self.data = data
    self.indices = indices if isinstance(indices, np.ndarray) else np.array(indices)
    self.shape = _compute_shape(data, indices)

  def __getitem__(self, i):
    """Return one element or a sub-view of the virtual array.

    Args:
      i: Integer index or slice over the selected elements.

    Returns:
      The selected underlying element for an integer index, or a new
      VirtArray over the selected index range for a slice.
    """
    if isinstance(i, slice):
      return type(self)(self.data, self.indices[i])

    return self.data[self.indices[i]]

  def __len__(self):
    """Return the number of selected elements."""
    return len(self.indices)

  def to_numpy(self, dtype=None):
    """Stack all selected elements into a numpy array.

    Args:
      dtype: Optional numpy dtype for the result array.

    Returns:
      A numpy array with the selected elements in index order.
    """
    parts = [self.data[i] for i in self.indices]
    if not parts:
      return np.empty((0,))
    if not isinstance(parts[0], np.ndarray):
      parts = [np.array(x) for x in parts]

    npa = np.stack(parts, axis=0)

    return npa.astype(dtype) if dtype is not None else npa

  def __array__(self, dtype=None):
    """Support numpy conversion protocols by materializing the array."""
    return self.to_numpy(dtype=dtype)

  def shuffle(self, rng=None):
    """Randomly shuffle the order of the selected elements.

    Args:
      rng: Optional numpy random Generator to use; a default one is
        created when not provided.
    """
    if rng is None:
      rng = np.random.default_rng()
    rng.shuffle(self.indices)
