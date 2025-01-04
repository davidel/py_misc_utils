import collections

import numpy as np

from . import core_utils as cu
from . import np_utils as npu


class TransformArray(collections.abc.Sequence):

  def __init__(self, data, pipeline):
    super().__init__()
    self.data = data
    self._pipeline = pipeline
    self.shape = cu.compute_shape(data)

  def __getitem__(self, idx):
    if isinstance(idx, slice):
      start, end, step = idx.indices(len(self))
      slices = [self.data[i] for i in range(start, end, step)]

      return __class__(npu.maybe_stack_slices(slices), self._pipeline)

    return self._pipeline(self.data[idx])

  def __len__(self):
    return len(self.data)

  def to_numpy(self, dtype=None):
    slices = [self[i] for i in range(len(self))]
    if not slices:
      return np.empty((0,))
    if not isinstance(slices[0], np.ndarray):
      slices = [np.array(x) for x in slices]

    npa = np.stack(slices, axis=0)

    return npa.astype(dtype) if dtype is not None else npa

  def __array__(self, dtype=None):
    return self.to_numpy(dtype=dtype)

