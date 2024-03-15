import collections

import numpy as np

from . import utils as ut


class TransformArray(collections.abc.Sequence):

  def __init__(self, data, transforms):
    super().__init__()
    self.data = data
    self._transforms = tuple(transforms)
    self.shape = ut.compute_shape(data)

  def __getitem__(self, i):
    if isinstance(i, slice):
      start, end, step = i.indices(len(self))
      slices = []
      for j in range(start, end, step):
        item = self.data[j]
        for trs in self._transforms:
          item = trs(item)
        slices.append(item)

      return ut.maybe_stack_np_slices(slices)

    item = self.data[i]
    for trs in self._transforms:
      item = trs(item)

    return item

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

