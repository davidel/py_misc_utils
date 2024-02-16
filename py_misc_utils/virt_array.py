import collections

from . import utils as ut
import numpy as np


def _compute_shape(data, indices):
  shape = [len(indices)]
  if shape[0] > 0:
    t = data[indices[0]]
    shape.extend(ut.compute_shape(t))

  return tuple(shape)


class VirtArray(collections.abc.Sequence):

  def __init__(self, data, indices):
    super().__init__()
    self._data = data
    self._indices = indices if isinstance(indices, np.ndarray) else np.array(indices)
    self._shape = _compute_shape(data, indices)

  @property
  def data(self):
    return self._data

  @property
  def indices(self):
    return self._indices

  @property
  def shape(self):
    return self._shape

  def __getitem__(self, i):
    if isinstance(i, slice):
      return type(self)(self._data, self._indices[i])

    return self._data[self._indices[i]]

  def __len__(self):
    return len(self._indices)

  def to_numpy(self, dtype=None):
    parts = [self._data[i] for i in self._indices]
    if not parts:
      return np.empty((0,))
    if not isinstance(parts[0], np.ndarray):
      parts = [np.array(x) for x in parts]

    npa = np.stack(parts, axis=0)

    return npa.astype(dtype) if dtype is not None else npa

  def __array__(self, dtype=None):
    return self.to_numpy(dtype=dtype)

  def shuffle(self, rng=None):
    if rng is None:
      rng = np.random.default_rng()
    rng.shuffle(self._indices)
