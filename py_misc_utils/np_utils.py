import types

import numpy as np

from . import alog
from . import assert_checks as tas
from . import core_utils as cu


def diff_split(data, mask_fn, sort=True):
  if sort:
    indices = np.argsort(data)
    sdata = np.take_along_axis(data, indices, axis=None)
  else:
    indices, sdata = None, data

  diff = np.diff(sdata)
  mask = mask_fn(diff)
  msteps = np.flatnonzero(np.asarray(mask))

  sindices = np.arange(len(data))
  splits = np.split(sindices, msteps + 1)

  # Caller should use data[result[i]] to fetch split data.
  return [indices[s] for s in splits] if indices is not None else splits


def group_splits(data, mask_fn):
  diff = np.diff(data)
  mask = mask_fn(diff)

  return np.flatnonzero(np.asarray(mask))


def group_by_delta(data, mask_fn):
  return np.split(data, group_splits(data, mask_fn) + 1)


def fillna(data, copy=False, defval=0):
  if copy:
    data = data.copy()
  fdata = data.flatten()
  mask = np.where(np.isnan(fdata))[0]
  for r in group_by_delta(mask, lambda x: x != 1):
    if r.size == 0:
      continue
    vi = r[-1] + 1
    if vi < len(fdata):
      rv = fdata[vi]
    else:
      vi = r[0] - 1
      if vi >= 0:
        rv = fdata[vi]
      else:
        rv = defval

    fdata[r] = rv

  return fdata.reshape(data.shape)


def infer_np_dtype(dtypes):
  dtype = None
  for t in dtypes:
    if dtype is None or t == np.float64:
      dtype = t
    elif t == np.float32:
      if dtype.itemsize > t.itemsize:
        dtype = np.float64
      else:
        dtype = t
    elif dtype != np.float64:
      if dtype == np.float32 and t.itemsize > dtype.itemsize:
        dtype = np.float64
      elif t.itemsize > dtype.itemsize:
        dtype = t

  return dtype if dtype is not None else np.float32


def maybe_stack_slices(slices, axis=0):
  if slices and isinstance(slices[0], np.ndarray):
    return np.stack(slices, axis=axis)

  return slices


def to_numpy(data):
  if isinstance(data, np.ndarray):
    return data
  npfn = getattr(data, 'to_numpy')
  if npfn is not None:
    return npfn()
  if isinstance(data, torch.Tensor):
    return data.detach().cpu().numpy()

  return np.array(data)


def is_numeric(dtype):
  return np.issubdtype(dtype, np.number)


def is_integer(dtype):
  return np.issubdtype(dtype, np.integer)


def is_floating(dtype):
  return np.issubdtype(dtype, np.floating)


def is_numpy(v):
  return type(v).__module__ == np.__name__


def is_sorted(data, descending=False):
  if not isinstance(data, np.ndarray):
    data = np.array(data)

  if descending:
    return np.all(data[:-1] >= data[1:])

  return np.all(data[:-1] <= data[1:])


def astype(data, col, dtype):
  if cu.isdict(dtype):
    cdtype = dtype.get(col)
  elif is_numeric(data.dtype):
    cdtype = dtype
  else:
    cdtype = None

  return data if cdtype is None else data.astype(cdtype)


def softmax(x):
  e_x = np.exp(x - np.max(x))

  return e_x / e_x.sum()


def categorical(un_probs, n=None):
  probs = softmax(un_probs)
  values = np.random.choice(len(probs), size=n or 1, p=probs)

  return values[0] if n is None else values


def onehot(values, num_categories=None):
  if num_categories is None:
    num_categories = np.max(values) + 1
  else:
    tas.check_lt(np.max(values), num_categories)

  return np.eye(num_categories)[values]


def normalize(data, axis=None):
  mean = np.mean(data, axis=axis)
  std = np.std(data, axis=axis)

  if std.ndim > 0:
    std[np.where(std == 0.0)] = 1.0
  elif std == 0.0:
    std = 1.0

  return (data - mean) / std


def moving_average(data, window, include_current=True):
  weights = np.ones(window, dtype=data.dtype) / window
  pdata = np.pad(data, (window, window), mode='edge')
  cdata = np.convolve(pdata, weights, mode='valid')
  base = 1 if include_current else 0

  return cdata[base: base + len(data)]


def rolling_window(a, window):
  shape = a.shape[:-1] + (a.shape[-1] - window + 1, window)
  strides = a.strides + (a.strides[-1],)

  return np.lib.stride_tricks.as_strided(a, shape=shape, strides=strides)


def shift(data, pos=1):
  result = np.empty_like(data)
  if pos > 0:
    result[: pos] = data[0]
    result[pos:] = data[: -pos]
  elif pos < 0:
    result[pos:] = data[-1]
    result[: pos] = data[-pos:]
  else:
    result[:] = data

  return result


def complement_indices(indices, size):
  all_indices = np.full(size, 1, dtype=np.int8)
  all_indices[indices] = 0

  return np.flatnonzero(all_indices)


def polyfit_std(yv, xv=None, deg=1):
  xv = np.arange(len(yv), dtype=np.float32) if xv is None else np.array(xv)
  yv = yv if isinstance(yv, np.ndarray) else np.array(yv)

  yfit = np.polynomial.Polynomial.fit(xv, yv, deg)
  fyv = yfit(xv)

  return np.std(yv - fyv), fyv, yfit


def npdict_clone(npd):
  cloned = type(npd)()
  for k, v in npd.items():
    cloned[k] = np.array(v)

  return cloned


def is_ordered(v, reverse=False):
  npv = to_numpy(v)

  return np.all(npv[:-1] >= npv[1:]) if reverse else np.all(npv[:-1] <= npv[1:])


class RingBuffer:

  def __init__(self, capacity, dtype, vshape):
    self.capacity = capacity
    self.dtype = dtype
    self._vshape = tuple(vshape)
    self._count = 0
    self._data = np.zeros((capacity,) + self._vshape, dtype=dtype)

  @property
  def shape(self):
    return (len(self),) + self._vshape

  def resize(self, capacity):
    self._data = np.resize(self._data, (capacity,) + self._vshape)
    self._count = min(self._count, self.capacity, capacity)
    self.capacity = capacity

  def select(self, indices):
    indices = indices[indices < len(self)]
    self._count = len(indices)
    self._data[: self._count] = self._data[indices]

  def append(self, v):
    self._data[self._count % self.capacity] = v
    self._count += 1

  def extend(self, v):
    arr = np.asarray(v, dtype=self.dtype)
    if self._vshape:
      arr = arr.reshape((-1,) + self._vshape)

    pos = self._count % self.capacity
    front = min(self.capacity - pos, len(arr))

    self._data[pos: pos + front] = arr[: front]

    back = min(pos, len(arr) - front)
    if back > 0:
      self._data[: back] = arr[front: front + back]

    self._count += front + back

  def to_numpy(self):
    return np.concatenate(tuple(self.iter_views()))

  def data(self, dtype=None):
    return np.array(self._data[: len(self)], dtype=dtype)

  def iter_views(self):
    if self._count <= self.capacity:
      yield self._data[: self._count]
    else:
      pos = self._count % self.capacity

      yield self._data[pos:]

      if pos > 0:
        yield self._data[: pos]

  def iter_indices(self):
    if self._count <= self.capacity:
      return np.arange(0, self._count)

    return np.arange(self._count, self._count + self.capacity) % self.capacity

  def __len__(self):
    return min(self.capacity, self._count)

  def __getitem__(self, i):
    if isinstance(i, int):
      idx = (max(self._count, self.capacity) + i) % self.capacity
    else:
      # Allow seamless slicing in case of non-integer indexing.
      idx = i

    return self._data[idx]

  def __array__(self, dtype=None):
    arr = self.to_numpy()

    return arr if dtype is None else arr.astype(dtype)


_NP_ARRAY_TYPECODES = {
  bool: 'B',
  int: 'q',
  np.int8: 'b',
  np.uint8: 'B',
  np.int16: 'h',
  np.uint16: 'H',
  np.int32: 'l',
  np.uint32: 'L',
  np.int64: 'q',
  np.uint64: 'Q',
  np.float16: 'f',
  np.float32: 'f',
  np.float64: 'd',
}

def array_typecode(dtype):
  return _NP_ARRAY_TYPECODES.get(dtype.type)

