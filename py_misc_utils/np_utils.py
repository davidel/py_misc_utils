import types

import numpy as np

from . import alog
from . import assert_checks as tas


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


def is_numpy(v):
  return type(v).__module__ == np.__name__


def astype(data, col, dtype):
  if isinstance(dtype, dict):
    cdtype = dtype.get(col)
  elif is_numeric(data.dtype):
    cdtype = dtype
  else:
    cdtype = None

  return data if cdtype is None else data.astype(cdtype)


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

  def __init__(self, size, dtype, init=None):
    self._size = size
    self._data = np.empty(size, dtype=dtype)
    self._rpos, self._wpos, self._avail = 0, 0, size

    if init is not None:
      vinit = tuple(init) if isinstance(init, types.GeneratorType) else init
      idata = np.array(vinit[-size:], dtype=dtype)
      self._data[: len(idata)] = idata
      self._avail -= len(idata)
      self._wpos = len(idata) % size

  @property
  def dtype(self):
    return self._data.dtype

  @property
  def shape(self):
    return (len(self), )

  def push(self, v):
    self._data[self._wpos] = v
    self._wpos = (self._wpos + 1) % self._size
    if self._avail > 0:
      self._avail -= 1
    else:
      self._rpos = self._wpos

  def pop(self):
    tas.check_lt(self._avail, self._size, msg=f'Empty buffer')

    v = self._data[self._rpos]
    self._rpos = (self._rpos + 1) % self._size
    self._avail += 1

    return v

  def to_numpy(self):
    if len(self) == 0:
      return np.empty(0, dtype=self.dtype)

    if self._wpos > self._rpos:
      arr = self._data[self._rpos: self._wpos]
    else:
      arr = np.concatenate((self._data[self._rpos:], self._data[: self._wpos]))

    return arr

  def __len__(self):
    return self._size - self._avail

  def __getitem__(self, i):
    if isinstance(i, slice):
      return np.concatenate(tuple(self._data[s] for s in self._slice(i)))

    return self._data[self._rindex(i)]

  def __array__(self, dtype=None):
    arr = self.to_numpy()

    return arr if dtype is None else arr.astype(dtype)

  def _rindex(self, i):
    return (self._rpos + i) % self._size

  def _slice(self, s):
    start = s.start if s.start is not None else 0
    stop = s.stop if s.stop is not None else len(self)
    step = s.step if s.step is not None else 1

    start = self._rindex(start)
    stop = self._rindex(stop)

    slices = []
    if step > 0:
      if start < stop:
        slices.append(slice(start, stop, step))
      else:
        slices.append(slice(start, self._size, step))

        base = step - 1 - (self._size - start - 1) % step
        slices.append(slice(base, stop, step))
    else:
      if start > stop:
        slices.append(slice(start, stop, step))
      else:
        slices.append(slice(start, None, step))

        rem = step + 1 + start % -step
        slices.append(slice(self._size - 1 + rem, stop, step))

    return slices

