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


def group_by_delta(data, mask_fn):
  diff = np.diff(data)
  mask = mask_fn(diff)
  msteps = np.flatnonzero(np.asarray(mask))

  return np.split(data, msteps + 1)


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


def maybe_stack_np_slices(slices, axis=0):
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

