"""NumPy utility functions for array manipulation and analysis.

This module provides a collection of NumPy-related utility functions including:
- Array splitting and grouping
- NaN filling and interpolation
- Data type inference and conversion
- Statistical functions (softmax, normalization, moving average)
- Array sampling (categorical, one-hot encoding)
- Array transformation (rolling window, shifting)
- Polynomial fitting
- Ring buffer implementation
"""

import numpy as np

from . import alog
from . import assert_checks as tas
from . import core_utils as cu


def diff_split(data, mask_fn, sort=True):
  """Split an array into groups based on a mask function applied to differences.

  Args:
    data: Input array to split.
    mask_fn: Function taking the diff array and returning a boolean mask.
    sort: Whether to sort the data before splitting (default: True).

  Returns:
    List of index arrays for each group. Use data[result[i]] to fetch split data.
  """
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
  """Find split indices in an array based on a mask function applied to differences.

  Args:
    data: Input array.
    mask_fn: Function taking the diff array and returning a boolean mask.

  Returns:
    Array of indices where splits should occur.
  """
  diff = np.diff(data)
  mask = mask_fn(diff)

  return np.flatnonzero(np.asarray(mask))


def group_by_delta(data, mask_fn):
  """Split an array into groups based on a mask function applied to differences.

  Args:
    data: Input array to split.
    mask_fn: Function taking the diff array and returning a boolean mask.

  Returns:
    List of arrays, each containing a contiguous group.
  """
  return np.split(data, group_splits(data, mask_fn) + 1)


def fillna(data, copy=False, defval=0):
  """Fill NaN entries with a neighbor value.

  Each run of NaN entries is filled with the value right after the run,
  or with the value right before it when the run ends the data, or with
  defval when the whole data is NaN.

  Args:
    data: A numpy array.
    copy: If True, fill a copy of the array, leaving the original
      unchanged (default: False).
    defval: Value used when there is no neighbor to take the value from
      (default: 0).

  Returns:
    The array with the NaN entries filled.
  """
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
  """Infer a common NumPy dtype from a sequence of dtypes.

  Prefers float64, then float32, then largest integer type.

  Args:
    dtypes: Sequence of NumPy dtypes.

  Returns:
    Inferred common dtype (default: float32 if no dtypes provided).
  """
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
  """Stack slices into an array if they are numpy arrays.

  Args:
    slices: List of arrays or other objects.
    axis: Axis along which to stack (default: 0).

  Returns:
    Stacked array if all elements are ndarrays, otherwise original list.
  """
  if slices and isinstance(slices[0], np.ndarray):
    return np.stack(slices, axis=axis)

  return slices


def to_numpy(data):
  """Convert various array-like objects to NumPy array.

  Handles numpy arrays, pandas DataFrames/Series (via to_numpy),
  PyTorch tensors (via .numpy()), and other array-likes.

  Args:
    data: Array-like object to convert.

  Returns:
    NumPy ndarray.
  """
  if isinstance(data, np.ndarray):
    return data

  npfn = getattr(data, 'to_numpy', None)
  if callable(npfn):
    return npfn()

  # This is PyTorch ...
  npfn = getattr(data, 'numpy', None)
  if callable(npfn):
    return npfn(force=True)

  return np.array(data)


def is_numeric(dtype):
  """Check if a dtype is numeric (integer or floating point).

  Args:
    dtype: NumPy dtype or dtype-like.

  Returns:
    True if dtype is a numeric type.
  """
  return np.issubdtype(dtype, np.number)


def is_integer(dtype):
  """Check if a dtype is integer.

  Args:
    dtype: NumPy dtype or dtype-like.

  Returns:
    True if dtype is an integer type.
  """
  return np.issubdtype(dtype, np.integer)


def is_floating(dtype):
  """Check if a dtype is floating point.

  Args:
    dtype: NumPy dtype or dtype-like.

  Returns:
    True if dtype is a floating point type.
  """
  return np.issubdtype(dtype, np.floating)


def is_numpy(v):
  """Check if an object is a NumPy array or scalar.

  Args:
    v: Object to check.

  Returns:
    True if object's type is from the numpy module.
  """
  return type(v).__module__ == np.__name__


def astype(data, col, dtype):
  """Convert a column of data to a specific dtype if applicable.

  Args:
    data: Input array.
    col: Column name or index.
    dtype: Target dtype or dict mapping column names to dtypes.

  Returns:
    Data array with column converted if dtype matches, otherwise unchanged.
  """
  if cu.isdict(dtype):
    cdtype = dtype.get(col)
  elif is_numeric(data.dtype):
    cdtype = dtype
  else:
    cdtype = None

  return data if cdtype is None else data.astype(cdtype)


def softmax(x):
  """Compute softmax along the last axis.

  Args:
    x: Input array.

  Returns:
    Softmax probabilities (same shape as input).
  """
  e_x = np.exp(x - np.max(x, axis=-1, keepdims=True))

  return e_x / np.sum(e_x, axis=-1, keepdims=True)


def categorical(un_probs, n=None):
  """Sample from a categorical distribution.

  Args:
    un_probs: Unnormalized probabilities (logits).
    n: Number of samples to draw (default: 1 for 1D, 1 per row for 2D).

  Returns:
    Sampled indices. Shape: () for 1D with n=None, (n,) for 1D with n>1,
    or (..., n) for higher dimensions.
  """
  probs = softmax(un_probs)
  if probs.ndim == 1:
    values = np.random.choice(len(probs), size=n or 1, p=probs)

    return values[0] if n is None else values
  else:
    fprobs = np.reshape(probs, (-1, probs.shape[-1]))
    values = []
    for p in fprobs:
      values.append(np.random.choice(len(p), size=n or 1, p=p))

    catv = np.vstack(values)

    return np.reshape(catv, tuple(probs.shape[:-1]) + (catv.shape[-1],))


def onehot(values, num_categories=None):
  """Convert integer indices to one-hot encoding.

  Args:
    values: Array of integer indices.
    num_categories: Number of categories (default: max(values) + 1).

  Returns:
    One-hot encoded array with shape values.shape + (num_categories,).

  Raises:
    AssertionError: If any value >= num_categories.
  """
  if num_categories is None:
    num_categories = np.max(values) + 1
  else:
    tas.check_lt(np.max(values), num_categories)

  return np.eye(num_categories)[values]


def normalize(data, axis=None):
  """Normalize data to zero mean and unit standard deviation.

  Args:
    data: A numpy array.
    axis: Optional axis the mean and standard deviation are computed
      along; a None value uses the whole array.

  Returns:
    The normalized array; zero standard deviations are treated as 1.0 to
    avoid division by zero.
  """
  mean = np.mean(data, axis=axis)
  std = np.std(data, axis=axis)

  if std.ndim > 0:
    std[np.where(std == 0.0)] = 1.0
  elif std == 0.0:
    std = 1.0

  return (data - mean) / std


def hasher(value):
  """Compute a hash of a numpy array.

  Args:
    value: A numpy array.

  Returns:
    The hash of the array, computed from its dtype, shape and raw bytes.
  """
  return hash((value.dtype, tuple(value.shape), value.tobytes()))


def moving_average(data, window, include_current=True):
  """Compute moving average of 1D data.

  Args:
    data: 1D input array.
    window: Window size for averaging.
    include_current: If True, average includes current element.
      If False, average uses previous window elements only (default: True).

  Returns:
    Moving average array of same length as input.
  """
  weights = np.ones(window, dtype=data.dtype) / window
  pdata = np.pad(data, (window, window), mode='edge')
  cdata = np.convolve(pdata, weights, mode='valid')
  base = 1 if include_current else 0

  return cdata[base:base + len(data)]


def rolling_window(a, window):
  """Create a rolling window view of the last dimension of an array.

  Uses stride tricks for zero-copy windowing.

  Args:
    a: Input array.
    window: Window size.

  Returns:
    View of array with shape (..., n_windows, window).
  """
  shape = a.shape[:-1] + (a.shape[-1] - window + 1, window)
  strides = a.strides + (a.strides[-1],)

  return np.lib.stride_tricks.as_strided(a, shape=shape, strides=strides)


def shift(data, pos=1):
  """Shift array elements along the first axis.

  Args:
    data: Input array.
    pos: Number of positions to shift. Positive shifts right (later),
      negative shifts left (earlier). Fill with edge values (default: 1).

  Returns:
    Shifted array of same shape.
  """
  result = np.empty_like(data)
  if pos > 0:
    result[:pos] = data[0]
    result[pos:] = data[:-pos]
  elif pos < 0:
    result[pos:] = data[-1]
    result[:pos] = data[-pos:]
  else:
    result[:] = data

  return result


def complement_indices(indices, size):
  """Get complement of indices within a range.

  Args:
    indices: Array of indices to exclude.
    size: Total size of the range [0, size).

  Returns:
    Array of indices not in the input indices.
  """
  all_indices = np.full(size, 1, dtype=np.int8)
  all_indices[indices] = 0

  return np.flatnonzero(all_indices)


def polyfit_std(yv, xv=None, deg=1):
  """Fit polynomial and compute standard deviation of residuals.

  Args:
    yv: Y values to fit.
    xv: X values (default: 0, 1, 2, ...).
    deg: Polynomial degree (default: 1).

  Returns:
    Tuple of (std_dev, fitted_values, polynomial_object).
  """
  xv = np.arange(len(yv), dtype=np.float32) if xv is None else np.array(xv)
  yv = yv if isinstance(yv, np.ndarray) else np.array(yv)

  yfit = np.polynomial.Polynomial.fit(xv, yv, deg)
  fyv = yfit(xv)

  return np.std(yv - fyv), fyv, yfit


def npdict_clone(npd):
  """Create a deep copy of a dictionary of numpy arrays.

  Args:
    npd: Dictionary mapping keys to numpy arrays.

  Returns:
    New dictionary with copied arrays.
  """
  cloned = type(npd)()
  for k, v in npd.items():
    cloned[k] = np.array(v)

  return cloned


def is_ordered(v, reverse=False):
  """Check if a sequence is monotonically ordered.

  Args:
    v: Sequence to check.
    reverse: If True, check for non-increasing order.
      If False, check for non-decreasing order (default: False).

  Returns:
    True if ordered according to the specified direction.
  """
  npv = to_numpy(v)

  return np.all(npv[:-1] >= npv[1:]) if reverse else np.all(npv[:-1] <= npv[1:])


class RingBuffer:
  """Fixed-capacity ring buffer for efficient append/extend operations.

  Supports multi-dimensional elements and provides zero-copy iteration
  via iter_views() and iter_indices().
  """

  def __init__(self, capacity, dtype, vshape):
    """Initialize a ring buffer.

    Args:
      capacity: Maximum number of elements.
      dtype: NumPy dtype for stored elements.
      vshape: Shape of each element (tuple or empty for scalars).
    """
    self.capacity = capacity
    self.dtype = dtype
    self._vshape = tuple(vshape)
    self._count = 0
    self._data = np.zeros((capacity,) + self._vshape, dtype=dtype)

  @property
  def shape(self):
    """Return shape of the buffer contents: (len,) + vshape."""
    return (len(self),) + self._vshape

  def resize(self, capacity):
    """Resize the buffer, preserving recent elements if possible.

    Args:
      capacity: New capacity.
    """
    self._data = np.resize(self._data, (capacity,) + self._vshape)
    self._count = min(self._count, self.capacity, capacity)
    self.capacity = capacity

  def select(self, indices):
    """Keep only elements at specified indices.

    Args:
      indices: Array of indices to keep (relative to current buffer).
    """
    indices = indices[indices < len(self)]
    self._count = len(indices)
    self._data[:self._count] = self._data[indices]

  def append(self, v):
    """Append a single element to the buffer.

    Args:
      v: Element to append (must match dtype and vshape).
    """
    self._data[self._count % self.capacity] = v
    self._count += 1

  def extend(self, v):
    """Extend the buffer with multiple elements.

    Args:
      v: Array-like of elements to append.
    """
    arr = np.asarray(v, dtype=self.dtype)
    if self._vshape:
      arr = arr.reshape((-1,) + self._vshape)

    pos = self._count % self.capacity
    front = min(self.capacity - pos, len(arr))

    self._data[pos:pos + front] = arr[:front]

    back = min(pos, len(arr) - front)
    if back > 0:
      self._data[:back] = arr[front:front + back]

    self._count += front + back

  def to_numpy(self):
    """Get all buffer contents as a single contiguous array.

    Returns:
      Array of shape (len,) + vshape with elements in chronological order.
    """
    return np.concatenate(tuple(self.iter_views()))

  def data(self, dtype=None):
    """Get buffer contents as a new array (copy).

    Args:
      dtype: Optional dtype for the returned array.

    Returns:
      Array of shape (len,) + vshape.
    """
    return np.array(self._data[:len(self)], dtype=dtype)

  def iter_views(self):
    """Iterate over contiguous views of the buffer contents.

    Yields:
      Views of the internal buffer in chronological order.
      Yields 1 or 2 views depending on wrap-around.
    """
    if self._count <= self.capacity:
      yield self._data[:self._count]
    else:
      pos = self._count % self.capacity

      yield self._data[pos:]

      if pos > 0:
        yield self._data[:pos]

  def iter_indices(self):
    """Get indices of valid elements in the internal buffer.

    Returns:
      Array of indices mapping chronological order to internal storage.
    """
    if self._count <= self.capacity:
      return np.arange(0, self._count)

    return np.arange(self._count, self._count + self.capacity) % self.capacity

  def __len__(self):
    """Return number of valid elements in the buffer."""
    return min(self.capacity, self._count)

  def __getitem__(self, i):
    """Get element(s) by index (chronological order, negative supported).

    Args:
      i: Integer index or slice.

    Returns:
      Element or slice of elements.
    """
    if isinstance(i, int):
      idx = (max(self._count, self.capacity) + i) % self.capacity
    else:
      # Allow seamless slicing in case of non-integer indexing.
      idx = i

    return self._data[idx]

  def __array__(self, dtype=None):
    """Enable direct conversion to numpy array via np.array(buffer)."""
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
  """Get the array typecode for a NumPy dtype.

  Args:
    dtype: NumPy dtype object.

  Returns:
    Array typecode character (e.g., 'f', 'd', 'i', 'l') or None if not found.
  """
  return _NP_ARRAY_TYPECODES.get(dtype.type)
