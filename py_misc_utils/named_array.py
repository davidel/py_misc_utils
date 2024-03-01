import array

from . import assert_checks as tas
from . import utils as ut
import numpy as np
import pandas as pd


_NOT_NUMERIC = 'xS'


def _fast_extend(dest, src):
  if isinstance(src, np.ndarray) and isinstance(dest, array.array):
    nptype = np.dtype(dest.typecode)
    if nptype != src.dtype:
      src = src.astype(nptype)
    dest.frombytes(src.tobytes())
  else:
    dest.extend(src)


class NamedArray:

  def __init__(self, names, fmt):
    tas.check_eq(len(names), len(fmt), msg=f'Mismatching names and format sizes: {names} vs "{fmt}"')

    self._names = tuple(names)
    self._fmt = fmt
    self._names_index = {n: i for i, n in enumerate(self._names)}
    self._str_tbl = ut.StringTable()
    self._data = tuple([[] if f in _NOT_NUMERIC else array.array(f) for f in fmt])

  def append(self, *args):
    if 'S' not in self._fmt:
      for data, arg in zip(self._data, args):
        data.append(arg)
    else:
      for i, (data, arg) in enumerate(zip(self._data, args)):
        if self._fmt[i] == 'S':
          arg = self._str_tbl.add(arg)
        data.append(arg)

  def extend(self, other):
    for data, odata in zip(self._data, other._data):
      _fast_extend(data, odata)

  def append_extend(self, *args):
    if 'S' not in self._fmt:
      for data, arg in zip(self._data, args):
        _fast_extend(data, arg)
    else:
      for i, (data, arg) in enumerate(zip(self._data, args)):
        if self._fmt[i] == 'S':
          arg = [self._str_tbl.add(x) for x in arg]
        _fast_extend(data, arg)

  def __len__(self):
    return len(self._data[0]) if self._data else 0

  def __getitem__(self, i):
    return {name: data[i] for name, data in zip(self._names, self._data)}

  def get_tuple_item(self, i):
    return tuple([data[i] for data in self._data])

  def get_arrays(self, names=None):
    if names:
      return [self._data[self._names_index[n]] for n in names]

    return self._data

  def get_array(self, name):
    return self._data[self._names_index[name]]

  def to_numpy(self, dtype=None):
    tas.check(all([x not in self._fmt for x in _NOT_NUMERIC]),
              msg=f'Only purely numeric arrays can be converted to numpy: {self._fmt}')

    if dtype is None:
      dtype = ut.infer_np_dtype([np.dtype(f) for f in self._fmt])

    na = np.empty(self.shape, dtype=dtype)
    for i in range(na.shape[1]):
      na[:, i] = self._data[i]

    return na

  def __array__(self, dtype=None):
    return self.to_numpy(dtype=dtype)

  def to_dataframe(self):
    return pd.DataFrame(data=self.data)

  @property
  def data(self):
    return {name: data for name, data in zip(self._names, self._data)}

  @property
  def shape(self):
    return (len(self), self.width)

  @property
  def width(self):
    return len(self._data)

  @property
  def names(self):
    return self._names

  @property
  def dtypes(self):
    return tuple([np.dtype('O') if f in _NOT_NUMERIC else np.dtype(f) for f in self._fmt])

  @property
  def fmt(self):
    return self._fmt

