import array
import re

import numpy as np
import pandas as pd

from . import assert_checks as tas
from . import core_utils as cu
from . import np_utils as npu


_NOT_NUMERIC = 'xS'


class Field:

  def __init__(self, name, data, size, fmt):
    self.name = name
    self.data = data
    self.size = size
    self.fmt = fmt

  def np_array(self):
    arr = np.array(self.data)

    return arr.reshape(-1, self.size) if self.size > 1 else arr

  def append(self, arg):
    if self.size == 1:
      self.data.append(arg)
    else:
      assert self.size == len(arg), f'{self.name}({self.size}) vs. {len(arg)}'
      self.data.extend(arg)

  def extend(self, arg):
    assert len(arg) % self.size == 0, f'{self.name}({self.size}) vs. {len(arg)}'
    self.fast_extend(arg)

  def fast_extend(self, arg):
    if isinstance(arg, np.ndarray) and isinstance(self.data, array.array):
      nptype = np.dtype(self.data.typecode)
      if nptype != arg.dtype:
        arg = arg.astype(nptype)
      self.data.frombytes(arg.tobytes())
    else:
      self.data.extend(arg)

  def __len__(self):
    return len(self.data) // self.size

  def __getitem__(self, i):
    if self.size == 1:
      return self.data[i]
    else:
      offset = i * self.size
      return self.data[offset: offset + self.size]


class NamedArray:

  def __init__(self, names, fmt=None):
    fnames = re.split(r'\s*,\s*', names) if isinstance(names, str) else names
    if fmt is None:
      cnames, ffmt = [], []
      for name_format in fnames:
        m = re.match(r'(\w+)\s*=\s*(\w+)$', name_format)
        tas.check(m, msg=f'Invalid name=format specification: {name_format}')
        cnames.append(m.group(1))
        ffmt.append(m.group(2))

      fnames = cnames
    else:
      ffmt = re.findall(r'\d*[a-zA-Z]', fmt)
      tas.check_eq(len(fnames), len(ffmt),
                   msg=f'Mismatching names and format sizes: {fnames} vs "{fmt}"')

    fields = dict()
    for name, fmt in zip(fnames, ffmt):
      m = re.match(r'(\d+)([a-zA-Z])', fmt)
      if m:
        size, efmt = int(m.group(1)), m.group(2)
      else:
        size, efmt = 1, fmt

      data = array.array(efmt) if not efmt in _NOT_NUMERIC else []

      fields[name] = Field(name, data, size, efmt)

    self._fields = fields
    self._fieldseq = tuple(fields.values())
    self._has_strings = any(field.fmt == 'S' for field in self._fieldseq)
    self._str_tbl = cu.StringTable()

  def append(self, *args):
    if self._has_strings:
      for field, arg in zip(self._fieldseq, args):
        if field.fmt == 'S':
          arg = self._str_tbl.add(arg)
        field.append(arg)
    else:
      for field, arg in zip(self._fieldseq, args):
        field.append(arg)

  def extend(self, other):
    for field, ofield in zip(self._fieldseq, other._fieldseq):
      field.fast_extend(ofield.data)

  def append_extend(self, *args):
    if self._has_strings:
      for field, arg in zip(self._fieldseq, args):
        if field.fmt == 'S':
          arg = [self._str_tbl.add(x) for x in arg]
        field.extend(arg)
    else:
      for field, arg in zip(self._fieldseq, args):
        field.extend(arg)

  def get_tuple_item(self, i):
    item = []
    for field in self._fieldseq:
      item.append(field[i])

    return tuple(item)

  def get_arrays(self, names=None):
    if names:
      return tuple(self._fields[name].np_array() for name in names)

    return tuple(field.np_array() for field in self._fieldseq)

  def get_array(self, name):
    return self._fields[name].np_array()

  def data(self):
    return {field.name: field.np_array() for field in self._fieldseq}

  def to_numpy(self, dtype=None):
    for field in self._fieldseq:
      tas.check(field.fmt not in _NOT_NUMERIC,
                msg=f'Only purely numeric arrays can be converted to ' \
                f'numpy: {field.name}={field.fmt}')
      tas.check_eq(field.size, 1,
                   msg=f'Only fields with size==1 can be converted to ' \
                   f'numpy: {field.name}={field.size}')

    if dtype is None:
      dtype = npu.infer_np_dtype(self.dtypes())

    na = np.empty(len(self), dtype=dtype)
    for i, field in enumerate(self._fieldseq):
      na[:, i] = field.data

    return na

  def __len__(self):
    size = None
    for field in self._fieldseq:
      fsize = len(field)
      if size is None:
        size = fsize
      else:
        tas.check_eq(fsize, size,
                     msg=f'Unmatching size for "{field.name}": {fsize} vs. {size}')

    return size or 0

  def __getitem__(self, i):
    item = dict()
    for field in self._fieldseq:
      item[field.name] = field[i]

    return item

  def __array__(self, dtype=None):
    return self.to_numpy(dtype=dtype)

  def to_dataframe(self):
    df_data = dict()
    for name, arr in self.data().items():
      df_data[name] = arr if arr.ndim == 1 else arr.tolist()

    return pd.DataFrame(data=df_data)

  @property
  def shape(self):
    return (len(self), len(self._fieldseq))

  def dtypes(self):
    types = []
    for field in self._fieldseq:
      types.append(np.dtype('O') if field.fmt in _NOT_NUMERIC else np.dtype(field.fmt))

    return tuple(types)

