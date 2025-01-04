import array

import numpy as np
import pandas as pd

from . import core_utils as cu
from . import np_utils as npu


class _BufferBase:

  def append(self, value):
    self.data.append(value)

  def __len__(self):
    return len(self.data)

  def __getitem__(self, i):
    return self.data[i]

  def get_buffer(self):
    return self.data


class _Buffer(_BufferBase):

  def __init__(self, typecode=None, vtype=None):
    super().__init__()
    self.vtype = vtype
    self.data = [] if typecode is None else array.array(typecode)

  def __getitem__(self, i):
    value = self.data[i]
    return value if self.vtype is None else self.vtype(value)

  def get_buffer(self):
    return self.data if self.vtype is None else [self.vtype(v) for v in self.data]


class _NpBuffer(_BufferBase):

  def __init__(self, typecode, dtype):
    super().__init__()
    self.dtype = dtype
    self.data = array.array(typecode)

  def __getitem__(self, i):
    return self.dtype.type(self.data[i])

  def get_buffer(self):
    return np.array(self.data, dtype=self.dtype)


class _StrBuffer(_BufferBase):

  def __init__(self, str_table):
    super().__init__()
    self.str_table = str_table
    self.data = []

  def append(self, value):
    self.data.append(self.str_table.add(value))


class ArrayStorage:

  def __init__(self):
    self.data = dict()
    self._str_table = cu.StringTable()

  def _create_buffer(self, value):
    if npu.is_numpy(value):
      if npu.is_integer(value.dtype):
        return _NpBuffer('q', value.dtype)
      else:
        return _NpBuffer('d', value.dtype)
    elif isinstance(value, bool):
      return _Buffer(typecode='B', vtype=bool)
    elif isinstance(value, int):
      return _Buffer(typecode='q')
    elif isinstance(value, float):
      return _Buffer(typecode='d')
    elif isinstance(value, str):
      return _StrBuffer(self._str_table)
    else:
      return _Buffer()

  def _get_buffer(self, name, value):
    buffer = self.data.get(name)
    if buffer is None:
      buffer = self._create_buffer(value)
      self.data[name] = buffer

    return buffer

  def __len__(self):
    return min(*[len(buffer) for buffer in self.data.values()])

  def __getitem__(self, i):
    return {name: buffer[i] for name, buffer in self.data.items()}

  def append(self, *args, **kwargs):
    for name, value in args:
      buffer = self._get_buffer(name, value)
      buffer.append(value)
    for name, value in kwargs.items():
      buffer = self._get_buffer(name, value)
      buffer.append(value)

  def dataframe(self):
    dfdata = {name: buffer.get_buffer() for name, buffer in self.data.items()}

    return pd.DataFrame(data=dfdata)

