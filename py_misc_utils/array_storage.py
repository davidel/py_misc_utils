import array
import functools

import numpy as np
import pandas as pd

from . import assert_checks as tas
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

  def __init__(self, typecode=None, vtype=None, buf_vtype=None):
    super().__init__()
    self.vtype = vtype
    self.buf_vtype = buf_vtype
    self.data = [] if typecode is None else array.array(typecode)

  def __getitem__(self, i):
    value = self.data[i]
    return value if self.vtype is None else self.vtype(value)

  def get_buffer(self):
    if self.buf_vtype is not None:
      return self.buf_vtype(self.data)

    return self.data if self.vtype is None else [self.vtype(v) for v in self.data]


class _NpBuffer(_BufferBase):

  def __init__(self, value, typecode):
    tas.check_le(len(value.shape), 1, msg=f'Invalid shape: {value.shape}')

    super().__init__()
    self.dtype = value.dtype
    self.data = array.array(typecode)
    self.size = value.size if value.shape else None

  def append(self, value):
    if self.size is None:
      self.data.append(value)
    else:
      tas.check_eq(value.size, self.size,
                   msg=f'Invalid size: {value.size} vs {self.size}')
      self.data.extend(value.flatten())

  def __len__(self):
    return len(self.data) if self.size is None else len(self.data) // self.size

  def __getitem__(self, i):
    if self.size is None:
      return self.dtype.type(self.data[i])

    offset = i * self.size
    return np.array(self.data[offset: offset + self.size], dtype=self.dtype)

  def get_buffer(self):
    data = np.array(self.data, dtype=self.dtype)
    if self.size is None:
      return data

    return [data[i: i + self.size] for i in range(0, len(self.data), self.size)]


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
      if (typecode := npu.array_typecode(value.dtype)) is None:
        return _Buffer(buf_vtype=functools.partial(np.array, dtype=value.dtype))

      return _NpBuffer(value, typecode)
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
      self.data[name] = buffer = self._create_buffer(value)

    return buffer

  def __len__(self):
    return min(len(buffer) for buffer in self.data.values())

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

