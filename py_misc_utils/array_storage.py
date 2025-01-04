import array

import numpy as np
import pandas as pd

from . import core_utils as cu
from . import np_utils as npu


class _NoopCaster:

  def in_cast(self, value):
    return value

  def out_cast(self, value):
    return value

  def buffer_cast(self, values):
    return values


class _NpCaster(_NoopCaster):

  def __init__(self, dtype):
    super().__init__()
    self.dtype = dtype

  def out_cast(self, value):
    return self.dtype.type(value)

  def buffer_cast(self, values):
    return np.array(values, dtype=self.dtype)


class _TypeCaster(_NoopCaster):

  def __init__(self, vtype):
    super().__init__()
    self.vtype = vtype

  def out_cast(self, value):
    return self.vtype(value)

  def buffer_cast(self, values):
    return [self.vtype(v) for v in values]


class _StrCaster(_NoopCaster):

  def __init__(self, str_table):
    super().__init__()
    self.str_table = str_table

  def in_cast(self, value):
    return self.str_table.add(value)


class ArrayStorage:

  def __init__(self):
    self.data = dict()
    self._str_table = cu.StringTable()

  def _create_buffer(self, value):
    if npu.is_numpy(value):
      caster = _NpCaster(value.dtype)
      if npu.is_integer(value.dtype):
        return array.array('q'), caster
      else:
        return array.array('d'), caster
    elif isinstance(value, bool):
      return array.array('B'), _TypeCaster(bool)
    elif isinstance(value, int):
      return array.array('q'), _NoopCaster()
    elif isinstance(value, float):
      return array.array('d'), _NoopCaster()
    elif isinstance(value, str):
      return [], _StrCaster(self._str_table)
    else:
      return [], _NoopCaster()

  def _get_buffer(self, name, value):
    buf_caster = self.data.get(name)
    if buf_caster is None:
      buf_caster = self._create_buffer(value)
      self.data[name] = buf_caster

    return buf_caster

  def __len__(self):
    return min(*[len(buf) for buf, caster in self.data.values()])

  def __getitem__(self, i):
    item = dict()
    for name, (buf, caster) in self.data.items():
      value = buf[i]
      item[name] = caster.out_cast(value)

    return item

  def append(self, *args, **kwargs):
    for name, value in args:
      buf, caster = self._get_buffer(name, value)
      buf.append(caster.in_cast(value))
    for name, value in kwargs.items():
      buf, caster = self._get_buffer(name, value)
      buf.append(caster.in_cast(value))

  def dataframe(self):
    dfdata = dict()
    for name, (buf, caster) in self.data.items():
      dfdata[name] = caster.buffer_cast(buf)

    return pd.DataFrame(data=dfdata)

