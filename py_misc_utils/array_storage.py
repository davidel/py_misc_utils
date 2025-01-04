import array

import numpy as np
import pandas as pd

from . import core_utils as cu
from . import np_utils as npu


class _NpCaster:

  def __init__(self, dtype):
    self.dtype = dtype

  def cast(self, value):
    return self.dtype.type(value)

  def buffer_cast(self, values):
    return np.array(values, dtype=self.dtype)


class _TypeCaster:

  def __init__(self, vtype):
    self.vtype = vtype

  def cast(self, value):
    return self.vtype(value)

  def buffer_cast(self, values):
    return [self.vtype(v) for v in values]


class _StrCaster:

  def __init__(self, str_table):
    self.str_table = str_table

  def cast(self, value):
    return self.str_table.add(value)

  def buffer_cast(self, values):
    return values


class ArrayStorage:

  def __init__(self):
    self.data = dict()
    self._casters = dict()
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
      return array.array('q'), None
    elif isinstance(value, float):
      return array.array('d'), None
    elif isinstance(value, str):
      return [], _StrCaster(self._str_table)
    else:
      return [], None

  def _get_buffer(self, name, value):
    buf = self.data.get(name)
    if buf is None:
      buf, caster = self._create_buffer(value)
      self.data[name] = buf
      if caster is not None:
        self._casters[name] = caster

    return buf

  def __len__(self):
    return min(*[len(buf) for buf in self.data.values()])

  def __getitem__(self, i):
    item = dict()
    for name, buf in self.data.items():
      value = buf[i]
      caster = self._casters.get(name)
      item[name] = value if caster is None else caster.cast(value)

    return item

  def append(self, *args, **kwargs):
    for name, value in args:
      buf = self._get_buffer(name, value)
      buf.append(value)
    for name, value in kwargs.items():
      buf = self._get_buffer(name, value)
      buf.append(value)

  def dataframe(self):
    dfdata = dict()
    for name, buf in self.data.items():
      caster = self._casters.get(name)
      if caster is None:
        dfdata[name] = buf
      else:
        dfdata[name] = caster.buffer_cast(buf)

    return pd.DataFrame(data=dfdata)

