import collections

import numpy as np
import pandas as pd

from . import assert_checks as tas
from . import tensor_stream as ts
from . import utils as ut


_WriteField = collections.namedtuple('WriteField', 'dtype')


class StreamDataWriter:

  def __init__(self, fields, path):
    self._writer = ts.Writer(path)
    self._fields = collections.OrderedDict()
    if isinstance(fields, str):
      sfields = tuple(tuple(ut.resplit(x, '=')) for x in ut.comma_split(fields))
    else:
      sfields = fields

    for field, dtype in sfields:
      self._fields[field] = _WriteField(dtype=np.dtype(dtype))

  def write(self, **kwargs):
    args = []
    for field, wfield in self._fields.items():
      data = kwargs.get(field, None)
      tas.check_is_not_none(data, msg=f'Missing "{field}" data in write operation')

      if data.dtype != wfield.dtype:
        data = data.astype(wfield.dtype)

      args.append(data)

    self._writer.write(*args)

  def write_dataframe(self, df):
    wargs = collections.OrderedDict()
    for field in self._fields.keys():
      wargs[field] = df[field].to_numpy()

    self.write(**wargs)

  def flush(self):
    state = dict(fields=tuple(self._fields.keys()))

    self._writer.flush(state=state)


class StreamDataReader:

  def __init__(self, path):
    self._reader = ts.Reader(path)
    self.fields = self._reader.state['fields']
    self.dtype = self._reader.dtype
    self._fields_id = {field: i for i, field in enumerate(self.fields)}

  def __len__(self):
    return len(self._reader)

  def get_slice(self, start, size=None):
    data = dict()
    for i, field in enumerate(self.fields):
      data[field] = self._reader.get_slice(i, start, size=size)

    return data

  def get_field_slice(self, field, start, size=None):
    fid = self._fields_id[field]

    return self._reader.get_slice(fid, start, size=size)


class StreamSortedScan:

  def __init__(self, reader, field, slice_size=None, max_slices=None, reverse=False):
    self._slice_size = slice_size or 100000
    self._max_slices = max_slices or 16
    self._reader = reader
    self._slices = collections.OrderedDict()
    self._indices = np.argsort(reader.get_field_slice(field, 0))
    if reverse:
      self._indices = np.flip(self._indices)

  def _get_slice(self, idx):
    sidx = (idx // self._slice_size) * self._slice_size
    data = self._slices.get(sidx)
    if data is None:
      if len(self._slices) >= self._max_slices:
        self._slices.popitem(0)

      slice_size = min(self._slice_size, len(self._indices) - sidx)

      data = self._reader.get_slice(sidx, size=slice_size)
      self._slices[sidx] = data
    else:
      self._slices.move_to_end(sidx)

    return data, idx - sidx

  def scan(self):
    rdata = dict()
    for field, dtype in zip(self._reader.fields, self._reader.dtype):
      rdata[field] = np.empty(self._slice_size, dtype=dtype)

    widx = 0
    for idx in self._indices:
      if widx == self._slice_size:
        yield rdata
        widx = 0

      sdata, sidx = self._get_slice(idx)
      for field, data in rdata.items():
        data[widx] = sdata[field][sidx]

      widx += 1

    if widx:
      for field, data in rdata.items():
        rdata[field] = data[: widx]

      yield rdata

