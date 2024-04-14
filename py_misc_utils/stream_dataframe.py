import collections

import numpy as np
import pandas as pd

from . import tensor_stream as ts
from . import utils as ut


_WriteField = collections.namedtuple('WriteField', 'seq, dtype')


class StreamDataWriter:

  def __init__(self, fields, path):
    self._writer = ts.Writer(path)
    self._fields = dict()
    for i, field_dtype in enumerate(ut.comma_split(fields)):
      field, dtype = ut.resplit(field_dtype, '=')
      self._fields[field] = _WriteField(seq=i, dtype=np.dtype(dtype))

  def fields(self):
    fields = [None] * len(self._fields)
    for field, wfield in self._fields.items():
      fields[wfield.seq] = field

    return fields

  def write(self, **kwargs):
    args = [None] * len(self._fields)
    for field, data in kwargs.items():
      wfield = self._fields[field]
      if data.dtype != wfield.dtype:
        data = data.astype(wfield.dtype)

      args[wfield.seq] = data

    assert all(x is not None for x in args)

    self._writer.write(*args)

  def write_dataframe(self, df):
    wargs = dict()
    for field in self._fields.keys():
      wargs[field] = df[field].to_numpy()

    self.write(**wargs)

  def flush(self):
    state = dict(fields=self.fields())

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
    self._slices = dict()
    self._sid = 0
    self._indices = np.argsort(reader.get_field_slice(field, 0))
    if reverse:
      self._indices = np.flip(self._indices)

  def _get_slice(self, idx):
    sidx = (idx // self._slice_size) * self._slice_size
    sobj = self._slices.get(sidx)
    if sobj is None:
      if len(self._slices) >= self._max_slices:
        sslices = sorted(tuple(self._slices.items()), key=lambda s: s[1].prio)
        self._slices.pop(sslices[0][0])

      slice_size = min(self._slice_size, len(self._indices) - sidx)

      data = self._reader.get_slice(sidx, size=slice_size)
      sobj = ut.make_object(data=data, prio=self._sid + 1)
      self._sid += 1
      self._slices[sidx] = sobj
    elif sobj.prio != self._sid + 1:
      sobj.prio = self._sid + 1
      self._sid += 1

    return sobj.data, idx - sidx

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



