import bisect
import collections

import numpy as np
import pandas as pd

from . import assert_checks as tas
from . import np_utils as npu
from . import tensor_stream as ts
from . import utils as ut


WriteField = collections.namedtuple('WriteField', 'dtype')


class StreamDataWriter:

  def __init__(self, fields, path):
    self._writer = ts.Writer(path)
    self._fields = collections.OrderedDict()
    if isinstance(fields, str):
      sfields = tuple(tuple(ut.resplit(x, '=')) for x in ut.comma_split(fields))
    else:
      sfields = fields

    for field, dtype in sfields:
      self._fields[field] = WriteField(dtype=np.dtype(dtype))

  # Note that the tensors handed over to the write() API will become owned by
  # the StreamDataWriter obect, and cannot be written over after the write operation.
  def write(self, **kwargs):
    args = []
    for field, wfield in self._fields.items():
      data = kwargs.get(field)
      tas.check_is_not_none(data, msg=f'Missing "{field}" data in write operation')

      if isinstance(data, np.ndarray):
        if data.dtype != wfield.dtype:
          data = data.astype(wfield.dtype)
      else:
        data = np.array(data, dtype=wfield.dtype)

      args.append(data)

    self._writer.write(*args)

  def write_dataframe(self, df):
    wargs = collections.OrderedDict()
    for field in self._fields.keys():
      wargs[field] = df[field].to_numpy()

    self.write(**wargs)

  def flush(self):
    state = dict(fields=self._fields)

    self._writer.flush(state=state)


class StreamDataReader:

  def __init__(self, path):
    self._reader = ts.Reader(path)
    self._fields = self._reader.state['fields']
    self._fields_id = {field: i for i, field in enumerate(self._fields.keys())}

  def __len__(self):
    return len(self._reader)

  def fields(self):
    return tuple(self._fields.keys())

  @property
  def dtype(self):
    return tuple(wfield.dtype for wfield in self._fields.values())

  def get_slice(self, start, size=None):
    data = collections.OrderedDict()
    for i, field in enumerate(self._fields.keys()):
      data[field] = self._reader.get_slice(i, start, size=size)

    return data

  def get_field_slice(self, field, start, size=None):
    fid = self._fields_id[field]

    return self._reader.get_slice(fid, start, size=size)

  def typed_fields(self):
    return tuple((field, wfield.dtype) for field, wfield in self._fields.items())

  def empty_array(self, size):
    rdata = collections.OrderedDict()
    for field, dtype in self.typed_fields():
      if npu.is_numeric(dtype):
        rdata[field] = np.empty(size, dtype=dtype)
      else:
        rdata[field] = [None] * size

    return rdata


def _compute_indices(reader, field, start=None, end=None, reverse=False):
  fvalues = reader.get_field_slice(field, 0)
  indices = np.argsort(fvalues)
  if reverse:
    indices = np.flip(indices)

  if start is not None or end is not None:
    fvalues = fvalues[indices]
    start_index = bisect.bisect(fvalues, start) if start is not None else 0
    end_index = bisect.bisect(fvalues, end) if end is not None else len(indices)
    if start_index > end_index:
      start_index, end_index = end_index, start_index

    indices = indices[start_index: end_index]

  return indices


class StreamSortedScan:

  def __init__(self, reader, field,
               start=None,
               end=None,
               slice_size=None,
               max_slices=None,
               reverse=False):
    self._slice_size = slice_size or 100000
    self._max_slices = max_slices or 16
    self._reader = reader
    self._slices = collections.OrderedDict()
    self._indices = _compute_indices(reader, field, start=start, end=end, reverse=reverse)

  def _get_slice(self, idx):
    sidx = (idx // self._slice_size) * self._slice_size
    data = self._slices.get(sidx)
    if data is None:
      if len(self._slices) >= self._max_slices:
        self._slices.popitem(last=False)

      slice_size = min(self._slice_size, len(self._indices) - sidx)

      data = self._reader.get_slice(sidx, size=slice_size)
      self._slices[sidx] = data
    else:
      self._slices.move_to_end(sidx)

    return data, idx - sidx

  def _as_numpy(self, rdata):
    return {field: np.array(data) for field, data in rdata}

  def scan(self):
    # An ampty array can contain fields which are Python lists, so _as_numpy() is
    # used when returning data to the caller.
    rdata = self._reader.empty_array(self._slice_size)
    widx = 0
    for idx in self._indices:
      if widx == self._slice_size:
        yield widx, self._as_numpy(rdata)
        widx = 0

      sdata, sidx = self._get_slice(idx)
      for field, data in rdata.items():
        data[widx] = sdata[field][sidx]

      widx += 1

    if widx:
      frdata = collections.OrderedDict()
      for field, data in rdata.items():
        frdata[field] = data[: widx]

      yield widx, self._as_numpy(frdata)

