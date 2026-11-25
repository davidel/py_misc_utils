"""Streaming columnar data writer/reader with sorted scan support.

This module provides :class:`StreamDataWriter` and :class:`StreamDataReader`
for writing and reading named, typed tensor streams, plus
:class:`StreamSortedScan` for scanning data in sorted order by a key field
with slice-based caching.
"""

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
  """Write named, typed columns to a tensor stream file.
  """

  def __init__(self, fields, path):
    """Initialize the writer with field definitions and an output path.

    Args:
      fields: A comma-separated string of 'name=dtype' pairs, or a sequence
        of (name, dtype) tuples.
      path: Filesystem path to write the stream to.
    """
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
    """Write one row of field data to the stream.

    The tensors handed over become owned by the StreamDataWriter object and
    cannot be modified after the write operation.

    Args:
      **kwargs: Field name to value/numpy array mappings. All fields must be
        provided.
    """
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
    """Write all rows of a DataFrame to the stream.

    Args:
      df: A pandas DataFrame whose columns match the writer's fields.
    """
    wargs = collections.OrderedDict()
    for field in self._fields.keys():
      wargs[field] = df[field].to_numpy()

    self.write(**wargs)

  def flush(self):
    """Flush pending writes and persist field metadata to the stream."""
    state = dict(fields=self._fields)

    self._writer.flush(state=state)


class StreamDataReader:
  """Read named, typed columns from a tensor stream file.
  """

  def __init__(self, path):
    """Initialize the reader from a tensor stream file path.

    Args:
      path: Filesystem path of the stream to read.
    """
    self._reader = ts.Reader(path)
    self._fields = self._reader.state['fields']
    self._fields_id = {field: i for i, field in enumerate(self._fields.keys())}

  def __len__(self):
    """Return the number of records in the stream.

    Returns:
      The total record count.
    """
    return len(self._reader)

  def fields(self):
    """Return the field names in the stream.

    Returns:
      A tuple of field names.
    """
    return tuple(self._fields.keys())

  @property
  def dtype(self):
    """Return the field dtypes in the stream.

    Returns:
      A tuple of numpy dtypes, one per field.
    """
    return tuple(wfield.dtype for wfield in self._fields.values())

  def get_slice(self, start, size=None):
    """Read a slice of records from the stream.

    Args:
      start: The starting record index.
      size: Optional number of records to read; defaults to all remaining.

    Returns:
      An ordered dict mapping field names to numpy arrays.
    """
    data = collections.OrderedDict()
    for i, field in enumerate(self._fields.keys()):
      data[field] = self._reader.get_slice(i, start, size=size)

    return data

  def get_field_slice(self, field, start, size=None):
    """Read a slice of a single field from the stream.

    Args:
      field: The field name to read.
      start: The starting record index.
      size: Optional number of records to read.

    Returns:
      A numpy array (or list) of the field's values.
    """
    fid = self._fields_id[field]

    return self._reader.get_slice(fid, start, size=size)

  def typed_fields(self):
    """Return the fields with their dtypes.

    Returns:
      A tuple of (field_name, dtype) pairs.
    """
    return tuple((field, wfield.dtype) for field, wfield in self._fields.items())

  def empty_array(self, size):
    """Create empty arrays for all fields.

    Numeric fields are allocated as numpy arrays; non-numeric fields as lists.

    Args:
      size: The number of elements to allocate.

    Returns:
      An ordered dict mapping field names to empty containers.
    """
    rdata = collections.OrderedDict()
    for field, dtype in self.typed_fields():
      if npu.is_numeric(dtype):
        rdata[field] = np.empty(size, dtype=dtype)
      else:
        rdata[field] = [None] * size

    return rdata


def _compute_indices(reader, field, start=None, end=None, reverse=False):
  """Compute sorted record indices for a field, optionally filtered.

  Args:
    reader: A StreamDataReader instance.
    field: The field name to sort by.
    start: Optional start value of the filter range.
    end: Optional end value of the filter range.
    reverse: If True, sort in descending order.

  Returns:
    A numpy array of sorted record indices.
  """
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

    indices = indices[start_index:end_index]

  return indices


class StreamSortedScan:
  """Scan records of a stream in sorted order by a key field.

  Uses an LRU cache of slices for efficient repeated access.
  """

  def __init__(self,
               reader,
               field,
               start=None,
               end=None,
               slice_size=None,
               max_slices=None,
               reverse=False):
    """Initialize the sorted scan.

    Args:
      reader: A StreamDataReader instance.
      field: The field name to sort by.
      start: Optional start value of the filter range.
      end: Optional end value of the filter range.
      slice_size: Optional number of records per cached slice.
      max_slices: Optional maximum number of cached slices.
      reverse: If True, scan in descending order.
    """
    self._slice_size = slice_size or 100000
    self._max_slices = max_slices or 16
    self._reader = reader
    self._slices = collections.OrderedDict()
    self._indices = _compute_indices(reader,
                                     field,
                                     start=start,
                                     end=end,
                                     reverse=reverse)

  def _get_slice(self, idx):
    """Fetch the cached slice containing record idx, loading it if needed.

    Args:
      idx: The record index to locate.

    Returns:
      The tuple (slice_start, slice_data).
    """
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
    """Convert field containers to numpy arrays.

    Args:
      rdata: An ordered dict mapping field names to arrays/lists.

    Returns:
      A dict mapping field names to numpy arrays.
    """
    return {field: np.array(data) for field, data in rdata.items()}

  def scan(self):
    """Yield records in sorted order in batches.

    An empty array can contain fields which are Python lists, so _as_numpy()
    is used when returning data to the caller.

    Yields:
      Tuples of (count, data) where count is the number of records in the
      batch and data is a dict of field name to numpy array.
    """
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
        frdata[field] = data[:widx]

      yield widx, self._as_numpy(frdata)
