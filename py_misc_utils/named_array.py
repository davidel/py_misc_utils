"""Named arrays with typed, named columns backed by array.array and numpy.

This module provides :class:`Field` and :class:`NamedArray` for managing
columnar data with named fields, type formats, and efficient conversion to
numpy arrays and pandas DataFrames.
"""

import array
import re

import numpy as np
import pandas as pd

from . import assert_checks as tas
from . import core_utils as cu
from . import np_utils as npu

_NOT_NUMERIC = 'xS'


class Field:
  """Represents a single typed column within a NamedArray.

  Attributes:
    name: The column name.
    data: The underlying storage (array.array or list).
    size: Number of elements per row (1 for scalar, >1 for fixed vectors).
    fmt: The data format character code.
    str_tbl: String table used for string columns ('S' format).
  """

  __slots__ = ('name', 'data', 'size', 'fmt', 'str_tbl')

  def __init__(self, name, data, size, fmt, str_tbl):
    """Initialize a Field with name, data storage, size, format, and string table.

    Args:
      name: The column name.
      data: The underlying storage (array.array or list).
      size: Number of elements per row.
      fmt: The data format character code.
      str_tbl: String table used for string columns.
    """
    self.name = name
    self.data = data
    self.size = size
    self.fmt = fmt
    self.str_tbl = str_tbl

  def np_array(self):
    """Return the field's data as a numpy array.

    Returns:
      A numpy array of shape (n,) if size==1, or (n, size) otherwise.
    """
    arr = np.array(self.data)

    return arr.reshape(-1, self.size) if self.size > 1 else arr

  def append(self, arg):
    """Append a single value to the field's data.

    Args:
      arg: The value to append. For string fields ('S' format), it is
        interned via the string table.
    """
    if self.fmt == 'S':
      arg = self.stringify(arg)

    if self.size == 1:
      self.data.append(arg)
    else:
      assert self.size == len(arg), f'{self.name}({self.size}) vs. {len(arg)}'
      self.data.extend(arg)

  def extend(self, arg):
    """Extend the field's data with multiple values.

    Args:
      arg: An iterable of values to append. Its length must be a multiple
        of self.size. String fields are interned via the string table.
    """
    assert len(arg) % self.size == 0, f'{self.name}({self.size}) vs. {len(arg)}'
    if self.fmt == 'S':
      arg = [self.str_tbl.add(x) for x in arg]

    if isinstance(arg, np.ndarray) and isinstance(self.data, array.array):
      nptype = np.dtype(self.data.typecode)
      if nptype != arg.dtype:
        arg = arg.astype(nptype)
      self.data.frombytes(arg.tobytes())
    else:
      self.data.extend(arg)

  def stringify(self, arg):
    """Intern string values through the string table.

    Args:
      arg: A string or tuple of strings to intern.

    Returns:
      The interned string (or tuple of interned strings).
    """
    if self.size == 1:
      return self.str_tbl.add(arg)

    return tuple(self.str_tbl.add(x) for x in arg)

  def __len__(self):
    """Return the number of rows of the field."""
    return len(self.data) // self.size

  def __getitem__(self, i):
    """Return the value of a row of the field.

    Args:
      i: The row index, or a slice of row indices.

    Returns:
      The row value - the element for size==1 fields, the tuple of the
      size elements otherwise.
    """
    if self.size == 1:
      return self.data[i]
    elif isinstance(i, int):
      offset = i * self.size

      return self.data[offset:offset + self.size]
    else:
      start, stop, step = (x * self.size for x in i.indices(len(self)))
      data = []
      for n in range(start, stop, step):
        data.append(self.data[n:n + self.size])

      return data

  @staticmethod
  def create(name, fmt, str_tbl):
    """Create a Field from a name and a format specification.

    Args:
      name: The column name.
      fmt: Format specification, optionally holding a size prefix (e.g.
        '3f'); non-numeric formats fall back to a plain list storage.
      str_tbl: String table used for string columns.

    Returns:
      The created Field.
    """
    m = re.match(r'(\d+)([a-zA-Z])', fmt)
    if m:
      size, efmt = int(m.group(1)), m.group(2)
    else:
      size, efmt = 1, fmt

    data = array.array(efmt) if not efmt in _NOT_NUMERIC else []

    return Field(name, data, size, efmt, str_tbl)


class NamedArray:
  """A columnar data container with named, typed fields.

  Fields are defined by name=format pairs (e.g. 'id=i name=S value=f').
  Supports appending rows, extending from other NamedArrays, conversion to
  numpy arrays and pandas DataFrames.

  Attributes:
    _fields: Mapping of field name to Field instance.
    _fieldseq: Ordered tuple of Field instances.
    _str_tbl: Shared string table for string columns.
  """

  def __init__(self, names, fmt=None):
    """Initialize a NamedArray with named fields.

    Args:
      names: A comma-separated string of 'name=format' pairs, or a list of
        field names. If fmt is provided, names is treated as a list of names
        and fmt as a list of format strings.
      fmt: Optional format specification. Can be a string of format codes
        or a list of format characters matching the names.
    """
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
      ffmt = re.findall(r'\d*[a-zA-Z]', fmt) if isinstance(fmt, str) else fmt
      tas.check_eq(len(fnames),
                   len(ffmt),
                   msg=f'Mismatching names and format sizes: {fnames} vs {ffmt}')

    str_tbl = cu.StringTable()
    fields = dict()
    for name, fmt in zip(fnames, ffmt):
      fields[name] = Field.create(name, fmt, str_tbl)

    self._fields = fields
    self._fieldseq = tuple(fields.values())
    self._str_tbl = str_tbl

  def add_column(self, name, fmt, data):
    """Add a new column to the NamedArray.

    Args:
      name: The column name (must not already exist).
      fmt: The format character code for the column.
      data: The column data (list, array, or numpy array).
    """
    tas.check(name not in self._fields, msg=f'Column "{name}" already exists')

    field = Field.create(name, fmt, self._str_tbl)

    if isinstance(data, np.ndarray):
      data = data.flatten()

    field.extend(data)

    self._fields[name] = field
    self._fieldseq += (field,)

  def append(self, *args):
    """Append a row of values to each field.

    Args:
      *args: One value per field, in field order.
    """
    for field, arg in zip(self._fieldseq, args):
      field.append(arg)

  def extend(self, other):
    """Extend this NamedArray with rows from another NamedArray.

    Args:
      other: Another NamedArray with the same fields.
    """
    for field, ofield in zip(self._fieldseq, other._fieldseq):
      field.extend(ofield.data)

  def append_extend(self, *args):
    """Extend each field with iterables of values.

    Args:
      *args: One iterable per field, in field order.
    """
    for field, arg in zip(self._fieldseq, args):
      field.extend(arg)

  def kwappend(self, **kwargs):
    """Append a row using keyword arguments keyed by field name.

    Args:
      **kwargs: Field name to value mappings.
    """
    for name, field in self._fields.items():
      arg = kwargs[name]
      field.append(arg)

  def get_tuple_item(self, i):
    """Return the i-th row as a tuple of field values.

    Args:
      i: The row index.

    Returns:
      A tuple of values, one per field.
    """
    item = []
    for field in self._fieldseq:
      item.append(field[i])

    return tuple(item)

  def get_arrays(self, names=None):
    """Return numpy arrays for the specified fields, or all fields.

    Args:
      names: Optional collection of field names. If None, all fields are returned.

    Returns:
      A tuple of numpy arrays, one per requested field.
    """
    if names:
      return tuple(self._fields[name].np_array() for name in names)

    return tuple(field.np_array() for field in self._fieldseq)

  def get_array(self, name):
    """Return the numpy array for a single field.

    Args:
      name: The field name.

    Returns:
      A numpy array containing the field's data.
    """
    return self._fields[name].np_array()

  def data(self):
    """Return a mapping of field name to its numpy array data.

    Returns:
      A dict mapping field names to their numpy arrays.
    """
    return {field.name: field.np_array() for field in self._fieldseq}

  def to_numpy(self, dtype=None):
    """Convert all fields to a single 2D numpy array.

    All fields must be numeric with size==1.

    Args:
      dtype: Optional numpy dtype. If None, inferred from field dtypes.

    Returns:
      A numpy array of shape (n_rows, n_fields).
    """
    for field in self._fieldseq:
      tas.check(field.fmt not in _NOT_NUMERIC,
                msg=f'Only purely numeric arrays can be converted to ' \
                f'numpy: {field.name}={field.fmt}')
      tas.check_eq(field.size, 1,
                   msg=f'Only fields with size==1 can be converted to ' \
                   f'numpy: {field.name}={field.size}')

    if dtype is None:
      dtype = npu.infer_np_dtype(self.dtypes())

    na = np.empty(self.shape, dtype=dtype)
    for i, field in enumerate(self._fieldseq):
      na[:, i] = field.data

    return na

  def __len__(self):
    """Return the number of rows in the NamedArray.

    Returns:
      The row count, or 0 if there are no fields.
    """
    size = None
    for field in self._fieldseq:
      fsize = len(field)
      if size is None:
        size = fsize
      else:
        tas.check_eq(fsize,
                     size,
                     msg=f'Unmatching size for "{field.name}": {fsize} vs. {size}')

    return size or 0

  def __getitem__(self, i):
    """Return the i-th row as a dict of field values.

    Args:
      i: The row index.

    Returns:
      A dict mapping each field name to the value of the row.
    """
    item = dict()
    for field in self._fieldseq:
      item[field.name] = field[i]

    return item

  def __array__(self, dtype=None):
    """Return the numeric contents as a numpy array.

    Args:
      dtype: Optional numpy dtype of the array.

    Returns:
      The 2D numpy array of the fields, as per to_numpy().
    """
    return self.to_numpy(dtype=dtype)

  def to_dataframe(self):
    """Convert the fields into a pandas DataFrame.

    Returns:
      A DataFrame with one column per field, holding the numpy arrays of
      the scalar fields and the lists of the vector fields.
    """
    df_data = dict()
    for name, arr in self.data().items():
      df_data[name] = arr if arr.ndim == 1 else arr.tolist()

    return pd.DataFrame(data=df_data)

  @property
  def shape(self):
    """Return the (rows, columns) shape of the array.

    Returns:
      A tuple with the number of rows and the number of fields.
    """
    return (len(self), len(self._fieldseq))

  def dtypes(self):
    """Return the numpy dtypes of the fields.

    Returns:
      A tuple with one numpy dtype per field: non-numeric fields get
      dtype('O').
    """
    types = []
    for field in self._fieldseq:
      types.append(np.dtype('O') if field.fmt in _NOT_NUMERIC else np.dtype(field.fmt))

    return tuple(types)
