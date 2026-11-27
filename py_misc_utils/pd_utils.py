"""Pandas DataFrame utilities for column selection, I/O, and transformation.

This module provides helper functions for working with pandas DataFrames,
including column selection by name/type/regex, CSV/pickle I/O, conversion
to numpy arrays/dicts, group-based limiting, column transforms, and
datetime-epoch conversion.
"""

import array
import collections
import datetime
import os
import re

import numpy as np
import pandas as pd

from . import alog
from . import core_utils as cu
from . import gfs
from . import np_utils as npu
from . import utils as ut


def get_df_columns(df, discards=None):
  """Return column names from df, excluding any in discards.

  Args:
    df: A pandas DataFrame.
    discards: Optional set of column names to exclude.

  Returns:
    A list of column names not in discards.

  Example:
    import pandas as pd

    df = pd.DataFrame({'a': [3, 1], 'b': [1.5, 2.5]})
    pd_utils.get_df_columns(df)  # ['a', 'b']
    pd_utils.get_df_columns(df, discards={'b'})  # ['a']
  """
  dset = discards or {}

  return [c for c in df.columns if c not in dset]


def get_typed_columns(df, type_fn, discards=None):
  """Return column names whose dtype satisfies type_fn.

  Args:
    df: A pandas DataFrame.
    type_fn: A callable taking a numpy dtype and returning True if the
      column should be included.
    discards: Optional set of column names to exclude.

  Returns:
    A list of column names matching the type predicate.

  Example:
    import pandas as pd

    df = pd.DataFrame({'a': [3, 1], 'c': ['x', 'y']})
    pd_utils.get_typed_columns(df, pd.api.types.is_numeric_dtype)
    # ['a']
  """
  cols = []
  for c in get_df_columns(df, discards=discards):
    if type_fn(df[c].dtype):
      cols.append(c)

  return cols


def is_numeric_dtype(dtype):
  """Check if a dtype is numeric (integer or floating point).

  Extension dtypes - which np.issubdtype() cannot interpret - are
  handled via pd.api.types.is_numeric_dtype(), while plain NumPy dtypes
  are delegated to npu.is_numeric() to keep the np_utils contract pure.

  Args:
    dtype: A pandas or NumPy dtype.

  Returns:
    True if dtype is a numeric type.

  Example:
    import numpy as np
    import pandas as pd

    pd_utils.is_numeric_dtype(np.dtype('int64'))  # True
    pd_utils.is_numeric_dtype(pd.Int64Dtype())  # True
    pd_utils.is_numeric_dtype(pd.StringDtype())  # False
    pd_utils.is_numeric_dtype(np.dtype(bool))  # False
  """
  if isinstance(dtype, np.dtype):
    return npu.is_numeric(dtype)

  return pd.api.types.is_numeric_dtype(dtype)


def re_select_columns(df, re_cols):
  """Return column names matching any of the given regex patterns.

  Args:
    df: A pandas DataFrame.
    re_cols: A collection of regex patterns to match against column names.

  Returns:
    A list of column names that match at least one pattern.

  Example:
    import pandas as pd

    df = pd.DataFrame({'ab': [1], 'cd': [2]})
    pd_utils.re_select_columns(df, ['a', 'c.+'])  # ['ab', 'cd']
  """
  cols = []
  for c in df.columns:
    for rc in re_cols:
      if re.match(rc, c):
        cols.append(c)
        break

  return cols


def read_csv(path, rows_sample=100, dtype=None, args=None):
  """Read a CSV file into a pandas DataFrame with optional dtype inference.

  If dtype is not provided, samples the first rows_sample rows to infer
  numeric column types automatically.

  A dict dtype is passed through to pd.read_csv() as-is, while a single
  dtype is applied to the numeric columns detected in the sample.

  Args:
    path: Filesystem path or URL to the CSV file.
    rows_sample: Number of rows to sample for dtype inference (default: 100).
    dtype: Optional dtype specification (dict mapping column to dtype, or
      a single dtype to apply to all numeric columns).
    args: Optional dict of extra keyword arguments for pd.read_csv().

  Returns:
    A pandas DataFrame.

  Example:
    import os
    import tempfile
    import numpy as np
    import pandas as pd

    df = pd.DataFrame({'a': [1, 2], 'b': [1.5, 2.5]})
    path = os.path.join(tempfile.mkdtemp(), 'data.csv')
    pd_utils.save_dataframe(df, path)
    pd_utils.read_csv(path, dtype={'a': np.int64})
  """
  with gfs.open(path, mode='r') as fd:
    args = dict() if args is None else args
    if args.get('index_col') is None:
      args = args.copy()
      fields = ut.comma_split(fd.readline())
      fd.seek(0)
      # If 'index_col' is not specified, we use column 0 if its name is empty, otherwise
      # we disable it setting it to False.
      args['index_col'] = False if fields[0] else 0

    if dtype is None:
      return pd.read_csv(fd, **args)
    if cu.isdict(dtype):
      dtype = {c: np.dtype(t) for c, t in dtype.items()}
    else:
      df_test = pd.read_csv(fd, nrows=rows_sample, **args)
      fd.seek(0)
      dtype = {c: dtype for c in get_typed_columns(df_test, is_numeric_dtype)}

    return pd.read_csv(fd, dtype=dtype, **args)


def save_dataframe(df, path, **kwargs):
  """Save a DataFrame to a pickle or CSV file based on extension.

  Only '.pkl' and '.csv' extensions are supported. For CSV files, the
  index is dropped when it has no name (unless index is given in kwargs).

  Args:
    df: A pandas DataFrame.
    path: Output filesystem path.
    **kwargs: Format-specific options passed to to_pickle or to_csv.

  Raises:
    RuntimeError: When the path extension is neither .pkl nor .csv.

  Example:
    import os
    import tempfile
    import pandas as pd

    df = pd.DataFrame({'a': [1, 2], 'b': [1.5, 2.5]})
    pd_utils.save_dataframe(df, os.path.join(tempfile.mkdtemp(), 'out.pkl'))
    pd_utils.save_dataframe(df, os.path.join(tempfile.mkdtemp(), 'out.csv'))
  """
  _, ext = os.path.splitext(os.path.basename(path))
  if ext == '.pkl':
    args = ut.dict_subset(kwargs, ('compression', 'protocol', 'storage_options'))
    if 'protocol' not in args:
      args['protocol'] = ut.pickle_proto()
    with gfs.open(path, mode='wb') as fd:
      df.to_pickle(fd, **args)
  elif ext == '.csv':
    args = ut.dict_subset(
        kwargs, ('float_format', 'columns', 'header', 'index', 'index_label', 'mode',
                 'encoding', 'quoting', 'quotechar', 'line_terminator', 'chunksize',
                 'date_format', 'doublequote', 'escapechar', 'decimal', 'compression',
                 'error', 'storage_options'))

    # For CSV file, unless otherwise specified, and the index has no name, drop
    # the index column as it adds no value to the output (it's simply a sequential).
    if not df.index.name:
      args = ut.dict_setmissing(args, index=None)

    with gfs.open(path, mode='w') as fd:
      df.to_csv(fd, **args)
  else:
    alog.xraise(RuntimeError, f'Unknown extension: {ext}')


def load_dataframe(path, **kwargs):
  """Load a DataFrame from a pickle or CSV file based on extension.

  Args:
    path: Filesystem path or URL to the data file.
    **kwargs: Format-specific options. For CSV, supports rows_sample and
      dtype (as per read_csv()), plus any pd.read_csv() option.

  Returns:
    A pandas DataFrame.

  Raises:
    RuntimeError: When the path extension is neither .pkl nor .csv.

  Example:
    import os
    import tempfile
    import numpy as np
    import pandas as pd

    df = pd.DataFrame({'a': [1, 2], 'b': [1.5, 2.5]})
    path = os.path.join(tempfile.mkdtemp(), 'data.csv')
    pd_utils.save_dataframe(df, path)
    pd_utils.load_dataframe(path, dtype={'a': np.int64})
  """
  _, ext = os.path.splitext(os.path.basename(path))
  if ext == '.pkl':
    with gfs.open(path, mode='rb') as fd:
      return pd.read_pickle(fd)
  elif ext == '.csv':
    rows_sample = kwargs.pop('rows_sample', 100)
    dtype = kwargs.pop('dtype', None)
    args = ut.dict_subset(
        kwargs,
        ('sep', 'delimiter', 'header', 'names', 'index_col', 'usecols', 'squeeze',
         'prefix', 'mangle_dupe_cols', 'dtype', 'engine', 'converters', 'true_values',
         'false_values', 'skipinitialspace', 'skiprows', 'skipfooter', 'nrows',
         'na_values', 'keep_default_na', 'na_filter', 'verbose', 'skip_blank_lines',
         'parse_dates', 'infer_datetime_format', 'keep_date_col', 'date_parser',
         'dayfirst', 'cache_dates', 'iterator', 'chunksize', 'compression', 'thousands',
         'decimal', 'lineterminator', 'quotechar', 'quoting', 'doublequote',
         'escapechar', 'comment', 'encoding', 'dialect', 'error_bad_lines',
         'warn_bad_lines', 'delim_whitespace', 'low_memory', 'memory_map',
         'float_precision', 'storage_options'))

    return read_csv(path, rows_sample=rows_sample, dtype=dtype, args=args)
  else:
    alog.xraise(RuntimeError, f'Unknown extension: {ext}')


def to_npdict(df, reset_index=False, dtype=None, no_convert=()):
  """Convert a DataFrame to a dict of numpy arrays keyed by column name.

  Args:
    df: A pandas DataFrame.
    reset_index: If True, a named index is added to the output as an
      extra column.
    dtype: Optional dtype specification applied via npu.astype.
    no_convert: Tuple of column names to skip dtype conversion.

  Returns:
    A dict mapping column names to numpy arrays.

  Example:
    import numpy as np
    import pandas as pd

    df = pd.DataFrame({'a': [3, 1], 'b': [1.5, 2.5]})
    pd_utils.to_npdict(df)  # {'a': array([3, 1]), 'b': array([1.5, 2.5])}
    pd_utils.to_npdict(df, dtype={'a': np.float32})
  """
  if reset_index and df.index.name:
    df = df.reset_index()

  cdata = dict()
  for c in df.columns:
    data = df[c].to_numpy()
    if dtype is not None and c not in no_convert:
      data = npu.astype(data, c, dtype)
    cdata[c] = data

  return cdata


def load_dataframe_as_npdict(path, reset_index=False, dtype=None, no_convert=()):
  """Load a DataFrame from file and convert it to a dict of numpy arrays.

  Args:
    path: Filesystem path or URL to the data file, as per load_dataframe().
    reset_index: If True, a named index is added to the output as an
      extra column.
    dtype: Optional dtype specification applied via npu.astype.
    no_convert: Tuple of column names to skip dtype conversion.

  Returns:
    A dict mapping column names to numpy arrays.

  Example:
    import os
    import tempfile
    import numpy as np
    import pandas as pd

    df = pd.DataFrame({'a': [1, 2], 'b': [1.5, 2.5]})
    path = os.path.join(tempfile.mkdtemp(), 'data.csv')
    pd_utils.save_dataframe(df, path)
    pd_utils.load_dataframe_as_npdict(path, dtype={'b': np.float32})
  """
  df = load_dataframe(path)

  return to_npdict(df, reset_index=reset_index, dtype=dtype, no_convert=no_convert)


def column_or_index(df, name, numpy=True):
  """Return the column data for name, or the index if name matches index.name.

  Args:
    df: A pandas DataFrame.
    name: The column or index name.
    numpy: If True, return data as a numpy array.

  Returns:
    The column/index data, or None when no column or index named name
    exists.

  Example:
    import pandas as pd

    df = pd.DataFrame({'a': [3, 1]})
    df.index.name = 'ts'
    pd_utils.column_or_index(df, 'a')  # The 'a' column values.
    pd_utils.column_or_index(df, 'ts')  # The index values.
  """
  data = df.get(name)
  if data is None and df.index.name == name:
    data = df.index
  if data is not None:
    return data.to_numpy() if numpy else data


def columns_transform(df, cols, tfn):
  """Apply a transformation function to specified columns or index.

  Args:
    df: A pandas DataFrame.
    cols: A collection of column names to transform: names matching a
      named index are applied to the index.
    tfn: A callable taking (name, data, index) and returning transformed
      data; data is the column (or index) values and index tells whether
      they belong to the index.

  Returns:
    The modified DataFrame.

  Raises:
    RuntimeError: When a name matches neither a column nor the index.

  Example:
    import pandas as pd

    df = pd.DataFrame({'a': [3, 1]})
    pd_utils.columns_transform(df, ['a'], lambda c, v, index: v * 10)
  """
  for c in cols:
    cv = df.get(c)
    if cv is not None:
      df[c] = tfn(c, cv, index=False)
    elif df.index.name == c:
      df.index = pd.Index(data=tfn(c, df.index, index=True), name=df.index.name)
    else:
      alog.xraise(RuntimeError, f'Unable to find column or index named "{c}"')

  return df


def get_columns_index(df):
  """Split a DataFrame into its column index mapping and column list.

  Args:
    df: A pandas DataFrame.

  Returns:
    A tuple with the dict mapping each column name to its ordinal
    position and the list of column names.

  Example:
    import pandas as pd

    pd_utils.get_columns_index(pd.DataFrame({'a': [0], 'b': [1]}))
    # ({'a': 0, 'b': 1}, ['a', 'b'])
  """
  cols = df.columns.tolist()

  return ut.make_index_dict(cols), cols


def concat_dataframes(files, **kwargs):
  """Load and concatenate the DataFrames of a list of files.

  Args:
    files: Iterable of filesystem paths or URLs, each loaded with
      load_dataframe().
    **kwargs: Format-specific options forwarded to load_dataframe().

  Returns:
    The concatenated pandas DataFrame, or None when no file is given.

  Example:
    import os
    import tempfile
    import pandas as pd

    df = pd.DataFrame({'a': [1, 2]})
    td = tempfile.mkdtemp()
    p1, p2 = os.path.join(td, 'a.csv'), os.path.join(td, 'b.csv')
    pd_utils.save_dataframe(df, p1)
    pd_utils.save_dataframe(df * 2, p2)
    pd_utils.concat_dataframes([p1, p2])
  """
  dfs = []
  for path in files:
    df = load_dataframe(path, **kwargs)
    dfs.append(df)

  return pd.concat(dfs, **kwargs) if dfs else None


def get_dataframe_groups(df, cols, cols_transforms=None):
  """Group the DataFrame rows by the tuple of values of some columns.

  Args:
    df: A pandas DataFrame.
    cols: Column names whose tuple of values forms the group key.
    cols_transforms: Optional dict mapping a column name to a callable
      applied to its values before building the group key.

  Returns:
    A dict mapping each group key tuple to the array.array of strictly
    ascending row numbers of the rows belonging to the group. The row
    numbers are strictly ascending within each group, do NOT change that!

  Example:
    import pandas as pd

    df = pd.DataFrame({'k': ['u', 'u', 'v'], 'v': [1, 2, 3]})
    pd_utils.get_dataframe_groups(df, ['k'])
    # {('u',): array([0, 1]), ('v',): array([2])}
    pd_utils.get_dataframe_groups(df, ['k'], cols_transforms={'k': str.upper})
  """
  # Pandas groupby() is extremely slow when there are many groups as it builds
  # a DataFrame for each group. This simply collects the rows associated with each
  # tuple values representing the grouped columns.
  # Row numbers must be strictly ascending within each group, do NOT change that!
  groups = collections.defaultdict(lambda: array.array('L'))
  if cols_transforms:
    tcols = [(df[c], cols_transforms.get(c, cu.ident)) for c in cols]
    for i in range(len(df)):
      k = tuple([f(d[i]) for d, f in tcols])
      groups[k].append(i)
  else:
    cdata = [df[c] for c in cols]
    for i in range(len(df)):
      k = tuple([d[i] for d in cdata])
      groups[k].append(i)

  return groups


def limit_per_group(df, cols, limit):
  """Limit the number of rows selected from each group.

  Args:
    df: A pandas DataFrame.
    cols: Column names whose tuple of values forms the group key, as per
      get_dataframe_groups().
    limit: Maximum number of rows to keep for each group; a negative
      value keeps the rows at the end of each group.

  Returns:
    The DataFrame containing the selected rows.

  Example:
    import pandas as pd

    df = pd.DataFrame({'k': ['u', 'u', 'v'], 'v': [1, 2, 3]})
    pd_utils.limit_per_group(df, ['k'], 1)  # The first row of each group.
    pd_utils.limit_per_group(df, ['k'], -1)  # The last row of each group.
  """
  mask = np.full(len(df), False)

  groups = get_dataframe_groups(df, cols)
  for g in groups.values():
    if limit > 0:
      rows = g[:limit]
    else:
      rows = g[limit:]
    mask[rows] = True

  return df[mask]


def dataframe_column_rewrite(df, name, fn):
  """Replace the values of a column or of the index in place.

  The DataFrame is modified in place; a new copy is still returned for
  chaining purposes.

  Args:
    df: A pandas DataFrame.
    name: Name of the column or index to rewrite.
    fn: Callable taking the numpy array of the values and returning the
      new values.

  Raises:
    RuntimeError: When no column or index named name exists.

  Example:
    import pandas as pd

    df = pd.DataFrame({'a': [3, 1]})
    pd_utils.dataframe_column_rewrite(df, 'a', lambda v: v * -1)
  """
  data = df.get(name)
  if data is not None:
    df[name] = fn(data.to_numpy())
  elif df.index.name == name:
    nvalues = fn(df.index.to_numpy())
    df.index = pd.Index(nvalues, name=df.index.name)
  else:
    alog.xraise(RuntimeError, f'No column or index named "{name}"')


def correlate(df, col, top_n=None):
  """Compute the columns most correlated with a given column.

  Args:
    df: A pandas DataFrame.
    col: Name of the column to correlate the other columns with.
    top_n: Maximum number of columns to return; defaults to all of them.

  Returns:
    A tuple of (column name, correlation) pairs sorted by descending
    absolute correlation value.

  Example:
    import pandas as pd

    df = pd.DataFrame({'x': [1, 2, 3], 'y': [2, 4, 6], 'z': [1, 0, 1]})
    pd_utils.correlate(df, 'x')  # (('x', 1.0), ('y', 1.0), ('z', -0.44...))
    pd_utils.correlate(df, 'x', top_n=2)  # The two most correlated columns.
  """
  ccorr = df.corrwith(df[col])
  scorr = ccorr.sort_values(key=lambda x: abs(x), ascending=False)
  top_n = len(scorr) if top_n is None else min(top_n, len(scorr))

  return tuple([(name, scorr[name]) for name in scorr.index[:top_n]])


def type_convert_dataframe(df, types):
  """Convert the types of DataFrame columns in place.

  Args:
    df: A pandas DataFrame.
    types: Dict mapping column names to the type to convert them to.

  Returns:
    The modified DataFrame.

  Example:
    import pandas as pd

    df = pd.DataFrame({'a': [1, 2]})
    pd_utils.type_convert_dataframe(df, {'a': 'float32'})
  """
  # Pandas DataFrame astype() doe not have an 'inplace' argument.
  for c, t in types.items():
    df[c] = df[c].astype(t)

  return df


def dataframe_rows_select(df, indices):
  """Select DataFrame rows by their ordinal position.

  Args:
    df: A pandas DataFrame.
    indices: Sequence of row positions to select.

  Returns:
    The DataFrame containing the selected rows.

  Example:
    import pandas as pd

    df = pd.DataFrame({'a': [1, 2, 3]})
    pd_utils.dataframe_rows_select(df, [0, 2])
  """
  return df.loc[df.index[indices]]


def sorted_index(df, col):
  """Sort a DataFrame and set a column as its index.

  Args:
    df: A pandas DataFrame.
    col: Name of the column to sort by and to use as index.

  Returns:
    The DataFrame sorted by col: when col is already the index name the
    rows are simply sorted, otherwise the sorted rows are re-indexed
    with the col values.

  Example:
    import pandas as pd

    df = pd.DataFrame({'a': [3, 1, 2]})
    sdf = pd_utils.sorted_index(df, 'a')
    sdf.index  # The sorted 'a' values: [1, 2, 3].
  """
  if df.index.name == col:
    return df.sort_values(col)

  sdf = df.sort_values(col, ignore_index=True)

  return sdf.set_index(col)


def datetime_to_epoch(data):
  """Convert datetime values into epoch seconds.

  Args:
    data: Sequence of datetime values convertible by pd.to_datetime().

  Returns:
    A pandas Series of epoch seconds with fractional part.

  Example:
    pd_utils.datetime_to_epoch(['1970-01-01', '1970-01-02'])
    # [0.0, 86400.0]
  """
  ts = pd.Series(pd.to_datetime(data))
  return (ts - datetime.datetime(1970, 1, 1)).dt.total_seconds()
