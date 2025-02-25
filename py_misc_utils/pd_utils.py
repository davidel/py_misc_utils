import array
import collections
import datetime
import os
import re

import numpy as np
import pandas as pd

from . import alog
from . import assert_checks as tas
from . import core_utils as cu
from . import gfs
from . import np_utils as npu
from . import utils as ut


def get_df_columns(df, discards=None):
  dset = discards or {}

  return [c for c in df.columns if c not in dset]


def get_typed_columns(df, type_fn, discards=None):
  cols = []
  for c in get_df_columns(df, discards=discards):
    if type_fn(df[c].dtype):
      cols.append(c)

  return cols


def re_select_columns(df, re_cols):
  cols = []
  for c in df.columns:
    for rc in re_cols:
      if re.match(rc, c):
        cols.append(c)
        break

  return cols


def read_csv(path, rows_sample=100, dtype=None, args=None):
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
      dtype = {c: dtype for c in get_typed_columns(df_test, npu.is_numeric)}

    return pd.read_csv(fd, dtype=dtype, **args)


def save_dataframe(df, path, **kwargs):
  _, ext = os.path.splitext(os.path.basename(path))
  if ext == '.pkl':
    args = ut.dict_subset(kwargs, ('compression', 'protocol', 'storage_options'))
    if 'protocol' not in args:
      args['protocol'] = ut.pickle_proto()
    with gfs.open(path, mode='wb') as fd:
      df.to_pickle(fd, **args)
  elif ext == '.csv':
    args = ut.dict_subset(kwargs, ('float_format', 'columns', 'header', 'index',
                                   'index_label', 'mode', 'encoding', 'quoting',
                                   'quotechar', 'line_terminator', 'chunksize',
                                   'date_format', 'doublequote', 'escapechar',
                                   'decimal', 'compression', 'error', 'storage_options'))

    # For CSV file, unless otherwise specified, and the index has no name, drop
    # the index column as it adds no value to the output (it's simply a sequential).
    if not df.index.name:
      args = ut.dict_setmissing(args, index=None)

    with gfs.open(path, mode='w') as fd:
      df.to_csv(fd, **args)
  else:
    alog.xraise(RuntimeError, f'Unknown extension: {ext}')


def load_dataframe(path, **kwargs):
  _, ext = os.path.splitext(os.path.basename(path))
  if ext == '.pkl':
    with gfs.open(path, mode='rb') as fd:
      return pd.read_pickle(fd)
  elif ext == '.csv':
    rows_sample = kwargs.pop('rows_sample', 100)
    dtype = kwargs.pop('dtype', None)
    args = ut.dict_subset(kwargs, ('sep', 'delimiter', 'header', 'names',
                                   'index_col', 'usecols', 'squeeze', 'prefix',
                                   'mangle_dupe_cols', 'dtype', 'engine',
                                   'converters', 'true_values', 'false_values',
                                   'skipinitialspace', 'skiprows', 'skipfooter',
                                   'nrows', 'na_values', 'keep_default_na', 'na_filter',
                                   'verbose', 'skip_blank_lines', 'parse_dates',
                                   'infer_datetime_format', 'keep_date_col', 'date_parser',
                                   'dayfirst', 'cache_dates', 'iterator', 'chunksize',
                                   'compression', 'thousands', 'decimal', 'lineterminator',
                                   'quotechar', 'quoting', 'doublequote', 'escapechar',
                                   'comment', 'encoding', 'dialect', 'error_bad_lines',
                                   'warn_bad_lines', 'delim_whitespace', 'low_memory',
                                   'memory_map', 'float_precision', 'storage_options'))

    return read_csv(path, rows_sample=rows_sample, dtype=dtype,
                    args=args)
  else:
    alog.xraise(RuntimeError, f'Unknown extension: {ext}')


def to_npdict(df, reset_index=False, dtype=None, no_convert=()):
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
  df = load_dataframe(path)

  return to_npdict(df, reset_index=reset_index, dtype=dtype, no_convert=no_convert)


def column_or_index(df, name, numpy=True):
  data = df.get(name)
  if data is None and df.index.name == name:
    data = df.index
  if data is not None:
    return data.to_numpy() if numpy else data


def columns_transform(df, cols, tfn):
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
  cols = df.columns.tolist()

  return ut.make_index_dict(cols), cols


def concat_dataframes(files, **kwargs):
  dfs = []
  for path in files:
    df = load_dataframe(path, **kwargs)
    dfs.append(df)

  return pd.concat(dfs, **kwargs) if dfs else None


def get_dataframe_groups(df, cols, cols_transforms=None):
  # Pandas groupby() is extremely slow when there are many groups as it builds
  # a DataFrame for each group. This simply collects the rows associated with each
  # tuple values representing the grouped columns.
  # Row numbers must be strictly ascending within each group, do NOT change that!
  groups = collections.defaultdict(lambda: array.array('L'))
  if cols_transforms:
    tcols = [(df[c], cols_transforms.get(c, pycu.ident)) for c in cols]
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
  mask = np.full(len(df), False)

  groups = get_dataframe_groups(df, cols)
  for k, g in groups.items():
    if limit > 0:
      rows = g[: limit]
    else:
      rows = g[limit:]
    mask[rows] = True

  return df[mask]


def dataframe_column_rewrite(df, name, fn):
  data = df.get(name)
  if data is not None:
    df[name] = fn(data.to_numpy())
  elif df.index.name == name:
    nvalues = fn(df.index.to_numpy())
    df.index = type(df.index)(data=nvalues, name=df.index.name)
  else:
    alog.xraise(RuntimeError, f'No column or index named "{name}"')


def correlate(df, col, top_n=None):
  ccorr = df.corrwith(df[col])
  scorr = ccorr.sort_values(key=lambda x: abs(x), ascending=False)
  top_n = len(scorr) if top_n is None else min(top_n, len(scorr))

  return tuple([(scorr.index[i], scorr[i]) for i in range(top_n)])


def type_convert_dataframe(df, types):
  # Pandas DataFrame astype() doe not have an 'inplace' argument.
  for c, t in types.items():
    df[c] = df[c].astype(t)

  return df


def dataframe_rows_select(df, indices):
  return df.loc[df.index[indices]]


def sorted_index(df, col):
  if df.index.name == col:
    return df.sort_values(col)

  sdf = df.sort_values(col, ignore_index=True)

  return sdf.set_index(col)


def datetime_to_epoch(data):
  return (pd.to_datetime(data) - datetime.datetime(1970, 1, 1)).dt.total_seconds()

