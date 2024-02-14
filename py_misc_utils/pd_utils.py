import os

import numpy as np
import pandas as pd

from . import alog
from . import assert_checks as tas
from . import utils as pyu


def get_df_columns(df, discards=None):
  dset = set(discards) if discards else None

  return [c for c in df.columns if not dset or c not in dset]


def get_typed_columns(df, type_fn, discards=None):
  cols = []
  for c in get_df_columns(df, discards=discards):
    if type_fn(df[c].dtype):
      cols.append(c)

  return cols


def read_csv(path, rows_sample=100, dtype=None, args=None):
  args = dict() if args is None else args
  if args.get('index_col', None) is None:
    args = args.copy()
    with open(path, mode='r') as f:
      fields = f.readline().rstrip().split(',')
    # If 'index_col' is not specified, we use column 0 if its name is empty, otherwise
    # we disable it setting it to False.
    args['index_col'] = False if fields[0] else 0

  if dtype is None:
    return pd.read_csv(path, **args)
  if isinstance(dtype, dict):
    numeric_cols = {c: np.dtype(t) for c, t in dtype.items()}
  else:
    df_test = pd.read_csv(path, nrows=rows_sample, **args)
    numeric_cols = {c: dtype for c in get_typed_columns(df_test, pyu.is_numeric)}

  return pd.read_csv(path, dtype=numeric_cols, **args)


def save_dataframe(df, path, **kwargs):
  _, ext = os.path.splitext(os.path.basename(path))
  if ext == '.pkl':
    args = pyu.dict_subset(kwargs, 'compression', 'protocol', 'storage_options')
    if 'protocol' not in args:
      args['protocol'] = pyu.pickle_proto()
    df.to_pickle(path, **args)
  elif ext == '.csv':
    args = pyu.dict_subset(kwargs, 'float_format', 'columns', 'header', 'index',
                           'index_label', 'mode', 'encoding', 'quoting',
                           'quotechar', 'line_terminator', 'chunksize',
                           'date_format', 'doublequote', 'escapechar',
                           'decimal', 'compression', 'error', 'storage_options')
    df.to_csv(path, **args)
  else:
    alog.xraise(RuntimeError, f'Unknown extension: {ext}')


def load_dataframe(path, **kwargs):
  _, ext = os.path.splitext(os.path.basename(path))
  if ext == '.pkl':
    return pd.read_pickle(path)
  elif ext == '.csv':
    rows_sample = kwargs.pop('rows_sample', 100)
    dtype = kwargs.pop('dtype', None)
    args = pyu.dict_subset(kwargs, 'sep', 'delimiter', 'header', 'names',
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
                           'memory_map', 'float_precision', 'storage_options')
    return read_csv(path, rows_sample=rows_sample, dtype=dtype,
                    args=args)
  else:
    alog.xraise(RuntimeError, f'Unknown extension: {ext}')

