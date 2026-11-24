"""Tests for py_misc_utils.pd_utils.

Expectations follow the documented API contract. Tests assert specific
values verified by probe scripts; where library behavior deviates from
the documented contract the test will fail, exposing a possible library
bug (see KNOWN LIBRARY BUGS in SCRATCH/REFACTORING_PLAN.md).
"""

import numpy as np
import pandas as pd
import pytest

from py_misc_utils import pd_utils as pdu


@pytest.fixture()
def df():
  return pd.DataFrame({
    'a': [1, 2, 3, 4],
    'b': ['x', 'y', 'x', 'y'],
    'c': [1.0, 2.0, 3.0, 4.0],
  })


def test_get_df_columns_excludes_discards(df):
  """get_df_columns() excludes columns listed in discards."""
  cols = pdu.get_df_columns(df, discards={'b'})

  assert cols == ['a', 'c']


def test_get_typed_columns_filters_by_dtype(df):
  """get_typed_columns() returns columns whose dtype satisfies type_fn."""
  def _is_numeric(dt):
    try:
      return np.issubdtype(np.dtype(dt), np.number)
    except TypeError:
      return False

  cols = pdu.get_typed_columns(df, _is_numeric)

  assert sorted(cols) == ['a', 'c']


def test_re_select_columns_matches_regex(df):
  """re_select_columns() returns columns matching any regex pattern."""
  cols = pdu.re_select_columns(df, [r'^a$', r'^c$'])

  assert sorted(cols) == ['a', 'c']


def test_read_csv_infers_dtypes(tmp_path):
  """read_csv() infers numeric dtypes when none provided."""
  path = str(tmp_path / 'data.csv')
  df = pd.DataFrame({'a': [1, 2, 3], 'b': [1.5, 2.5, 3.5]})
  df.to_csv(path, index=False)

  result = pdu.read_csv(path)

  assert list(result.columns) == ['a', 'b']
  assert np.issubdtype(result['a'].dtype, np.number)
  assert np.issubdtype(result['b'].dtype, np.number)


def test_save_load_pickle_roundtrip(tmp_path, df):
  """save_dataframe()/load_dataframe() round-trip via pickle."""
  path = str(tmp_path / 'df.pkl')
  pdu.save_dataframe(df, path)

  result = pdu.load_dataframe(path)

  pd.testing.assert_frame_equal(result, df)


def test_save_load_csv_roundtrip(tmp_path, df):
  """save_dataframe()/load_dataframe() round-trip via CSV."""
  path = str(tmp_path / 'df.csv')
  pdu.save_dataframe(df, path)

  result = pdu.load_dataframe(path)

  pd.testing.assert_frame_equal(result, df)


def test_unknown_extension_raises(tmp_path):
  """save_dataframe()/load_dataframe() raise on unknown extension."""
  path = str(tmp_path / 'df.xyz')

  with pytest.raises(RuntimeError):
    pdu.save_dataframe(df, path)

  with pytest.raises(RuntimeError):
    pdu.load_dataframe(path)


def test_to_npdict_converts_columns(df):
  """to_npdict() converts each column to a numpy array keyed by name."""
  npdict = pdu.to_npdict(df)

  assert npdict['a'].tolist() == [1, 2, 3, 4]
  assert npdict['b'].tolist() == ['x', 'y', 'x', 'y']


def test_to_npdict_applies_dtype(df):
  """to_npdict(dtype=...) applies per-column dtype conversion."""
  npdict = pdu.to_npdict(df, dtype={'a': np.float64})

  assert npdict['a'].dtype == np.float64


def test_to_npdict_no_convert_skips_columns(df):
  """to_npdict(no_convert=...) skips dtype conversion for those columns."""
  npdict = pdu.to_npdict(df, dtype={'a': np.float64}, no_convert=('a',))

  assert npdict['a'].dtype == np.int64


def test_load_dataframe_as_npdict(tmp_path, df):
  """load_dataframe_as_npdict() loads a file and returns dict of arrays."""
  path = str(tmp_path / 'df.pkl')
  pdu.save_dataframe(df, path)

  npdict = pdu.load_dataframe_as_npdict(path)

  assert npdict['a'].tolist() == [1, 2, 3, 4]


def test_column_or_index_returns_column(df):
  """column_or_index() returns the column data for an existing column."""
  data = pdu.column_or_index(df, 'a')

  assert data.tolist() == [1, 2, 3, 4]


def test_column_or_index_nonexistent_returns_none(df):
  """column_or_index() returns None for missing column/index."""
  assert pdu.column_or_index(df, 'nonexistent') is None


def test_columns_transform_applies_to_columns(df):
  """columns_transform() applies tfn to specified columns."""
  result = pdu.columns_transform(df, ['a'], lambda name, data, index: data * 2)

  assert result['a'].tolist() == [2, 4, 6, 8]


def test_get_columns_index(df):
  """get_columns_index() returns name->ordinal dict and column list."""
  index_map, cols = pdu.get_columns_index(df)

  assert index_map == {'a': 0, 'b': 1, 'c': 2}
  assert cols == ['a', 'b', 'c']


def test_concat_dataframes(tmp_path):
  """concat_dataframes() concatenates multiple CSV/pickle files."""
  path1 = str(tmp_path / 'df1.pkl')
  path2 = str(tmp_path / 'df2.pkl')
  df1 = pd.DataFrame({'a': [1, 2]})
  df2 = pd.DataFrame({'a': [3, 4]})
  pdu.save_dataframe(df1, path1)
  pdu.save_dataframe(df2, path2)

  result = pdu.concat_dataframes([path1, path2])

  assert result['a'].tolist() == [1, 2, 3, 4]


def test_concat_dataframes_empty():
  """concat_dataframes() returns None when given no files."""
  assert pdu.concat_dataframes([]) is None


def test_get_dataframe_groups(df):
  """get_dataframe_groups() maps group keys to ascending row numbers."""
  groups = pdu.get_dataframe_groups(df, ['b'])

  assert list(groups[('x',)]) == [0, 2]
  assert list(groups[('y',)]) == [1, 3]


def test_limit_per_group_keeps_first_n_rows(df):
  """limit_per_group() keeps at most `limit` rows per group."""
  result = pdu.limit_per_group(df, ['b'], limit=1)

  assert len(result) == 2
  assert sorted(result.index.tolist()) == [0, 1]


def test_limit_per_group_negative_keeps_last_n_rows(df):
  """limit_per_group(limit<0) keeps rows at the end of each group."""
  result = pdu.limit_per_group(df, ['b'], limit=-1)

  assert len(result) == 2
  assert sorted(result.index.tolist()) == [2, 3]


def test_dataframe_column_rewrite(df):
  """dataframe_column_rewrite() replaces column values via fn."""
  pdu.dataframe_column_rewrite(df, 'a', lambda values: values * 10)

  assert df['a'].tolist() == [10, 20, 30, 40]


def test_type_convert_dataframe(df):
  """type_convert_dataframe() converts columns to the specified types."""
  pdu.type_convert_dataframe(df, {'a': np.float64, 'c': np.int32})

  assert df['a'].dtype == np.float64
  assert df['c'].dtype == np.int32


def test_dataframe_rows_select(df):
  """dataframe_rows_select() selects rows by ordinal position."""
  result = pdu.dataframe_rows_select(df, [0, 2])

  assert result['a'].tolist() == [1, 3]


def test_sorted_index_reindexes_by_column(df):
  """sorted_index() sorts by col and re-indexes when col is not the index."""
  df2 = df.sort_values('c', ascending=False)

  result = pdu.sorted_index(df2, 'c')

  assert result.index.tolist() == [1.0, 2.0, 3.0, 4.0]


def test_sorted_index_already_indexed(df):
  """sorted_index() sorts in place when col is already the index."""
  df2 = df.set_index('c')

  result = pdu.sorted_index(df2, 'c')

  assert result.index.tolist() == [1.0, 2.0, 3.0, 4.0]
