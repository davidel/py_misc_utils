"""Tests for py_misc_utils.parquet_streamer.

Requires pyarrow. Uses pytest.importorskip("pyarrow") so the file
is skipped when pyarrow is not installed.
"""

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

pyarrow = pytest.importorskip("pyarrow")

from py_misc_utils import parquet_streamer as ps


def test_parquet_streamer_basic_read(tmp_path):
  """ParquetStreamer yields all records from a Parquet file."""
  path = str(tmp_path / 'data.parquet')
  table = pa.table({'a': [1, 2, 3], 'b': [4.0, 5.0, 6.0]})
  pq.write_table(table, path)

  streamer = ps.ParquetStreamer(path)
  records = list(streamer)

  assert len(records) == 3
  assert records[0] == {'a': 1, 'b': 4.0}
  assert records[1] == {'a': 2, 'b': 5.0}
  assert records[2] == {'a': 3, 'b': 6.0}


def test_parquet_streamer_batch_size(tmp_path):
  """ParquetStreamer reads file in batches of the specified size."""
  path = str(tmp_path / 'batch.parquet')
  table = pa.table({'x': list(range(10))})
  pq.write_table(table, path)

  streamer = ps.ParquetStreamer(path, batch_size=3)
  records = list(streamer)

  assert len(records) == 10
  assert [r['x'] for r in records] == list(range(10))


def test_parquet_streamer_rename_columns(tmp_path):
  """ParquetStreamer renames columns per rename_columns mapping."""
  path = str(tmp_path / 'rename.parquet')
  table = pa.table({'old_a': [1, 2], 'old_b': [3.0, 4.0]})
  pq.write_table(table, path)

  streamer = ps.ParquetStreamer(path, rename_columns={'old_a': 'new_a'})
  records = list(streamer)

  assert len(records) == 2
  assert list(records[0].keys()) == ['old_b', 'new_a']
  assert records[0] == {'old_b': 3.0, 'new_a': 1}
  assert records[1] == {'old_b': 4.0, 'new_a': 2}


def test_parquet_streamer_iter_alias(tmp_path):
  """ParquetStreamer.__iter__() is an alias for generate()."""
  path = str(tmp_path / 'alias.parquet')
  table = pa.table({'z': [42]})
  pq.write_table(table, path)

  streamer = ps.ParquetStreamer(path)
  records = list(iter(streamer))

  assert len(records) == 1
  assert records[0] == {'z': 42}


def test_parquet_streamer_empty_file(tmp_path):
  """ParquetStreamer on an empty file yields no records."""
  path = str(tmp_path / 'empty.parquet')
  table = pa.table({'a': pa.array([], type=pa.int64())})
  pq.write_table(table, path)

  streamer = ps.ParquetStreamer(path)
  records = list(streamer)

  assert records == []
