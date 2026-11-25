"""Tests for py_misc_utils.stream_dataframe.

Expectations follow the documented API contract. Tests assert specific
values verified by probe scripts; where library behavior deviates from
the documented contract the test will fail, exposing a possible library
bug.
"""

import os

import numpy as np
import pandas as pd
import pytest

from py_misc_utils import stream_dataframe as sdf


def test_stream_data_writer_write_and_read(temp_path):
  """StreamDataWriter.write() stores records; StreamDataReader reads them back."""
  path = os.path.join(temp_path, 'stream.ts')
  writer = sdf.StreamDataWriter('id=i,name=S5,value=f', path)
  writer.write(id=np.array([1]), name=np.array([b'alice']), value=np.array([1.5]))
  writer.write(id=np.array([2]), name=np.array([b'bob']), value=np.array([2.5]))
  writer.flush()

  reader = sdf.StreamDataReader(path)
  assert len(reader) == 2

  sl = reader.get_slice(0, 2)
  assert sl['id'].tolist() == [1, 2]
  assert sl['name'].tolist() == [b'alice', b'bob']
  assert sl['value'].tolist() == [1.5, 2.5]


def test_stream_data_writer_write_dataframe(temp_path):
  """StreamDataWriter.write_dataframe() stores a pandas DataFrame."""
  path = os.path.join(temp_path, 'df_stream.ts')
  writer = sdf.StreamDataWriter('a=i,b=f', path)
  df = pd.DataFrame({'a': [1, 2, 3], 'b': [1.0, 2.0, 3.0]})
  writer.write_dataframe(df)
  writer.flush()

  reader = sdf.StreamDataReader(path)
  assert len(reader) == 3
  assert tuple(reader.fields()) == ('a', 'b')

  sl = reader.get_slice(0, 3)
  assert sl['a'].tolist() == [1, 2, 3]
  assert sl['b'].tolist() == [1.0, 2.0, 3.0]


def test_stream_data_reader_fields_and_dtype(temp_path):
  """StreamDataReader exposes correct field names and dtypes."""
  path = os.path.join(temp_path, 'meta_stream.ts')
  writer = sdf.StreamDataWriter('x=f,y=i,z=S3', path)
  writer.write(x=np.array([1.0]), y=np.array([2]), z=np.array([b'foo']))
  writer.flush()

  reader = sdf.StreamDataReader(path)
  assert tuple(reader.fields()) == ('x', 'y', 'z')
  assert reader.dtype == (np.float32, np.int32, np.dtype('S3'))


def test_stream_sorted_scan_returns_sorted(temp_path):
  """StreamSortedScan.scan() yields records sorted by key field."""
  path = os.path.join(temp_path, 'sorted_stream.ts')
  writer = sdf.StreamDataWriter('id=i,val=f', path)
  writer.write(id=np.array([3]), val=np.array([30.0]))
  writer.write(id=np.array([1]), val=np.array([10.0]))
  writer.write(id=np.array([2]), val=np.array([20.0]))
  writer.flush()

  reader = sdf.StreamDataReader(path)
  scan = sdf.StreamSortedScan(reader, field='id', reverse=False)
  results = list(scan.scan())

  ids = [r['id'].tolist() for _, r in results]
  vals = [r['val'].tolist() for _, r in results]
  assert ids == [[1, 2, 3]]
  assert vals == [[10.0, 20.0, 30.0]]


def test_stream_sorted_scan_reverse(temp_path):
  """StreamSortedScan.scan(reverse=True) yields records in descending order."""
  path = os.path.join(temp_path, 'rev_stream.ts')
  writer = sdf.StreamDataWriter('id=i,val=f', path)
  writer.write(id=np.array([1]), val=np.array([10.0]))
  writer.write(id=np.array([2]), val=np.array([20.0]))
  writer.flush()

  reader = sdf.StreamDataReader(path)
  scan = sdf.StreamSortedScan(reader, field='id', reverse=True)
  results = list(scan.scan())

  ids = [r['id'].tolist() for _, r in results]
  assert ids == [[2, 1]]


def test_stream_sorted_scan_batch_size(temp_path):
  """StreamSortedScan.scan() yields batches of up to slice_size records."""
  path = os.path.join(temp_path, 'batch_stream.ts')
  writer = sdf.StreamDataWriter('id=i,val=f', path)
  for i in range(5):
    writer.write(id=np.array([i]), val=np.array([float(i * 10)]))
  writer.flush()

  reader = sdf.StreamDataReader(path)
  scan = sdf.StreamSortedScan(reader, field='id', slice_size=2)
  results = list(scan.scan())

  total = sum(len(r['id']) for _, r in results)
  assert total == 5
