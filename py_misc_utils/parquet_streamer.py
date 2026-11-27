"""Parquet file streaming reader with lazy column loading.

This module provides :class:`ParquetStreamer` which reads Parquet files in
batches, optionally prefetching external columns via URLs, and supports
column renaming. Records are yielded one at a time for memory-efficient
iteration over large Parquet datasets.

Example:
  import os
  import tempfile

  import pyarrow as pa
  import pyarrow.parquet as pq

  import py_misc_utils.parquet_streamer as ps


  tdir = tempfile.mkdtemp()
  path = os.path.join(tdir, 'data.parquet')
  pq.write_table(pa.table({'id': [1, 2], 'val': ['one', 'two']}), path)

  recs = list(ps.ParquetStreamer(path))
  assert recs == [{'id': 1, 'val': 'one'}, {'id': 2, 'val': 'two'}]
"""

import contextlib

import pyarrow.parquet as pq

from . import alog
from . import gfs
from . import url_fetcher as urlf
from . import utils as ut


class ParquetStreamer:
  """Stream records from a Parquet file in batches with optional column loading.

  Example:
    import os
    import tempfile

    import pyarrow as pa
    import pyarrow.parquet as pq

    import py_misc_utils.parquet_streamer as ps


    tdir = tempfile.mkdtemp()
    path = os.path.join(tdir, 'data.parquet')
    pq.write_table(pa.table({'id': [1, 2], 'val': ['one', 'two']}), path)

    streamer = ps.ParquetStreamer(path, batch_size=1,
                                  rename_columns={'id': 'ident'})
    assert list(streamer) == [{'val': 'one', 'ident': 1},
                              {'val': 'two', 'ident': 2}]
  """

  def __init__(self,
               url,
               batch_size=128,
               load_columns=None,
               rename_columns=None,
               num_workers=None,
               **kwargs):
    """Initialize the Parquet streamer.

    Args:
      url: The file path or URL to read Parquet data from.
      batch_size: Number of rows to read per batch (default: 128).
      load_columns: Optional mapping of source columns to prefetch via URL fetcher.
      rename_columns: Optional mapping of old column names to new column names.
      num_workers: Number of worker threads for URL prefetching.
      **kwargs: Additional keyword arguments passed to gfs.open.
    """
    # File path or URL to read from.
    self._url = url
    # Number of rows per batch.
    self._batch_size = batch_size
    # Mapping of source columns to prefetch via URL fetcher.
    self._load_columns = ut.value_or(load_columns, dict())
    # Mapping of old column names to new column names.
    self._rename_columns = ut.value_or(rename_columns, dict())
    # Number of worker threads for URL prefetching.
    self._num_workers = num_workers
    # Keyword arguments passed to gfs.open.
    self._kwargs = kwargs

  def _fetcher(self):
    """Create a URL fetcher for prefetching external columns, or a no-op context.

    Returns:
      A UrlFetcher instance if load_columns is configured, otherwise a
      nullcontext that yields None.
    """
    if self._load_columns:
      return urlf.UrlFetcher(num_workers=self._num_workers, fs_kwargs=self._kwargs)
    else:
      return contextlib.nullcontext()

  def _prefetch(self, fetcher, recs):
    """Enqueue URL fetches for load_columns of the given records.

    Args:
      fetcher: The UrlFetcher instance, or None.
      recs: A list of record dictionaries.
    """
    if fetcher is not None:
      for recd in recs:
        for key in self._load_columns:
          fetcher.enqueue(recd[key])

  def _transform(self, fetcher, recd):
    """Resolve prefetched URLs and rename columns in a record.

    Args:
      fetcher: The UrlFetcher instance, or None.
      recd: A record dictionary.

    Returns:
      The transformed record with loaded columns and renamed keys.
    """
    if fetcher is not None:
      for key, name in self._load_columns.items():
        recd[name] = fetcher.wait(recd[key])

    for key, name in self._rename_columns.items():
      recd[name] = recd.pop(key)

    return recd

  def generate(self):
    """Yield transformed records from the Parquet file.

    Reads the Parquet file in batches, prefetches load_columns via URL
    fetcher, transforms each record, and yields it. Errors in individual
    records are logged verbosely and skipped.

    Yields:
      Transformed record dictionaries.
    """
    with (self._fetcher() as fetcher, gfs.open(self._url, mode='rb', **self._kwargs) as
          stream):
      pqfd = pq.ParquetFile(stream)
      for batch in pqfd.iter_batches(batch_size=self._batch_size):
        recs = batch.to_pylist()

        self._prefetch(fetcher, recs)
        for recd in recs:
          try:
            yield self._transform(fetcher, recd)
          except GeneratorExit:
            raise
          except Exception as ex:  # noqa: BLE001 -- per-record isolation: a failed record transform is skipped, not fatal to the stream
            alog.verbose(f'Unable to create parquet entry ({recd}): {ex}')

  def __iter__(self):
    """Return an iterator over the transformed records of the file."""
    return self.generate()
