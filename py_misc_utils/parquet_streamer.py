import contextlib
import functools

import pyarrow.parquet as pq

from . import alog
from . import fin_wrap as fw
from . import gfs
from . import url_fetcher as urlf
from . import utils as ut


class ParquetStreamer:

  def __init__(self, url,
               batch_size=128,
               load_columns=None,
               rename_columns=None,
               num_workers=None,
               **kwargs):
    self._url = url
    self._batch_size = batch_size
    self._load_columns = ut.value_or(load_columns, dict())
    self._rename_columns = ut.value_or(rename_columns, dict())
    self._num_workers = num_workers
    self._kwargs = kwargs

  def _fetcher(self):
    if self._load_columns:
      return urlf.UrlFetcher(num_workers=self._num_workers,
                             fs_kwargs=self._kwargs)
    else:
      return contextlib.nullcontext()

  def _prefetch(self, fetcher, recs):
    if fetcher is not None:
      for recd in recs:
        for key in self._load_columns.keys():
          fetcher.enqueue(recd[key])

  def _transform(self, fetcher, recd):
    if fetcher is not None:
      for key, name in self._load_columns.items():
        recd[name] = fetcher.wait(recd[key])

    for key, name in self._rename_columns.items():
      recd[name] = recd.pop(key)

    return recd

  def generate(self):
    with (self._fetcher() as fetcher,
          gfs.open(self._url, mode='rb', **self._kwargs) as stream):
      pqfd = pq.ParquetFile(stream)
      for batch in pqfd.iter_batches(batch_size=self._batch_size):
        recs = batch.to_pylist()

        self._prefetch(fetcher, recs)
        for recd in recs:
          try:
            yield self._transform(fetcher, recd)
          except GeneratorExit:
            raise
          except Exception as ex:
            alog.verbose(f'Unable to create parquet entry ({recd}): {ex}')

  def __iter__(self):
    return iter(self.generate())

