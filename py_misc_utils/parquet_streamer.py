import functools

import pyarrow.parquet as pq

from . import alog as alog
from . import assert_checks as tas
from . import fin_wrap as fw
from . import gfs
from . import img_utils as imgu
from . import tempdir as tmpd
from . import url_fetcher as urlf
from . import utils as ut


class ParquetStreamer:

  def __init__(self, url,
               batch_size=None,
               load_columns=None,
               rename_columns=None,
               num_workers=None,
               **kwargs):
    self._url = url
    self._batch_size = ut.value_or(batch_size, 128)
    self._load_columns = ut.value_or(load_columns, dict())
    self._rename_columns = ut.value_or(rename_columns, dict())
    self._kwargs = kwargs

    if self._load_columns:
      fetcher = urlf.UrlFetcher(num_workers=num_workers, fs_kwargs=kwargs)
      fetcher.start()

      finfn = functools.partial(self._cleaner, fetcher)
      fw.fin_wrap(self, '_fetcher', fetcher, finfn=finfn)
    else:
      self._fetcher = None

  @classmethod
  def _cleaner(cls, fetcher):
    fetcher.shutdown()

  def _prefetch(self, recs):
    if self._fetcher is not None:
      for recd in recs:
        for key in self._load_columns.keys():
          self._fetcher.enqueue(recd[key])

  def _transform(self, recd):
    if self._fetcher is not None:
      for key, name in self._load_columns.items():
        recd[name] = self._fetcher.wait(recd[key])

    for key, name in self._rename_columns.items():
      recd[name] = recd.pop(key)

    return recd

  def generate(self):
    with gfs.open(self._url, mode='rb', **self._kwargs) as stream:
      pqfd = pq.ParquetFile(stream)
      for batch in pqfd.iter_batches(batch_size=self._batch_size):
        recs = batch.to_pylist()

        self._prefetch(recs)
        for recd in recs:
          try:
            yield self._transform(recd)
          except Exception as ex:
            alog.verbose(f'Unable to create parquet entry ({recd}): {ex}')

  def __iter__(self):
    return iter(self.generate())

