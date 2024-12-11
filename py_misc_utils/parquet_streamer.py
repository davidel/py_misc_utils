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
               num_workers=None,
               **kwargs):
    self._url = url
    self._batch_size = ut.value_or(batch_size, 128)
    self._load_columns = ut.value_or(load_columns, dict())
    self._kwargs = kwargs

    if self._load_columns:
      fetch_path = tmpd.create()
      fetcher = urlf.UrlFetcher(fetch_path, num_workers=num_workers, fs_kwargs=kwargs)
      fetcher.start()

      finfn = functools.partial(self._cleaner, fetcher, fetch_path)
      fw.fin_wrap(self, '_fetcher', fetcher, finfn=finfn)
    else:
      self._fetcher = None

  @classmethod
  def _cleaner(cls, fetcher, fetch_path):
    fetcher.shutdown()
    gfs.rmtree(fetch_path, ignore_errors=True)

  def _prefetch(self, recs):
    if self._fetcher is not None:
      for recd in recs:
        for key in self._load_columns.keys():
          self._fetcher.enqueue(recd[key])

  def generate(self):
    with gfs.open(self._url, mode='rb', **self._kwargs) as stream:
      pqfd = pq.ParquetFile(stream)
      for batch in pqfd.iter_batches(batch_size=self._batch_size):
        recs = batch.to_pylist()

        self._prefetch(recs)
        for recd in recs:
          try:
            for key, kind in self._load_columns.items():
              data = self._fetcher.wait(recd[key])
              if kind == 'img':
                kdata = imgu.from_bytes(data)
              elif kind == 'rgbimg':
                kdata = imgu.from_bytes(data, convert='RGB')
              elif kind == 'raw':
                kdata = data
              else:
                alog.xraise(ValueError, f'Unknown load column kind: {kind}')

              recd[f'{key}_{kind}'] = kdata

            yield recd
          except Exception as ex:
            alog.verbose(f'Unable to create parquet entry ({recd}): {ex}')

  def __iter__(self):
    return iter(self.generate())

