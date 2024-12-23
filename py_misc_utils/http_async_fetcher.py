import functools
import os

import httpx

from . import async_manager as asym
from . import file_overwrite as fow
from . import fin_wrap as fw
from . import gfs as gfs
from . import tempdir as tmpd
from . import utils as ut
from . import work_results as wres


async def http_fetch_url(url, context=None, path=None, http_args=None):
  wpath = wres.work_path(path, url)
  try:
    client = await context.get('httpx.AsyncClient', httpx.AsyncClient)

    resp = await client.get(url, **http_args)
    resp.raise_for_status()

    with fow.FileOverwrite(wpath, mode='wb') as fd:
      fd.write(resp.content)
  except Exception as ex:
    wres.write_error(wpath, ex, url=url)
  finally:
    return wpath


class HttpAsyncFetcher:

  def __init__(self, path=None, num_workers=None, http_args=None):
    self._path, self._tmp_path = path, None
    self._http_args = ut.dict_setmissing(
      http_args or dict(),
      timeout=ut.getenv('FETCHER_TIMEO', dtype=float, defval=10.0),
    )
    self._num_workers = num_workers
    self._async_manager = None

  @classmethod
  def _cleaner(cls, async_manager, path):
    async_manager.close()
    if path is not None:
      gfs.rmtree(path, ignore_errors=True)

  def start(self):
    if self._path is None:
      self._tmp_path = tmpd.fastfs_dir()
      self._path = self._tmp_path

    async_manager = asym.AsyncManager(num_workers=self._num_workers)

    finfn = functools.partial(self._cleaner, async_manager, self._tmp_path)
    fw.fin_wrap(self, '_async_manager', async_manager, finfn=finfn)

  def shutdown(self):
    async_manager = self._async_manager
    if async_manager is not None:
      fw.fin_wrap(self, '_async_manager', None)
      self._cleaner(async_manager, self._tmp_path)

  def enqueue(self, *urls):
    wmap = dict()
    for url in urls:
      if url:
        work_ctor = functools.partial(http_fetch_url, url,
                                      path=self._path,
                                      http_args=self._http_args)
        self._async_manager.enqueue_work(url, work_ctor)
        wmap[url] = wres.work_hash(url)

    return wmap

  def try_get(self, url):
    return wres.tryget_work(self._path, url)

  def wait(self, url):
    wpath = wres.work_path(self._path, url)
    if not os.path.isfile(wpath):
      while True:
        (rurl, result) = self._async_manager.fetch_result()

        wres.raise_if_error(result)
        if rurl == url:
          break

    return wres.get_work(wpath)

  def __enter__(self):
    self.start()

    return self

  def __exit__(self, *exc):
    self.shutdown()

    return False

