import functools
import os

import httpx

from . import async_manager as asym
from . import assert_checks as tas
from . import core_utils as cu
from . import file_overwrite as fow
from . import fin_wrap as fw
from . import gfs
from . import tempdir as tmpd
from . import utils as ut
from . import work_results as wres


async def http_fetch_url(url, context=None, path=None, http_args=None):
  wpath = wres.work_path(path, url)
  try:
    client = await context.get('httpx.AsyncClient', httpx.AsyncClient)

    resp = await client.get(url, **http_args)
    resp.raise_for_status()

    with wres.write_result(wpath) as fd:
      fd.write(resp.content)
  except Exception as ex:
    wres.write_error(wpath, ex, workid=url)
  finally:
    return wpath


class HttpAsyncFetcher:

  def __init__(self,
               path=None,
               num_workers=None,
               http_args=None,
               mpctx=None):
    self._ctor_path = path
    self._path = None
    self._num_workers = num_workers
    self._http_args = ut.dict_setmissing(
      http_args or dict(),
      timeout=ut.getenv('FETCHER_TIMEO', dtype=float, defval=10.0),
    )
    self._mpctx = mpctx
    self._async_manager = None
    self._pending = set()

  @classmethod
  def _cleaner(cls, self):
    self._async_manager.close()
    if self._path != self._ctor_path:
      gfs.rmtree(self._path, ignore_errors=True)

  def start(self):
    if self._ctor_path is None:
      self._path = tmpd.fastfs_dir()
    else:
      self._path = self._ctor_path

    async_manager = asym.AsyncManager(**cu.denone(num_workers=self._num_workers,
                                                  mpctx=self._mpctx))

    finfn = functools.partial(self._cleaner,
                              cu.object_context(self, _async_manager=async_manager))
    fw.fin_wrap(self, '_async_manager', async_manager, finfn=finfn)

  def shutdown(self):
    async_manager = self._async_manager
    if async_manager is not None:
      fw.fin_wrap(self, '_async_manager', None, cleanup=True)
      self._path = None
      self._pending = set()

  def enqueue(self, *urls):
    wmap = dict()
    for url in urls:
      if url:
        work_ctor = functools.partial(http_fetch_url, url,
                                      path=self._path,
                                      http_args=self._http_args)
        self._async_manager.enqueue_work(url, work_ctor)
        self._pending.add(url)
        wmap[url] = wres.work_hash(url)

    return wmap

  def wait(self, url):
    wpath = wres.work_path(self._path, url)
    if not os.path.isfile(wpath):
      tas.check(url in self._pending, msg=f'URL already retired: {url}')

      while self._pending:
        (rurl, result) = self._async_manager.fetch_result()

        self._pending.discard(rurl)
        wres.raise_if_error(result)
        if rurl == url:
          break

    try:
      return wres.get_work(wpath)
    finally:
      os.remove(wpath)

  def iter_results(self, max_results=None, block=True, timeout=None):
    count = 0
    while self._pending:
      if (fetchres := self._async_manager.fetch_result(block=block,
                                                       timeout=timeout)) is None:
        break

      rurl, result = fetchres

      self._pending.discard(rurl)
      wpath = wres.work_path(self._path, rurl)

      wdata = wres.load_work(wpath)

      os.remove(wpath)

      yield rurl, wdata

      count += 1
      if max_results is not None and count >= max_results:
        break

  def __enter__(self):
    self.start()

    return self

  def __exit__(self, *exc):
    self.shutdown()

    return False

