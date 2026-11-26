"""Asynchronous HTTP fetching utilities with concurrent request management.

This module provides http_fetch_url() for fetching a single URL asynchronously
and HttpAsyncFetcher, a class that manages concurrent HTTP fetches with
automatic cleanup, work result tracking, and result iteration.
"""

import functools
import os

import httpx

from . import assert_checks as tas
from . import async_manager as asym
from . import core_utils as cu
from . import fin_wrap as fw
from . import gfs
from . import tempdir as tmpd
from . import utils as ut
from . import work_results as wres


async def http_fetch_url(url, context=None, path=None, http_args=None):
  """Asynchronously fetch a URL and save its content to a work result file.

  Uses an httpx AsyncClient from the provided context (or creates a default one)
  to GET the URL, writes the response content to a work result path, and handles
  errors by writing them to the work result system.

  Args:
    url: The URL to fetch.
    context: Optional async context manager providing an httpx.AsyncClient.
      Defaults to None.
    path: Optional base path for work results. Defaults to None.
    http_args: Optional keyword arguments for the httpx GET request.
      Defaults to None.

  Returns:
    The work result file path where the content was saved.
  """
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
  """Manages concurrent asynchronous HTTP fetches with work result tracking.

  Provides a context manager that enqueues URLs for async fetching, tracks
  pending requests, and yields results as they complete. Uses AsyncManager
  for concurrent execution and fin_wrap for automatic cleanup.
  """

  def __init__(self, path=None, num_workers=None, http_args=None, mpctx=None):
    """Initialize the HttpAsyncFetcher.

    Args:
      num_workers: Number of concurrent workers. Defaults to None.
      path: Base path for storing work results. If None, a temp dir is used.
        Defaults to None.
      http_args: Default keyword arguments for httpx requests. Defaults to None.
      mpctx: Multiprocessing context for async execution. Defaults to None.
    """
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
    """Class method cleaner for fin_wrap cleanup.

    Closes the async manager and removes the temp path if it differs from
    the constructor path.

    Args:
      self: The HttpAsyncFetcher instance being cleaned up.
    """
    self._async_manager.close()
    if self._path != self._ctor_path:
      gfs.rmtree(self._path, ignore_errors=True)

  def start(self):
    """Initialize the async manager and set the working path.

    Creates a temp directory if no path was specified, then starts the
    AsyncManager with the configured number of workers.
    """
    if self._ctor_path is None:
      self._path = tmpd.fastfs_dir()
    else:
      self._path = self._ctor_path

    async_manager = asym.AsyncManager(
        **cu.denone(num_workers=self._num_workers, mpctx=self._mpctx))

    finfn = functools.partial(self._cleaner,
                              cu.object_context(self, _async_manager=async_manager))
    fw.fin_wrap(self, '_async_manager', async_manager, finfn=finfn)

  def shutdown(self):
    """Shut down the fetcher and clean up resources.

    Closes the async manager, clears the path and pending set.
    """
    async_manager = self._async_manager
    if async_manager is not None:
      fw.fin_wrap(self, '_async_manager', None, cleanup=True)
      self._path = None
      self._pending = set()

  def enqueue(self, *urls):
    """Enqueue URLs for asynchronous fetching.

    Args:
      *urls: Variable number of URL strings to fetch.

    Returns:
      A dict mapping each URL to its work result hash.
    """
    wmap = dict()
    for url in urls:
      if url:
        work_ctor = functools.partial(http_fetch_url,
                                      url,
                                      path=self._path,
                                      http_args=self._http_args)
        self._async_manager.enqueue_work(url, work_ctor)
        self._pending.add(url)
        wmap[url] = wres.work_hash(url)

    return wmap

  def wait(self, url):
    """Wait for a specific URL's fetch to complete and return its result.

    Args:
      url: The URL string to wait for.

    Returns:
      The fetched work result data.

    Raises:
      AssertionError: If the URL is not in the pending set.
    """
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
    """Iterate over completed fetch results as they become available.

    Args:
      max_results: Maximum number of results to yield. Defaults to None (all).
      block: Whether to block waiting for results. Defaults to True.
      timeout: Timeout in seconds for blocking fetch. Defaults to None.

    Yields:
      Tuples of (url, data) for each completed fetch.
    """
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
    """Enter the context manager by starting the fetcher.

    Returns:
      self.
    """
    self.start()

    return self

  def __exit__(self, *exc):
    """Exit the context manager by shutting down the fetcher.

    Args:
      *exc: Exception info (unused).

    Returns:
      False, to not suppress exceptions.
    """
    self.shutdown()

    return False
