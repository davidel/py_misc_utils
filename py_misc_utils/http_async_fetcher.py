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


async def http_fetch_url(url, context, path=None, http_args=None):
  """Asynchronously fetch a URL and save its content to a work result file.

  Uses an httpx AsyncClient obtained from the provided context to GET the
  URL, writes the response content to a work result path, and handles
  errors by writing them to the work result system.

  Args:
    url: The URL to fetch.
    context: AsyncContext providing an httpx.AsyncClient through its
      get() method. Mandatory - the HttpAsyncFetcher class supplies it
      automatically, using the context owned by each worker process.
    path: Optional base path for work results. Defaults to None - which
      fails, as a work results folder is required.
    http_args: Optional keyword arguments for the httpx GET request
      (default: no extra arguments).

  Returns:
    The work result file path where the content was saved.

  Example:
    import asyncio
    import http.server
    import os
    import tempfile
    import threading

    import httpx

    from py_misc_utils import http_async_fetcher as haf
    from py_misc_utils import work_results as wres


    class NotFoundHandler(http.server.BaseHTTPRequestHandler):
      def do_GET(self):
        self.send_response(404)
        self.send_header('Content-Length', '0')
        self.end_headers()

      def log_message(self, *args):
        pass


    class ClientCtx:
      async def get(self, name, ctor):
        return httpx.AsyncClient()

    srv = http.server.ThreadingHTTPServer(('127.0.0.1', 0), NotFoundHandler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    url = 'http://127.0.0.1:{}/nope'.format(srv.server_address[1])

    wpath = asyncio.run(
        haf.http_fetch_url(url, context=ClientCtx(), path=tempfile.mkdtemp(),
                           http_args={}))
    err = wres.load_work(wpath=wpath)
    assert err.is_instance(httpx.HTTPStatusError)
    srv.shutdown()
    srv.server_close()
  """
  wpath = wres.work_path(path, url)
  try:
    client = await context.get('httpx.AsyncClient', httpx.AsyncClient)

    resp = await client.get(url, **(http_args or dict()))
    resp.raise_for_status()

    with wres.write_result(wpath) as fd:
      fd.write(resp.content)
  except Exception as ex:  # noqa: BLE001 -- worker boundary: fetch failures are recorded via write_error, not propagated
    wres.write_error(wpath, ex, workid=url)

  return wpath


class HttpAsyncFetcher:
  """Manages concurrent asynchronous HTTP fetches with work result tracking.

  Provides a context manager that enqueues URLs for async fetching, tracks
  pending requests, and yields results as they complete. Uses AsyncManager
  for concurrent execution and fin_wrap for automatic cleanup.

  Example:
    import functools
    import http.server
    import os
    import threading

    from py_misc_utils import http_async_fetcher as haf


    def serve():
      class H(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
          body = os.path.basename(self.path).encode() + b'-body'
          self.send_response(200)
          self.send_header('Content-Length', str(len(body)))
          self.end_headers()
          self.wfile.write(body)

        def log_message(self, *args):
          pass

      srv = http.server.ThreadingHTTPServer(('127.0.0.1', 0), H)
      threading.Thread(target=srv.serve_forever, daemon=True).start()

      return srv


    srv = serve()
    try:
      port = srv.server_address[1]
      with haf.HttpAsyncFetcher(num_workers=2) as fetcher:
        fetcher.enqueue(f'http://127.0.0.1:{port}/one.bin')
        results = dict(fetcher.iter_results())
        assert results == {f'http://127.0.0.1:{port}/one.bin': b'one.bin-body'}
    finally:
      srv.shutdown()
      srv.server_close()
  """

  def __init__(self, path=None, num_workers=None, http_args=None, mpctx=None):
    """Initialize the HttpAsyncFetcher.

    Args:
      num_workers: Number of concurrent workers (default: CPU count).
      path: Base path for storing work results. If None, a fast filesystem
        temp dir is used, which is removed upon shutdown.
      http_args: Default keyword arguments for httpx requests.
      mpctx: Multiprocessing context for async execution
        (default: multiprocessing).
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
      *urls: Variable number of URL strings to fetch - empty (falsy) URLs
        are skipped.

    Returns:
      A dict mapping each enqueued URL to its work result hash.
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
      Exception: The error which made the fetch fail - if it failed.
      AssertionError: If the URL is not in the pending set (i.e. already
        waited for or never enqueued).
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
      Tuples of (url, data) for each completed fetch - where data holds
      the fetched content, or the error object if the fetch failed.

    """
    count = 0
    while self._pending:
      if (fetchres := self._async_manager.fetch_result(block=block,
                                                       timeout=timeout)) is None:
        break

      rurl, _ = fetchres

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

    Starts the AsyncManager workers, creating the work results folder - a
    fast filesystem temp dir - if no path was specified.

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
