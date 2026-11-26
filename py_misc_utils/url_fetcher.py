"""Parallel URL fetching.

This module provides UrlFetcher, a multi-threaded URL fetcher which
downloads files in parallel - one worker thread per concurrent fetch -
storing the results in a working folder, so that they can be retrieved
either one by one (wait) or streamed (iter_results).

The URLs are fetched via the filesystem abstraction layer (see gfs), so
any supported URL protocol - local files, http(s), GCS, etc. - can be
used transparently.
"""
import os
import queue
import threading

from . import alog
from . import assert_checks as tas
from . import gfs
from . import tempdir as tmpd
from . import utils as ut
from . import work_results as wres


def resolve_url(fss, url, fs_kwargs):
  """Resolves an URL into its filesystem object and normalized path.

  Args:
    fss: A dict of already-resolved filesystem objects, keyed by their
      protocol ids - updated in place upon a successful resolution.
    url: The URL to resolve.
    fs_kwargs: Optional keyword arguments passed to the filesystem
      resolver.

  Returns:
    A (fs, fpath) tuple, where fs is the filesystem object handling the
    URL and fpath the URL path normalized with respect to it.
  """
  proto = gfs.get_proto(url)
  fs = fss.get(proto)
  if fs is None:
    fs, fpath = gfs.resolve_fs(url, **fs_kwargs)
    for fsid in fs.IDS:
      fss[fsid] = fs
  else:
    fpath = fs.norm_url(url)

  return fs, fpath


def fetcher(path, fs_kwargs, uqueue, rqueue):
  """The URL fetcher worker function.

  Fetches the URLs received from a work queue - one at a time - storing
  their content in the working folder, until a shutdown message is
  received.

  Args:
    path: The working folder path where the fetched files are stored.
    fs_kwargs: Optional keyword arguments passed to the filesystem
      resolver.
    uqueue: The queue holding the URLs to be fetched - and the shutdown
      messages ('').
    rqueue: The queue onto which the URLs of the completed - or failed -
      fetches are posted.
  """
  fss = dict()
  while True:
    url = uqueue.get()
    if not url:
      break

    wpath = wres.work_path(path, url)

    alog.verbose(f'Fetching "{url}"')
    try:
      fs, fpath = resolve_url(fss, url, fs_kwargs)

      with wres.write_result(wpath) as fd:
        for data in fs.get_file(fpath):
          fd.write(data)
    except Exception as ex:  # noqa: BLE001 -- worker boundary: any fetch failure is recorded via write_error, never propagated
      wres.write_error(wpath, ex, workid=url)
    finally:
      rqueue.put(url)


class UrlFetcher:
  """A multi-threaded parallel URL fetcher.

  URLs submitted via enqueue() are fetched concurrently by a pool of
  worker threads, each of them resolving the URLs through the filesystem
  abstraction layer.

  A UrlFetcher is a context manager: entering it starts the workers,
  exiting it shuts them down.
  """

  def __init__(self, path=None, num_workers=None, fs_kwargs=None):
    """Creates an URL fetcher.

    Args:
      path: An optional working folder path where the fetched files are
        stored. If None, a fast filesystem folder - automatically cleaned
        up upon shutdown - is used.
      num_workers: The number of fetcher worker threads. If None, a
        default value - based on the CPU count - is used.
      fs_kwargs: Optional keyword arguments passed to the filesystem
        resolver - like the fetch timeout.
    """
    fs_kwargs = fs_kwargs or dict()
    fs_kwargs = ut.dict_setmissing(
        fs_kwargs,
        timeout=ut.getenv('FETCHER_TIMEO', dtype=float, defval=10.0),
    )

    self._ctor_path = path
    self._path = None
    self._num_workers = num_workers or max(os.cpu_count() * 4, 128)
    self._fs_kwargs = fs_kwargs
    self._uqueue = self._rqueue = None
    self._workers = []
    self._pending = set()

  def start(self):
    """Starts the fetcher workers."""
    if self._ctor_path is None:
      self._path = tmpd.fastfs_dir()
    else:
      self._path = self._ctor_path

    self._uqueue = queue.Queue()
    self._rqueue = queue.Queue()
    for i in range(self._num_workers):
      worker = threading.Thread(
          target=fetcher,
          args=(self._path, self._fs_kwargs, self._uqueue, self._rqueue),
          daemon=True,
      )
      worker.start()
      self._workers.append(worker)

  def shutdown(self):
    """Shuts down the fetcher workers.

    Waits for all the worker threads to complete their pending fetches,
    and removes the working folder if it has been created by the fetcher
    itself.
    """
    alog.verbose('Sending shutdowns down the queue')
    for _ in range(len(self._workers)):
      self._uqueue.put('')

    alog.verbose('Joining fetcher workers')
    for worker in self._workers:
      worker.join()

    self._uqueue = self._rqueue = None
    self._workers = []

    if self._path != self._ctor_path:
      gfs.rmtree(self._path, ignore_errors=True)

    self._path = None
    self._pending = set()

  def enqueue(self, *urls):
    """Enqueues URLs to be fetched.

    Args:
      *urls: The URLs to enqueue. Empty (falsy) URLs are skipped.

    Returns:
      A dict mapping each enqueued URL to its work-result hash - as
      expected by the work-results API.
    """
    wmap = dict()
    for url in urls:
      if url:
        self._uqueue.put(url)
        self._pending.add(url)
        wmap[url] = wres.work_hash(url)

    return wmap

  def wait(self, url):
    """Waits for the fetch of an URL to be completed - or to have failed.

    Blocks until the given URL fetch completes, consuming the completion
    notifications of the URLs completed in the meantime.

    Args:
      url: The URL to wait for - it must have been enqueued.

    Returns:
      The work result of the fetch: the fetched content, or an error
      object if the fetch failed.

    Raises:
      AssertionError: If the URL has already been waited for - i.e. it is
        not pending anymore - and its work result is not found.
    """
    wpath = wres.work_path(self._path, url)
    if not os.path.isfile(wpath):
      tas.check(url in self._pending, msg=f'URL already retired: {url}')

      while self._pending:
        rurl = self._rqueue.get()
        self._pending.discard(rurl)
        if rurl == url:
          break

    try:
      return wres.get_work(wpath)
    finally:
      os.remove(wpath)

  def iter_results(self, max_results=None, block=True, timeout=None):
    """Iterates over the results of the completed fetches.

    Args:
      max_results: An optional maximum number of results to yield.
      block: Whether to block waiting for the next result.
      timeout: An optional maximum number of seconds to block.

    Yields:
      (url, data) tuples - one per completed fetch - where data holds
      either the fetched content or an error object if the fetch failed.

    Raises:
      AssertionError: If block is False and no completed fetch is
        available.
    """
    count = 0
    while self._pending:
      try:
        rurl = self._rqueue.get(block=block, timeout=timeout)

        self._pending.discard(rurl)
        wpath = wres.work_path(self._path, rurl)

        wdata = wres.load_work(wpath)

        os.remove(wpath)

        yield rurl, wdata
      except queue.Empty:
        break

      count += 1
      if max_results is not None and count >= max_results:
        break

  def __enter__(self):
    """Starts the fetcher workers."""

    self.start()

    return self

  def __exit__(self, *exc):
    """Shuts down the fetcher workers."""

    self.shutdown()

    return False
