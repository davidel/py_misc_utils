import os
import queue
import threading

from . import alog as alog
from . import assert_checks as tas
from . import file_overwrite as fow
from . import gfs as gfs
from . import tempdir as tmpd
from . import utils as ut
from . import work_results as wres


def resolve_url(fss, url, fs_kwargs):
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
  fss = dict()
  while True:
    url = uqueue.get()
    if not url:
      break

    wpath = wres.work_path(path, url)

    alog.verbose(f'Fetching "{url}"')
    try:
      fs, fpath = resolve_url(fss, url, fs_kwargs)

      with fow.FileOverwrite(wpath, mode='wb') as fd:
        for data in fs.get_file(fpath):
          fd.write(data)
    except Exception as ex:
      wres.write_error(wpath, ex, url=url)
    finally:
      rqueue.put(url)


class UrlFetcher:

  def __init__(self, path=None, num_workers=None, fs_kwargs=None):
    fs_kwargs = fs_kwargs or dict()
    fs_kwargs = ut.dict_setmissing(
      fs_kwargs,
      timeout=ut.getenv('FETCHER_TIMEO', dtype=float, defval=10.0),
    )

    self._path, self._tmp_path = path, None
    self._num_workers = num_workers or max(os.cpu_count() * 4, 128)
    self._fs_kwargs = fs_kwargs
    self._uqueue = self._rqueue = None
    self._workers = []

  def start(self):
    if self._path is None:
      self._tmp_path = tmpd.fastfs_dir()
      self._path = self._tmp_path

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
    alog.verbose(f'Sending shutdowns down the queue')
    for _ in range(len(self._workers)):
      self._uqueue.put('')

    alog.verbose(f'Joining fetcher workers')
    for worker in self._workers:
      worker.join()

    self._uqueue = self._rqueue = None
    self._workers = []

    if self._tmp_path is not None:
      gfs.rmtree(self._tmp_path, ignore_errors=True)

  def enqueue(self, *urls):
    for url in urls:
      if url:
        self._uqueue.put(url)

  def try_get(self, url):
    return wres.tryget_work(self._path, url)

  def wait(self, url):
    wpath = wres.work_path(self._path, url)
    if not os.path.isfile(wpath):
      while True:
        rurl = self._rqueue.get()
        if rurl == url:
          break

    return wres.get_work(wpath)

  def __enter__(self):
    self.start()

    return self

  def __exit__(self, *exc):
    self.shutdown()

    return False

