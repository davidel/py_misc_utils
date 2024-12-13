import hashlib
import os
import pickle
import queue
import threading

from . import alog as alog
from . import assert_checks as tas
from . import file_overwrite as fow
from . import gfs as gfs
from . import tempdir as tmpd
from . import utils as ut
from . import weak_call as wcall


def url_path(path, url, dirlen=2):
  uhash = hashlib.sha1(url.encode()).hexdigest()
  udir = os.path.join(path, uhash[-dirlen:])
  os.makedirs(udir, exist_ok=True)

  return os.path.join(udir, uhash)


_ERROR_TAG = b'#!$ERROR\n\n'

def make_error(msg):
  return _ERROR_TAG + msg


def write_error(path, **kwargs):
  with fow.FileOverwrite(path, mode='wb') as fd:
    fd.write(make_error(pickle.dumps(kwargs)))


def get_error(data):
  emask = data[: len(_ERROR_TAG)]
  if emask == _ERROR_TAG:
    return pickle.loads(data[len(emask):])


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

    upath = url_path(path, url)

    alog.verbose(f'Fetching "{url}"')
    try:
      fs, fpath = resolve_url(fss, url, fs_kwargs)

      with fow.FileOverwrite(upath, mode='wb') as fd:
        for data in fs.get_file(fpath):
          fd.write(data)
    except Exception as ex:
      write_error(upath, url=url, exception=ex)
    finally:
      rqueue.put(url)


class UrlFetcher:

  def __init__(self, path=None, num_workers=None, fs_kwargs=None):
    fs_kwargs = fs_kwargs or dict()
    fs_kwargs = ut.dict_setmissing(
      fs_kwargs,
      timeout=ut.getenv('FETCHER_TIMEO', dtype=float, defval=10.0),
    )

    self._ctor_path = self._path = path
    self._num_workers = num_workers or max(os.cpu_count() * 4, 128)
    self._fs_kwargs = fs_kwargs
    self._uqueue = self._rqueue = None
    self._workers = []

  def start(self):
    self._path = self._ctor_path or tmpd.create()
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

    if self._ctor_path != self._path:
      gfs.rmtree(self._path, ignore_errors=True)
      self._path = self._ctor_path

  def enqueue(self, *urls):
    for url in urls:
      if url:
        self._uqueue.put(url)

  def _get(self, url, upath):
    with open(upath, mode='rb') as fd:
      data = fd.read()

    error = get_error(data)
    if error is not None:
      raise error['exception']

    return data

  def try_get(self, url):
    upath = url_path(self._path, url)

    return self._get(url, upath) if os.path.isfile(upath) else None

  def wait(self, url):
    upath = url_path(self._path, url)
    if not os.path.isfile(upath):
      while True:
        rurl = self._rqueue.get()
        if rurl == url:
          break

    return self._get(url, upath)

  def __enter__(self):
    self.start()

    return self

  def __exit__(self, *exc):
    self.shutdown()

    return False

