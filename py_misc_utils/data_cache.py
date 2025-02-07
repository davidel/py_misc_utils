import hashlib
import os
import pickle
import time

from . import gfs


class DataCache:

  def __init__(self, data_id, cache_dir=None, max_age=None):
    cache_dir = gfs.cache_dir(path=cache_dir)

    fsid = hashlib.sha1(data_id.encode()).hexdigest()

    cache_path = os.path.join(cache_dir, 'data_cache')
    gfs.makedirs(cache_path, exist_ok=True)

    self._data_path = os.path.join(cache_path, fsid)
    try:
      sres = gfs.stat(self._data_path)
      if max_age is None or time.time() > sres.st_ctime + max_age:
        with gfs.open(self._data_path, mode='rb') as fd:
          self._orig_data = pickle.load(fd)

        self._data = self._orig_data
    except:
      self._data = self._orig_data = None

  def data(self):
    return self._data

  def store(self, data):
    self._data = data

  def __enter__(self):
    return self

  def __exit__(self, *exc):
    if self._data is not None and self._data is not self._orig_data:
      with gfs.open(self._data_path, mode='wb') as fd:
        pickle.dump(self._data, fd)

    return False

