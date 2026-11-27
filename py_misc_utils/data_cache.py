"""Pickled data caching.

This module provides DataCache, a context manager caching a pickled data
object - identified by a data id - in the gfs cache folder, so that the
computation producing the data can be skipped if a not-too-old cached
copy exists.
"""
import hashlib
import os
import pickle
import time

from . import gfs

_MAX_AGE = int(os.getenv('DATACACHE_MAX_AGE', str(3600 * 24)))
# The default maximum age - in seconds - of a cached data object, set
# via the DATACACHE_MAX_AGE environment variable.


class DataCache:
  """A pickled data object cache.

  A DataCache loads - at creation time - the cached data object held in
  the gfs cache folder, if one exists and is not older than the maximum
  age. Within the context the data can be replaced via store(): at exit,
  if the data has been changed, the updated copy is pickled back, so
  that the next run finds it and skips the computation.

  Example:
    from py_misc_utils import data_cache as dc

    with dc.DataCache('daily-stats') as cache:
      data = cache.data()

      if data is None:
        # Missing or too old: recompute and store the new value.
        data = {'visits': 42}
        cache.store(data)

      print(data)  # {'visits': 42}
  """

  def __init__(self, data_id, max_age=_MAX_AGE):
    """Creates a data cache.

    Args:
      data_id: The id uniquely identifying the cached data.
      max_age: The maximum age - in seconds - of the cached data to be
        considered valid. If None, the cached data never expires.
    """
    fsid = hashlib.sha1(data_id.encode()).hexdigest()

    cache_path = os.path.join(gfs.cache_dir(), 'data_cache')
    gfs.makedirs(cache_path, exist_ok=True)

    self._data_path = os.path.join(cache_path, fsid)
    self._orig_data = None
    try:
      sres = gfs.stat(self._data_path)
      if max_age is None or (sres.st_ctime + max_age) > time.time():
        with gfs.open(self._data_path, mode='rb') as fd:
          self._orig_data = pickle.load(fd)
    # Pickle payloads can raise nearly anything when stale/corrupt: treat
    # every load failure as a cache miss - the source data is the fallback.
    except:  # noqa: E722, S110
      pass

    self._data = self._orig_data

  def data(self):
    """Returns the current data object - the cached one, if any.

    Returns:
      The cached data object, or None if no valid cached copy exists.
    """
    return self._data

  def store(self, data):
    """Stores a new data object, to be pickled at context exit.

    The data is written back only if it differs - by identity - from the
    object loaded at creation time.

    Args:
      data: The data object to store.

    Example:
      from py_misc_utils import data_cache as dc

      with dc.DataCache('user-prefs') as cache:
        cache.store({'theme': 'dark'})
    """
    self._data = data

  def __enter__(self):
    """Enters the data cache context."""

    return self

  def __exit__(self, *exc):
    """Exits the data cache context, pickling back changed data."""
    if self._data is not None and self._data is not self._orig_data:
      with gfs.open(self._data_path, mode='wb') as fd:
        pickle.dump(self._data, fd)

    return False
