"""Tests for py_misc_utils.data_cache.

Expectations pinned to the observed behavior: DataCache loads cached
pickled data at creation if not expired; data() returns current data;
store() replaces it and pickles back at context exit only if changed;
cache folder lives under gfs.cache_dir()/data_cache.
"""

import hashlib
import os

from py_misc_utils import data_cache as dc
from py_misc_utils import gfs as gfs_mod


def _data_path(data_id):
  fsid = hashlib.sha1(data_id.encode()).hexdigest()
  return os.path.join(gfs_mod.cache_dir(), 'data_cache', fsid)


def test_data_cache_store_and_persist():
  """Stored data is pickled back at exit if changed."""
  data_id = 'test_store_persist_67890'
  # Ensure clean slate.
  path = _data_path(data_id)
  if os.path.exists(path):
    os.remove(path)

  with dc.DataCache(data_id) as cache:
    assert cache.data() is None
    cache.store({'key': [1, 2, 3]})

  # Reload - data should be persisted.
  with dc.DataCache(data_id) as cache:
    assert cache.data() == {'key': [1, 2, 3]}


def test_data_cache_unchanged_not_persisted():
  """Unchanged data is not rewritten at exit."""
  data_id = 'test_unchanged_not_persisted'
  path = _data_path(data_id)
  if os.path.exists(path):
    os.remove(path)

  with dc.DataCache(data_id) as cache:
    cache.store({'a': 1})

  mtime1 = os.path.getmtime(path)

  with dc.DataCache(data_id) as cache:
    assert cache.data() == {'a': 1}
    cache.store(cache.data())

  mtime2 = os.path.getmtime(path)
  assert mtime2 == mtime1


def test_data_cache_none_never_written():
  """Storing None does not write anything at exit."""
  data_id = 'test_none_never_written'
  path = _data_path(data_id)
  if os.path.exists(path):
    os.remove(path)

  with dc.DataCache(data_id) as cache:
    assert cache.data() is None
    cache.store(None)

  assert not os.path.exists(path)


def test_data_cache_loads_existing():
  """DataCache loads previously persisted data."""
  data_id = 'test_loads_existing'
  path = _data_path(data_id)
  if os.path.exists(path):
    os.remove(path)

  with dc.DataCache(data_id) as cache:
    cache.store({'preloaded': True})

  with dc.DataCache(data_id) as cache:
    assert cache.data() == {'preloaded': True}
