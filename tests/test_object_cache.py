"""Tests for py_misc_utils.object_cache.

Exercises Cache get/set/close, TTL expiry, and Handler lifecycle.
"""

import gc
import time

import pytest

from py_misc_utils import object_cache as oc


class _CountingHandler(oc.Handler):

  def __init__(self):
    self.created = 0
    self.closed = 0

  def create(self):
    self.created += 1
    return {'value': self.created}

  def is_alive(self, obj):
    return True

  def close(self, obj):
    self.closed += 1

  def max_age(self):
    return 0.02


def test_cache_get_creates_and_returns():
  handler = _CountingHandler()
  cache = oc.Cache()
  cache.get('key1', handler)
  assert handler.created == 1


def test_cache_get_returns_cached():
  handler = _CountingHandler()
  cache = oc.Cache()
  cache.get('key1', handler)
  cache.get('key1', handler)
  assert handler.created == 1


def test_cache_release_returns_to_pool():
  handler = _CountingHandler()
  cache = oc.Cache()
  fw = cache.get('key1', handler)
  del fw
  gc.collect()
  # Object returned to pool, not closed yet.
  assert handler.closed == 0


def test_cache_ttl_expiry():
  handler = _CountingHandler()
  cache = oc.Cache(clean_timeo=0.01)
  fw = cache.get('key1', handler)
  del fw
  gc.collect()
  # Object returned to pool with current timestamp; the background
  # cleaner expires it after max_age.
  time.sleep(0.15)  # past clean_timeo + max_age of 0.02s
  cache.get('key1', handler)
  assert handler.created == 2
  assert handler.closed == 1
  cache.shutdown()


def test_global_cache_singleton():
  c1 = oc.cache()
  c2 = oc.cache()
  assert c1 is c2


def test_cache_shutdown():
  handler = _CountingHandler()
  cache = oc.Cache()
  cache.get('key1', handler)
  cache.shutdown()
