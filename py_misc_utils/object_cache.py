"""Object cache with TTL-based eviction and automatic cleanup.

This module provides:
- Cache: Thread-safe object pool with time-based expiration
- Handler: Abstract base class for custom object lifecycle management
- cache(): Global singleton cache instance

Objects are automatically returned to the cache when the FinWrapper is
closed or garbage collected.
"""

import abc
import collections
import functools
import os
import threading
import time

from . import alog
from . import fin_wrap as fw
from . import periodic_task as ptsk

_Entry = collections.namedtuple('Entry', 'name, obj, handler, time')


class Handler(abc.ABC):
  """Abstract base class for defining object lifecycle in the cache.

  Subclasses implement create(), and optionally max_age(), is_alive(),
  and close() for custom behavior.
  """

  @abc.abstractmethod
  def create(self):
    """Create a new object instance.

    Called when no suitable cached object is available.

    Returns:
      A new object instance.
    """
    ...

  def max_age(self):
    """Maximum age of cached objects in seconds before eviction.

    Returns:
      Maximum age in seconds (default: 60).
    """
    return 60

  def is_alive(self, obj):
    """Check if a cached object is still valid.

    Called when retrieving an object from the cache.

    Args:
      obj: Cached object to check.

    Returns:
      True if object is valid, False if it should be discarded.
    """
    return True

  def close(self, obj):
    """Clean up an object when evicted or discarded.

    Args:
      obj: Object to clean up.
    """
    pass


class Cache:
  """Thread-safe object cache with TTL-based eviction.

  Maintains pools of objects by name, with a background cleaner that
  periodically removes expired objects.
  """

  def __init__(self, clean_timeo=None):
    """Initialize the cache.

    Args:
      clean_timeo: Cleanup interval in seconds (default: from
        CACHE_CLEAN_TIMEO env var or 8).
    """
    self._lock = threading.Lock()
    self._cond = threading.Condition(lock=self._lock)
    self._cache = collections.defaultdict(collections.deque)
    self._cleaner = ptsk.PeriodicTask(
        'CacheCleaner',
        self._try_cleanup,
        clean_timeo or int(os.getenv('CACHE_CLEAN_TIMEO', 8)),
        stop_on_error=False,
    )
    self._cleaner.start()

  def _try_cleanup(self):
    """Periodic cleanup task: remove expired objects from all pools."""
    alog.verbose(f'Object cache cleanup running')
    cleans = []
    with self._lock:
      new_cache = collections.defaultdict(collections.deque)
      for name, cqueue in self._cache.items():
        for entry in cqueue:
          age = time.time() - entry.time
          if age > entry.handler.max_age():
            cleans.append(entry)
          else:
            new_cache[name].append(entry)

      self._cache = new_cache

    for entry in cleans:
      alog.debug(f'Cache Clean: name={entry.name} obj={entry.obj}')
      entry.handler.close(entry.obj)

  def shutdown(self):
    """Stop the background cleaner."""
    self._cleaner.stop()

  def _release(self, name, handler, obj):
    """Return an object to the cache pool.

    Args:
      name: Cache pool name.
      handler: Handler instance for the object.
      obj: Object to return to the pool.
    """
    alog.debug(f'Cache Release: name={name} obj={obj}')
    with self._lock:
      self._cache[name].append(
          _Entry(name=name, obj=obj, handler=handler, time=time.time()))

  def get(self, name, handler):
    """Get an object from the cache or create a new one.

    Returns a FinWrapper that automatically returns the object to the
    cache when closed or garbage collected.

    Args:
      name: Cache pool name.
      handler: Handler instance defining object lifecycle.

    Returns:
      FinWrapper containing the object.
    """
    with self._lock:
      cqueue, obj = self._cache[name], None
      if cqueue:
        entry = cqueue.popleft()
        if not entry.handler.is_alive(entry.obj):
          entry.handler.close(obj)
          obj = None
        else:
          obj = entry.obj
          alog.debug(f'Cache Hit: name={name} obj={obj}')

      if obj is None:
        obj = handler.create()

      finfn = functools.partial(self._release, name, handler, obj)

      return fw.FinWrapper(obj, finfn)


_CACHE = Cache()


def cache():
  """Get the global singleton cache instance.

  Returns:
    The global Cache instance.
  """
  return _CACHE
