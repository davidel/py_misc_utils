import abc
import collections
import functools
import os
import threading
import time

from . import alog as alog
from . import fin_wrap as fw
from . import periodic_task as ptsk


_Entry = collections.namedtuple('Entry', 'name, obj, handler, time')


class Handler(abc.ABC):

  @abc.abstractmethod
  def create(self):
    ...

  @abc.abstractmethod
  def is_alive(self, obj):
    ...

  @abc.abstractmethod
  def close(self, obj):
    ...

  @abc.abstractmethod
  def max_age(self):
    ...


class Cache:

  def __init__(self, clean_timeo=None):
    self._lock = threading.Lock()
    self._cond = threading.Condition(lock=self._lock)
    self._cache = collections.defaultdict(collections.deque)
    self._cleaner = ptsk.PeriodicTask(
      'CacheCleaner',
      self._try_cleanup,
      clean_timeo or int(os.getenv('CACHE_CLEAN_TIMEO', 2)),
      stop_on_error=False,
    )
    self._cleaner.start()

  def _try_cleanup(self):
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
    self._cleaner.stop()

  def _release(self, name, handler, obj):
    alog.debug(f'Cache Release: name={name} obj={obj}')
    with self._lock:
      self._cache[name].append(_Entry(name=name,
                                      obj=obj,
                                      handler=handler,
                                      time=time.time()))

  def get(self, name, handler):
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
  return _CACHE

