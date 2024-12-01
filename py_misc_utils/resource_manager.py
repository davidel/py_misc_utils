import collections
import functools
import logging
import multiprocessing
import multiprocessing.managers as mpmgr
import os
import queue
import tempfile
import threading
import time
import weakref

from . import alog
from . import daemon_process as dp


class ResourceManager:

  def __init__(self):
    self._lock = threading.Lock()
    self._resources = collections.defaultdict(dict)

  def get(self, cls, ctor, name, *args, **kwargs):
    alog.debug(f'Get resource {cls}.{name}')
    with self._lock:
      cdict = self._resources[cls]
      res = cdict.get(name)
      if res is not None:
        res = res()
      if res is None:
        alog.debug(f'Creating resource {cls}.{name}')
        res = ctor(*args, **kwargs)
        cdict[name] = weakref.ref(res)

      return res

  def delete(self, cls, name):
    alog.debug(f'Remove resource {cls}.{name}')
    with self._lock:
      cdict = self._resources[cls]
      cdict.pop(name, None)


_RESMGR = ResourceManager()

def get_resource_manager():
  return _RESMGR


def create_manager(*args, register_fn=None, **kwargs):
  # https://github.com/python/cpython/blob/2f56c68dec97002fdd8563a0e4977b75eb191ab9/Lib/multiprocessing/managers.py#L1043
  # https://github.com/python/cpython/blob/4cba0e66c29b46afbb1eee1d0428f5a2f5b891bb/Lib/multiprocessing/managers.py#L189
  manager = mpmgr.SyncManager(*args, **kwargs)

  resmgr = get_resource_manager()

  manager.register('get_lock',
                   functools.partial(resmgr.get, 'LOCKS', threading.Lock),
                   mpmgr.AcquirerProxy)
  manager.register('rm_lock',
                   functools.partial(resmgr.delete, 'LOCKS'))

  manager.register('get_event',
                   functools.partial(resmgr.get, 'EVENTS', threading.Event),
                   mpmgr.EventProxy)
  manager.register('rm_event',
                   functools.partial(resmgr.delete, 'EVENTS'))

  manager.register('get_condition',
                   functools.partial(resmgr.get, 'CONDITIONS', threading.Condition),
                   mpmgr.ConditionProxy)
  manager.register('rm_condition',
                   functools.partial(resmgr.delete, 'CONDITIONS'))

  manager.register('get_barrier',
                   functools.partial(resmgr.get, 'BARRIERS', threading.Barrier),
                   mpmgr.BarrierProxy)
  manager.register('rm_barrier',
                   functools.partial(resmgr.delete, 'BARRIERS'))

  manager.register('get_queue',
                   functools.partial(resmgr.get, 'QUEUES', queue.Queue))
  manager.register('rm_queue',
                   functools.partial(resmgr.delete, 'QUEUES'))

  manager.register('get_lifo',
                   functools.partial(resmgr.get, 'LIFOS', queue.LifoQueue))
  manager.register('rm_lifo',
                   functools.partial(resmgr.delete, 'LIFOS'))

  manager.register('get_namespace',
                   functools.partial(resmgr.get, 'NAMESPACES', mpmgr.Namespace),
                   mpmgr.NamespaceProxy)
  manager.register('rm_namespace',
                   functools.partial(resmgr.delete, 'NAMESPACES'))

  if register_fn is not None:
    register_fn(manager, resmgr)

  return manager


def _get_logdir():
  logdir = os.path.join(tempfile.gettempdir(), 'log')
  os.makedirs(logdir, exist_ok=True)

  return logdir


def _server_runner(name, *args, **kwargs):
  alog.basic_setup(log_level=os.getenv('RESMGR_LOG_LEVEL', 'INFO'),
                   log_file=os.path.join(_get_logdir(), f'{name}.log'))

  alog.info(f'[{name}] server starting')
  manager = create_manager(*args, **kwargs)

  try:
    server = manager.get_server()
    server.serve_forever()
  except Exception as ex:
    alog.error(f'[{name}] server start failed: {ex}')
  finally:
    alog.info(f'[{name}] server gone!')


def get_manager(name, *args, **kwargs):
  daemon = dp.Daemon(name)
  while daemon.getpid() is None:
    alog.info(f'[{name}] Starting server daemon')
    try:
      daemon.start(functools.partial(_server_runner, name, *args, **kwargs))
    except FileExistsError as ex:
      pass
    time.sleep(0.5)

  alog.info(f'[{name}] Connecting to server')

  manager = create_manager(*args, **kwargs)
  while True:
    try:
      manager.connect()
      alog.info(f'[{name}] Connected to the manager')
      break
    except Exception as ex:
      alog.debug(f'[{name}] Connection failed, retrying ...: {ex}')
      time.sleep(0.5)

  return manager


def stop_manager(name):
  daemon = dp.Daemon(name)

  return daemon.stop()

