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

from . import alog
from . import daemon_process as dp


_LOCK = threading.Lock()
_RESOURCES = collections.defaultdict(dict)

def get_resource(cls, ctor, name, *args, **kwargs):
  alog.debug(f'Get resource {cls}.{name}')
  with _LOCK:
    cdict = _RESOURCES[cls]
    res = cdict.get(name)
    if res is None:
      res = ctor(*args, **kwargs)
      cdict[name] = res

    return res


def rm_resource(cls, name):
  alog.debug(f'Remove resource {cls}.{name}')
  with _LOCK:
    cdict = _RESOURCES[cls]
    cdict.pop(name, None)


def create_manager(*args, register_fn=None, **kwargs):
  # https://github.com/python/cpython/blob/2f56c68dec97002fdd8563a0e4977b75eb191ab9/Lib/multiprocessing/managers.py#L1043
  manager = mpmgr.SyncManager(*args, **kwargs)

  manager.register('get_lock',
                   functools.partial(get_resource, 'LOCKS', threading.Lock),
                   mpmgr.AcquirerProxy)
  manager.register('rm_lock',
                   functools.partial(rm_resource, 'LOCKS'))

  manager.register('get_event',
                   functools.partial(get_resource, 'EVENTS', threading.Event),
                   mpmgr.EventProxy)
  manager.register('rm_event',
                   functools.partial(rm_resource, 'EVENTS'))

  manager.register('get_condition',
                   functools.partial(get_resource, 'CONDITIONS', threading.Condition),
                   mpmgr.ConditionProxy)
  manager.register('rm_condition',
                   functools.partial(rm_resource, 'CONDITIONS'))

  manager.register('get_barrier',
                   functools.partial(get_resource, 'BARRIERS', threading.Barrier),
                   mpmgr.BarrierProxy)
  manager.register('rm_barrier',
                   functools.partial(rm_resource, 'BARRIERS'))

  manager.register('get_queue',
                   functools.partial(get_resource, 'QUEUES', queue.Queue))
  manager.register('rm_queue',
                   functools.partial(rm_resource, 'QUEUES'))

  if register_fn is not None:
    register_fn(manager)

  return manager


def _get_logdir():
  logdir = os.path.join(tempfile.gettempdir(), 'log')
  os.makedirs(logdir, exist_ok=True)

  return logdir


def _server_runner(name, *args, **kwargs):
  alog.basic_setup(log_file=os.path.join(_get_logdir(), f'{name}.log'))

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

