"""Tests for py_misc_utils.resource_manager.

In-process coverage of ResourceManager caching semantics and handler
registration. The server/daemon paths are exercised with a stubbed Daemon
class - no daemon_process spawning (excluded by agreement).
"""

import queue
import threading
import weakref

import pytest

from py_misc_utils import resource_manager as rm


def test_get_creates_and_caches():
  m = rm.ResourceManager()
  r1 = m.get('K1', threading.Lock, 'n1')
  r2 = m.get('K1', threading.Lock, 'n1')
  assert r1 is r2


def test_get_different_names():
  m = rm.ResourceManager()
  r1 = m.get('K1', threading.Lock, 'a')
  r2 = m.get('K1', threading.Lock, 'b')
  assert r1 is not r2


def test_get_with_ctor_args():
  m = rm.ResourceManager()
  r = m.get('K1', queue.Queue, 'q', 5)
  assert r.maxsize == 5


def test_weakref_recreation_after_gc():
  # NOTE: relies on the ResourceManager caching resources via weakrefs -
  # once the last live reference is dropped the slot is released and the
  # next get() creates a fresh instance. To make the GC observable (CPython
  # may recycle the same memory slot), the dead weakref (None) is checked
  # instead of object identity.
  class Res:
    pass

  m = rm.ResourceManager()
  r1 = m.get('K1', Res, 'obj')
  wr = weakref.ref(r1)
  del r1
  r2 = m.get('K1', Res, 'obj')
  assert wr() is None
  assert isinstance(r2, Res)
  # Cached again after recreation.
  assert m.get('K1', Res, 'obj') is r2


def test_delete_removes_resource():
  m = rm.ResourceManager()
  r = m.get('K1', threading.Lock, 'gone')
  m.delete('K1', 'gone')
  r2 = m.get('K1', threading.Lock, 'gone')
  assert r2 is not r


def test_create_manager_registers_handlers():
  rm._create_manager()
  # BaseManager.register is a classmethod: handlers end up on the
  # SyncManager class (an unstarted manager cannot invoke them yet).
  for name in [
      'get_lock', 'rm_lock', 'get_event', 'rm_event', 'get_condition', 'rm_condition',
      'get_barrier', 'rm_barrier', 'get_queue', 'rm_queue', 'get_lifo', 'rm_lifo',
      'get_namespace', 'rm_namespace'
  ]:
    assert hasattr(rm.mpmgr.SyncManager, name), name


@pytest.mark.slow
def test_get_and_delete_via_started_manager():
  manager = rm._create_manager()
  manager.start()
  try:
    lk = manager.get_lock('shared-lock')
    # The AcquirerProxy exposes the lock interface.
    assert hasattr(lk, 'acquire')
    assert lk.acquire(False) is True
    assert lk.release() is None
    manager.rm_lock('shared-lock')
  finally:
    manager.shutdown()


def test_get_manager_connect_retry(monkeypatch):
  # Stub the Daemon class: fail connect once, then succeed.
  attempts = []

  class FakeDaemon:

    def __init__(self, name):
      self.name = name
      self.started = False

    def getpid(self):
      # After the first start() the daemon is (fake) running.
      return 4242 if self.started else None

    def start(self, runner):
      self.started = True

    def stop(self):
      return True

  class FakeManager:
    pass

  fake_manager = FakeManager()
  fake_manager.connect = lambda: (_lambda_connect(attempts, fake_manager))

  def _lambda_connect(attempts, mgr):
    attempts.append(1)
    if len(attempts) == 1:
      raise ConnectionError('first attempt fails')
    return 'connected'

  monkeypatch.setattr(rm, 'dp', _FakeDPModule(FakeDaemon))
  monkeypatch.setattr(rm, '_create_manager', lambda *a, **k: fake_manager)
  mgr = rm.get_manager('fake-resmgr')
  assert mgr is fake_manager
  assert len(attempts) == 2


class _FakeDPModule:

  def __init__(self, daemon_cls):
    self.Daemon = daemon_cls


def test_stop_manager(monkeypatch):
  stopped = []

  class FakeDaemon:

    def __init__(self, name):
      pass

    def stop(self):
      stopped.append(True)
      return True

  monkeypatch.setattr(rm, 'dp', _FakeDPModule(FakeDaemon))
  assert rm.stop_manager('fake-resmgr') is True
  assert stopped == [True]
