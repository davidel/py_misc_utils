"""Tests for py_misc_utils.resource_manager.

Public-interface coverage of the ResourceManager caching semantics. The
manager/daemon plumbing (get_manager, stop_manager) is excluded: it is
implemented over private _create_manager() machinery and its daemon
round-trip is currently broken (the client manager is created without
the daemon address/authkey, so connect() can never succeed - reported
as a library bug, not fixed here).
"""

import queue
import threading
import weakref

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
