"""Tests for py_misc_utils.resource_manager.

Public-interface coverage of the ResourceManager caching semantics plus
the manager/daemon plumbing (get_manager, stop_manager) over the
encrypted status-directory rendezvous.
"""

import os
import queue
import subprocess
import sys
import threading
import weakref

import pytest

from py_misc_utils import daemon_process
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


def test_crypt_round_trip():
  key = rm._crypt_key(b'test-authkey', 'test-resmgr')
  status = {
      'ok': True,
      'pid': 1234,
      'address': '/tmp/pymp-x/listener',
      'family': 'AF_UNIX'
  }

  blob = rm.crypt_object(status, key)
  assert rm.decrypt_object(blob, key) == status

  # Wrong key and tampered blobs must not validate.
  assert rm.decrypt_object(blob, rm._crypt_key(b'other', 'test-resmgr')) is None

  tampered = bytearray(blob)
  tampered[20] ^= 0x01
  assert rm.decrypt_object(bytes(tampered), key) is None


def _daemon_mode_is_compat():
  import multiprocessing.process

  return hasattr(multiprocessing.process, '_children')


@pytest.mark.skipif(not _daemon_mode_is_compat(), reason='DaemonCompat unavailable')
def test_get_manager_round_trip_compat(monkeypatch):
  # The in-process test always runs the ambient default Daemon alias:
  # pin the environment to the DaemonCompat mode and guard that the
  # alias indeed resolved to it, or the round-trip would silently test
  # the posix path instead.
  monkeypatch.delenv('DAEMON_MODE', raising=False)
  assert daemon_process.Daemon is daemon_process.DaemonCompat

  name = 'pytest-resmgr-compat'
  authkey = b'pytest-resmgr-authkey'

  m1 = rm.get_manager(name, authkey=authkey)
  try:
    lk = m1.get_lock('lk1')
    lk.acquire()
    lk.release()

    # Late attachers discover the address through the status directory.
    m2 = rm.get_manager(name, authkey=authkey)
    assert m2._address == m1._address
  finally:
    rm.stop_manager(name)

  assert rm.get_status_dirs(name) == []


@pytest.mark.skipif(not _daemon_mode_is_compat(), reason='DaemonCompat unavailable')
def test_get_manager_wrong_authkey_attaching():
  name = 'pytest-resmgr-wrongkey'
  authkey = b'pytest-resmgr-authkey'

  rm.get_manager(name, authkey=authkey)
  try:
    with pytest.raises(PermissionError):
      rm.get_manager(name, authkey=b'different-authkey')
  finally:
    rm.stop_manager(name)

  assert rm.get_status_dirs(name) == []


def test_get_manager_round_trip_posix():
  """Round-trip under DAEMON_MODE=posix (double-fork)."""
  if not _daemon_mode_is_compat():
    pytest.skip('DAEMON_MODE=posix requires DaemonCompat default')

  name = 'pytest-resmgr-posix'
  authkey = b'pytest-resmgr-authkey'

  code = f'''
import sys
sys.path.insert(0, {os.getcwd()!r})

import py_misc_utils.resource_manager as rm

name = {name!r}
authkey = {authkey!r}

m1 = rm.get_manager(name, authkey=authkey)
lk = m1.get_lock('lk1')
lk.acquire()
lk.release()

m2 = rm.get_manager(name, authkey=authkey)
if m2._address != m1._address:
    raise AssertionError('attach address mismatch')

if not rm.stop_manager(name):
    raise AssertionError('stop_manager failed')
if rm.get_status_dirs(name):
    raise AssertionError('status dirs left behind')
'''

  env = dict(os.environ)
  env['DAEMON_MODE'] = 'posix'
  env['PYTHONPATH'] = os.getcwd()
  res = subprocess.run([sys.executable, '-c', code],
                       check=False,
                       env=env,
                       capture_output=True,
                       text=True,
                       timeout=180)

  assert res.returncode == 0, f'posix round-trip failed:\n{res.stdout}\n{res.stderr}'
  assert rm.get_status_dirs(name) == []


def test_stop_manager_idempotent():
  name = 'pytest-resmgr-stop'
  assert rm.stop_manager(name) is False
  assert rm.get_status_dirs(name) == []
