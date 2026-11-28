"""Resource manager for shared multiprocessing synchronization primitives.

This module provides a ResourceManager class and multiprocessing manager
integration for sharing locks, events, queues, and other synchronization
objects across processes.
"""

import collections
import errno
import functools
import glob
import hashlib
import hmac
import multiprocessing.managers as mpmgr
import os
import pickle
import queue
import shutil
import tempfile
import threading
import time
import weakref

import psutil

from . import alog
from . import daemon_process as dp
from . import fs_utils as fsu
from . import osfd


class ResourceManager:
  """Thread-safe resource manager with weak reference tracking.

  Manages named resources by class, creating them on demand and
  automatically cleaning up when no longer referenced.

  Example:
    import threading

    from py_misc_utils import resource_manager as rm

    m = rm.ResourceManager()
    lk = m.get('LOCKS', threading.Lock, 'job1')

    print(lk.acquire())  # True
    lk.release()

    m.delete('LOCKS', 'job1')
    print(m.get('LOCKS', threading.Lock, 'job1') is lk)  # False
  """

  def __init__(self):
    """Initialize an empty resource manager."""
    self._lock = threading.Lock()
    self._resources = collections.defaultdict(dict)

  def get(self, cls, ctor, name, *args, **kwargs):
    """Get or create a named resource.

    Args:
      cls: Resource class/category name (e.g., 'LOCKS', 'EVENTS').
      ctor: Callable to create the resource if needed.
      name: Resource name.
      *args: Positional arguments passed to ctor.
      **kwargs: Keyword arguments passed to ctor.

    Returns:
      The resource instance (existing or newly created).

    Example:
      import threading

      from py_misc_utils import resource_manager as rm

      m = rm.ResourceManager()
      lk = m.get('LOCKS', threading.Lock, 'job1')

      print(type(lk).__name__)  # lock
    """
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
    """Remove a named resource from the manager.

    Args:
      cls: Resource class/category name.
      name: Resource name to remove.

    Example:
      import threading

      from py_misc_utils import resource_manager as rm

      m = rm.ResourceManager()
      lk = m.get('LOCKS', threading.Lock, 'job2')

      m.delete('LOCKS', 'job2')
      print(m.get('LOCKS', threading.Lock, 'job2') is lk)  # False
    """
    alog.debug(f'Remove resource {cls}.{name}')
    with self._lock:
      cdict = self._resources[cls]
      cdict.pop(name, None)


_RESMGR = ResourceManager()


def get_resource_manager():
  """Get the global singleton resource manager.

  Returns:
    The global ResourceManager instance.

  Example:
    import threading

    from py_misc_utils import resource_manager as rm

    resmgr = rm.get_resource_manager()
    lk = resmgr.get('LOCKS', threading.Lock, 'shared')

    print(type(lk).__name__)  # lock
  """
  return _RESMGR


_RESMGR_STATUS_PREFIX = '.resmgr_'
_RESMGR_STATUS_DEADLINE = 60.0
_RESMGR_STATUS_POLL = 0.5


def _crypt_key(authkey, name):
  """Derive the status-file crypto key from the authkey and the name."""
  return hashlib.sha512(authkey + b':' + name.encode()).digest()


def _crypt_seed(key, nonce):
  """Derive the cipher seed and the HMAC key out of the file key and nonce."""
  sres = hashlib.sha512(key + nonce).digest()

  return sres[:32], sres[32:]


def _crypt_xor(data, seed):
  """XOR data with a SHA512-based counter-mode keystream."""
  nblocks = (len(data) + 63) // 64
  stream = b''.join(
      hashlib.sha512(seed + counter.to_bytes(4, 'big')).digest()
      for counter in range(nblocks))

  # int.from_bytes()/to_bytes() have no 64-bit limit: the whole buffer is
  # converted into a single arbitrary-precision integer, and 'big' only fixes
  # which end of the buffer is the most significant. Since XOR cannot carry
  # between bit positions, the big-int XOR below is exactly equivalent to a
  # positional byte-wise XOR of the whole buffer. to_bytes() re-serializes the
  # result at the exact input width (zero-padding leading zero bytes), so the
  # output length always matches len(data).
  iv = int.from_bytes(data, 'big') ^ int.from_bytes(stream[:len(data)], 'big')

  return iv.to_bytes(len(data), 'big')


def crypt_object(obj, key):
  """Serialize and encrypt an object into an authenticated blob.

  Args:
    obj: The object to serialize and encrypt.
    key: The encryption key - bytes.

  Returns:
    The nonce || ciphertext || tag blob.
  """
  nonce = os.urandom(16)
  seed, mackey = _crypt_seed(key, nonce)
  pdata = pickle.dumps(obj)
  cdata = _crypt_xor(pdata, seed)
  tag = hmac.new(mackey, nonce + cdata, hashlib.sha256).digest()

  return nonce + cdata + tag


def decrypt_object(blob, key):
  """Decrypt an authenticated blob back into the serialized object.

  Args:
    blob: The nonce || ciphertext || tag blob to decrypt.
    key: The decryption key - bytes.

  Returns:
    The deserialized object, or None when the tag does not validate
    (wrong key or corrupted blob).
  """
  if len(blob) < 16 + 32:
    return None

  nonce, cdata, tag = blob[:16], blob[16:-32], blob[-32:]
  seed, mackey = _crypt_seed(key, nonce)
  xtag = hmac.new(mackey, nonce + cdata, hashlib.sha256).digest()
  if not hmac.compare_digest(tag, xtag):
    return None

  return pickle.loads(_crypt_xor(cdata, seed))


def _create_manager(*args, register_fn=None, **kwargs):
  """Create a multiprocessing SyncManager with registered shared resources.

  Registers standard synchronization primitives:
  - Locks (get_lock, rm_lock)
  - Events (get_event, rm_event)
  - Conditions (get_condition, rm_condition)
  - Barriers (get_barrier, rm_barrier)
  - Queues (get_queue, rm_queue)
  - LifoQueues (get_lifo, rm_lifo)
  - Namespaces (get_namespace, rm_namespace)

  Args:
    *args: Positional arguments for SyncManager.
    register_fn: Optional callable(manager, resmgr) for custom registrations.
    **kwargs: Keyword arguments for SyncManager.

  Returns:
    Configured SyncManager instance (not yet started).
  """
  # https://github.com/python/cpython/blob/2f56c68dec97002fdd8563a0e4977b75eb191ab9/Lib/multiprocessing/managers.py#L1043
  # https://github.com/python/cpython/blob/4cba0e66c29b46afbb1eee1d0428f5a2f5b891bb/Lib/multiprocessing/managers.py#L189
  manager = mpmgr.SyncManager(*args, **kwargs)

  resmgr = get_resource_manager()

  manager.register('get_lock', functools.partial(resmgr.get, 'LOCKS', threading.Lock),
                   mpmgr.AcquirerProxy)
  manager.register('rm_lock', functools.partial(resmgr.delete, 'LOCKS'))

  manager.register('get_event', functools.partial(resmgr.get, 'EVENTS',
                                                  threading.Event), mpmgr.EventProxy)
  manager.register('rm_event', functools.partial(resmgr.delete, 'EVENTS'))

  manager.register('get_condition',
                   functools.partial(resmgr.get, 'CONDITIONS', threading.Condition),
                   mpmgr.ConditionProxy)
  manager.register('rm_condition', functools.partial(resmgr.delete, 'CONDITIONS'))

  manager.register('get_barrier',
                   functools.partial(resmgr.get, 'BARRIERS', threading.Barrier),
                   mpmgr.BarrierProxy)
  manager.register('rm_barrier', functools.partial(resmgr.delete, 'BARRIERS'))

  manager.register('get_queue', functools.partial(resmgr.get, 'QUEUES', queue.Queue))
  manager.register('rm_queue', functools.partial(resmgr.delete, 'QUEUES'))

  manager.register('get_lifo', functools.partial(resmgr.get, 'LIFOS', queue.LifoQueue))
  manager.register('rm_lifo', functools.partial(resmgr.delete, 'LIFOS'))

  manager.register('get_namespace',
                   functools.partial(resmgr.get, 'NAMESPACES', mpmgr.Namespace),
                   mpmgr.NamespaceProxy)
  manager.register('rm_namespace', functools.partial(resmgr.delete, 'NAMESPACES'))

  if register_fn is not None:
    register_fn(manager, resmgr)

  return manager


def _get_logdir():
  """Get the log directory for resource manager servers.

  Returns:
    Path to the log directory (creates if needed).
  """
  logdir = os.path.join(tempfile.gettempdir(), 'log')
  os.makedirs(logdir, exist_ok=True)

  return logdir


def _status_glob(name):
  """Get the status-directory glob pattern of a resource manager server."""
  return os.path.join(tempfile.gettempdir(), f'{_RESMGR_STATUS_PREFIX}{name}_*')


def _read_status(name, key):
  """Read the newest valid server status of a resource manager server.

  Scans the status directories - newest first - returning the first
  status whose tag validates under the given key and whose daemon is
  still alive.

  Args:
    name: Server name to look up.
    key: The crypto key of the status files - as derived by _crypt_key().

  Returns:
    The status dict, or - when no valid status can be found - None, or
    the ENODATA error number when at least one status directory exists
    but none of its files decrypts under the given key.
  """
  status_dirs = sorted(glob.glob(_status_glob(name)), reverse=True)
  found = False
  for sdir in status_dirs:
    for sname in os.listdir(sdir):
      sres = decrypt_object(fsu.read_content(os.path.join(sdir, sname)), key)
      if sres is None:
        found = True
        continue

      if sres['ok'] and not psutil.pid_exists(sres['pid']):
        continue

      return sres

  return errno.ENODATA if found else None


def get_status_dirs(name):
  """Get the status directories of a resource manager server.

  Args:
    name: Server name to look up.

  Returns:
    The list of the status directory paths - possibly stale ones - of
    the resource manager server.
  """
  return glob.glob(_status_glob(name))


def _remove_status(name):
  """Remove all the status directories of a resource manager server."""
  for sdir in glob.glob(_status_glob(name)):
    shutil.rmtree(sdir, ignore_errors=True)


def _server_runner(name, *args, authkey=None, address=None, **kwargs):
  """Run a resource manager server in a daemon process.

  Args:
    name: Server name (used for logging).
    *args: Positional arguments for _create_manager.
    authkey: The shared authentication key of the server.
    address: An optional address to bind the server to. When None the
      server binds an ephemeral address, published through the status
      directory.
    **kwargs: Keyword arguments for _create_manager.
  """
  alog.basic_setup(log_level=os.getenv('RESMGR_LOG_LEVEL', 'INFO'),
                   log_file=os.path.join(_get_logdir(), f'{name}.log'))

  alog.info(f'[{name}] server starting')
  sdir = None
  try:
    manager = _create_manager(*args, authkey=authkey, address=address, **kwargs)
    server = manager.get_server()
    family = 'AF_UNIX' if isinstance(server.address, str) else 'AF_INET'
    status = dict(ok=True, pid=os.getpid(), address=server.address, family=family)

    sdir = tempfile.mkdtemp(prefix=f'{_RESMGR_STATUS_PREFIX}{name}_')
    sname = os.path.join(sdir, 'status')
    with osfd.OsFd(sname, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode=0o600) as wfd:
      os.write(wfd, crypt_object(status, _crypt_key(authkey, name)))

    alog.info(f'[{name}] server published status: {server.address!r}')
    server.serve_forever()
  except Exception as ex:  # noqa: BLE001 -- process-finalizer boundary: a daemon server body can only log its failure before exiting
    alog.error(f'[{name}] server start failed: {ex}')
    if sdir is not None:
      # Failure statuses are written unencrypted: they carry no secrets,
      # and a plain message is more useful than silence.
      status = dict(ok=False, pid=os.getpid(), error=str(ex))
      try:
        with osfd.OsFd(os.path.join(sdir, 'status'),
                       os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                       mode=0o600) as wfd:
          os.write(wfd, pickle.dumps(status))

        alog.info(f'[{name}] server published failure status: {ex}')
      except Exception as wex:  # noqa: BLE001 -- status-write failure boundary
        alog.error(f'[{name}] failed to publish failure status: {wex}')
  finally:
    alog.info(f'[{name}] server gone!')


def get_manager(name, *args, authkey=None, address=None, **kwargs):
  """Start or connect to a resource manager server.

  Starts a daemon process running the server if not already running,
  then connects to it - reading the server address, published by the
  daemon through its encrypted status directory, when not provided.

  Args:
    name: Server name (used for daemon identification and logging).
    *args: Positional arguments for _create_manager.
    authkey: The shared authentication key of the server - generated
      when None. Callers attaching to an already running server must
      pass the same authkey the starting caller used.
    address: An optional address to connect to. When None the address
      is discovered from the server status directory.
    **kwargs: Keyword arguments for _create_manager.

  Returns:
    Connected SyncManager instance.
  """
  if authkey is None:
    authkey = os.urandom(32)

  if address is None:
    daemon = dp.Daemon(name)
    key = _crypt_key(authkey, name)
    deadline = time.monotonic() + _RESMGR_STATUS_DEADLINE
    while True:
      # Restart the daemon each round: its death - and its status write -
      # is re-evaluated every poll cycle.
      if daemon.getpid() is None:
        alog.info(f'[{name}] Starting server daemon')
        try:
          daemon.start(
              functools.partial(_server_runner, name, *args, authkey=authkey, **kwargs))
        except FileExistsError:
          pass

      status = _read_status(name, key)
      if isinstance(status, dict):
        break
      if status == errno.ENODATA:
        # Status files exist but none decrypts under the given key: the
        # daemon was started with a different authkey.
        alog.xraise(
            PermissionError, f'Unable to decrypt the "{name}" resource manager '
            'server status - wrong authkey?')

      if time.monotonic() > deadline:
        alog.xraise(ConnectionError, f'Unable to get the "{name}" resource manager '
                    'server status')

      time.sleep(_RESMGR_STATUS_POLL)

    address = status['address']
    if status['family'] == 'AF_INET':
      address = tuple(address)

    if not status['ok']:
      alog.xraise(
          ConnectionError, f'Unable to start the "{name}" resource manager '
          f'server: {status["error"]}')

  alog.info(f'[{name}] Connecting to {address!r}')

  manager = _create_manager(*args, authkey=authkey, address=address, **kwargs)
  manager.connect()

  alog.info(f'[{name}] Connected to the manager')

  return manager


def stop_manager(name):
  """Stop a resource manager server daemon.

  Args:
    name: Server name to stop.

  Returns:
    True if daemon was stopped, False otherwise.

  Example:
    from py_misc_utils import resource_manager as rm

    rm.stop_manager('my-resmgr')
  """
  xpid = dp.Daemon(name).stop()

  _remove_status(name)

  return xpid
