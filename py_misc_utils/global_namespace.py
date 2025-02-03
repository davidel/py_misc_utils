# When using multiprocessing, there are two distinct behaviours if fork start method
# is used, WRT spawn/forkserver.  In the latter case the global context
# accumulated by the running process is not pickled-through the child, so the new
# process start with wiped out global namespace.
# Using this API, together with the app_main.create_process(), it is possible to
# have global data transfered to the child.
# All data stored in the global namespace must be pickle-able, unless fork_init is
# set to True.

import collections
import inspect
import os
import threading


Var = collections.namedtuple(
  'Var',
  'name, parent_fn, child_fn, data, fork_init, defval',
  defaults=(None, None, None, False, None))

_NS = dict()
_LOCK = threading.RLock()


def _child_fork():
  global _NS, _LOCK

  cns = dict()
  for var in _NS.values():
    if not var.fork_init:
      cns[var.name] = var

  _NS = cns
  _LOCK = threading.RLock()


if os.name == 'posix':
  os.register_at_fork(after_in_child=_child_fork)


def parent_switch():
  ns = dict()
  with _LOCK:
    for var in _NS.values():
      if not var.fork_init:
        if var.parent_fn is not None:
          data = var.parent_fn(var.data)
          if data is not var.data:
            var = var._replace(data=data)

        ns[var.name] = var

  return ns


def child_switch(ns):
  global _NS

  cns = _NS.copy()
  for var in ns.values():
    if var.child_fn is not None:
      data = var.child_fn(var.data)
      if data is not var.data:
        var = var._replace(data=data)

    cns[var.name] = var

  _NS = cns


def get(var, force=True):
  with _LOCK:
    value = _NS.get(var.name)
    if value is None and force:
      data = var.defval() if inspect.isfunction(var.defval) else var.defval
      value = var._replace(data=data)
      _NS[value.name] = value

  return value.data if value is not None else None


def set(var, data):
  with _LOCK:
    prev_value = _NS.get(var.name)
    value = var._replace(data=data)
    _NS[value.name] = value

  return prev_value.data if prev_value is not None else None

