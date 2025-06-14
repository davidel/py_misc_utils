# When using multiprocessing, there are two distinct behaviours if fork start method
# is used, WRT spawn/forkserver.  In the latter case the global context
# accumulated by the running process is not pickled-through the child, so the new
# process start with wiped out global namespace.
# Using this API, together with the multiprocessing.create_process(), it is possible to
# have global data transfered to the child.
# All data stored in the global namespace must be pickle-able, unless fork_init is
# set to True.
# If the fork_init attribute is True, it means the variables data must be cleared
# within the child process, and not carried over (COW-ed) like it would happen when
# using the fork(2) system call.
# NOTE: This is a low level module which should have no explicit local dependencies.

import collections
import inspect
import multiprocessing
import os
import threading


# The parent_fn function is called (if present) before the creation of a new process,
# within the parent, with the current value of the variable, and is supposed to be
# returning the "state" of such variable. The state must be pickle-able.
# The child_fn function is called (if present) after the creation of a new process,
# within the child, to restore a variable from its state (the value of the new variable
# should be returned).
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


def parent_switch(method):
  assert method in multiprocessing.get_all_start_methods(), method

  pns = dict()
  with _LOCK:
    for var in _NS.values():
      # Variables with fork_init=True are the ones that are supposed to be
      # initialized in every process, and as such do not have to be carried over
      # from the parent context. Also, fork_init=True variables might contain data
      # which is not pickle-able, and carrying them over will fail.
      if not var.fork_init:
        if var.parent_fn is not None:
          data = var.parent_fn(var.data)
          if data is not var.data:
            var = None if data is None else var._replace(data=data)

        if var is not None:
          pns[var.name] = var

  return pns


def child_switch(method, ns):
  global _NS

  assert method in multiprocessing.get_all_start_methods(), method

  cns = dict()
  for var in ns.values():
    if var.child_fn is not None:
      data = var.child_fn(var.data)
      if data is not var.data:
        var = None if data is None else var._replace(data=data)

    if var is not None:
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

