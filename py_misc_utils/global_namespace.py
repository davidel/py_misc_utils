"""Global namespace management for multiprocessing.

This module provides a thread-safe global namespace that can be transferred
across multiprocessing boundaries. It handles the differences between
fork and spawn/forkserver start methods by allowing custom parent/child
callbacks for state serialization.

Key concepts:
- Var: Named variable with optional parent_fn (serialize) and child_fn (deserialize)
- fork_init: If True, variable is reinitialized in child (not pickled from parent)
- parent_switch(): Called in parent before process creation to capture state
- child_switch(): Called in child after process creation to restore state

All data must be pickle-able unless fork_init=True.

Example:
  import py_misc_utils.global_namespace as gns

  v = gns.Var('b4_greeting')
  gns.set(v, 'hello')
  assert gns.get(v) == 'hello'

  # A callable defval is invoked when the variable is unset (force=True).
  v2 = gns.Var('b4_level', defval=lambda: 'defaulted')
  assert gns.get(v2, force=True) == 'defaulted'
"""

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
Var = collections.namedtuple('Var',
                             'name, parent_fn, child_fn, data, fork_init, defval',
                             defaults=(None, None, None, False, None))

_NS = dict()
_LOCK = threading.RLock()


def _child_fork():
  """Handle fork() by filtering fork_init variables.

  Called via os.register_at_fork(after_in_child) on POSIX systems.
  Preserves only variables with fork_init=False.
  """
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
  """Capture global namespace state for transfer to child process.

  Called in the parent process before creating a new process using
  spawn/forkserver methods.

  Args:
    method: Multiprocessing start method ('spawn', 'forkserver', etc.).

  Returns:
    Dictionary mapping variable names to Var instances with serialized data.
  """
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
  """Restore global namespace state in child process.

  Called in the child process after creation using spawn/forkserver methods.

  Args:
    method: Multiprocessing start method.
    ns: Dictionary of Var instances returned by parent_switch().
  """
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
  """Get a variable's value from the global namespace.

  Args:
    var: Var instance to retrieve.
    force: If True, create with default value if not present (default: True).

  Returns:
    The variable's data, or None if not found and force=False.
  """
  with _LOCK:
    value = _NS.get(var.name)
    if value is None and force:
      data = var.defval() if inspect.isfunction(var.defval) else var.defval
      value = var._replace(data=data)
      _NS[value.name] = value

  return value.data if value is not None else None


def set(var, data):
  """Set a variable's value in the global namespace.

  Args:
    var: Var instance to set.
    data: New data value.

  Returns:
    The previous data value, or None if the variable was not previously
    set or was set to None.
  """
  with _LOCK:
    prev_value = _NS.get(var.name)
    value = var._replace(data=data)
    _NS[value.name] = value

  return prev_value.data if prev_value is not None else None
