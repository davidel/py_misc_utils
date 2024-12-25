import os
import threading


def varid(prefix, name):
  return f'{prefix}.{name}'


def get(name, initfn):
  with _LOCK:
    value = _VARS.get(name, _NONE)

  if value is _NONE:
    # Do not create the new value within the lock since init functions using
    # the init_variables module will deadlock.
    new_value = initfn()
    with _LOCK:
      value = _VARS.get(name, _NONE)
      if value is _NONE:
        _VARS[name] = value = new_value

    # It can happen that two instances gets created due to initializing outside
    # the lock. Functions that cares can create objects with a cleanup() API which
    # get called to give the new object a chance to undo possible side effects of
    # its creation.
    if new_value is not value and hasattr(new_value, 'cleanup'):
      new_value.cleanup()

  return value


def _init_vars():
  global _NONE, _VARS, _LOCK

  _NONE = object()
  _VARS = dict()
  _LOCK = threading.Lock()



_init_vars()

if os.name == 'posix':
  os.register_at_fork(after_in_child=_init_vars)

