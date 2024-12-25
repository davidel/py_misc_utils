import os
import threading


def varid(prefix, name):
  return f'{prefix}.{name}'


def get(name, initfn):
  with _LOCK:
    value = _VARS.get(name, _NONE)
    if value is _NONE:
      value = initfn()
      _VARS[name] = value

    return value


def _init_vars():
  global _NONE, _VARS, _LOCK

  _NONE = object()
  _VARS = dict()
  _LOCK = threading.Lock()



_init_vars()

os.register_at_fork(after_in_child=_init_vars)

