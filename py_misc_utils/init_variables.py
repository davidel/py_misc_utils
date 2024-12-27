import abc
import os
import threading


class VarBase(abc.ABC):
  ...


def varid(root, name):
  return f'{root}:{name}'


def get(vid, initfn):
  with _LOCK:
    value = _VARS.get(vid, _NONE)
    if value is _NONE:
      _VARS[vid] = value = initfn()

    return value


def _init_vars():
  global _NONE, _VARS, _LOCK

  _NONE = object()
  _VARS = dict()
  _LOCK = threading.RLock()



_init_vars()

if os.name == 'posix':
  os.register_at_fork(after_in_child=_init_vars)

