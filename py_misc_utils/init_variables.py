import abc
import os
import threading


class VarBase(abc.ABC):

  @abc.abstractmethod
  def cleanup(self):
    ...


def varid(root, name):
  return f'{root}:{name}'


def get(vid, initfn):
  with _LOCK:
    value = _VARS.get(vid, _NONE)

  if value is _NONE:
    # Do not create the new value within the lock since init functions using
    # the init_variables module will deadlock.
    new_value = initfn()
    with _LOCK:
      value = _VARS.get(vid, _NONE)
      if value is _NONE:
        _VARS[vid] = value = new_value

    # It can happen that more instances gets created due to initializing outside
    # the lock. Calling the cleanup() API will give the new object a chance to
    # undo possible side effects of its creation.
    if new_value is not value:
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

