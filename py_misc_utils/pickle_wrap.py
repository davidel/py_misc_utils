import importlib
import os
import pickle
import sys

from . import alog
from . import core_utils as cu
from . import inspect_utils as iu


def _root_module(modname):
  return modname.split('.', maxsplit=1)[0]


KNOWN_MODULES = {
  'builtins',
  'numpy',
  'pandas',
  'torch',
  _root_module(__name__),
}

def add_known_module(modname):
  KNOWN_MODULES.add(_root_module(modname))


def _module_origin(modname):
  module = sys.modules.get(modname)
  if module is None:
    try:
      module = importlib.import_module(modname)
    except ModuleNotFoundError:
      pass

  if module is not None:
    path = getattr(module, '__file__', None)
    if path is None:
      spec = getattr(module, '__spec__', None)
      path = spec.origin if spec is not None else None

    return path


def _module_libpath(modname):
  origin = _module_origin(modname)
  if origin not in {None, 'built-in'}:
    return os.path.dirname(origin)


_STDLIB_MODULES = ('abc', 'copy', 'io', 'os', 'pickle', 'random', 'string')
_STDLIB_PATHS = set(_module_libpath(m) for m in _STDLIB_MODULES)

def _needs_wrap(obj):
  objmod = iu.moduleof(obj)
  if objmod is not None:
    modname = _root_module(objmod)
    if modname in KNOWN_MODULES or _module_libpath(modname) in _STDLIB_PATHS:
      return False

  return True


def _wrap(obj, pickle_module):
  wrapped = 0
  if isinstance(obj, (list, tuple)):
    wobj = []
    for v in obj:
      wv = _wrap(v, pickle_module)
      if wv is not v:
        wrapped += 1

      wobj.append(wv)

    return type(obj)(wobj) if wrapped else obj
  elif cu.isdict(obj):
    wobj = type(obj)()
    for k, v in obj.items():
      wk = _wrap(k, pickle_module)
      if wk is not k:
        wrapped += 1
      wv = _wrap(v, pickle_module)
      if wv is not v:
        wrapped += 1

      wobj[wk] = wv

    return wobj if wrapped else obj
  elif isinstance(obj, PickleWrap):
    return obj
  elif _needs_wrap(obj):
    return PickleWrap(obj, pickle_module=pickle_module)
  else:
    return obj


def _unwrap(obj, pickle_module):
  unwrapped = 0
  if isinstance(obj, (list, tuple)):
    uwobj = []
    for v in obj:
      wv = _unwrap(v, pickle_module)
      if wv is not v:
        unwrapped += 1

      uwobj.append(wv)

    return type(obj)(uwobj) if unwrapped else obj
  elif cu.isdict(obj):
    uwobj = type(obj)()
    for k, v in obj.items():
      wk = _unwrap(k, pickle_module)
      if wk is not k:
        unwrapped += 1
      wv = _unwrap(v, pickle_module)
      if wv is not v:
        unwrapped += 1

      uwobj[wk] = wv

    return uwobj if unwrapped else obj
  elif isinstance(obj, PickleWrap):
    try:
      return obj.load(pickle_module=pickle_module)
    except Exception as ex:
      alog.debug(f'Unable to reload pickle-wrapped data ({obj.wrapped_class()}): {ex}')
      return obj
  else:
    return obj


def wrap(obj, pickle_module=pickle):
  return _wrap(obj, pickle_module)


def unwrap(obj, pickle_module=pickle):
  return _unwrap(obj, pickle_module)


class PickleWrap:

  def __init__(self, obj, pickle_module=pickle):
    self._class = iu.qual_name(obj)
    self._data = pickle_module.dumps(obj)

  def wrapped_class(self):
    return self._class

  def load(self, pickle_module=pickle):
    return pickle_module.loads(self._data)

