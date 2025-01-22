import pickle

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


def _needs_wrap(obj):
  objmod = iu.moduleof(obj)
  if objmod is not None:
    if _root_module(objmod) in KNOWN_MODULES:
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
  elif _needs_wrap(obj):
    return PickleWrap(obj, pickle_module=pickle_module)
  elif isinstance(obj, PickleWrap):
    return obj
  elif hasattr(obj, '__dict__'):
    state = dict()
    for k, v in obj.__dict__.items():
      wv = _wrap(v, pickle_module)
      if wv is not v:
        wrapped += 1

      wobj[k] = wv

    return cu.new_with(obj, **state) if wrapped else obj
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
  elif hasattr(obj, '__dict__'):
    state = dict()
    for k, v in obj.__dict__.items():
      wv = _unwrap(v, pickle_module)
      if wv is not v:
        unwrapped += 1

      uwobj[k] = wv

    return cu.new_with(obj, **state) if unwrapped else obj
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

