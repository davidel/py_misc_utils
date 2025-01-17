import pickle

from . import core_utils as cu


KNOWN_MODULES = {
  'builtins',
  'numpy',
  'pandas',
  'py_misc_utils',
  'torch',
}

def add_known_module(modname):
  KNOWN_MODULES.add(_root_module(modname))


def _root_module(modname):
  return modname.split('.', maxsplit=1)[0]


def _obj_module(obj):
  ref = getattr(obj, '__class__', obj)

  return getattr(ref, '__module__', None)


def _fqcname(obj):
  cls = getattr(obj, '__class__', None)
  if cls is not None:
    return f'{cls.__module__}.{cls.__qualname__}'


def _needs_wrap(obj):
  objmod = _obj_module(obj)
  if objmod is not None:
    if _root_module(objmod) in KNOWN_MODULES:
      return False

  return True


def _wrap(obj):
  wrapped = 0
  if isinstance(obj, (list, tuple)):
    wobj = []
    for v in obj:
      wv = _wrap(v)
      if wv is not v:
        wrapped += 1

      wobj.append(wv)

    return type(obj)(wobj) if wrapped else obj
  elif cu.isdict(obj):
    wobj = type(obj)()
    for k, v in obj.items():
      wk = _wrap(k)
      if wk is not k:
        wrapped += 1
      wv = _wrap(v)
      if wv is not v:
        wrapped += 1

      wobj[wk] = wv

    return wobj if wrapped else obj
  elif _needs_wrap(obj):
    return PickleWrap(obj)
  elif hasattr(obj, '__dict__'):
    state = dict()
    for k, v in obj.__dict__.items():
      wv = _wrap(v)
      if wv is not v:
        wrapped += 1

      wobj[k] = wv

    if wrapped:
      wobj = obj.__class__.new(obj.__class__)
      wobj.__dict__.update(state)

      return wobj
    else:
      return obj
  else:
    return obj


def _unwrap(obj):
  unwrapped = 0
  if isinstance(obj, list):
    for i, v in enumerate(obj):
      obj[i] = _unwrap(v)

    return obj
  elif isinstance(obj, tuple):
    uwobj = []
    for v in obj:
      wv = _unwrap(v)
      if wv is not v:
        unwrapped += 1

      uwobj.append(wv)

    return tuple(uwobj) if unwrapped else obj
  elif cu.isdict(obj):
    uwobj = type(obj)()
    for k, v in obj.items():
      wk = _unwrap(k)
      if wk is not k:
        unwrapped += 1
      wv = _unwrap(v)
      if wv is not v:
        unwrapped += 1

      uwobj[wk] = wv

    return uwobj if unwrapped else obj
  elif isinstance(obj, PickleWrap):
    try:
      return obj.load()
    except:
      return obj
  elif hasattr(obj, '__dict__'):
    state = dict()
    for k, v in obj.__dict__.items():
      wv = _unwrap(v)
      if wv is not v:
        unwrapped += 1

      uwobj[k] = wv

    if unwrapped:
      uwobj = obj.__class__.new(obj.__class__)
      uwobj.__dict__.update(state)

      return uwobj
    else:
      return obj
  else:
    return obj


def wrap(obj):
  return _wrap(obj)


def unwrap(obj):
  return _unwrap(obj)


class PickleWrap:

  def __init__(self, obj):
    self._class = _fqcname(obj)
    self._data = pickle.dumps(obj)

  def wrapped_class(self):
    return self._class

  def load(self):
    return pickle.loads(self._data)

