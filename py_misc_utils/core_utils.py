# This module is for APIs which has no local dependecies.
import array
import collections
import sys
import types


def is_builtin_function(obj):
  return isinstance(obj, types.BuiltinFunctionType)


def refcount(obj):
  # Discard 2 frame references (our own, and the sys.getrefcount() one).
  return sys.getrefcount(obj) - 2


def denone(**kwargs):
  return {k: v for k, v in kwargs.items() if v is not None}


def expand_strings(*args):
  margs = []
  for arg in args:
    if isinstance(arg, (list, tuple, types.GeneratorType)):
      margs.extend(arg)
    else:
      margs.extend(comma_split(arg))

  return tuple(margs)


def enum_values(obj):
  if isinstance(obj, dict):
    for k, v in obj.items():
      yield k, v
  else:
    for k, v in vars(obj):
      yield k, v


def size_str(size):
  syms = ('B', 'KB', 'MB', 'GB', 'TB')

  for i, sym in enumerate(syms):
    if size < 1024:
      return f'{size} {sym}' if i == 0 else f'{size:.2f} {sym}'

    size /= 1024

  return f'{size * 1024:.2f} {syms[-1]}'


def maybe_call(obj, name, *args, **kwargs):
  fn = getattr(obj, name, None)

  return fn(*args, **kwargs) if fn is not None else None


def maybe_call_dv(obj, name, defval, *args, **kwargs):
  fn = getattr(obj, name, None)

  return fn(*args, **kwargs) if fn is not None else defval


def unique(data):
  udata = collections.defaultdict(lambda: array.array('L'))
  for i, v in enumerate(data):
    udata[v].append(i)

  return udata


def signature(v):
  if isinstance(v, dict):
    vdata = dict()
    for k in sorted(v.keys()):
      vdata[k] = signature(v[k])

    return vdata
  elif isinstance(v, (list, tuple)):
    vdata = [signature(e) for e in v]

    return type(v)(vdata)

  return type(v)


def equal_signature(a, b, subcls=True):
  if isinstance(a, dict):
    if not isinstance(b, dict) or len(a) != len(b):
      return False

    for k, t in a.items():
      tb = b.get(k)
      if tb is None or not equal_signature(t, tb, subcls=subcls):
        return False

    return True
  elif isinstance(a, (list, tuple)):
    if type(a) != type(b) or len(a) != len(b):
      return False

    for ea, eb in zip(a, b):
      if not equal_signature(ea, eb, subcls=subcls):
        return False

    return True

  return issubclass(a, b) or issubclass(b, a) if subcls else a == b


def genhash(v):
  if isinstance(v, dict):
    vdata = []
    for k in sorted(v.keys()):
      vdata.append(genhash(k))
      vdata.append(genhash(v[k]))

    return hash((type(v), tuple(vdata)))
  elif isinstance(v, (list, tuple)):
    vdata = [genhash(e) for e in v]

    return hash((type(v), tuple(vdata)))

  return hash((type(v), v))

