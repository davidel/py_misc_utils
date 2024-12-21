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

