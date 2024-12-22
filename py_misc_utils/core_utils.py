# This module is for APIs which has no local dependecies.
import array
import collections
import copy
import sys
import threading
import types


_NONE = object()


def is_builtin_function(obj):
  return isinstance(obj, types.BuiltinFunctionType)


def refcount(obj):
  # Discard 2 frame references (our own, and the sys.getrefcount() one).
  return sys.getrefcount(obj) - 2


def denone(**kwargs):
  return {k: v for k, v in kwargs.items() if v is not None}


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


def norm_slice(start, stop, size):
  if start is None:
    start = 0
  elif start < 0:
    start = size + start
  if stop is None:
    stop = size
  elif stop < 0:
    stop = size + stop

  return start, stop


def run_async(fn, *args, **kwargs):
  thread = threading.Thread(target=fn, args=args, kwargs=kwargs, daemon=True)
  thread.start()

  return thread


def bisect_right(x, key, hi, lo=0):
  tas.check_ge(lo, 0)
  while lo < hi:
    mid = (lo + hi) // 2
    if x < key(mid):
      hi = mid
    else:
      lo = mid + 1

  return lo


def bisect_left(x, key, hi, lo=0):
  tas.check_ge(lo, 0)
  while lo < hi:
    mid = (lo + hi) // 2
    if key(mid) < x:
      lo = mid + 1
    else:
      hi = mid

  return lo


def is_namedtuple(obj):
  return isinstance(obj, tuple) and hasattr(obj, '_asdict') and hasattr(obj, '_fields')


def new_with(obj, **kwargs):
  if is_namedtuple(obj):
    return obj._replace(**kwargs)

  nobj = copy.copy(obj)
  if isinstance(nobj, dict):
    nobj.update(kwargs)
  else:
    for k, v in kwargs.items():
      setattr(nobj, k, v)

  return nobj


def make_ntuple(ntc, args):
  targs = []
  for f in ntc._fields:
    fv = args.get(f, _NONE)
    if fv is _NONE:
      fv = ntc._field_defaults.get(f)

    targs.append(fv)

  return ntc._make(targs)


class StringTable:

  def __init__(self):
    self._tbl = dict()

  def __len__(self):
    return len(self._tbl)

  def add(self, s):
    x = self._tbl.get(s)
    if x is None:
      x = s
      self._tbl[x] = x

    return x


class ArgList(list):
  pass

def dict_add(ddict, name, value):
  ivalue = ddict.get(name, _NONE)
  if ivalue is not _NONE:
    if isinstance(ivalue, ArgList):
      ivalue.append(value)
    else:
      ddict[name] = ArgList((ivalue, value))
  else:
    ddict[name] = value


def dict_update_append(d, **kwargs):
  for k, v in kwargs.items():
    dict_add(d, k, v)

