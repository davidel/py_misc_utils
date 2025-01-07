# This module is for APIs which has no local dependecies.
import array
import collections
import copy
import os
import sys
import threading
import types

_NONE = object()


def ident(x):
  return x


def noop(*args, **kwargs):
  pass


def object_context(sobj, **kwargs):
  ctx_args = vars(sobj).copy()
  ctx_args.update(**kwargs)

  return types.SimpleNamespace(**ctx_args)


def args(*uargs, **kwargs):
  return uargs, kwargs


def is_builtin_function(obj):
  return isinstance(obj, types.BuiltinFunctionType)


def refcount(obj):
  # Discard 2 frame references (our own, and the sys.getrefcount() one).
  return sys.getrefcount(obj) - 2


def denone(**kwargs):
  return {k: v for k, v in kwargs.items() if v is not None}


def seqfirst(s):
  return next(iter(s))


def iter_next(it, defval=None):
  try:
    return next(it)
  except StopIteration:
    return defval


def enum_values(obj):
  if isinstance(obj, dict):
    for k, v in obj.items():
      yield k, v
  else:
    for k, v in vars(obj):
      yield k, v


def dmerge(*args):
  mdict = dict()
  for d in args:
    mdict.update(d)

  return mdict


def dict_extract(d, prefix=None, rx=None):
  if rx is None:
    rx = f'{prefix}(.*)'
  xd = dict()
  for k, v in d.items():
    m = re.match(rx, k)
    if m:
      xd[m.group(1)] = v

  return xd


def dget(sdict, name, defval, dtype=None):
  v = sdict.get(name, _NONE)
  if v is _NONE:
    return defval

  if dtype is None and defval is not None:
    dtype = type(defval)

  return dtype(v) if v is not None and dtype is not None else v


def ns_lookup(ns, key):
  for part in key.split('.'):
    if isinstance(ns, collections.abc.Mapping):
      ns = ns.get(part)
    else:
      ns = getattr(ns, part, None)
    if ns is None:
      break

  return ns


def index_select(arr, idx):
  return arr[idx] if isinstance(idx, slice) else [arr[i] for i in idx]


def lindex(l, e, start=0, end=None):
  try:
    return l.index(e, start, end if end is not None else len(l))
  except ValueError:
    return -1


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
      self._tbl[s] = x = s

    return x


class ValueList(list):
  pass

def dict_add(ddict, name, value):
  ivalue = ddict.get(name, _NONE)
  if ivalue is not _NONE:
    if isinstance(ivalue, ValueList):
      ivalue.append(value)
    else:
      ddict[name] = ValueList((ivalue, value))
  else:
    ddict[name] = value


def dict_update_append(d, **kwargs):
  for k, v in kwargs.items():
    dict_add(d, k, v)


def enum_dict_values(ddict, name):
  ivalue = ddict.get(name, _NONE)
  if ivalue is not _NONE:
    if isinstance(ivalue, ValueList):
      for value in ivalue:
        yield value
    else:
      yield ivalue


def get_property(obj, name, defval=None):
  p = getattr(obj, name, _NONE)
  if p is _NONE:
    return defval

  return p() if callable(p) else p


def compute_shape(data):
  sp = get_property(data, 'shape')
  if sp is not None:
    return tuple(sp)
  shape = []
  if hasattr(data, '__len__'):
    shape.append(len(data))
    if shape[0] > 0 and hasattr(data, '__getitem__'):
      shape.extend(compute_shape(data[0]))

  return tuple(shape)


class RevGen:

  def __init__(self, fmt=None):
    self._fmt = fmt or '{name}_{ver}'
    self._revdb = dict()

  def getver(self, name, defval=None):
    return self._revdb.get(name, defval)

  def newver(self, name):
    ver = self._revdb.get(name, 0)
    self._revdb[name] = ver + 1

    return ver

  def newname(self, name, shortzero=False):
    ver = self.newver(name)

    return self._fmt.format(name=name, ver=ver) if ver != 0 or not shortzero else name

