# NOTE: This module is for APIs which has no local dependecies!
import array
import collections
import copy
import inspect
import os
import re
import sys
import threading
import types
import yaml

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


def isdict(value):
  return isinstance(value, collections.abc.Mapping)


def refcount(obj):
  # Discard 2 frame references (our own, and the sys.getrefcount() one).
  return sys.getrefcount(obj) - 2


def denone(**kwargs):
  return {k: v for k, v in kwargs.items() if v is not None}


def vfind(mem, b):
  # Hmmm, a memoryview() should really have a find() API...
  m = re.search(b, mem)

  return m.start() if m is not None else -1


def seqfirst(s):
  return next(iter(s))


def iter_next(it, defval=None):
  try:
    return next(it)
  except StopIteration:
    return defval


def enum_values(obj):
  if isdict(obj):
    for k, v in obj.items():
      yield k, v
  else:
    for k, v in vars(obj):
      yield k, v


def idx_expand(data, idx, filler=None):
  if idx >= len(data):
    data = data + [filler] * (idx + 1 - len(data))

  return data


_SEQUENCE_TYPES = (list, tuple, types.GeneratorType)

def is_sequence(v):
  return isinstance(v, _SEQUENCE_TYPES)


def expand(data, n):
  return data if is_sequence(data) else (data,) * n


def range_split(n, split, minsize, reverse=False):
  splits = list(range(0, n, split))
  if len(splits) > 1 and (n - splits[-1]) < minsize:
    splits.pop()

  rsplits = []
  for i, base in enumerate(splits):
    top = splits[i + 1] if (i + 1) < len(splits) else n
    rsplits.append((base, top))

  return tuple(reversed(rsplits)) if reverse else tuple(rsplits)


def partition(data, n):
  parts = []
  for i in range(n):
    parts.append(tuple(data[j] for j in range(i, len(data), n)))

  return tuple(parts)


def is_iterator(obj):
  return hasattr(obj, '__iter__') and hasattr(obj, '__next__')


def as_iterator(obj):
  return obj if is_iterator(obj) else iter([obj])


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


def to_type(v, vtype):
  if isinstance(v, str):
    if vtype in (list, tuple):
      m = re.match(r'\s*\((.*)\)\s*$', v)
      if m:
        v = f'[{m.group(1)}]'
      elif not re.match(r'\s*\[.*\]\s*$', v):
        v = f'[{v}]'

    v = yaml.safe_load(v)

  return vtype(v)


def cast(v, vtype):
  return to_type(v, vtype) if v is not None else None


def infer_value(v, vtype=None):
  return yaml.safe_load(v) if vtype is None else to_type(v, vtype)


def splitstrip(data, sep):
  return tuple(s.strip() for s in data.split(sep))


def separate(data, sep, reverse=False, filler=_NONE):
  pos = data.rfind(sep) if reverse else data.find(sep)
  if pos < 0:
    if filler is not _NONE:
      return data.strip(), filler
    else:
      return data.strip(),

  return data[: pos].strip(), data[pos + 1:].strip()


def root_module(modname):
  return separate(modname, '.')[0]


def parent_module(modname):
  return separate(modname, '.', reverse=True)[0]


def ns_lookup(key, mappings):
  kparts = key.split('.')
  for ns in mappings:
    for part in kparts:
      if isdict(ns):
        ns = ns.get(part)
      else:
        ns = getattr(ns, part, None)
      if ns is None:
        break

    if ns is not None:
      return ns


def index_select(arr, idx):
  return arr[idx] if isinstance(idx, slice) else [arr[i] for i in idx]


def lindex(l, e, start=0, end=None):
  try:
    return l.index(e, start, end if end is not None else len(l))
  except ValueError:
    return -1


def append_if_missing(arr, elem):
  if elem not in arr:
    arr.append(elem)


def size_str(size):
  syms = ('B', 'KB', 'MB', 'GB', 'TB')

  for i, sym in enumerate(syms):
    if size < 1024:
      return f'{size} {sym}' if i == 0 else f'{size:.2f} {sym}'

    size /= 1024

  return f'{size * 1024:.2f} {syms[-1]}'


def maybe_call(obj, name, *args, **kwargs):
  fn = getattr(obj, name, None)

  return fn(*args, **kwargs) if callable(fn) else None


def maybe_call_dv(obj, name, defval, *args, **kwargs):
  fn = getattr(obj, name, None)

  return fn(*args, **kwargs) if callable(fn) else defval


def unique(data):
  udata = collections.defaultdict(lambda: array.array('L'))
  for i, v in enumerate(data):
    udata[v].append(i)

  return udata


def enum_max(cls):
  return max(x for x in cls)


def enum_bits(cls):
  return enum_max(cls).bit_length()


def signature(v):
  if isdict(v):
    vdata = dict()
    for k in sorted(v.keys()):
      vdata[k] = signature(v[k])

    return vdata
  elif isinstance(v, (list, tuple)):
    vdata = [signature(e) for e in v]

    return type(v)(vdata)

  return type(v)


def equal_signature(a, b, subcls=True):
  if isdict(a):
    if not isdict(b) or len(a) != len(b):
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
  if isdict(v):
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


def rewrited_exception(ex, msg):
  xmsg = f'{ex}{msg}'

  return ex.__class__(xmsg).with_traceback(ex.__traceback__)


def obj_from_dict(cls, data):
  args, kwargs = [], dict()
  slots = getattr(cls, '__slots__', None)
  if slots is not None:
    kwargs = {k: data.get(k) for k in slots}
  else:
    sig = inspect.signature(cls)

    for n, p in sig.parameters.items():
      if p.kind == p.POSITIONAL_ONLY:
        args.append(data.get(n))
      else:
        kwargs[n] = data.get(n)

  return cls(*args, **kwargs)


def clone(obj):
  clone_fn = getattr(obj, 'clone', None)

  return clone_fn() if callable(clone_fn) else copy.copy(obj)


def is_namedtuple(obj):
  return isinstance(obj, tuple) and hasattr(obj, '_asdict') and hasattr(obj, '_fields')


def new_with(obj, **kwargs):
  if is_namedtuple(obj):
    return obj._replace(**kwargs)

  nobj = copy.copy(obj)
  if isdict(nobj):
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


def enum_unique(data, skip=None):
  uskip = set(skip or ())
  for x in data:
    if x not in uskip:
      uskip.add(x)
      yield x


def get_property(obj, name, defval=None):
  p = getattr(obj, name, _NONE)
  if p is _NONE:
    return defval

  return p() if callable(p) else p


def data_rewrite(v, rwfn):
  rwv = rwfn(v)
  if rwv is not None:
    return rwv
  elif isinstance(v, (list, tuple)):
    new_values, rewritten = [], False
    for x in v:
      new_obj = data_rewrite(x, rwfn)
      new_values.append(new_obj)
      rewritten = rewritten or new_obj is not x

    return type(v)(new_values) if rewritten else v
  elif isdict(v):
    new_values, rewritten = [], False
    for k, x in v.items():
      new_k = data_rewrite(k, rwfn)
      new_x = data_rewrite(x, rwfn)
      new_values.append((new_k, new_x))
      rewritten = rewritten or new_k is not k or new_x is not x

    return type(v)(new_values) if rewritten else v
  elif hasattr(v, '__dict__'):
    new_values, rewritten = [], False
    for k, x in v.__dict__.items():
      new_k = data_rewrite(k, rwfn)
      new_x = data_rewrite(x, rwfn)
      new_values.append((new_k, new_x))
      rewritten = rewritten or new_k is not k or new_x is not x

    if rewritten:
      new_obj = copy.copy(v)
      new_obj.__dict__.update(**dict(new_values))

      return new_obj

    return v
  else:
    return v


def recurse_apply(obj, name, fn):
  if not fn(obj, name):
    if isinstance(obj, (list, tuple)):
      for i, v in enumerate(obj):
        recurse_apply(v, f'{name}[{i}]', fn)
    elif isdict(obj):
      for k, v in obj.items():
        recurse_apply(v, f'{name}.{k}', fn)
    elif hasattr(obj, '__dict__'):
      for k, v in obj.__dict__.items():
        recurse_apply(v, f'{name}.{k}', fn)


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

  def __init__(self, fmt=None, revbase=0):
    self._fmt = fmt or '{name}_{ver}'
    self._revbase = revbase
    self._revdb = dict()

  def getver(self, name, defval=None):
    return self._revdb.get(name, defval)

  def newver(self, name):
    ver = self._revdb.get(name, self._revbase)
    self._revdb[name] = ver + 1

    return ver

  def newname(self, name, shortzero=False):
    ver = self.newver(name)

    return self._fmt.format(name=name, ver=ver) if ver != 0 or not shortzero else name

