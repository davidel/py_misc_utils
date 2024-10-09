import array
import binascii
import bz2
import collections
import copy
import datetime
import gzip
import inspect
import json
import logging
import math
import os
import pickle
import random
import re
import shutil
import string
import struct
import sys
import threading
import time
import traceback
import types
import yaml

import numpy as np

from . import alog
from . import assert_checks as tas
from . import file_overwrite as fow
from . import obj
from . import split as sp
from . import template_replace as tr
from . import traceback as tb


class _None:
  pass


NONE = _None()


def pickle_proto():
  return getenv('PICKLE_PROTO', dtype=int, defval=pickle.HIGHEST_PROTOCOL)


def cache_dir(path=None):
  path = path or os.path.join(os.getenv('HOME', '.'), '.cache')

  return os.path.normpath(os.path.expanduser(path))


def make_ntuple(ntc, args):
  targs = []
  for f in ntc._fields:
    fv = args.get(f, NONE)
    if fv is NONE:
      fv = ntc._field_defaults.get(f)

    targs.append(fv)

  return ntc._make(targs)


def enum_values(obj):
  if isinstance(obj, dict):
    for k, v in obj.items():
      yield k, v
  else:
    for k in dir(obj):
      yield k, getattr(obj, k)


def fname():
  return tb.get_frame(1).f_code.co_name


def cname(obj):
  cls = classof(obj)
  return cls.__name__ if cls is not None else None


def qual_cname(obj):
  cls = classof(obj)
  if cls is not None:
    module = cls.__module__
    name = cls.__qualname__
    if module is not None and module != '__builtin__':
      name = module + '.' + name

    return name


def func_name(func):
  fname = getattr(func, '__name__', None)

  return fname if fname is not None else cname(func)


def is_builtin_function(obj):
  return isinstance(obj, types.BuiltinFunctionType)


def is_subclass(cls, cls_group):
  return inspect.isclass(cls) and issubclass(cls, cls_group)


def classof(obj):
  return obj if inspect.isclass(obj) else getattr(obj, '__class__', None)


def infer_str(v):
  return infer_value(v) if isinstance(v, str) else v


def get_fn_kwargs(args, fn, prefix=None, roffset=None):
  aspec = inspect.getfullargspec(fn)

  sdefaults = aspec.defaults or ()
  sargs = aspec.args or ()
  ndelta = len(sargs) - len(sdefaults)
  fnargs = dict()
  for i, an in enumerate(sargs):
    if i != 0 or an != 'self':
      nn = f'{prefix}.{an}' if prefix else an
      di = i - ndelta
      if di >= 0:
        d = sdefaults[di]
        if isinstance(d, (int, float, str)):
          dtype = type(d)
        else:
          dtype = infer_str
        fnargs[an] = dget(args, nn, d, dtype=dtype)
      elif roffset is not None and i >= roffset:
        tas.check(nn in args, msg=f'The "{an}" argument must be present as "{nn}"')
        fnargs[an] = args[nn]

  if aspec.kwonlyargs:
    for an in aspec.kwonlyargs:
      nn = f'{prefix}.{an}' if prefix else an
      d = aspec.kwonlydefaults.get(an, NONE)
      if isinstance(d, (int, float, str)):
        dtype = type(d)
      else:
        dtype = infer_str
      argval = dget(args, nn, d, dtype=dtype)
      if argval is not NONE:
        fnargs[an] = argval

  return fnargs


def _stri(l, d, float_fmt):
  s = d.get(id(l), NONE)
  if s is None:
    return '...'
  elif s is not NONE:
    return s

  d[id(l)] = None
  if isinstance(l, str):
    result = f'"{l}"'
  elif isinstance(l, float):
    result = f'{l:{float_fmt}}'
  elif isinstance(l, bytes):
    result = l.decode()
  elif is_namedtuple(l):
    result = str(l)
  elif isinstance(l, (tuple, list, types.GeneratorType)):
    sl = ', '.join(_stri(x, d, float_fmt) for x in l)

    result = '[' + sl + ']' if isinstance(l, list) else '(' + sl + ')'
  elif isinstance(l, dict):
    result = '{' + ', '.join(f'{k}={_stri(v, d, float_fmt)}' for k, v in l.items()) + '}'
  elif hasattr(l, '__dict__'):
    result = _stri(l.__dict__, d, float_fmt)
  else:
    result = str(l)

  d[id(l)] = result

  return result


def stri(l, float_fmt=None):
  return _stri(l, dict(), float_fmt or '.3e')


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


def dmerge(*args):
  mdict = dict()
  for d in args:
    mdict.update(d)

  return mdict


def dget(sdict, name, defval, dtype=None):
  v = sdict.get(name, NONE)
  if v is NONE:
    return defval

  if dtype is None and defval is not None:
    dtype = type(defval)

  return dtype(v) if v is not None and dtype is not None else v


def expand_strings(*args):
  margs = []
  for arg in args:
    if isinstance(arg, (list, tuple, types.GeneratorType)):
      margs.extend(arg)
    else:
      margs.extend(comma_split(arg))

  return tuple(margs)


def mget(d, *args, as_dict=False):
  margs = expand_strings(*args)
  if as_dict:
    return {f: d.get(f) for f in margs}
  else:
    return tuple(d.get(f) for f in margs)


def get_property(obj, name, defval=None):
  p = getattr(obj, name, NONE)
  if p is NONE:
    return defval

  return p() if callable(p) else p


def dict_subset(d, *keys):
  mkeys = expand_strings(*keys)
  subd = dict()
  for k in mkeys:
    v = d.get(k, NONE)
    if v is not NONE:
      subd[k] = v

  return subd


def dict_extract(d, prefix=None, rx=None):
  if rx is None:
    rx = f'{prefix}(.*)'
  xd = dict()
  for k, v in d.items():
    m = re.match(rx, k)
    if m:
      xd[m.group(1)] = v

  return xd


def dict_setmissing(d, **kwargs):
  kwargs.update(d)

  return kwargs


def pop_kwargs(kwargs, names, args_key=None):
  xargs = kwargs.pop(args_key or '_', None)
  if xargs is not None:
    args = [xargs.get(name) for name in as_sequence(names)]
  else:
    args = [kwargs.pop(name, None) for name in as_sequence(names)]

  return tuple(args)


def index_select(arr, idx):
  return arr[idx] if isinstance(idx, slice) else [arr[i] for i in idx]


def append_if_missing(arr, elem):
  if elem not in arr:
    arr.append(elem)


def state_override(obj, state, keys):
  for key in keys:
    sv = state.get(key, NONE)
    if sv is not NONE:
      curv = getattr(obj, key, NONE)
      if curv is not NONE:
        setattr(obj, key, sv)


def resplit(csstr, sep):
  return sp.split(csstr, r'\s*' + sep + r'\s*')


def comma_split(csstr):
  return resplit(csstr, ',')


def ws_split(data):
  return sp.split(data, r'\s+')


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


_TLS = threading.local()

class Context:

  def __init__(self, name, obj):
    self._name = name
    self._obj = obj

  def __enter__(self):
    stack = getattr(_TLS, self._name, None)
    if stack is None:
      stack = []
      setattr(_TLS, self._name, stack)

    stack.append(self._obj)

    return self._obj

  def __exit__(self, *exc):
    stack = getattr(_TLS, self._name)
    obj = stack.pop()

    return False


def get_context(name):
  stack = getattr(_TLS, name, None)

  return stack[-1] if stack else None


def load_config(cfg_file=None, **kwargs):
  if cfg_file is not None:
    with open(cfg_file, mode='r') as cf:
      cfg = yaml.safe_load(cf)
  else:
    cfg = dict()

  for k, v in kwargs.items():
    if v is not None:
      cfg[k] = v

  return cfg


def parse_config(cfg, **kwargs):
  if os.path.exists(cfg):
    with open(cfg, mode='r') as fp:
      cfgd = yaml.safe_load(fp)
  elif cfg.startswith('{'):
    cfgd = json.loads(cfg)
  else:
    cfgd = parse_dict(cfg)

  for k, v in kwargs.items():
    if v is not None:
      cfgd[k] = v

  return cfgd


def fatal(msg, exc=RuntimeError):
  alog.xraise(exc, msg, stacklevel=2)


def assert_instance(msg, t, ta):
  if not isinstance(t, ta):
    parts = [msg, f': {cname(t)} is not ']
    if isinstance(ta, (list, tuple)):
      parts.append('one of (')
      parts.append(', '.join(cname(x) for x in ta))
      parts.append(')')
    else:
      parts.append(f'a {cname(ta)}')

    alog.xraise(ValueError, ''.join(parts))


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


class CtxManager:

  def __init__(self, infn, outfn):
    self._infn = infn
    self._outfn = outfn

  def __enter__(self):
    return self._infn()

  def __exit__(self, *exc):
    return self._outfn(*exc)


class CtxManagerWrapper:

  def __init__(self, wrap_ctx, wrap_obj=None):
    self._wrap_ctx = wrap_ctx
    self._wrap_obj = wrap_obj

  def __enter__(self):
    wres = self._wrap_ctx.__enter__()

    return wres if self._wrap_obj is None else self._wrap_obj

  def __exit__(self, *exc):
    return self._wrap_ctx.__exit__(*exc)


class StringTable:

  def __init__(self):
    self._tbl = dict()

  def add(self, s):
    x = self._tbl.get(s)
    if x is None:
      x = s
      self._tbl[x] = x

    return x


def make_object(**kwargs):
  return obj.Obj(**kwargs)


def make_object_recursive(**kwargs):
  for k, v in kwargs.items():
    if isinstance(v, dict):
      kwargs[k] = make_object_recursive(**v)

  return make_object(**kwargs)


def sreplace(rex, data, mapfn, nmapfn=None, join=True):
  nmapfn = nmapfn if nmapfn is not None else lambda x: x

  lastpos, parts = 0, []
  for m in re.finditer(rex, data):
    start, end = m.span()
    if start > lastpos:
      parts.append(nmapfn(data[lastpos: start]))

    lastpos = end
    mid = mapfn(m.group(1))
    parts.append(mid)

  if lastpos < len(data):
    parts.append(nmapfn(data[lastpos:]))

  return ''.join(parts) if join else parts


def lindex(l, e, start=0, end=None):
  try:
    return l.index(e, start, end if end is not None else len(l))
  except ValueError:
    return -1


def idx_expand(data, idx, filler=None):
  if idx >= len(data):
    data = data + [filler] * (idx + 1 - len(data))

  return data


def expand_args(args):
  # Allows APIs to be called both with expanded arguments, and with a single
  # argument which is a {list, tuple, generator}.
  if len(args) == 1 and isinstance(args[0], (list, tuple, types.GeneratorType)):
    return tuple(args[0])

  return xargs


def as_sequence(v, t=tuple):
  if isinstance(t, (list, tuple)):
    for st in t:
      if isinstance(v, st):
        return v

    return t[0]([v]) if not isinstance(v, types.GeneratorType) else t[0](v)

  if isinstance(v, t):
    return v

  return t(v) if isinstance(v, (list, tuple, types.GeneratorType)) else t([v])


def format(seq, fmt):
  sfmt = f'{{:{fmt}}}'

  return type(seq)(sfmt.format(x) for x in seq)


def seqfirst(s):
  return next(iter(s))


def value_or(v, defval):
  return v if v is not None else defval


class _ArgList(list):

  def __init__(self, *args):
    super().__init__(args)


def dict_add(ddict, name, value):
  ivalue = ddict.get(name, NONE)
  if ivalue is not NONE:
    if isinstance(ivalue, _ArgList):
      ivalue.append(value)
    else:
      ddict[name] = _ArgList(ivalue, value)
  else:
    ddict[name] = value


def dict_update_append(d, **kwargs):
  for k, v in kwargs.items():
    dict_add(d, k, v)


def dict_rget(sdict, path, defval=None, sep='/'):
  if not isinstance(path, (list, tuple)):
    path = path.strip(sep).split(sep)

  result = sdict
  for key in path:
    if not isinstance(result, dict):
      return defval
    result = result.get(key, defval)

  return result


def make_index_dict(vals):
  return {v: i for i, v in enumerate(vals)}


def append_index_dict(xlist, xdict, value):
  xlist.append(value)
  xdict[value] = len(xlist) - 1


def compile(code, syms, env=None, vals=None, lookup_fn=None, delim=None):
  env = value_or(env, dict())
  if vals is not None or lookup_fn is not None:
    code = tr.template_replace(code, vals=vals, lookup_fn=lookup_fn, delim=delim)

  exec(code, env)

  return tuple(env[s] for s in as_sequence(syms))


def unpack_n(l, n, defval=None):
  return tuple(l[:n] if len(l) >= n else l + [defval] * (n - len(l)))


def randseed(seed=None):
  if seed is not None:
    if isinstance(seed, int):
      seed = binascii.crc32(struct.pack('=q', seed))
    elif isinstance(seed, float):
      seed = binascii.crc32(struct.pack('=d', seed))
    elif isinstance(seed, bytes):
      seed = binascii.crc32(seed)
    elif isinstance(seed, str):
      seed = binascii.crc32(seed.encode())
    else:
      seed = binascii.crc32(struct.pack('=Q', id(seed)))

  random.seed(seed)
  np.random.seed(seed)

  return seed


def shuffle(args):
  return random.sample(args, k=len(args))


def sign_extend(value, nbits):
  sign = 1 << (nbits - 1)

  return (value & (sign - 1)) - (value & sign)


def range_split(n, split, minsize, reverse=False):
  splits = list(range(0, n, split))
  if len(splits) > 1 and (n - splits[-1]) < minsize:
    splits.pop()

  rsplits = []
  for i, base in enumerate(splits):
    top = splits[i + 1] if (i + 1) < len(splits) else n
    rsplits.append((base, top))

  return tuple(reversed(rsplits)) if reverse else tuple(rsplits)


def getenv(name, dtype=None, defval=None):
  # os.getenv expects the default value to be a string, so cannot be passed in there.
  env = os.getenv(name, None)
  if env is None:
    env = defval
  if env is not None:
    return to_type(env, dtype) if dtype is not None else env


def env(name, defval, vtype=None):
  return getenv(name, dtype=vtype, defval=defval)


def envs(*args, as_dict=False):
  return mget(os.environ, *args, as_dict=as_dict)


def import_env(dest, *args):
  ivars = envs(*args, as_dict=True)
  for k, v in ivars.items():
    dest[k] = infer_value(v)

  return dest


def map_env(g, prefix=''):
  ovr = dict()
  for k, v in g.items():
    ev = getenv(f'{prefix}{k}', dtype=type(v))
    if ev is not None:
      ovr[k] = ev

  g.update(ovr)

  return g


MAJOR = 1
MINOR = -1

def squeeze(shape, keep_dims=0, sdir=MAJOR):
  sshape = list(shape)
  if sdir == MAJOR:
    while len(sshape) > keep_dims and sshape[0] == 1:
      sshape = sshape[1:]
  elif sdir == MINOR:
    while len(sshape) > keep_dims and sshape[-1] == 1:
      sshape = sshape[: -1]
  else:
    alog.xraise(ValueError, f'Unknown squeeze direction: {sdir}')

  return type(shape)(sshape)


def flat2shape(data, shape):
  assert len(data) == np.prod(shape), f'Shape {shape} is unsuitable for a {len(data)} long array'

  # For an Mx...xK input shape, return a M elements (nested) list.
  for n in reversed(shape[1:]):
    data = [data[i: i + n] for i in range(len(data), n)]

  return data


def shape2flat(data, shape):
  for _ in range(len(shape) - 1):
    assert isinstance(data, (list, tuple))
    ndata = []
    for av in data:
      assert isinstance(av, (list, tuple))
      ndata.extend(av)

    data = ndata

  assert len(data) == np.prod(shape)

  return tuple(data)


def binary_reduce(parts, reduce_fn):
  while len(parts) > 1:
    nparts, base = [], 0
    if len(parts) % 2 != 0:
      nparts.append(parts[0])
      base = 1

    nparts.extend(reduce_fn(parts[i], parts[i + 1]) for i in range(base, len(parts) - 1, 2))

    parts = nparts

  return parts[0]


def data_rewrite(v, rwfn):
  rwv = rwfn(v)
  if rwv is not None:
    return rwv
  elif isinstance(v, (list, tuple)):
    vals = [data_rewrite(x, rwfn) for x in v]
    return type(v)(vals)
  elif isinstance(v, dict):
    return {data_rewrite(k, rwfn): data_rewrite(x, rwfn) for k, x in v.items()}
  else:
    return v


def stringify(s):
  def rwfn(v):
    if not isinstance(v, (list, tuple, dict)):
      return str(v)

  return data_rewrite(s, rwfn)


def mlog(msg, level=logging.DEBUG):
  # No reason to split the message in lines, as the GLOG formatter alreay handles it.
  if logging.getLogger().isEnabledFor(level):
    logging.log(level, msg() if callable(msg) else msg)


def seq_rewrite(seq, sd):
  return type(seq)(sd.get(s, s) for s in seq)


def varint_encode(v, encbuf):
  if isinstance(v, (int, np.integer)):
    tas.check_ge(v, 0, msg=f'Cannot encode negative values: {v}')

    cv = v
    while (cv & ~0x7f) != 0:
      encbuf.append(0x80 | (cv & 0x7f))
      cv >>= 7

    encbuf.append(cv & 0x7f)
  elif isinstance(v, (list, tuple, array.array, np.ndarray)):
    for xv in v:
      varint_encode(xv, encbuf)
  else:
    alog.xraise(RuntimeError, f'Unsupported type: {v} ({type(v)})')


def _varint_decode(encbuf, pos):
  value, cpos, nbits = 0, pos, 0
  while True:
    b = encbuf[cpos]
    value |= (b & 0x7f) << nbits
    nbits += 7
    cpos += 1
    if (b & 0x80) == 0:
      break

  return value, cpos


def varint_decode(encbuf):
  values, cpos = [], 0
  try:
    while cpos < len(encbuf):
      value, cpos = _varint_decode(encbuf, cpos)
      values.append(value)
  except IndexError:
    # Handle out-of-range buffer access outside, to avoid checks within the inner loop.
    enc_data = ','.join(f'0x{x:02x}' for x in encbuf[cpos: cpos + 10])
    alog.xraise(ValueError,
                f'Invalid varint encoded buffer content at offset {cpos}: {enc_data} ...')

  return values


def dfetch(d, *args):
  return tuple(d[n] for n in args)


def enum_set(l, s, present):
  ss = set(s) if not isinstance(s, set) else s
  for x in l:
    if present == (x in ss):
      yield x


class RevGen:

  def __init__(self):
    self._revdb = dict()

  def getver(self, name, defval=None):
    return self._revdb.get(name, defval)

  def newver(self, name):
    ver = self._revdb.get(name, 0)
    self._revdb[name] = ver + 1

    return ver

  def newname(self, name, shortzero=False):
    ver = self.newver(name)

    return f'{name}_{ver}' if ver != 0 or not shortzero else name


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


def numel(t):
  sp = get_property(t, 'shape')

  return np.prod(sp) if sp is not None else len(t)


def is_sorted(data, descending=False):
  if not isinstance(data, np.ndarray):
    data = np.array(data)

  if descending:
    return np.all(data[:-1] >= data[1:])

  return np.all(data[:-1] <= data[1:])


def round_up(v, step):
  return ((v + step - 1) // step) * step


def scale_data(data, base_data, scale):
  return ((data - base_data) / base_data) * scale


_ARRAY_SIZES = tuple((array.array(c).itemsize, c) for c in 'B,H,I,L,Q'.split(','))

def array_code(size):
  nbytes = math.ceil(math.log2(size)) / 8
  for cb, code in _ARRAY_SIZES:
    if cb >= nbytes:
      return code

  alog.xraise(ValueError,
              f'Size {size} too big to fit inside any array integer types')


def checked_remove(l, o):
  try:
    l.remove(o)
  except ValueError:
    return False

  return True


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


def run_async(fn):
  thread = threading.Thread(target=fn, daemon=True)
  thread.start()

  return thread


def sleep_until(date, msg=None):
  now = datetime.datetime.now(tz=date.tzinfo)
  if date > now:
    if msg:
      alog.info(msg)
    time.sleep(date.timestamp() - now.timestamp())


_BOOL_MAP = {
  'true': True,
  '1': True,
  'false': False,
  '0': False,
}

def to_type(v, vtype):
  return _BOOL_MAP[v.lower()] if vtype == bool else vtype(v)


_SPECIAL_VALUES = {
  'True': True,
  'False': False,
  'None': None,
}

def infer_value(v, vtype=None):
  if vtype is not None:
    return to_type(v, vtype)
  if re.match(r'[+-]?([1-9]\d*|0)$', v):
    return int(v)
  if re.match(r'0x[0-9a-fA-F]+$', v):
    return int(v, 16)
  if re.match(r'0o[0-7]+$', v):
    return int(v, 8)

  #    [-+]? # optional sign
  #    (?:
  #      (?: \d* \. \d+ ) # .1 .12 .123 etc 9.1 etc 98.1 etc
  #      |
  #      (?: \d+ \.? ) # 1. 12. 123. etc 1 12 123 etc
  #    )
  #    (?: [Ee] [+-]? \d+ ) ? # optional exponent part
  float_rx = r'[-+]?(?:(?:\d*\.\d+)|(?:\d+\.?))(?:[Ee][+-]?\d+)?$'
  if re.match(float_rx, v):
    return float(v)

  uv = sp.unquote(v)
  if uv is not v:
    if v[0] in '\'"':
      return uv
    elif v[0] in '[(':
      values = []
      for part in comma_split(uv):
        values.append(infer_value(part))

      return tuple(values) if v[0] == '(' else values

  sv = _SPECIAL_VALUES.get(v, NONE)

  return v if sv is NONE else sv


def parse_dict(data, ktype=str, vtype=None, allow_args=False):
  ma_dict, ma_args = dict(), []
  for part in comma_split(data):
    parts = resplit(part, '=')
    if len(parts) == 2:
      name, value = parts
      ma_dict[infer_value(name, vtype=ktype)] = infer_value(value, vtype=vtype)
    elif len(parts) == 1:
      if not allow_args:
        alog.xraise(ValueError, f'Arguments parsing disabled: {data}')
      if ma_dict:
        alog.xraise(ValueError, f'Arguments can appear only at the beginning: {data}')
      ma_args.append(infer_value(parts[0], vtype=vtype))
    else:
      alog.xraise(ValueError, f'Syntax error: {part}')

  return (ma_dict, tuple(ma_args)) if allow_args else ma_dict


_BOOLS = {
  'True': True,
  'true': True,
  '1': True,
  1: True,
  'False': False,
  'false': False,
  '0': False,
  0: False,
}

def to_bool(v):
  if isinstance(v, bool):
    return v

  bv = _BOOLS.get(v)
  tas.check_is_not_none(bv, msg=f'Unable to convert to bool: {v}')

  return bv


def cast(v, vtype):
  return vtype(v) if v is not None else v


def xwrap_fn(fn, *args, **kwargs):

  def fwrap():
    try:
      return fn(*args, **kwargs)
    except Exception as e:
      tb = traceback.format_exc()
      alog.error(f'Exception while running wrapped function: {e}\n{tb}')

  return fwrap


def add_bool_argument(parser, name, defval, help=None):
  parser.add_argument(f'--{name}', dest=name, action='store_true',
                      help=f'Enable {help or name}' if help else None)
  parser.add_argument(f'--no-{name}', dest=name, action='store_false',
                      help=f'Disable {help or name}' if help else None)
  parser.set_defaults(**{name: defval})


def state_update(path, **kwargs):
  if os.path.isfile(path):
    with open(path, mode='rb') as f:
      state = pickle.load(f)
  else:
    state = dict()

  if kwargs:
    state.update(kwargs)
    with fow.FileOverwrite(path, mode='wb') as f:
      pickle.dump(state, f, protocol=pickle_proto())

  return state


def args(*uargs, **kwargs):
  return uargs, kwargs


def maybe_call(obj, name, *args, **kwargs):
  fn = getattr(obj, name, None)

  return fn(*args, **kwargs) if fn is not None else NONE


def maybe_call_dv(obj, name, defval, *args, **kwargs):
  fn = getattr(obj, name, None)

  return fn(*args, **kwargs) if fn is not None else defval


def unique(data):
  udata = collections.defaultdict(lambda: array.array('L'))
  for i, v in enumerate(data):
    udata[v].append(i)

  return udata


def enumerate_files(path, matcher, fullpath=False):
  for fname in sorted(os.listdir(path)):
    if matcher(fname):
      yield fname if not fullpath else os.path.join(path, fname)


def re_enumerate_files(path, rex, fullpath=False):
  for fname in sorted(os.listdir(path)):
    m = re.match(rex, fname)
    if m:
      mpath = fname if not fullpath else os.path.join(path, fname)
      yield mpath, m


def fgzip(src, dest):
  with open(src, mode='rb') as infd:
    with gzip.open(dest, mode='wb') as outfd:
      shutil.copyfileobj(infd, outfd)


def fgunzip(src, dest):
  with gzip.open(src, mode='rb') as infd:
    with open(dest, mode='wb') as outfd:
      shutil.copyfileobj(infd, outfd)


def fbunzip2(src, dest):
  with bz2.open(src, mode='rb') as infd:
    with open(dest, mode='wb') as outfd:
      shutil.copyfileobj(infd, outfd)


def drop_ext(path, exts):
  xpath, ext = os.path.splitext(path)

  return xpath if ext in as_sequence(exts) else path


def path_split(path):
  path_parts = []
  while True:
    parts = os.path.split(path)
    if parts[0] == path:
      path_parts.append(parts[0])
      break
    elif parts[1] == path:
      path_parts.append(parts[1])
      break
    else:
      path = parts[0]
      path_parts.append(parts[1])

  path_parts.reverse()

  return path_parts


def is_newer_file(path, other):
  return os.stat(path).st_mtime > os.stat(other).st_mtime


def link_or_copy(src_path, dest_path):
  try:
    os.link(src_path, dest_path)

    return dest_path
  except OSError as ex:
    alog.debug(f'Harklink failed from "{src_path}" to "{dest_path}", trying symlink. ' \
               f'Error was: {ex}')

  try:
    os.symlink(src_path, dest_path)

    return dest_path
  except OSError:
    alog.debug(f'Symlink failed from "{src_path}" to "{dest_path}", going to copy. ' \
               f'Error was: {ex}')

  shutil.copyfile(src_path, dest_path)
  shutil.copystat(src_path, dest_path)

  return dest_path

