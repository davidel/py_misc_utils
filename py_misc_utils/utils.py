import array
import ast
import collections
import copy
import datetime
import inspect
import json
import logging
import math
import os
import pickle
import random
import re
import sys
import threading
import time
import types
import yaml

import numpy as np

from . import alog
from . import assert_checks as tas
from . import core_utils as cu
from . import file_overwrite as fow
from . import gfs
from . import mmap as mm
from . import obj
from . import split as sp
from . import template_replace as tr
from . import traceback as tb


_NONE = object()


def pickle_proto():
  return getenv('PICKLE_PROTO', dtype=int, defval=pickle.HIGHEST_PROTOCOL)


def ident(x):
  return x


def make_ntuple(ntc, args):
  targs = []
  for f in ntc._fields:
    fv = args.get(f, _NONE)
    if fv is _NONE:
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


def func_name(func):
  fname = getattr(func, '__name__', None)

  return fname if fname is not None else cname(func)


def is_subclass(cls, cls_group):
  return inspect.isclass(cls) and issubclass(cls, cls_group)


def classof(obj):
  return obj if inspect.isclass(obj) else getattr(obj, '__class__', None)


def moduleof(obj):
  return getattr(classof(obj), '__module__', None)


def infer_str(v):
  return infer_value(v) if isinstance(v, str) else v


def _stri(obj, seen, float_fmt):
  oid = id(obj)
  sres = seen.get(oid, _NONE)
  if sres is None:
    return '...'
  elif sres is not _NONE:
    return sres

  seen[oid] = None
  if isinstance(obj, str):
    obj_str = obj.replace('"', '\\"')
    result = f'"{obj_str}"'
  elif isinstance(obj, float):
    result = f'{obj:{float_fmt}}'
  elif isinstance(obj, bytes):
    result = obj.decode()
  elif is_namedtuple(obj):
    result = str(obj)
  elif isinstance(obj, (tuple, list, types.GeneratorType)):
    sl = ', '.join(_stri(x, seen, float_fmt) for x in obj)

    result = '[' + sl + ']' if isinstance(obj, list) else '(' + sl + ')'
  elif isinstance(obj, dict):
    result = '{' + ', '.join(f'{k}={_stri(v, seen, float_fmt)}' for k, v in obj.items()) + '}'
  elif hasattr(obj, '__dict__'):
    # Drop the braces around the __dict__ output, and use the "Classname(...)" format.
    drepr = _stri(obj.__dict__, seen, float_fmt)
    result = f'{cname(obj)}({drepr[1: -1]})'
  else:
    result = str(obj)

  seen[oid] = result

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
  v = sdict.get(name, _NONE)
  if v is _NONE:
    return defval

  if dtype is None and defval is not None:
    dtype = type(defval)

  return dtype(v) if v is not None and dtype is not None else v


def mget(d, *args, as_dict=False):
  margs = cu.expand_strings(*args)
  if as_dict:
    return {f: d.get(f) for f in margs}
  else:
    return tuple(d.get(f) for f in margs)


def getvar(obj, name, defval=None):
  return obj.get(name, defval) if isinstance(obj, dict) else getattr(obj, name, defval)


def get_property(obj, name, defval=None):
  p = getattr(obj, name, _NONE)
  if p is _NONE:
    return defval

  return p() if callable(p) else p


def dict_subset(d, *keys):
  mkeys = cu.expand_strings(*keys)
  subd = dict()
  for k in mkeys:
    v = d.get(k, _NONE)
    if v is not _NONE:
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
    args = [xargs.get(name) for name in cu.expand_strings(names)]
  else:
    args = [kwargs.pop(name, None) for name in cu.expand_strings(names)]

  return tuple(args)


def index_select(arr, idx):
  return arr[idx] if isinstance(idx, slice) else [arr[i] for i in idx]


def append_if_missing(arr, elem):
  if elem not in arr:
    arr.append(elem)


def state_override(obj, state, keys):
  for key in keys:
    sv = state.get(key, _NONE)
    if sv is not _NONE:
      curv = getattr(obj, key, _NONE)
      if curv is not _NONE:
        setattr(obj, key, sv)


def resplit(csstr, sep):
  return sp.split(csstr, r'\s*' + sep + r'\s*')


def comma_split(csstr):
  return sp.split(csstr, r'\s*,\s*')


def ws_split(data):
  return sp.split(data, r'\s+')


def name_values(base_name, values, force_expand=False):
  names = []
  if isinstance(values, (list, tuple)) or force_expand:
    if len(values) == 1:
      names.append((base_name, values[0]))
    else:
      for i, v in enumerate(values):
        names.append((f'{base_name}.{i}', v))
  else:
    names.append((base_name, values))

  return tuple(names)


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
    with gfs.open(cfg_file, mode='r') as cf:
      cfg = yaml.safe_load(cf)
  else:
    cfg = dict()

  for k, v in kwargs.items():
    if v is not None:
      cfg[k] = v

  return cfg


def write_config(cfg, dest, **kwargs):
  default_flow_style = kwargs.get('default_flow_style', False)

  with fow.FileOverwrite(dest, mode='wt') as df:
    yaml.dump(cfg, df, default_flow_style=default_flow_style, **kwargs)


def config_to_string(cfg, **kwargs):
  default_flow_style = kwargs.get('default_flow_style', False)

  return yaml.dump(cfg, default_flow_style=default_flow_style, **kwargs)


def parse_config(cfg, **kwargs):
  if cfg.startswith('{'):
    cfgd = json.loads(cfg)
  elif fdctx := gfs.maybe_open(cfg, mode='r'):
    with fdctx as fp:
      cfgd = yaml.safe_load(fp)
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


def make_object(**kwargs):
  return obj.Obj(**kwargs)


def make_object_recursive(**kwargs):
  for k, v in kwargs.items():
    if isinstance(v, dict):
      kwargs[k] = make_object_recursive(**v)

  return make_object(**kwargs)


def locals_capture(locs, exclude=None):
  exclude = set(cu.expand_strings(value_or(exclude, 'self')))

  return make_object(**{k: v for k, v in locs.items() if k not in exclude})


def sreplace(rex, data, mapfn, nmapfn=None, join=True):
  nmapfn = nmapfn if nmapfn is not None else ident

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
  ivalue = ddict.get(name, _NONE)
  if ivalue is not _NONE:
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

  return tuple(env.get(s) for s in cu.expand_strings(syms))


def run(path, fnname, *args, **kwargs):
  compile_args, = pop_kwargs(kwargs, 'compile_args')

  fn, = compile(mm.file_view(path), fnname, **(compile_args or dict()))

  return fn(*args, **kwargs)


def unpack_n(l, n, defval=None):
  l = as_sequence(l)

  return tuple(l[:n] if len(l) >= n else l + [defval] * (n - len(l)))


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
  tas.check_eq(len(data), np.prod(shape),
               msg=f'Shape {shape} is unsuitable for a {len(data)} long array')

  # For an Mx...xK input shape, return a M elements (nested) list.
  for n in reversed(shape[1:]):
    data = [data[i: i + n] for i in range(len(data), n)]

  return data


def shape2flat(data, shape):
  for _ in range(len(shape) - 1):
    tas.check(hasattr(data, '__iter__'), msg=f'Wrong data type: {type(data)}')
    ndata = []
    for av in data:
      tas.check(hasattr(av, '__iter__'), msg=f'Wrong data type: {type(data)}')
      ndata.extend(av)

    data = ndata

  tas.check_eq(len(data), np.prod(shape),
               msg=f'Shape {shape} is unsuitable for a {len(data)} long array')

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


def dfetch(d, *args):
  return tuple(d[n] for n in args)


def enum_set(l, s, present):
  ss = set(s) if not isinstance(s, set) else s
  for x in l:
    if present == (x in ss):
      yield x


class RevGen:

  def __init__(self, fmt=None):
    self._fmt = value_or(fmt, '{name}_{ver}')
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


def to_type(v, vtype):
  return vtype(ast.literal_eval(v)) if isinstance(v, str) else vtype(v)


def to_bool(v):
  return to_type(v, bool)


def cast(v, vtype):
  return to_type(v, vtype) if v is not None else None


def infer_value(v, vtype=None, allow_exec=False):
  if vtype is not None:
    return to_type(v, vtype)

  uv = sp.unquote(v)
  if v is not uv:
    if v[0] in '"\'':
      return uv
    elif v[0] in '[(':
      values = [infer_value(part, allow_exec=allow_exec) for part in comma_split(uv)]

      return tuple(values) if v[0] == '(' else values
    elif v[0] == '{':
      pdict, pargs = parse_dict(uv, allow_args=True, allow_exec=allow_exec)
      if not pdict:
        return set(pargs)
      if pargs:
        alog.xraise(ValueError, f'Cannot return both arguments and dictionary: {pargs} {pdict}')

      return pdict
    elif v[0] == '`':
      tas.check(allow_exec, msg=f'Exec not allowed: {uv}')

      pdict, pargs = parse_dict(uv, allow_args=True, allow_exec=allow_exec)
      tas.check_ge(len(pargs), 2, msg=f'Wrong exec args: {uv}')

      return run(*pargs, **pdict)

  try:
    return ast.literal_eval(v)
  except:
    return v


def parse_dict(data, vtype=None, allow_args=False, allow_exec=False):
  ma_dict, ma_args = dict(), []
  for part in comma_split(data):
    parts = resplit(part, '=')
    if len(parts) == 2:
      name, value = parts
      ma_dict[name] = infer_value(value, vtype=vtype, allow_exec=allow_exec)
    elif len(parts) == 1:
      if not allow_args:
        alog.xraise(ValueError, f'Arguments parsing disabled: {data}')
      if ma_dict:
        alog.xraise(ValueError, f'Arguments can appear only at the beginning: {data}')
      ma_args.append(infer_value(parts[0], vtype=vtype, allow_exec=allow_exec))
    else:
      alog.xraise(ValueError, f'Syntax error: {part}')

  return (ma_dict, tuple(ma_args)) if allow_args else ma_dict


def add_bool_argument(parser, name, defval, help=None):
  parser.add_argument(f'--{name}', dest=name, action='store_true',
                      help=f'Enable {help or name}' if help else None)
  parser.add_argument(f'--no-{name}', dest=name, action='store_false',
                      help=f'Disable {help or name}' if help else None)
  parser.set_defaults(**{name: defval})


def state_update(path, **kwargs):
  if sfile := gfs.maybe_open(path, mode='rb'):
    with sfile as fd:
      state = pickle.load(fd)
  else:
    state = dict()

  if kwargs:
    state.update(kwargs)
    with fow.FileOverwrite(path, mode='wb') as f:
      pickle.dump(state, f, protocol=pickle_proto())

  return state


def args(*uargs, **kwargs):
  return uargs, kwargs

