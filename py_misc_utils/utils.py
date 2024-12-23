import array
import collections
import datetime
import inspect
import json
import logging
import math
import os
import pickle
import re
import sys
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


def fname():
  return tb.get_frame(1).f_code.co_name


def cname(obj):
  cls = classof(obj)
  return cls.__name__ if cls is not None else None


def func_name(func):
  fname = getattr(func, '__name__', None)

  return fname if fname is not None else cname(func)


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
  elif cu.is_namedtuple(obj):
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


def mget(d, *args, as_dict=False):
  margs = expand_strings(*args)
  if as_dict:
    return {f: d.get(f) for f in margs}
  else:
    return tuple(d.get(f) for f in margs)


def getvar(obj, name, defval=None):
  return obj.get(name, defval) if isinstance(obj, dict) else getattr(obj, name, defval)


def dict_subset(d, *keys):
  mkeys = expand_strings(*keys)
  subd = dict()
  for k in mkeys:
    v = d.get(k, _NONE)
    if v is not _NONE:
      subd[k] = v

  return subd


def dict_setmissing(d, **kwargs):
  kwargs.update(d)

  return kwargs


def pop_kwargs(kwargs, names, args_key=None):
  xargs = kwargs.pop(args_key or '_', None)
  if xargs is not None:
    args = [xargs.get(name) for name in expand_strings(names)]
  else:
    args = [kwargs.pop(name, None) for name in expand_strings(names)]

  return tuple(args)


def append_if_missing(arr, elem):
  if elem not in arr:
    arr.append(elem)


def resplit(csstr, sep):
  return sp.split(csstr, r'\s*' + sep + r'\s*')


def comma_split(csstr):
  return sp.split(csstr, r'\s*,\s*')


def ws_split(data):
  return sp.split(data, r'\s+')


def expand_strings(*args):
  margs = []
  for arg in args:
    if isinstance(arg, (list, tuple, types.GeneratorType)):
      margs.extend(arg)
    else:
      margs.extend(comma_split(arg))

  return tuple(margs)


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
  if not cfg.startswith('{'):
    # It must be either a dictionary in YAML format, or a valid path.
    with gfs.open(cfg, mode='r') as fd:
      cfg = fd.read()

  cfgd = yaml.safe_load(cfg)

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


def make_object(**kwargs):
  return obj.Obj(**kwargs)


def make_object_recursive(**kwargs):
  for k, v in kwargs.items():
    if isinstance(v, dict):
      kwargs[k] = make_object_recursive(**v)

  return make_object(**kwargs)


def locals_capture(locs, exclude=None):
  exclude = set(expand_strings(value_or(exclude, 'self')))

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

  return tuple(env.get(s) for s in expand_strings(syms))


def run(path, fnname, *args, **kwargs):
  compile_args, = pop_kwargs(kwargs, 'compile_args')

  fn, = compile(mm.file_view(path), fnname, **(compile_args or dict()))

  return fn(*args, **kwargs)


def unpack_n(l, n, defval=None):
  l = as_sequence(l)

  return tuple(l[:n] if len(l) >= n else l + [defval] * (n - len(l)))


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
  if alog.level_active(level):
    alog.log(level, msg() if callable(msg) else msg)


def seq_rewrite(seq, sd):
  return type(seq)(sd.get(s, s) for s in seq)


def dfetch(d, *args):
  return tuple(d[n] for n in args)


def enum_set(l, s, present):
  ss = set(s) if not isinstance(s, set) else s
  for x in l:
    if present == (x in ss):
      yield x


def numel(t):
  sp = cu.get_property(t, 'shape')

  return np.prod(sp) if sp is not None else len(t)


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


def sleep_until(date, msg=None):
  now = datetime.datetime.now(tz=date.tzinfo)
  if date > now:
    if msg:
      alog.info(msg)
    time.sleep(date.timestamp() - now.timestamp())


def to_type(v, vtype):
  return vtype(yaml.safe_load(v)) if isinstance(v, str) else vtype(v)


def to_bool(v):
  return to_type(v, bool)


def cast(v, vtype):
  return to_type(v, vtype) if v is not None else None


def infer_value(v, vtype=None):
  return yaml.safe_load(v) if vtype is None else to_type(v, vtype)


def parse_dict(data):
  return yaml.safe_load(data)


def parse_args(in_args):
  args, kwargs = [], dict()
  for arg in in_args:
    parts = resplit(arg, '=')
    if len(parts) == 2:
      kwargs[parts[0]] = yaml.safe_load(parts[1])
    elif len(parts) == 1:
      args.append(yaml.safe_load(parts[0]))
    else:
      alog.xraise(ValueError, f'Syntax error: {arg}')

  return args, kwargs


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

