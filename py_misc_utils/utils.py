import array
import binascii
import collections
import copy
import importlib.util
import inspect
import logging
import os
import random
import re
import string
import struct
import sys
import threading
import time
import types
import yaml

import numpy as np


class _None(object):
  pass


NONE = _None()


_LOG_LEVELS = dict(
  DEBUG=logging.DEBUG,
  INFO=logging.INFO,
  WARNING=logging.WARNING,
  ERROR=logging.ERROR,
  CRITICAL=logging.CRITICAL,
  FATAL=logging.FATAL,
)


def _format_message(record):
  try:
    record_message = '%s' % (record.msg % record.args)
  except TypeError:
    record_message = record.msg

  return record_message


class _GlogFormatter(logging.Formatter):
  LEVEL_MAP = {
    # FATAL maps to CRITICAL within the logging module, so avoid key dup error.
    logging.FATAL: 'F',
    logging.ERROR: 'E',
    logging.WARN: 'W',
    logging.INFO: 'I',
    logging.DEBUG: 'D',
  }

  def __init__(self):
    logging.Formatter.__init__(self)

  def format(self, record):
    try:
      level = _GlogFormatter.LEVEL_MAP[record.levelno]
    except KeyError:
      level = '?'

    date = time.localtime(record.created)
    date_usec = (record.created - int(record.created)) * 1e6
    record_hdr = '%c%02d%02d %02d:%02d:%02d.%06d %s %s:%d] ' % (
      level, date.tm_mon, date.tm_mday, date.tm_hour, date.tm_min,
      date.tm_sec, date_usec,
      record.process if record.process is not None else '?????',
      record.filename, record.lineno)

    lines = []
    for ln in _format_message(record).split('\n'):
      lines.append(record_hdr + ln)

    return '\n'.join(lines)


def setup_logging(log_level='INFO', log_file=None):
  log_level = _LOG_LEVELS[log_level]
  handlers = []

  h = logging.StreamHandler()
  h.setLevel(log_level)
  h.setFormatter(_GlogFormatter())
  handlers.append(h)

  if log_file:
    h = logging.FileHandler(log_file)
    h.setLevel(log_level)
    h.setFormatter(formatter)
    handlers.append(h)

  logging.basicConfig(level=log_level, handlers=handlers)


def maybe_add_path(plist, path):
  if path not in plist:
    plist.append(path)


def load_module(path, install=False):
  maybe_add_path(sys.path, os.path.dirname(os.path.abspath(path)))

  modname = os.path.splitext(os.path.basename(path))[0]
  modspec = importlib.util.spec_from_file_location(modname, path)
  mod = importlib.util.module_from_spec(modspec)

  modspec.loader.exec_module(mod)

  if install and modname not in sys.modules:
    sys.modules[modname] = mod

  return mod


def make_ntuple(ntc, args):
  targs = []
  for f in ntc._fields:
    fv = args.get(f, NONE)
    if fv is NONE:
      fv = ntc._field_defaults.get(f, NONE)

    targs.append(None if fv is NONE else fv)

  return ntc._make(targs)


def enum_values(obj):
  if isinstance(obj, dict):
    for k, v in obj.items():
      yield k, v
  else:
    for k in dir(obj):
      yield k, getattr(obj, k)


def fname():
  return inspect.currentframe().f_back.f_code.co_name


def get_back_frame(level):
  frame = inspect.currentframe()
  while frame is not None and level >= 0:
    frame = frame.f_back
    level -= 1

  return frame

def parent_coords(level=1):
  frame = get_back_frame(level + 1)

  return frame.f_code.co_filename, frame.f_lineno


def parent_locals(level=1):
  frame = get_back_frame(level + 1)

  return frame.f_locals


def cname(obj):
  cls = classof(obj)
  return cls.__name__ if cls is not None else None


def func_name(func):
  fname = getattr(func, '__name__', None)

  return fname if fname is not None else cname(func)


def is_builtin_function(obj):
  return isinstance(obj, types.BuiltinFunctionType)


def is_subclass(cls, cls_group):
  return inspect.isclass(cls) and issubclass(cls, cls_group)


def classof(obj):
  return obj if inspect.isclass(obj) else getattr(obj, '__class__', None)


def _stri(l, d):
  s = d.get(id(l), NONE)
  if s is None:
    return '...'
  elif s is not NONE:
    return s

  d[id(l)] = None
  if isinstance(l, (tuple, list)):
    sl = ', '.join(_stri(x, d) for x in l)

    result = '[' + sl + ']' if isinstance(l, list) else '(' + sl + ')'
  elif isinstance(l, dict):
    result = '{' + ', '.join(f'{k}: {_stri(v, d)}' for k, v in l.items()) + '}'
  else:
    result = f'"{l}"' if isinstance(l, str) else str(l)

  d[id(l)] = result

  return result


def stri(l):
  return _stri(l, dict())


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
      tb = b.get(k, None)
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

class Context(object):

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
      cfg = yaml.load(cf, Loader=yaml.Loader)
  else:
    cfg = dict()

  for k, v in kwargs:
    if v is not None:
      cfg[k] = v

  return cfg


def fatal(msg, exc=RuntimeError):
  logging.error(msg)

  raise exc(msg)


def assert_instance(msg, t, ta):
  if not isinstance(t, ta):
    parts = [msg, f': {cname(t)} is not ']
    if isinstance(ta, (list, tuple)):
      parts.append('one of (')
      parts.append(', '.join(cname(x) for x in ta))
      parts.append(')')
    else:
      parts.append(f'a {cname(ta)}')

    fatal(''.join(parts))


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


class CtxManager(object):

  def __init__(self, infn, outfn):
    self._infn = infn
    self._outfn = outfn

  def __enter__(self):
    return self._infn()

  def __exit__(self, *exc):
    return self._outfn(*exc)


class Obj(object):

  def __init__(self, **kwargs):
    for k, v in kwargs.items():
      setattr(self, k, v)

  def as_dict(self):
    ad = dict()
    for k, v in self.__dict__.items():
      if isinstance(v, Obj):
        v = v.as_dict()
      elif isinstance(v, (list, tuple)):
        vals = []
        for x in v:
          if isinstance(x, Obj):
            x = x.as_dict()
          vals.append(x)

        v = type(v)(vals)
      elif isinstance(v, dict):
        vd = dict()
        for z, x in v.items():
          if isinstance(x, Obj):
            x = x.as_dict()
          vd[z] = x

        v = vd

      ad[k] = v

    return ad


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
    parts.append(nmapfn(data[lastpos: ]))

  return ''.join(parts) if join else parts


def lindex(l, e, start=0, end=None):
  try:
    return l.index(e, start, end if end is not None else len(l))
  except ValueError:
    return -1


def as_sequence(v, t=tuple):
  if isinstance(v, t):
    return v
  if isinstance(t, (list, tuple)):
    return t[0]([v])

  return t([v])


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


def dict_rget(sdict, path, defval=None, sep='/'):
  if not isinstance(path, (list, tuple)):
    path = path.strip(sep).split(sep)

  result = sdict
  for key in path:
    if not isinstance(result, dict):
      return defval
    result = result.get(key, defval)

  return result


def compile(code, syms, env=None, vals=None, lookup_fn=None, delim=None):
  xenv = dict() if env is None else env
  if vals is not None or lookup_fn is not None:
    xcode = template_replace(code, vals=vals, lookup_fn=lookup_fn, delim=delim)
  else:
    xcode = code

  exec(xcode, xenv)

  return tuple(xenv[s] for s in as_sequence(syms))


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


def env(name, defval, vtype=None):
  ev = os.getenv(name, None)
  vtype = type(defval) if vtype is None else vtype

  return defval if ev is None else vtype(ev)


MAJOR = 1
MINOR = -1

def squeeze(shape, keep_dims=0, sdir=MAJOR):
  sshape = list(shape)
  if sdir == MAJOR:
    while len(sshape) > keep_dims and sshape[0] == 1:
      sshape = sshape[1: ]
  elif sdir == MINOR:
    while len(sshape) > keep_dims and sshape[-1] == 1:
      sshape = sshape[: -1]
  else:
    fatal(f'Unknown squeeze direction: {sdir}')

  return type(shape)(sshape)


def flat2shape(data, shape):
  assert len(data) == np.prod(shape), f'Shape {shape} is unsuitable for a {len(data)} long array'

  # For an Mx...xK input shape, return a M elements (nested) list.
  for n in reversed(shape[1: ]):
    data = [data[i: i + n] for i in range(0, len(data), n)]

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


def mlog(msg, level=logging.DEBUG):
  # No reason to split the message in lines, as the GLOG formatter alreay handles it.
  if logging.getLogger().isEnabledFor(level):
    logging.log(level, msg() if callable(msg) else msg)


def seq_rewrite(seq, sd):
  return type(seq)(sd.get(s, s) for s in seq)


def varint_encode(v, encbuf):
  if isinstance(v, (int, np.integer)):
    if v < 0:
      raise RuntimeError(f'Cannot encode negative values: {v}')

    cv = v
    while (cv & ~0x7f) != 0:
      encbuf.append(0x80 | (cv & 0x7f))
      cv >>= 7

    encbuf.append(cv & 0x7f)
  elif isinstance(v, (list, tuple, array.array, np.ndarray)):
    for xv in v:
      varint_encode(xv, encbuf)
  else:
    raise RuntimeError(f'Unsupported type: {v} ({type(v)})')


def _varint_decode(encbuf, pos):
  value, cpos, nbits = 0, pos, 0
  while True:
    b = encbuf[cpos]
    value |= (b & 0x7f) << nbits
    nbits += 7
    cpos += 1

    if (b & 0x80) == 0:
      break
    if cpos >= len(encbuf):
      fatal(f'Invalid varint encode buffer content')

  return value, cpos


def varint_decode(encbuf):
  values, cpos = [], 0
  while cpos < len(encbuf):
    value, cpos = _varint_decode(encbuf, cpos)
    values.append(value)

  return values


def dfetch(d, *args):
  return tuple(d[n] for n in args)


def enum_set(l, s, present):
  ss = set(s) if not isinstance(s, set) else s
  for x in l:
    if present == (x in ss):
      yield x


class RevGen(object):

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


class _FnDict(object):

  def __init__(self, fn):
    self._fn = fn

  def __getitem__(self, key):
    m = re.match(r'([^:]+):(.*)', key)
    if m:
      lkey, defval = m.group(1), m.group(2)
    else:
      lkey, defval = key, None

    return self._fn(lkey, defval=defval)

  @staticmethod
  def dict_lookup_fn(d):
    def fn(k, defval=None):
      v = d.get(k, defval)
      if v is None: fatal(f'String template replace missing value for key: {k}')

      return v

    return fn


def template_replace(st, vals=None, lookup_fn=None, delim=None):

  class _Template(string.Template):

    # Allow for brace ID with the format ${ID:DEFAULT_VALUE}.
    braceidpattern = r'((?a:[_a-z][_a-z0-9]*)(:[^}]*)?)'
    delimiter = delim or '$'

  if lookup_fn is None:
    lookup_fn = _FnDict.dict_lookup_fn(vals)

  return _Template(st).substitute(_FnDict(lookup_fn))


def strip_split(svalue, delim, maxsplit=-1):
  return [x.strip() for x in svalue.split(delim, maxsplit=maxsplit)]

