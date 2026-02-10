import inspect
import itertools
import sys
import types

from . import assert_checks as tas
from . import traceback as tb


_NONE = object()


def classof(obj):
  return obj if inspect.isclass(obj) else getattr(obj, '__class__', None)


def moduleof(obj):
  cls = classof(obj)

  return getattr(cls, '__module__', None) if cls is not None else None


def cname(obj):
  cls = classof(obj)

  return cls.__name__ if cls is not None else None


def func_name(func):
  fname = getattr(func, '__name__', None)

  return fname if fname is not None else cname(func)


_BUILTIN_NAMES = {'__builtin__', 'builtins'}

def _qual_name(obj, builtin_strip=False):
  module = getattr(obj, '__module__', None)
  name = getattr(obj, '__qualname__', None)
  if name is None:
    name = getattr(obj, '__name__', None)
    tas.check_is_not_none(name, msg=f'Unable to reference name: {obj}')
  if module is not None and not (builtin_strip and module in _BUILTIN_NAMES):
    name = module + '.' + name

  return name


def qual_name(obj, builtin_strip=False):
  if (inspect.isclass(obj) or inspect.isfunction(obj) or
      inspect.ismethod(obj) or inspect.ismodule(obj)):
    ref = obj
  else:
    ref = obj.__class__

  return _qual_name(ref, builtin_strip=builtin_strip)


def qual_mro(obj, builtin_strip=False):
  cls = obj if inspect.isclass(obj) else obj.__class__

  for scls in cls.__mro__:
    yield _qual_name(scls, builtin_strip=builtin_strip)


def is_subclass(cls, cls_group):
  return inspect.isclass(cls) and issubclass(cls, cls_group)


def _fn_lookup(frame, name):
  xns, xname = None, name
  while True:
    dpos = xname.find('.')
    if dpos > 0:
      cname, fname = xname[: dpos], xname[dpos + 1:]
      if cname == '<locals>':
        code = getattr(xns, '__code__', None)
        if code is not None:
          for cv in code.co_consts:
            if inspect.iscode(cv) and cv.co_name == fname:
              return types.FunctionType(cv, frame.f_globals, fname)

        return None
      else:
        xns = frame.f_globals[cname] if xns is None else getattr(xns, cname)
        xname = fname
    else:
      xns = frame.f_globals[xname] if xns is None else getattr(xns, xname)
      break

  return xns


def get_caller_function(back=0, frame=None):
  if frame is None:
    frame = tb.get_frame(back + 1)

  return _fn_lookup(frame, frame.f_code.co_qualname)


def current_module():
  return inspect.getmodule(tb.get_frame(1))


def fetch_args(func, locs, input_args=()):
  sig = inspect.signature(func)

  def args_append(args, n):
    if len(args) < len(input_args):
      args.append(input_args[len(args)])
    else:
      pv = locs.get(n, _NONE)
      if pv is not _NONE:
        args.append(pv)
      elif args:
        alog.xraise(RuntimeError, f'Missing argument: {n}')

  def kwargs_assign(kwargs, n, p):
    pv = locs.get(n, _NONE)
    if pv is _NONE or (pv is None and p.default is not inspect.Signature.empty):
      pv = p.default
    if pv is not inspect.Signature.empty:
      kwargs[n] = pv

  args, kwargs = [], dict()
  for n, p in sig.parameters.items():
    if p.kind == p.POSITIONAL_ONLY:
      args_append(args, n)
    elif p.kind == p.POSITIONAL_OR_KEYWORD:
      if p.default is inspect.Signature.empty:
        args_append(args, n)
      else:
        kwargs_assign(kwargs, n, p)
    else:
      kwargs_assign(kwargs, n, p)

  return args, kwargs


def fetch_call(func, locs, input_args=()):
  args, kwargs = fetch_args(func, locs, input_args=input_args)

  return func(*args, **kwargs)


def get_fn_kwargs(args, func, prefix=None, roffset=None):
  aspec = inspect.getfullargspec(func)

  sdefaults = aspec.defaults or ()
  sargs = aspec.args or ()
  ndelta = len(sargs) - len(sdefaults)

  fnargs = dict()
  for i, an in enumerate(sargs):
    if i != 0 or an != 'self':
      nn = f'{prefix}.{an}' if prefix else an
      di = i - ndelta
      if di >= 0:
        fnargs[an] = args.get(nn, sdefaults[di])
      elif roffset is not None and i >= roffset:
        aval = args.get(nn, _NONE)
        tas.check(aval is not _NONE,
                  msg=f'The "{an}" argument must be present as "{nn}": {args}')
        fnargs[an] = aval

  if aspec.kwonlyargs:
    for an in aspec.kwonlyargs:
      nn = f'{prefix}.{an}' if prefix else an
      aval = args.get(nn, aspec.kwonlydefaults.get(an, inspect.Signature.empty))
      if aval is not inspect.Signature.empty:
        fnargs[an] = aval

  return fnargs


def get_defaulted_params(func):
  sig = inspect.signature(func)

  return tuple(p for p in sig.parameters.values()
               if p.default is not inspect.Signature.empty)


def parent_locals(level=0):
  frame = tb.get_frame(level + 2)

  return frame.f_locals


def parent_globals(level=0):
  frame = tb.get_frame(level + 2)

  return frame.f_globals


def parent_coords(level=0):
  frame = tb.get_frame(level + 2)

  return frame.f_code.co_filename, frame.f_lineno


def class_slots(cls):
  slots = itertools.chain.from_iterable(getattr(mcls, '__slots__', []) for mcls in cls.__mro__)

  return tuple(slots)

