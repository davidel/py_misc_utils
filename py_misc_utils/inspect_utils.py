import inspect
import sys
import types

from . import assert_checks as tas
from . import traceback as tb


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


def fetch_args(func, locs):
  sig = inspect.signature(func)

  def args_append(args, n):
    pv = locs.get(n, inspect.Signature.empty)
    if pv is not inspect.Signature.empty:
      args.append(pv)
    else:
      fself = getattr(func, '__self__', None)
      if args or fself is not None:
        alog.xraise(RuntimeError, f'Missing argument: {n}')


  args, kwargs = [], dict()
  for n, p in sig.parameters.items():
    if p.kind == p.POSITIONAL_ONLY:
      args_append(args, n)
    elif p.kind == p.POSITIONAL_OR_KEYWORD:
      if p.default is inspect.Signature.empty:
        args_append(args, n)
      else:
        kwargs[n] = locs.get(n, p.default)
    else:
      pv = locs.get(n, p.default)
      if pv is not inspect.Signature.empty:
        kwargs[n] = pv

  return args, kwargs


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
        aval = args.get(nn, inspect.Signature.empty)
        tas.check(aval is not inspect.Signature.empty,
                  msg=f'The "{an}" argument must be present as "{nn}": {args}')
        fnargs[an] = aval

  if aspec.kwonlyargs:
    for an in aspec.kwonlyargs:
      nn = f'{prefix}.{an}' if prefix else an
      aval = args.get(nn, aspec.kwonlydefaults.get(an, inspect.Signature.empty))
      if aval is not inspect.Signature.empty:
        fnargs[an] = aval

  return fnargs


def parent_locals(level=0):
  frame = tb.get_frame(level + 2)

  return frame.f_locals


def parent_globals(level=0):
  frame = tb.get_frame(level + 2)

  return frame.f_globals


def parent_coords(level=0):
  frame = tb.get_frame(level + 2)

  return frame.f_code.co_filename, frame.f_lineno


def qual_name(obj):
  if (inspect.isclass(obj) or inspect.isfunction(obj) or
      inspect.ismethod(obj) or inspect.ismodule(obj)):
    ref = obj
  else:
    ref = obj.__class__

  module = getattr(ref, '__module__', None)
  name = getattr(ref, '__qualname__', None)
  if name is None:
    name = getattr(ref, '__name__', None)
    tas.check_is_not_none(name, msg=f'Unable to reference name: {ref}')
  if module is not None and module != '__builtin__':
    name = module + '.' + name

  return name


def is_subclass(cls, cls_group):
  return inspect.isclass(cls) and issubclass(cls, cls_group)

