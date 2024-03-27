import inspect
import sys
import types


def _fn_lookup(frame, name):
  xns, xname = None, name
  while True:
    dpos = xname.find('.')
    if dpos > 0:
      cname, fname = xname[: dpos], xname[dpos + 1: ]
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
    frame = get_back_frame(back + 1)

  return _fn_lookup(frame, frame.f_code.co_qualname)


def fetch_args(func, locs, skips=()):
  sig = inspect.signature(func)

  args, kwargs = [], dict()
  for n, p in sig.parameters.items():
    if n not in skips:
      if p.kind == p.POSITIONAL_ONLY:
        args.append(locs[n])
      elif p.kind == p.POSITIONAL_OR_KEYWORD:
        if p.default is inspect.Signature.empty:
          args.append(locs[n])
        else:
          kwargs[n] = locs.get(n, p.default)
      else:
        pv = locs.get(n, p.default)
        if pv is not inspect.Signature.empty:
          kwargs[n] = pv

  return args, kwargs


def get_back_frame(level):
  frame = inspect.currentframe()
  while frame is not None and level >= 0:
    frame = frame.f_back
    level -= 1

  return frame


def parent_locals(level=1):
  frame = get_back_frame(level + 1)

  return frame.f_locals


def parent_coords(level=1):
  frame = get_back_frame(level + 1)

  return frame.f_code.co_filename, frame.f_lineno

