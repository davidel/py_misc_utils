"""Utilities for inspecting Python objects, classes, functions, and frames.

This module provides helper functions for runtime introspection, including
qualified name resolution, caller frame inspection, argument extraction from
locals, class slot enumeration, and MRO traversal.

Functions:
  classof, moduleof, cname, func_name, qual_name, qual_mro,
  is_subclass, get_caller_function, current_module, fetch_args,
  fetch_call, get_fn_kwargs, get_defaulted_params, parent_locals,
  parent_globals, parent_coords, class_slots
"""

import inspect
import itertools
import types

from . import alog
from . import assert_checks as tas
from . import traceback as tb

_NONE = object()


def classof(obj):
  """Returns the class of an object, or the object itself if it is a class.

  Args:
    obj: An object or class.

  Returns:
    ``obj`` if it is a class, otherwise ``obj.__class__`` or None if
    unavailable.
  """
  return obj if inspect.isclass(obj) else getattr(obj, '__class__', None)


def moduleof(obj):
  """Returns the module name where an object's class is defined.

  Args:
    obj: An object or class.

  Returns:
    The ``__module__`` attribute of the object's class, or None if not
    available.
  """
  cls = classof(obj)

  return getattr(cls, '__module__', None) if cls is not None else None


def cname(obj):
  """Returns the class name of an object.

  Args:
    obj: An object or class.

  Returns:
    The ``__name__`` attribute of the object's class, or None if not
    available.
  """
  cls = classof(obj)

  return cls.__name__ if cls is not None else None


def func_name(func):
  """Returns the name of a function or class.

  Args:
    func: A function, method, class, or other callable object.

  Returns:
    The ``__name__`` attribute if available, otherwise the class name via
    ``cname(func)``.
  """
  fname = getattr(func, '__name__', None)

  return fname if fname is not None else cname(func)


_BUILTIN_NAMES = {'__builtin__', 'builtins'}


def _qual_name(obj, builtin_strip=False):
  """Computes the fully qualified name of an object.

  Args:
    obj: An object with ``__module__`` and ``__qualname__`` attributes.
    builtin_strip: If True, strips builtin module prefixes (e.g. 'builtins').

  Returns:
    A dotted name string like ``'module.ClassName'`` or just ``name`` for
    builtins when ``builtin_strip`` is True.
  """
  module = getattr(obj, '__module__', None)
  name = getattr(obj, '__qualname__', None)
  if name is None:
    name = getattr(obj, '__name__', None)
    tas.check_is_not_none(name, msg=f'Unable to reference name: {obj}')
  if module is not None and not (builtin_strip and module in _BUILTIN_NAMES):
    name = module + '.' + name

  return name


def qual_name(obj, builtin_strip=False):
  """Returns the qualified name for a class, function, method, or module.

  For non-class/function/method/module objects, uses the object's class.

  Args:
    obj: The object to get the qualified name for.
    builtin_strip: If True, strips builtin module prefixes.

  Returns:
    A dotted qualified name string.
  """
  if (inspect.isclass(obj) or inspect.isroutine(obj) or inspect.ismodule(obj)):
    ref = obj
  else:
    ref = obj.__class__

  return _qual_name(ref, builtin_strip=builtin_strip)


def qual_mro(obj, builtin_strip=False):
  """Yields qualified names for all classes in an object's MRO.

  Args:
    obj: A class or an instance of a class.
    builtin_strip: If True, strips builtin module prefixes.

  Yields:
    Qualified name strings for each class in ``cls.__mro__``.
  """
  cls = obj if inspect.isclass(obj) else obj.__class__

  for scls in cls.__mro__:
    yield _qual_name(scls, builtin_strip=builtin_strip)


def is_subclass(cls, cls_group):
  """Checks whether cls is a subclass of cls_group.

  Args:
    cls: The class to check.
    cls_group: A class or tuple of classes to check against.

  Returns:
    True if ``cls`` is a class and is a subclass of ``cls_group``, False
    otherwise.
  """
  return inspect.isclass(cls) and issubclass(cls, cls_group)


def _fn_lookup(frame, name):
  """Looks up a function by dotted name in a frame's globals.

  Args:
    frame: A frame object to search in.
    name: A dotted name string (e.g. 'module.Class.method').

  Returns:
    The resolved function object, or None if not found.
  """
  xns, xname = None, name
  while True:
    dpos = xname.find('.')
    if dpos > 0:
      cname, fname = xname[:dpos], xname[dpos + 1:]
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
  """Returns the function object of the caller at a given stack depth.

  Args:
    back: Number of frames to go back from the current frame (default 0).
    frame: An optional frame object to use instead of walking the stack.

  Returns:
    The function object of the caller, resolved via qualified name lookup.
  """
  if frame is None:
    frame = tb.get_frame(back + 1)

  return _fn_lookup(frame, frame.f_code.co_qualname)


def current_module():
  """Returns the module of the caller's frame.

  Returns:
    The module object of the frame one level up the call stack.
  """
  return inspect.getmodule(tb.get_frame(1))


def fetch_args(func, locs, input_args=()):
  """Extracts positional and keyword arguments for a function from locals.

  Args:
    func: The function whose signature is used to extract arguments.
    locs: A dict-like mapping of local variable names to values.
    input_args: A tuple of input arguments to use as positional defaults.

  Returns:
    A tuple ``(args, kwargs)`` where ``args`` is a list of positional
    arguments and ``kwargs`` is a dict of keyword arguments.

  Raises:
    RuntimeError: If a required positional argument is missing from locals
      and no input_args remain.
  """
  sig = inspect.signature(func)

  def args_append(args, n):
    """Append the value of a positional parameter to the arguments.

    Args:
      args: The list of positional arguments being built.
      n: The name of the parameter to append.
    """
    if len(args) < len(input_args):
      args.append(input_args[len(args)])
    else:
      pv = locs.get(n, _NONE)
      if pv is not _NONE:
        args.append(pv)
      else:
        alog.xraise(RuntimeError, f'Missing argument: {n}')

  def kwargs_assign(kwargs, n, p):
    """Assign the value of an optional parameter to the kwargs.

    Args:
      kwargs: The dict of keyword arguments being built.
      n: The name of the parameter to assign.
      p: The inspect.Parameter of the parameter.
    """
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
  """Calls a function with arguments extracted from locals.

  Args:
    func: The function to call.
    locs: A dict-like mapping of local variable names to values.
    input_args: A tuple of input arguments to use as positional defaults.

  Returns:
    The result of calling ``func`` with the extracted arguments.
  """
  args, kwargs = fetch_args(func, locs, input_args=input_args)

  return func(*args, **kwargs)


def get_fn_kwargs(args, func, prefix=None, roffset=None):
  """Extracts keyword arguments for a function from an args dict.

  Uses inspect.getfullargspec to determine parameter names and defaults.

  Args:
    args: A dict mapping argument names to values.
    func: The function whose signature is used.
    prefix: An optional prefix string for argument names (e.g. 'config.').
    roffset: An offset index for required arguments.

  Returns:
    A dict of parameter names to values, using defaults where available.
  """
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
  """Returns parameters of a function that have default values.

  Args:
    func: The function to inspect.

  Returns:
    A tuple of ``inspect.Parameter`` objects whose default is not empty.
  """
  sig = inspect.signature(func)

  return tuple(
      p for p in sig.parameters.values() if p.default is not inspect.Signature.empty)


def parent_locals(level=0):
  """Returns the locals dict of a parent frame.

  Args:
    level: Additional levels to go up (default 0 means the immediate caller's
      parent).

  Returns:
    The ``f_locals`` dict of the frame at ``level + 2`` up the stack.
  """
  frame = tb.get_frame(level + 2)

  return frame.f_locals


def parent_globals(level=0):
  """Returns the globals dict of a parent frame.

  Args:
    level: Additional levels to go up (default 0 means the immediate caller's
      parent).

  Returns:
    The ``f_globals`` dict of the frame at ``level + 2`` up the stack.
  """
  frame = tb.get_frame(level + 2)

  return frame.f_globals


def parent_coords(level=0):
  """Returns the filename and line number of a parent frame.

  Args:
    level: Additional levels to go up (default 0 means the immediate caller's
      parent).

  Returns:
    A tuple ``(filename, lineno)`` for the frame at ``level + 2`` up the stack.
  """
  frame = tb.get_frame(level + 2)

  return frame.f_code.co_filename, frame.f_lineno


def class_slots(cls):
  """Returns all slot names defined across a class's MRO.

  Args:
    cls: A class object.

  Returns:
    A tuple of slot name strings from all classes in ``cls.__mro__``.
  """
  slots = itertools.chain.from_iterable(
      getattr(mcls, '__slots__', []) for mcls in cls.__mro__)

  return tuple(slots)
