import argparse
import functools
import gc
import inspect
import multiprocessing
import sys
import yaml

from . import alog
from . import cleanups
from . import core_utils as cu


def _cleanup():
  cleanups.run()
  gc.collect()


def _get_init_modules():
  # Here is the place to import (import here to avoid cycling dependencies) and
  # call the get_main_config() API of modules which require setting up a
  # command line and configuring themselves with the parsed arguments.
  # Note that alog is imported at the top since it is used in other places (and
  # also has minimal dependencies which do not create issues).
  # Objects returned by the get_main_config() API must have a add_arguments(parser)
  # API to allow them to add command line arguments, and a config_module(args) API
  # to configure themselves with the parsed arguments.
  # Example:
  #
  #   from . import foo
  #   modules.append(foo.get_main_config())
  #
  modules = []
  modules.append(alog.get_main_config())

  return tuple(modules)


def _add_arguments(init_modules, parser):
  for module in init_modules:
    module.add_arguments(parser)


def _config_modules(init_modules, args):
  for module in init_modules:
    module.config_module(args)


_ARGS = None

def _main(parser, mainfn, args, rem_args):
  global _ARGS

  if isinstance(mainfn, Main):
    mainfn.add_arguments(parser)

  init_modules = _get_init_modules()
  _add_arguments(init_modules, parser)

  if rem_args:
    xargs = args or sys.argv[1:]

    ddpos = cu.lindex(xargs, '--')
    if ddpos >= 0:
      rargs = xargs[ddpos + 1:]
      xargs = xargs[: ddpos]
    else:
      rargs = []

    parsed_args = parser.parse_args(args=xargs)
    setattr(parsed_args, rem_args, tuple(rargs))
  else:
    parsed_args = parser.parse_args(args=args)

  _ARGS = parsed_args
  _config_modules(init_modules, parsed_args)

  mainfn(parsed_args)


def main(parser, mainfn, args=None, rem_args=None):
  try:
    _main(parser, mainfn, args, rem_args)
  except Exception as ex:
    alog.exception(ex, exmsg=f'Exception while running main function')
    raise
  finally:
    _cleanup()


def basic_main(mainfn, description='Basic Main'):
  parser = argparse.ArgumentParser(
    description=description,
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
  )
  main(parser, mainfn)


_ARGS_KEY = 'main_args'

def _wrap_procfn_parent(method, ctx):
  if method == 'spawn':
    ctx.update({_ARGS_KEY: _ARGS})

  return ctx


def _wrap_procfn_child(ctx):
  global _ARGS

  if (args := ctx.pop(_ARGS_KEY, None)) is not None:
    _ARGS = args
    init_modules = _get_init_modules()
    _config_modules(init_modules, args)

  return ctx


_CONTEXT_KEY = '_parent_context'

def _capture_parent_context(method, kwargs):
  ctx = dict()

  ctx = _wrap_procfn_parent(method, ctx)

  from . import dynamod
  ctx = dynamod.wrap_procfn_parent(method, ctx)

  if ctx:
    kwargs.update({_CONTEXT_KEY: ctx})

  return kwargs


def _apply_child_context(kwargs):
  ctx = kwargs.pop(_CONTEXT_KEY, None)
  if ctx is not None:
    ctx = _wrap_procfn_child(ctx)

    from . import dynamod
    ctx = dynamod.wrap_procfn_child(ctx)

  return kwargs


def _wrapped_main(mainfn, *args, **kwargs):
  try:
    kwargs = _apply_child_context(kwargs)

    return mainfn(*args, **kwargs)
  except KeyboardInterrupt:
    sys.exit(1)
  except Exception as ex:
    alog.exception(ex, exmsg=f'Exception while running main function')
    raise
  finally:
    _cleanup()


def create_process(mainfn, args=None, kwargs=None, context=None):
  if context is None:
    mpctx = multiprocessing
  elif isinstance(context, str):
    mpctx = multiprocessing.get_context(method=context)
  else:
    mpctx = context

  args = () if args is None else args
  kwargs = {} if kwargs is None else kwargs

  kwargs = _capture_parent_context(mpctx.get_start_method(), kwargs)
  target = functools.partial(_wrapped_main, mainfn, *args, **kwargs)

  return mpctx.Process(target=target)


# This is similar to Fire but brings up the app_main infrastructure.
# Use as:
#
# @app_main.Main
# def my_main(arg, ..., kwarg=17, ...):
#   ...
class Main:

  def __init__(self, func):
    self._func = func
    self._sig = inspect.signature(func)
    functools.update_wrapper(self, func)

  def __call__(self, parsed_args):
    args, kwargs = [], {}
    for n, p in self._sig.parameters.items():
      pv = getattr(parsed_args, n, None)
      if p.kind == p.POSITIONAL_ONLY:
        args.append(pv)
      else:
        kwargs[n] = pv

    return self._func(*args, **kwargs)

  def add_arguments(self, parser):
    fname = self._func.__name__

    for n, p in self._sig.parameters.items():
      defval = p.default if p.default is not p.empty else None
      if p.annotation is not p.empty:
        ptype = p.annotation
      else:
        ptype = type(defval) if defval is not None else yaml.safe_load

      action = argparse.BooleanOptionalAction if ptype is bool else None

      help_str = f'Argument "{n}" (type={ptype.__name__}) of function {fname}(...)'
      if p.default is p.empty or p.kind == p.POSITIONAL_ONLY:
        parser.add_argument(n,
                            metavar=n.upper(),
                            action=action,
                            type=ptype,
                            default=defval,
                            help=help_str)
      else:
        parser.add_argument(f'--{n}',
                            action=action,
                            type=ptype,
                            default=defval,
                            help=help_str)

