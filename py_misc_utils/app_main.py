import argparse
import functools
import gc
import inspect
import multiprocessing
import sys
import typing
import yaml

from . import alog
from . import cleanups
from . import core_utils as cu
from . import global_namespace as gns


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


def _child_setup_modules(args):
  init_modules = _get_init_modules()
  _config_modules(init_modules, args)

  return args


_ARGS = gns.Var(f'{__name__}.ARGS', child_fn=_child_setup_modules)

def _main(parser, mainfn, args, rem_args):
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

  gns.set(_ARGS, parsed_args)
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



def _child_setup_functions(setup_functions):
  for setupfn in setup_functions:
    setupfn()

  return setup_functions


_SETUP_FUNCTIONS = gns.Var(f'{__name__}.SETUP_FUNCTIONS',
                           child_fn=_child_setup_functions,
                           defval=[])

def add_setupfn(setupfn, run=True):
  if run:
    setupfn()

  setup_functions = gns.get(_SETUP_FUNCTIONS)
  setup_functions.append(setupfn)


_GNS_KEY = 'gns'

def _wrap_procfn_parent(method):
  ctx = dict(method=method)
  ctx.update({_GNS_KEY: gns.parent_switch(method)})

  return ctx


def _wrap_procfn_child(method, pctx):
  parent_gns = pctx.pop(_GNS_KEY, None)
  if parent_gns is not None:
    gns.child_switch(method, parent_gns)

  return pctx


_CONTEXT_KEY = '_parent_context'

def _capture_parent_context(method, kwargs):
  pctx = _wrap_procfn_parent(method)
  kwargs.update({_CONTEXT_KEY: pctx})

  return kwargs


def _apply_child_context(kwargs):
  pctx = kwargs.pop(_CONTEXT_KEY)
  pctx = _wrap_procfn_child(pctx['method'], pctx)

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


def create_process(mainfn, args=None, kwargs=None, context=None, daemon=None):
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

  return mpctx.Process(target=target, daemon=daemon)


# This is similar to Fire but brings up the app_main infrastructure.
# Use as:
#
# @app_main.Main
# def my_main(arg, ..., kwarg=17, ...):
#   ...
#
# if __name__ == '__main__':
#   parser = argparse.ArgumentParser(...)
#   ...
#   app_main.main(parser, my_main, ...)
#
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
      choices = None
      defval = p.default if p.default is not p.empty else None
      if p.annotation is not p.empty:
        ptype = p.annotation
        if typing.get_origin(ptype) == typing.Literal:
          choices = typing.get_args(ptype)
          ptype = type(choices[0])

        type_cast = functools.partial(cu.to_type, vtype=ptype)
      elif defval is not None:
        ptype = type(defval)
        type_cast = functools.partial(cu.to_type, vtype=ptype)
      else:
        ptype, type_cast = str, yaml.safe_load

      action = argparse.BooleanOptionalAction if ptype is bool else None

      help_str = f'Argument "{n}" (type={ptype.__name__}) of function {fname}(...)'
      if p.default is p.empty or p.kind == p.POSITIONAL_ONLY:
        parser.add_argument(n,
                            metavar=n.upper(),
                            action=action,
                            type=type_cast,
                            default=defval,
                            choices=choices,
                            help=help_str)
      else:
        parser.add_argument(f'--{n}',
                            action=action,
                            type=type_cast,
                            default=defval,
                            choices=choices,
                            help=help_str)

