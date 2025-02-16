import argparse
import functools
import inspect
import signal
import sys
import typing
import yaml

from . import alog
from . import core_utils as cu
from . import global_namespace as gns
from . import multiprocessing as mp


class TerminationError(Exception):
  pass


def _sig_handler(sig, frame):
  if sig == signal.SIGINT:
    raise KeyboardInterrupt()
  elif sig == signal.SIGTERM:
    raise TerminationError()


def _signals_setup():
  signal.signal(signal.SIGINT, _sig_handler)
  signal.signal(signal.SIGTERM, _sig_handler)


def _cleanup(init_modules):
  _cleanup_modules(init_modules)
  mp.cleanup()
  # Ignore {INT, TERM} signals on exit.
  signal.signal(signal.SIGINT, signal.SIG_IGN)
  signal.signal(signal.SIGTERM, signal.SIG_IGN)


def _get_init_modules():
  # Here is the place to import (import here to avoid cycling dependencies) and
  # call the get_main_config() API of modules which require setting up a
  # command line and configuring themselves with the parsed arguments.
  # Note that alog is imported at the top since it is used in other places (and
  # also has minimal dependencies which do not create issues).
  # Objects returned by the get_main_config() API must have a add_arguments(parser)
  # API to allow them to add command line arguments, and a config_module(args) API
  # to configure themselves with the parsed arguments. An optional cleanup_module()
  # is allowed, to give the module a chance to cleanup when exiting the main function.
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


def _cleanup_modules(init_modules):
  for module in init_modules:
    if (cleanup_module := getattr(module, 'cleanup_module', None)) is not None:
      cleanup_module()


def _child_setup_modules(args):
  init_modules = _get_init_modules()
  _config_modules(init_modules, args)

  return args


_ARGS = gns.Var(f'{__name__}.ARGS', child_fn=_child_setup_modules)

def _main(parser, init_modules, mainfn, args, rem_args):
  _signals_setup()

  if isinstance(mainfn, Main):
    mainfn.add_arguments(parser)

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
  init_modules = _get_init_modules()
  try:
    _main(parser, init_modules, mainfn, args, rem_args)
  except Exception as ex:
    alog.exception(ex, exmsg=f'Exception while running main function')
    raise
  finally:
    _cleanup(init_modules)


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

