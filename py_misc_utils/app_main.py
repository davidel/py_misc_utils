import argparse
import functools
import gc
import multiprocessing
import sys

from . import alog
from . import cleanups
from . import core_utils as cu
from . import dynamod


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


def basic_main(mainfn):
  parser = argparse.ArgumentParser()
  main(parser, mainfn)


_CONTEXT_KEY = '_parent_context'

def _apply_child_context(kwargs):
  global _ARGS

  ctx = kwargs.pop(_CONTEXT_KEY, None)
  if ctx is not None:
    if (args := ctx.pop('main_args', None)) is not None:
      _ARGS = args
      init_modules = _get_init_modules()
      _config_modules(init_modules, args)

    ctx = dynamod.wrap_procfn_child(ctx)

  return kwargs


def _capture_parent_context(kwargs):
  ctx = dict()

  ctx.update(main_args=_ARGS)
  ctx = dynamod.wrap_procfn_parent(ctx)

  kwargs.update({_CONTEXT_KEY: ctx})

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

  if mpctx.get_start_method() == 'fork':
    target = functools.partial(_wrapped_main, mainfn, *args, **kwargs)
  else:
    kwargs = _capture_parent_context(kwargs)
    target = functools.partial(_wrapped_main, mainfn, *args, **kwargs)

  return mpctx.Process(target=target)

