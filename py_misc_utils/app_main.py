import argparse
import functools
import gc
import sys

from . import alog
from . import cleanups
from . import core_utils as cu
from . import dynamod


def _cleanup():
  cleanups.run()
  gc.collect()


def _add_modules_arguments(parser):
  alog.add_logging_options(parser)


def _setup_modules(args):
  alog.setup_logging(args)


_ARGS = None

def _main(parser, mainfn, args, rem_args):
  global _ARGS

  _add_modules_arguments(parser)

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
  _setup_modules(parsed_args)

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


def _apply_child_context(kwargs):
  global _ARGS

  if (args := kwargs.pop('_main_args', None)) is not None:
    _ARGS = args
    _setup_modules(args)

  kwargs = dynamod.wrap_procfn_child(kwargs)

  return kwargs


def _capture_parent_context(kwargs):
  kwargs.update(_main_args=_ARGS)

  kwargs = dynamod.wrap_procfn_parent(kwargs)

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


def wrap_main(mainfn, *args, **kwargs):
  kwargs = _capture_parent_context(kwargs)

  return functools.partial(_wrapped_main, mainfn, *args, **kwargs)

