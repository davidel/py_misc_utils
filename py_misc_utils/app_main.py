import argparse
import gc
import sys

from . import alog
from . import cleanups
from . import utils as ut


def _cleanup():
  cleanups.run()
  gc.collect()


def _main(parser, mainfn, args, setupfn, rem_args):
  if setupfn is not None:
    setupfn(parser, args)
  alog.add_logging_options(parser)

  if rem_args:
    xargs = args or sys.argv[1: ]

    ddpos = ut.lindex(xargs, '--')
    if ddpos >= 0:
      rargs = xargs[ddpos + 1: ]
      xargs = xargs[: ddpos]
    else:
      rargs = []

    parsed_args = parser.parse_args(args=xargs)
    setattr(parsed_args, rem_args, tuple(rargs))
  else:
    parsed_args = parser.parse_args(args=args)

  alog.setup_logging(parsed_args)

  mainfn(parsed_args)


def main(parser, mainfn, args=None, setupfn=None, rem_args=None):
  try:
    _main(parser, mainfn, args, setupfn, rem_args)
  except Exception as e:
    alog.exception(e, exmsg=f'Exception while running main function')
    raise
  finally:
    _cleanup()

