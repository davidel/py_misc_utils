import argparse
import gc
import sys

from . import alog
from . import cleanups
from . import utils as ut


def _cleanup():
  cleanups.run()
  gc.collect()


def _main(parser, mainfn, args, setupfn):
  if setupfn is not None:
    setupfn(parser, args)
  alog.add_logging_options(parser)

  xargs = args or sys.argv

  ddpos = ut.lindex(args, '--')
  if ddpos >= 0:
    rem_args = xargs[ddpos + 1: ]
    xargs = xargs[: ddpos]
  else:
    rem_args = []

  parsed_args = parser.parse_args(args=xargs)
  parsed_args.rem_args = tuple(rem_args)

  alog.setup_logging(parsed_args)

  mainfn(parsed_args)


def main(parser, mainfn, args=None, setupfn=None):
  try:
    _main(parser, mainfn, args, setupfn)
  except Exception as e:
    alog.exception(e, exmsg=f'Exception while running main function')
    raise
  finally:
    _cleanup()

