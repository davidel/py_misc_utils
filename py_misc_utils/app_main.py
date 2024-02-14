import argparse
import gc

from . import alog
from . import cleanups


def _cleanup():
  cleanups.run()
  gc.collect()


def _main(parser, mainfn, args=None, setupfn=None):
  if setupfn is not None:
    setupfn(parser, args)
  alog.add_logging_options(parser)

  parsed_args = parser.parse_args(args=args)
  alog.setup_logging(parsed_args)

  mainfn(parsed_args)


def main(parser, mainfn, args=None, setupfn=None):
  try:
    _main(parser, mainfn, args=args, setupfn=setupfn)
  except Exception as e:
    alog.exception(e, exmsg=f'Exception while running main function')
    raise
  finally:
    _cleanup()

