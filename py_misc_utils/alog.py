import argparse
import logging
import os
import sys
import time
import traceback

from . import run_once as ro


DEBUG0 = logging.DEBUG + 1
DEBUG1 = logging.DEBUG + 2
DEBUG2 = logging.DEBUG + 3
DEBUG3 = logging.DEBUG + 4

_SHORT_LEV = {
  DEBUG0: '0D',
  DEBUG1: '1D',
  DEBUG2: '2D',
  DEBUG3: '3D',
  logging.DEBUG: 'DD',
  logging.INFO: 'IN',
  logging.WARNING: 'WA',
  logging.ERROR: 'ER',
  logging.CRITICAL: 'CR',
}

_HAS_STACKLEVEL = sys.version_info >= (3, 8)


class Formatter(logging.Formatter):

  def format(self, r):
    hdr = self.make_header(r)
    msg = (r.msg % r.args) if r.args else r.msg

    return '\n'.join([f'{hdr}: {ln}' for ln in msg.split('\n')])

  def formatTime(self, r, datefmt=None):
    if datefmt:
      return time.strftime(datefmt, r.created)

    tstr = time.strftime('%Y%m%d %H:%M:%S', time.localtime(r.created))

    return f'{tstr}.{r.msecs * 1000:06.0f}'

  def make_header(self, r):
    tstr = self.formatTime(r)
    lid = _SHORT_LEV.get(r.levelno, r.levelname[:2])

    if _HAS_STACKLEVEL:
      return f'{lid}{tstr};{os.getpid()};{r.module}'

    # No point of returning the module if 'stacklevel' is not supported, as the
    # module name will be 'alog' itself.
    return f'{lid}{tstr};{os.getpid()}'



def add_logging_options(parser):
  parser.add_argument('--log_level', type=str, default='INFO',
                      choices={'DEBUG', 'DEBUG0', 'DEBUG1', 'DEBUG2', 'DEBUG3', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'},
                      help='The logging level')
  parser.add_argument('--log_file', type=str, default='STDERR',
                      help='Comma separated list of target log files (STDOUT, STDERR are also recognized)')


@ro.run_once
def _add_levels():
  logging.addLevelName(DEBUG0, 'DEBUG0')
  logging.addLevelName(DEBUG1, 'DEBUG1')
  logging.addLevelName(DEBUG2, 'DEBUG2')
  logging.addLevelName(DEBUG3, 'DEBUG3')


def _clear_logging_handlers():
  # Python >= 3.8 has a force=True argument to logging.basicConfig() to force
  # initialization, but since Colab is not there yet, we do it manually.
  root_logger = logging.getLogger()
  for handler in tuple(root_logger.handlers):
    handler.flush()
    root_logger.removeHandler(handler)


def setup_logging(args):
  _add_levels()
  _clear_logging_handlers()

  numeric_level = logging.getLevelName(args.log_level.upper())
  handlers = []
  if args.log_file:
    for fname in args.log_file.split(','):
      if fname == 'STDOUT':
        handler = logging.StreamHandler(sys.stdout)
      elif fname == 'STDERR':
        handler = logging.StreamHandler(sys.stderr)
      else:
        handler = logging.StreamHandler(open(fname, mode='a'))

      handler.setLevel(numeric_level)
      handler.setFormatter(Formatter())
      handlers.append(handler)

  logging.basicConfig(level=numeric_level, handlers=handlers)


def level_active(level):
  return logging.getLogger().getEffectiveLevel() <= level


def level_run(level, fn):
  return fn() if level_active(level) else None


def logging_args(kwargs):
  limit = kwargs.pop('limit', -1)
  if limit < 0 or cl.trigger(2, limit):
    if _HAS_STACKLEVEL:
      kwargs['stacklevel'] = kwargs.get('stacklevel', 1) + 1

    return kwargs


def log(level, msg, *args, **kwargs):
  kwargs = logging_args(kwargs)
  if kwargs is not None:
    logging.log(level, msg, *args, **kwargs)


def debug0(msg, *args, **kwargs):
  kwargs = logging_args(kwargs)
  if kwargs is not None:
    logging.log(DEBUG0, msg, *args, **kwargs)


def debug1(msg, *args, **kwargs):
  kwargs = logging_args(kwargs)
  if kwargs is not None:
    logging.log(DEBUG1, msg, *args, **kwargs)


def debug2(msg, *args, **kwargs):
  kwargs = logging_args(kwargs)
  if kwargs is not None:
    logging.log(DEBUG2, msg, *args, **kwargs)


def debug3(msg, *args, **kwargs):
  kwargs = logging_args(kwargs)
  if kwargs is not None:
    logging.log(DEBUG3, msg, *args, **kwargs)


def debug(msg, *args, **kwargs):
  kwargs = logging_args(kwargs)
  if kwargs is not None:
    logging.debug(msg, *args, **kwargs)


def info(msg, *args, **kwargs):
  kwargs = logging_args(kwargs)
  if kwargs is not None:
    logging.info(msg, *args, **kwargs)


def warning(msg, *args, **kwargs):
  kwargs = logging_args(kwargs)
  if kwargs is not None:
    logging.warning(msg, *args, **kwargs)


def error(msg, *args, **kwargs):
  kwargs = logging_args(kwargs)
  if kwargs is not None:
    logging.error(msg, *args, **kwargs)


def critical(msg, *args, **kwargs):
  kwargs = logging_args(kwargs)
  if kwargs is not None:
    logging.critical(msg, *args, **kwargs)


def exception(e, *args, **kwargs):
  kwargs = logging_args(kwargs)
  if kwargs is not None:
    msg = kwargs.pop('exmsg', 'Exception')
    tb = traceback.format_exc()
    logging.error(f'{msg}: {e}\n{tb}', *args, **kwargs)


def xraise(e, msg, *args, **kwargs):
  kwargs = logging_args(kwargs)
  if kwargs is not None and kwargs.pop('logit', False):
    logging.error(msg, *args, **kwargs)

  raise e(msg)

