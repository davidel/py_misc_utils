import argparse
import logging
import os
import sys
import time
import traceback

from . import call_limiter as cl
from . import obj
from . import run_once as ro


DEBUG = logging.DEBUG
INFO = logging.INFO
WARNING = logging.WARNING
ERROR = logging.ERROR
CRITICAL = logging.CRITICAL

SPAM = DEBUG - 1
DEBUG0 = DEBUG + 1
DEBUG1 = DEBUG + 2
DEBUG2 = DEBUG + 3
DEBUG3 = DEBUG + 4

_SHORT_LEV = {
  SPAM: 'SP',
  DEBUG0: '0D',
  DEBUG1: '1D',
  DEBUG2: '2D',
  DEBUG3: '3D',
  DEBUG: 'DD',
  INFO: 'IN',
  WARNING: 'WA',
  ERROR: 'ER',
  CRITICAL: 'CR',
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


_DEFAULT_ARGS = dict(
  log_level='INFO',
  log_file='STDERR',
)

def add_logging_options(parser):
  parser.add_argument('--log_level', type=str, default=_DEFAULT_ARGS.get('log_level'),
                      choices={'SPAM', 'DEBUG', 'DEBUG0', 'DEBUG1', 'DEBUG2', 'DEBUG3', \
                               'INFO', 'WARNING', 'ERROR', 'CRITICAL'},
                      help='The logging level')
  parser.add_argument('--log_file', type=str, default=_DEFAULT_ARGS.get('log_file'),
                      help='Comma separated list of target log files (STDOUT, STDERR ' \
                      f'are also recognized)')


@ro.run_once
def _add_levels():
  logging.addLevelName(SPAM, 'SPAM')
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

  set_current_level(numeric_level, set_logger=False)


def basic_setup(**kwargs):
  args = _DEFAULT_ARGS.copy()
  args.update(kwargs)
  setup_logging(obj.Obj(**args))


_LEVEL = DEBUG

def set_current_level(level, set_logger=True):
  if set_logger:
    logger = logging.getLogger()
    logger.setLevel(level)
    for handler in logger.handlers:
      handler.setLevel(level)

  global _LEVEL

  _LEVEL = level


def level_active(level):
  return _LEVEL <= level


def level_run(level, fn):
  return fn() if level_active(level) else None


def logging_args(kwargs):
  limit = kwargs.pop('limit', -1)
  if limit < 0 or cl.trigger(2, limit):
    if _HAS_STACKLEVEL:
      kwargs['stacklevel'] = kwargs.get('stacklevel', 1) + 2

    return kwargs


def _nested_args(kwargs):
  kwargs['stacklevel'] = kwargs.get('stacklevel', 1) + 1

  return kwargs


def log(level, msg, *args, **kwargs):
  kwargs = logging_args(kwargs)
  if kwargs is not None:
    logging.log(level, msg, *args, **kwargs)


def spam(msg, *args, **kwargs):
  if SPAM >= _LEVEL:
    log(SPAM, msg, *args, **_nested_args(kwargs))


def debug0(msg, *args, **kwargs):
  if DEBUG0 >= _LEVEL:
    log(DEBUG0, msg, *args, **_nested_args(kwargs))


def debug1(msg, *args, **kwargs):
  if DEBUG1 >= _LEVEL:
    log(DEBUG1, msg, *args, **_nested_args(kwargs))


def debug2(msg, *args, **kwargs):
  if DEBUG2 >= _LEVEL:
    log(DEBUG2, msg, *args, **_nested_args(kwargs))


def debug3(msg, *args, **kwargs):
  if DEBUG3 >= _LEVEL:
    log(DEBUG3, msg, *args, **_nested_args(kwargs))


def debug(msg, *args, **kwargs):
  if DEBUG >= _LEVEL:
    log(DEBUG, msg, *args, **_nested_args(kwargs))


def info(msg, *args, **kwargs):
  if INFO >= _LEVEL:
    log(INFO, msg, *args, **_nested_args(kwargs))


def warning(msg, *args, **kwargs):
  if WARNING >= _LEVEL:
    log(WARNING, msg, *args, **_nested_args(kwargs))


def error(msg, *args, **kwargs):
  if ERROR >= _LEVEL:
    log(ERROR, msg, *args, **_nested_args(kwargs))


def critical(msg, *args, **kwargs):
  if CRITICAL >= _LEVEL:
    log(CRITICAL, msg, *args, **_nested_args(kwargs))


def exception(e, *args, **kwargs):
  kwargs = logging_args(kwargs)
  if kwargs is not None:
    msg = kwargs.pop('exmsg', 'Exception')
    tb = traceback.format_exc()
    error(f'{msg}: {e}\n{tb}', *args, **_nested_args(kwargs))


def xraise(e, msg, *args, **kwargs):
  if kwargs.pop('logit', False):
    error(msg, *args, **_nested_args(kwargs))

  raise e(msg)

