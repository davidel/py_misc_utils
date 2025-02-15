import argparse
import logging
import math
import os
import sys
import time
import traceback
import types

from . import call_limiter as cl
from . import run_once as ro
from . import traceback as tb


DEBUG = logging.DEBUG
INFO = logging.INFO
WARNING = logging.WARNING
ERROR = logging.ERROR
CRITICAL = logging.CRITICAL

SPAM = DEBUG - 2
VERBOSE = DEBUG - 1
DEBUG0 = DEBUG + 1
DEBUG1 = DEBUG + 2
DEBUG2 = DEBUG + 3
DEBUG3 = DEBUG + 4

_SHORT_LEV = {
  SPAM: 'SP',
  VERBOSE: 'VB',
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


class Formatter(logging.Formatter):

  def __init__(self, emit_extra=None):
    super().__init__()
    self.emit_extra = emit_extra

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
    hdr = f'{lid}{tstr};{os.getpid()};{r.module}'
    if self.emit_extra:
      extras = [str(getattr(r, name, None)) for name in self.emit_extra]
      hdr = f'{hdr};{";".join(extras)}'

    return hdr


_DEFAULT_ARGS = dict(
  log_level=os.getenv('LOG_LEVEL', 'INFO'),
  log_file=os.getenv('LOG_FILE', 'STDERR'),
  log_mod_levels=[],
  log_emit_extra=[],
)

def add_logging_options(parser):
  parser.add_argument('--log_level', type=str, default=_DEFAULT_ARGS.get('log_level'),
                      choices={'SPAM', 'VERBOSE', 'DEBUG', 'DEBUG0', 'DEBUG1', 'DEBUG2',
                               'DEBUG3', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'},
                      help='The logging level')
  parser.add_argument('--log_file', type=str, default=_DEFAULT_ARGS.get('log_file'),
                      help='Comma separated list of target log files (STDOUT, STDERR ' \
                      f'are also recognized)')
  parser.add_argument('--log_mod_levels', nargs='*',
                      help='Comma separated list of LOGGER_NAME,LEVEL to set the log level at')
  parser.add_argument('--log_emit_extra', nargs='*',
                      help='Which other logging record fields should be emitted')


@ro.run_once
def _add_levels():
  logging.addLevelName(SPAM, 'SPAM')
  logging.addLevelName(VERBOSE, 'VERBOSE')
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


def _set_logmod_levels(mlevels):
  mlevels = list(mlevels) if mlevels else []
  env_mlevels = os.getenv('LOGMOD_LEVELS', None)
  if env_mlevels is not None:
    mlevels.extend(env_mlevels.split(':'))
  for mlev in mlevels:
    mod, level = mlev.split(',')
    logging.getLogger(mod).setLevel(logging.getLevelName(level.upper()))


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
      handler.setFormatter(Formatter(emit_extra=args.log_emit_extra))
      handlers.append(handler)

  logging.basicConfig(level=numeric_level, handlers=handlers, force=True)

  set_current_level(numeric_level, set_logger=False)

  _set_logmod_levels(args.log_mod_levels)


def basic_setup(**kwargs):
  args = _DEFAULT_ARGS.copy()
  args.update(kwargs)
  setup_logging(types.SimpleNamespace(**args))


def get_main_config():
  return types.SimpleNamespace(add_arguments=add_logging_options,
                               config_module=setup_logging)


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


_LOGGING_FRAMES = 1 if sys.version_info >= (3, 11) else 2

def logging_args(kwargs):
  limit = kwargs.pop('limit', -1)
  stacklevel = kwargs.get('stacklevel', 1)
  if limit < 0 or cl.trigger(__file__, limit):
    kwargs['stacklevel'] = stacklevel + _LOGGING_FRAMES

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


def verbose(msg, *args, **kwargs):
  if VERBOSE >= _LEVEL:
    log(VERBOSE, msg, *args, **_nested_args(kwargs))


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


def async_log(level, msg, *args, **kwargs):
  # This one cannot use the logging module as it could be called from signal
  # handler asycnhronously. The logging.getLevelName() is safe since it is simply
  # a dictionary lookup.
  # Similarly, no other APIs taking locks can be caller from this context.
  if level >= _LEVEL:
    kwargs = logging_args(kwargs)
    if kwargs is not None:
      # Fake a logging record. Do not call logging APIs for that, for the same
      # reasons cited above.
      frame = tb.get_frame(n=1)
      module = frame.f_globals.get('__name__', 'ASYNC').split('.')[-1]

      now = time.time()
      record = types.SimpleNamespace(
        msg=msg,
        args=args,
        created=now,
        msecs=math.modf(now)[0] * 1000,
        levelno=level,
        levelname=logging.getLevelName(level),
        module=module,
      )

      formatter = Formatter()

      logfd = kwargs.pop('file', sys.stderr)
      logfd.write(formatter.format(record))
      logfd.write('\n')
      logfd.flush()

