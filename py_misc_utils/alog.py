"""Advanced logging utilities with custom levels, formatting, and async support.

This module provides:
- Extended log levels (SPAM, VERBOSE, DEBUG0-3)
- Custom formatter with compact headers
- Easy setup via command-line arguments or programmatic config
- Async-safe logging for signal handlers
- Convenience functions for all log levels
- Exception logging and raising helpers

The numeric values of the extended levels are: SPAM=8, VERBOSE=9,
DEBUG0..3=11..14 (between DEBUG=10 and INFO=20).
"""

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
  """Custom log formatter with compact headers and optional extra fields.

  Header format: LEVELYYMMDD HH:MM:SS.mmmmmm;PID;MODULE[;EXTRA...]

  Example:
    import io
    import logging
    import py_misc_utils.alog as alog

    buf = io.StringIO()
    logging.basicConfig(level=logging.INFO, stream=buf)
    alog.set_current_level(logging.INFO, set_logger=False)

    alog.info('hello %s', 'world')

    # Header: 'IN' + timestamp;PID;module, then the message.
    assert buf.getvalue().startswith('IN')
    assert 'hello world' in buf.getvalue()
  """

  def __init__(self, emit_extra=None):
    """Initialize the formatter.

    Args:
      emit_extra: List of LogRecord attribute names to include in header
        (default: None).
    """
    super().__init__()
    self.emit_extra = emit_extra

  def format(self, r):
    """Format a log record.

    Args:
      r: LogRecord to format.

    Returns:
      Formatted log string with header and message.
    """
    hdr = self.make_header(r)
    msg = (r.msg % r.args) if r.args else r.msg

    return '\n'.join([f'{hdr}: {ln}' for ln in msg.split('\n')])

  def formatTime(self, r, datefmt=None):
    """Format the timestamp of a log record.

    Args:
      r: LogRecord.
      datefmt: Optional strftime format string.

    Returns:
      Formatted timestamp string with microsecond precision.
    """
    if datefmt:
      return time.strftime(datefmt, time.localtime(r.created))

    tstr = time.strftime('%Y%m%d %H:%M:%S', time.localtime(r.created))
    usecs = math.modf(r.created)[0] * 1e6

    return f'{tstr}.{usecs:06.0f}'

  def make_header(self, r):
    """Create the log header for a record.

    Args:
      r: LogRecord.

    Returns:
      Header string: LEVELYYMMDD HH:MM:SS.mmmmmm;PID;MODULE[;EXTRA...]
    """
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
  """Add logging-related command-line arguments to an ArgumentParser.

  Args:
    parser: argparse.ArgumentParser instance to add arguments to.
  """
  parser.add_argument('--log_level',
                      type=str,
                      default=_DEFAULT_ARGS.get('log_level'),
                      choices={
                          'SPAM', 'VERBOSE', 'DEBUG', 'DEBUG0', 'DEBUG1', 'DEBUG2',
                          'DEBUG3', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'
                      },
                      help='The logging level')
  parser.add_argument('--log_file', type=str, default=_DEFAULT_ARGS.get('log_file'),
                      help='Comma separated list of target log files (STDOUT, STDERR ' \
                      'are also recognized)')
  parser.add_argument(
      '--log_mod_levels',
      nargs='*',
      help='Comma separated list of LOGGER_NAME,LEVEL to set the log level at')
  parser.add_argument('--log_emit_extra',
                      nargs='*',
                      help='Which other logging record fields should be emitted')


@ro.run_once
def _add_levels():
  """Register custom log levels with the logging module (run once)."""
  logging.addLevelName(SPAM, 'SPAM')
  logging.addLevelName(VERBOSE, 'VERBOSE')
  logging.addLevelName(DEBUG0, 'DEBUG0')
  logging.addLevelName(DEBUG1, 'DEBUG1')
  logging.addLevelName(DEBUG2, 'DEBUG2')
  logging.addLevelName(DEBUG3, 'DEBUG3')


def _clear_logging_handlers():
  """Clear all existing logging handlers from the root logger.

  Needed for Python < 3.8 where basicConfig doesn't have force=True.
  """
  # Python >= 3.8 has a force=True argument to logging.basicConfig() to force
  # initialization, but since Colab is not there yet, we do it manually.
  root_logger = logging.getLogger()
  for handler in tuple(root_logger.handlers):
    handler.flush()
    root_logger.removeHandler(handler)


def _set_logmod_levels(mlevels):
  """Set log levels for specific modules.

  Args:
    mlevels: List of 'MODULE,LEVEL' strings.
  """
  mlevels = list(mlevels) if mlevels else []
  env_mlevels = os.getenv('LOGMOD_LEVELS', None)
  if env_mlevels is not None:
    mlevels.extend(env_mlevels.split(':'))
  for mlev in mlevels:
    mod, level = mlev.split(',')
    logging.getLogger(mod).setLevel(logging.getLevelName(level.upper()))


def setup_logging(args):
  """Configure logging from parsed command-line arguments or namespace.

  Args:
    args: Namespace or object with attributes:
      - log_level: Log level name (default: 'INFO')
      - log_file: Comma-separated list of files ('STDOUT', 'STDERR', or paths)
      - log_mod_levels: List of 'MODULE,LEVEL' strings
      - log_emit_extra: List of LogRecord fields to include in header
  """
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
        handler = logging.FileHandler(fname, mode='a')

      handler.setLevel(numeric_level)
      handler.setFormatter(Formatter(emit_extra=args.log_emit_extra))
      handlers.append(handler)

  logging.basicConfig(level=numeric_level, handlers=handlers, force=True)

  set_current_level(numeric_level, set_logger=False)

  _set_logmod_levels(args.log_mod_levels)


def basic_setup(**kwargs):
  """Simple programmatic logging setup.

  Args:
    **kwargs: Logging configuration options (log_level, log_file, etc.).
  """
  args = _DEFAULT_ARGS.copy()
  args.update(kwargs)
  setup_logging(types.SimpleNamespace(**args))


def get_main_config():
  """Get a config object for use with py_misc_utils.config system.

  Returns:
    SimpleNamespace with add_arguments and config_module attributes.
  """
  return types.SimpleNamespace(add_arguments=add_logging_options,
                               config_module=setup_logging)


_LEVEL = DEBUG


def set_current_level(level, set_logger=True):
  """Set the current log level threshold.

  Args:
    level: Numeric log level (e.g., logging.INFO).
    set_logger: Whether to also update the root logger and handlers (default: True).
  """
  if set_logger:
    logger = logging.getLogger()
    logger.setLevel(level)
    for handler in logger.handlers:
      handler.setLevel(level)

  global _LEVEL

  _LEVEL = level


def level_active(level):
  """Check if a log level is currently active (enabled).

  Args:
    level: Numeric log level to check.

  Returns:
    True if the level is at or above the current threshold - e.g. with
    the threshold at INFO, level_active(INFO) is True while
    level_active(DEBUG) is False.
  """
  return _LEVEL <= level


def level_run(level, fn):
  """Execute a function only if a log level is active.

  Args:
    level: Numeric log level to check.
    fn: Callable to execute if level is active.

  Returns:
    Result of fn() if active, otherwise None.
  """
  return fn() if level_active(level) else None


_LOGGING_FRAMES = 1 if sys.version_info >= (3, 11) else 2


def logging_args(kwargs):
  """Prepare logging keyword arguments with call limiting and stacklevel adjustment.

  Args:
    kwargs: Keyword arguments for logging call.

  Returns:
    Modified kwargs with adjusted stacklevel, or None if call limited.
  """
  limit = kwargs.pop('limit', -1)
  stacklevel = kwargs.get('stacklevel', 1)
  if limit < 0 or cl.trigger(__file__, limit):
    kwargs['stacklevel'] = stacklevel + _LOGGING_FRAMES

    return kwargs


def _nested_args(kwargs):
  """Increment stacklevel for nested logging calls.

  Args:
    kwargs: Keyword arguments for logging call.

  Returns:
    Modified kwargs with incremented stacklevel.
  """
  kwargs['stacklevel'] = kwargs.get('stacklevel', 1) + 1

  return kwargs


def _dmsg(msg):
  """Evaluate message if callable (lazy evaluation).

  Args:
    msg: String or callable returning string.

  Returns:
    String message.
  """
  return msg() if callable(msg) else msg


def log(level, msg, *args, **kwargs):
  """Log a message at the specified level with call limiting.

  Args:
    level: Numeric log level (e.g., logging.INFO).
    msg: Message string or callable returning string.
    *args: Format arguments for the message.
    **kwargs: Additional logging arguments (limit, stacklevel, etc.).
  """
  kwargs = logging_args(kwargs)
  if kwargs is not None:
    logging.log(level, _dmsg(msg), *args, **kwargs)


def spam(msg, *args, **kwargs):
  """Log a SPAM level message (very verbose).

  Args:
    msg: Message string or callable.
    *args: Format arguments.
    **kwargs: Additional logging arguments.
  """
  if SPAM >= _LEVEL:
    log(SPAM, _dmsg(msg), *args, **_nested_args(kwargs))


def verbose(msg, *args, **kwargs):
  """Log a VERBOSE level message.

  Args:
    msg: Message string or callable.
    *args: Format arguments.
    **kwargs: Additional logging arguments.
  """
  if VERBOSE >= _LEVEL:
    log(VERBOSE, _dmsg(msg), *args, **_nested_args(kwargs))


def debug0(msg, *args, **kwargs):
  """Log a DEBUG0 level message (least verbose debug).

  Args:
    msg: Message string or callable.
    *args: Format arguments.
    **kwargs: Additional logging arguments.
  """
  if DEBUG0 >= _LEVEL:
    log(DEBUG0, _dmsg(msg), *args, **_nested_args(kwargs))


def debug1(msg, *args, **kwargs):
  """Log a DEBUG1 level message.

  Args:
    msg: Message string or callable.
    *args: Format arguments.
    **kwargs: Additional logging arguments.
  """
  if DEBUG1 >= _LEVEL:
    log(DEBUG1, _dmsg(msg), *args, **_nested_args(kwargs))


def debug2(msg, *args, **kwargs):
  """Log a DEBUG2 level message.

  Args:
    msg: Message string or callable.
    *args: Format arguments.
    **kwargs: Additional logging arguments.
  """
  if DEBUG2 >= _LEVEL:
    log(DEBUG2, _dmsg(msg), *args, **_nested_args(kwargs))


def debug3(msg, *args, **kwargs):
  """Log a DEBUG3 level message (most verbose debug).

  Args:
    msg: Message string or callable.
    *args: Format arguments.
    **kwargs: Additional logging arguments.
  """
  if DEBUG3 >= _LEVEL:
    log(DEBUG3, _dmsg(msg), *args, **_nested_args(kwargs))


def debug(msg, *args, **kwargs):
  """Log a standard DEBUG level message.

  Args:
    msg: Message string or callable.
    *args: Format arguments.
    **kwargs: Additional logging arguments.
  """
  if DEBUG >= _LEVEL:
    log(DEBUG, _dmsg(msg), *args, **_nested_args(kwargs))


def info(msg, *args, **kwargs):
  """Log an INFO level message.

  Args:
    msg: Message string or callable.
    *args: Format arguments.
    **kwargs: Additional logging arguments.
  """
  if INFO >= _LEVEL:
    log(INFO, _dmsg(msg), *args, **_nested_args(kwargs))


def warning(msg, *args, **kwargs):
  """Log a WARNING level message.

  Args:
    msg: Message string or callable.
    *args: Format arguments.
    **kwargs: Additional logging arguments.
  """
  if WARNING >= _LEVEL:
    log(WARNING, _dmsg(msg), *args, **_nested_args(kwargs))


def error(msg, *args, **kwargs):
  """Log an ERROR level message.

  Args:
    msg: Message string or callable.
    *args: Format arguments.
    **kwargs: Additional logging arguments.
  """
  if ERROR >= _LEVEL:
    log(ERROR, _dmsg(msg), *args, **_nested_args(kwargs))


def critical(msg, *args, **kwargs):
  """Log a CRITICAL level message.

  Args:
    msg: Message string or callable.
    *args: Format arguments.
    **kwargs: Additional logging arguments.
  """
  if CRITICAL >= _LEVEL:
    log(CRITICAL, _dmsg(msg), *args, **_nested_args(kwargs))


def exception(e, *args, **kwargs):
  """Log an exception with traceback at ERROR level.

  Args:
    e: Exception instance.
    *args: Format arguments.
    **kwargs: Additional logging arguments. Supports 'exmsg' for custom prefix.
  """
  kwargs = logging_args(kwargs)
  if kwargs is not None:
    msg = kwargs.pop('exmsg', 'Exception')
    tb = traceback.format_exc()
    error(f'{_dmsg(msg)}: {e}\n{tb}', *args, **_nested_args(kwargs))


def xraise(e, msg, *args, **kwargs):
  """Raise an exception carrying a formatted message (and optionally log it).

  The message is formatted as ``msg % args`` when format arguments are
  given - unlike the log functions, which use ``%``-formatting at record
  rendering time.

  Args:
    e: Exception class to raise (e.g., ValueError).
    msg: Error message string or callable.
    *args: Format arguments - applied with the % operator immediately.
    **kwargs: Additional logging arguments. If 'logit' is True, the
      message is logged at ERROR level before raising.

  Raises:
    e: Instance of the provided exception class with the formatted message.
  """
  if kwargs.pop('logit', False):
    error(msg, *args, **_nested_args(kwargs))

  raise e(_dmsg(msg) % args if args else _dmsg(msg))


def async_log(level, msg, *args, **kwargs):
  """Log a message asynchronously (safe for signal handlers).

  This function avoids the logging module's locks and can be safely called
  from signal handlers or other asynchronous contexts. It bypasses the
  configured handlers entirely, formatting the record with the default
  Formatter() and writing it to the output stream.

  Args:
    level: Numeric log level.
    msg: Message string or callable.
    *args: Format arguments.
    **kwargs: Additional arguments. 'file' specifies the output stream -
      stderr by default.

  Example:
    import io
    import py_misc_utils.alog as alog

    buf = io.StringIO()
    alog.async_log(alog.INFO, 'async %s', 'msg', file=buf)

    # Header: 'IN' + timestamp;PID;module, then the message.
    assert buf.getvalue().startswith('IN')
    assert 'async msg' in buf.getvalue()
  """
  # This one cannot use the logging module as it could be called from signal
  # handler asycnhronously. The logging.getLevelName() is safe since it is simply
  # a (lockless) dictionary lookup.
  # Similarly, no other APIs taking locks can be called from this context.
  if level >= _LEVEL:
    kwargs = logging_args(kwargs)
    if kwargs is not None:
      # Fake a logging record, filling up only the fields used by the Formatter.
      # Do not call logging APIs for that, for the same reasons cited above.
      frame = tb.get_frame(n=1)
      module = frame.f_globals.get('__name__', 'ASYNC').split('.')[-1]

      now = time.time()
      record = types.SimpleNamespace(
          msg=_dmsg(msg),
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
