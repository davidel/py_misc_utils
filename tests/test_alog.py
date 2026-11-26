"""Tests for py_misc_utils.alog.

Covers the custom formatter, extended log levels, CLI option parsing,
setup/teardown of logging handlers, call limiting and the async-safe
logging path.

setup_logging() reconfigures the root logger, so every test runs under a
fixture that restores the root handlers and level, and resets the module
level threshold via its public set_current_level() API.
"""

import argparse
import io
import logging
import math
import os
import time
import types

import pytest

from py_misc_utils import alog

_MODULE_NAME = 'test_alog'


class _Capture(logging.Handler):

  def __init__(self):
    super().__init__()
    self.records = []

  def emit(self, record):
    self.records.append(record)


@pytest.fixture
def log_state():
  """Save and restore the root logger state around each test."""
  root = logging.getLogger()
  saved_handlers = root.handlers[:]
  saved_level = root.level
  try:
    yield root
  finally:
    root.handlers[:] = saved_handlers
    root.setLevel(saved_level)
    alog.set_current_level(logging.DEBUG, set_logger=False)


def _capture(root):
  handler = _Capture()
  root.addHandler(handler)
  # SPAM is the lowest extended level; DEBUG would block SPAM/VERBOSE at the
  # logging-module layer.
  root.setLevel(alog.SPAM)

  return handler


def _rec(levelno, msg='m', args=(), path='x/f.py', lineno=10):
  return logging.LogRecord(_MODULE_NAME, levelno, path, lineno, msg, args, None)


def _tstr(created):
  return time.strftime('%Y%m%d %H:%M:%S', time.localtime(created))


def _fts(created):
  """The exact formatTime() output for a given epoch (incl. microseconds)."""
  tstr = _tstr(created)
  usecs = math.modf(created)[0] * 1e6

  return f'{tstr}.{usecs:06.0f}'


# ---------------------------------------------------------------------------
# Level constants
# ---------------------------------------------------------------------------


def test_level_constants():
  assert alog.SPAM == logging.DEBUG - 2
  assert alog.VERBOSE == logging.DEBUG - 1
  assert alog.DEBUG0 == logging.DEBUG + 1
  assert alog.DEBUG1 == logging.DEBUG + 2
  assert alog.DEBUG2 == logging.DEBUG + 3
  assert alog.DEBUG3 == logging.DEBUG + 4
  assert alog.INFO == logging.INFO
  assert alog.WARNING == logging.WARNING
  assert alog.ERROR == logging.ERROR
  assert alog.CRITICAL == logging.CRITICAL


# ---------------------------------------------------------------------------
# Formatter
# ---------------------------------------------------------------------------


def test_formatter_header(log_state):
  rec = _rec(logging.ERROR, path='x/f.py')
  rec.created = 1000000.5
  hdr = alog.Formatter().make_header(rec)
  assert hdr == f'ER{_fts(rec.created)};{os.getpid()};f'


@pytest.mark.parametrize('levelno, lid', [
    (alog.SPAM, 'SP'),
    (alog.VERBOSE, 'VB'),
    (alog.DEBUG0, '0D'),
    (alog.DEBUG1, '1D'),
    (alog.DEBUG2, '2D'),
    (alog.DEBUG3, '3D'),
    (logging.DEBUG, 'DD'),
    (logging.INFO, 'IN'),
    (logging.WARNING, 'WA'),
    (logging.ERROR, 'ER'),
    (logging.CRITICAL, 'CR'),
])
def test_formatter_short_level_ids(levelno, lid):
  rec = _rec(levelno)
  hdr = alog.Formatter().make_header(rec)
  assert hdr.startswith(lid)


def test_formatter_unknown_level_fallback():
  rec = _rec(95)
  assert rec.levelname == 'Level 95'
  hdr = alog.Formatter().make_header(rec)
  assert hdr.startswith('Le')


def test_formatter_single_line(log_state):
  rec = _rec(logging.ERROR, msg='v=%d', args=(7,))
  rec.created = 1000000.5
  out = alog.Formatter().format(rec)
  assert out == f'ER{_fts(rec.created)};{os.getpid()};f: v=7'


def test_formatter_multi_line(log_state):
  rec = _rec(logging.WARNING, msg='l1\nl2')
  rec.created = 1000000.5
  out = alog.Formatter().format(rec)
  prefix = f'WA{_fts(rec.created)};{os.getpid()};f'
  assert out == f'{prefix}: l1\n{prefix}: l2'


def test_formatter_format_time_default(log_state):
  rec = _rec(logging.INFO)
  rec.created = 1000000.5
  assert alog.Formatter().formatTime(rec) == _fts(rec.created)


def test_formatter_format_time_datefmt():
  rec = _rec(logging.INFO)
  rec.created = 1000000.5
  out = alog.Formatter().formatTime(rec, datefmt='%Y-%m-%d %H:%M:%S')
  expected = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(1000000.5))
  assert out == expected


def test_formatter_emit_extra():
  rec = _rec(logging.INFO, lineno=10)
  rec.created = 1000000.5
  hdr = alog.Formatter(emit_extra=['name', 'lineno']).make_header(rec)
  assert hdr == f'IN{_fts(rec.created)};{os.getpid()};f;test_alog;10'


def test_formatter_emit_extra_missing_field():
  rec = _rec(logging.INFO)
  hdr = alog.Formatter(emit_extra=['no_such_field']).make_header(rec)
  assert hdr.endswith(';None')


# ---------------------------------------------------------------------------
# log / logging_args / call limiting
# ---------------------------------------------------------------------------


def test_log_reaches_handler(log_state):
  handler = _capture(log_state)
  alog.set_current_level(logging.DEBUG, set_logger=False)
  alog.log(logging.INFO, 'hello %s', 'root', stacklevel=2)
  assert len(handler.records) == 1
  rec = handler.records[0]
  assert rec.getMessage() == 'hello root'
  assert rec.levelname == 'INFO'


def test_log_not_gated_by_module_threshold(log_state):
  # The module threshold gates the convenience wrappers only; log() defers
  # the decision to the logging module itself.
  handler = _capture(log_state)
  alog.set_current_level(logging.WARNING, set_logger=False)
  alog.log(logging.INFO, 'nope')
  assert len(handler.records) == 1
  assert handler.records[0].getMessage() == 'nope'


def test_log_lazy_msg_not_evaluated(log_state):
  handler = _capture(log_state)
  alog.set_current_level(logging.WARNING, set_logger=False)

  def _boom():
    raise AssertionError('lazy msg must not be evaluated')

  alog.info(_boom)
  alog.debug0(_boom)
  assert handler.records == []


def test_log_lazy_msg_evaluated_when_active(log_state):
  handler = _capture(log_state)
  alog.set_current_level(logging.DEBUG, set_logger=False)

  def _lazy():
    return 'lazy-5'

  alog.log(logging.INFO, _lazy)
  assert handler.records[-1].getMessage() == 'lazy-5'


def test_log_call_limited_same_site(log_state):
  handler = _capture(log_state)
  for idx in range(4):
    alog.log(logging.WARNING, 'lim-%d', idx, limit=2)
  assert len(handler.records) == 2


def test_log_call_limit_zero_blocks(log_state):
  handler = _capture(log_state)
  alog.log(logging.WARNING, 'blocked', limit=0)
  assert handler.records == []


def test_log_call_limit_is_per_site(log_state):
  handler = _capture(log_state)

  def _site_a():
    alog.log(logging.WARNING, 'from-a', limit=1)

  def _site_b():
    alog.log(logging.WARNING, 'from-b', limit=1)

  _site_a()
  _site_a()
  _site_b()
  assert [rec.getMessage() for rec in handler.records] == ['from-a', 'from-b']


def test_logging_args_stacklevel_adjusted(log_state):
  _capture(log_state)
  kwargs = alog.logging_args({})
  assert kwargs['stacklevel'] == 1 + alog._LOGGING_FRAMES


def test_logging_args_limit_absent_passes_through(log_state):
  _capture(log_state)
  kwargs = alog.logging_args({'stacklevel': 3})
  assert kwargs == {'stacklevel': 3 + alog._LOGGING_FRAMES}


# ---------------------------------------------------------------------------
# Convenience wrappers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize('fn, levelname', [
    (alog.spam, 'SPAM'),
    (alog.verbose, 'VERBOSE'),
    (alog.debug0, 'DEBUG0'),
    (alog.debug1, 'DEBUG1'),
    (alog.debug2, 'DEBUG2'),
    (alog.debug3, 'DEBUG3'),
    (alog.debug, 'DEBUG'),
    (alog.info, 'INFO'),
    (alog.warning, 'WARNING'),
    (alog.error, 'ERROR'),
    (alog.critical, 'CRITICAL'),
])
def test_level_wrappers(log_state, fn, levelname):
  # basic_setup() is the public path that registers the custom level names
  # and aligns both the module threshold and the logging-module level.
  alog.basic_setup(log_level='SPAM', log_file='')
  handler = _capture(log_state)
  fn('wrapped-%s', levelname)
  assert len(handler.records) == 1
  assert handler.records[0].levelname == levelname
  assert handler.records[0].getMessage() == f'wrapped-{levelname}'


def test_level_wrapper_gated(log_state):
  handler = _capture(log_state)
  alog.set_current_level(logging.INFO, set_logger=False)
  alog.debug0('too verbose')
  alog.info('visible')
  assert [rec.getMessage() for rec in handler.records] == ['visible']


# ---------------------------------------------------------------------------
# exception / xraise
# ---------------------------------------------------------------------------


def test_exception_logs_traceback(log_state):
  handler = _capture(log_state)
  alog.set_current_level(logging.DEBUG, set_logger=False)
  try:
    raise ValueError('the-error')
  except ValueError as exc:
    alog.exception(exc)

  assert len(handler.records) == 1
  msg = handler.records[0].getMessage()
  assert msg.startswith('Exception: the-error')
  assert 'Traceback (most recent call last)' in msg


def test_exception_custom_exmsg(log_state):
  handler = _capture(log_state)
  try:
    raise ValueError('boom')
  except ValueError as exc:
    alog.exception(exc, exmsg='custom-prefix')

  assert handler.records[0].getMessage().startswith('custom-prefix: boom')


def test_xraise_raises_with_message(log_state):
  handler = _capture(log_state)
  with pytest.raises(ValueError, match='plain-msg'):
    alog.xraise(ValueError, 'plain-msg')

  assert handler.records == []


def test_xraise_logit_logs_then_raises(log_state):
  handler = _capture(log_state)
  with pytest.raises(RuntimeError, match='bad value'):
    alog.xraise(RuntimeError, 'bad %s', 'value', logit=True)

  assert len(handler.records) == 1
  assert handler.records[0].getMessage() == 'bad value'


def test_xraise_formats_message(log_state):
  with pytest.raises(ValueError, match='bad value'):
    alog.xraise(ValueError, 'bad %s', 'value')


# ---------------------------------------------------------------------------
# setup_logging / add_logging_options / set_current_level
# ---------------------------------------------------------------------------


def test_set_current_level_threshold_only(log_state):
  alog.set_current_level(logging.WARNING, set_logger=False)
  assert alog.level_active(logging.INFO) is False
  assert alog.level_active(logging.WARNING) is True
  assert alog.level_active(logging.ERROR) is True


def test_set_current_level_updates_logger(log_state):
  root = log_state
  alog.set_current_level(logging.WARNING, set_logger=True)
  assert root.level == logging.WARNING


def test_level_run(log_state):
  alog.set_current_level(logging.WARNING, set_logger=False)
  assert alog.level_run(logging.INFO, lambda: 'no') is None
  assert alog.level_run(logging.ERROR, lambda: 'yes') == 'yes'


def test_add_logging_options_and_parse():
  parser = argparse.ArgumentParser()
  alog.add_logging_options(parser)
  ns = parser.parse_args(['--log_level', 'DEBUG2', '--log_file', 'STDERR,STDOUT'])
  assert ns.log_level == 'DEBUG2'
  assert ns.log_file == 'STDERR,STDOUT'


def test_add_logging_options_defaults():
  parser = argparse.ArgumentParser()
  alog.add_logging_options(parser)
  ns = parser.parse_args([])
  assert ns.log_level == alog._DEFAULT_ARGS['log_level']
  assert ns.log_file == alog._DEFAULT_ARGS['log_file']


def test_setup_logging_stderr(log_state, capsys):
  alog.setup_logging(
      types.SimpleNamespace(log_level='INFO',
                            log_file='STDERR',
                            log_mod_levels=[],
                            log_emit_extra=[]))
  root = logging.getLogger()
  assert [type(h) for h in root.handlers] == [logging.StreamHandler]
  assert root.level == logging.INFO
  logging.getLogger().warning('setup-marker')
  captured = capsys.readouterr()
  assert 'setup-marker' in captured.err
  assert 'WA' in captured.err


def test_setup_logging_file_target(log_state, temp_path):
  fname = os.path.join(temp_path, 'app.log')
  alog.setup_logging(
      types.SimpleNamespace(log_level='INFO',
                            log_file=fname,
                            log_mod_levels=[],
                            log_emit_extra=[]))
  logging.getLogger().info('file-marker')
  for handler in tuple(logging.getLogger().handlers):
    handler.close()
  with open(fname) as fpt:
    assert 'file-marker' in fpt.read()


def test_setup_logging_mod_levels(log_state):
  alog.setup_logging(
      types.SimpleNamespace(log_level='INFO',
                            log_file='STDERR',
                            log_mod_levels=['tests.mod_a,DEBUG', 'tests.mod_b,WARNING'],
                            log_emit_extra=[]))
  assert logging.getLogger('tests.mod_a').getEffectiveLevel() == logging.DEBUG
  assert logging.getLogger('tests.mod_b').getEffectiveLevel() == logging.WARNING


def test_setup_logging_replaces_handlers(log_state):
  root = log_state
  probe = _capture(root)
  alog.setup_logging(
      types.SimpleNamespace(log_level='INFO',
                            log_file='STDERR',
                            log_mod_levels=[],
                            log_emit_extra=[]))
  assert probe not in root.handlers


# ---------------------------------------------------------------------------
# async_log
# ---------------------------------------------------------------------------


def test_async_log_writes_formatted_line(log_state):
  buf = io.StringIO()
  alog.async_log(logging.ERROR, 'async %s', 'msg', file=buf)
  out = buf.getvalue()
  prefix = f'ER{_tstr(time.time())}'[:10]
  assert out.startswith(prefix)
  assert ';test_alog: async msg\n' in out
  assert out.endswith('\n')


def test_async_log_level_gating(log_state):
  alog.set_current_level(logging.CRITICAL, set_logger=False)
  buf = io.StringIO()
  alog.async_log(logging.ERROR, 'gated', file=buf)
  assert buf.getvalue() == ''


def test_async_log_lazy_msg(log_state):
  buf = io.StringIO()
  alog.set_current_level(logging.DEBUG, set_logger=False)

  def _lazy():
    return 'lazy-async'

  alog.async_log(logging.INFO, _lazy, file=buf)
  assert 'lazy-async' in buf.getvalue()
