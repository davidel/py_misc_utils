"""Tests for py_misc_utils.no_except.

Expectations pinned to the observed behavior (verified by probe scripts
before writing): no_except() returns fn's result on success and the
exception object on failure (logging a warning); qno_except() behaves
the same without logging; xwrap_fn() builds a zero-argument wrapper of
fn with bound args.
"""

import logging

from py_misc_utils import no_except as ne


class Boom(Exception):
  pass


def _boom():
  raise Boom('kaboom')


def test_success_returns_result():
  """The function result is returned unchanged on success."""
  assert ne.no_except(lambda a, b: a + b, 1, 2) == 3


def test_exception_returned():
  """The exception object is returned, not raised."""
  res = ne.no_except(_boom)
  assert isinstance(res, Boom)
  assert str(res) == 'kaboom'


def test_exception_logged(caplog):
  """no_except logs a WARNING containing the message."""
  with caplog.at_level(logging.WARNING):
    ne.no_except(_boom)

  assert any(
      rec.levelname == 'WARNING' and 'kaboom' in rec.message for rec in caplog.records)


def test_qno_except_quiet(caplog):
  """qno_except returns the exception without logging it."""
  with caplog.at_level(logging.WARNING):
    res = ne.qno_except(_boom)

  assert isinstance(res, Boom)
  assert not any('kaboom' in rec.message for rec in caplog.records)


def test_qno_except_success():
  """qno_except returns the result on success."""
  assert ne.qno_except(lambda: 'ok') == 'ok'


def test_xwrap_fn():
  """xwrap_fn() returns a zero-arg callable with bound args."""
  fwrap = ne.xwrap_fn(lambda a, b: a * b, 3, b=4)
  assert fwrap() == 12


def test_xwrap_fn_catches():
  """xwrap_fn-wrapped calls catch exceptions like no_except."""
  fwrap = ne.xwrap_fn(_boom)
  assert isinstance(fwrap(), Boom)
