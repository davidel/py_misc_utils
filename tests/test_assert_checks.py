"""Tests for py_misc_utils.assert_checks.

The tas.* helpers log a formatted failure message - source location,
operation and involved values - and raise. The tests pin success and
failure behavior for every public check, using custom exception classes
so that a raised error is unambiguously attributable to the check itself
(and not to a pytest assertion), and pin the message shape via content
substrings, never exact full strings.
"""

import logging

import pytest

from py_misc_utils import assert_checks as tas


class CheckError(Exception):
  pass


def _get_fail(fn, *args, msg=None, exception=CheckError):
  """Runs a check expecting a failure; returns the raised exception."""
  with pytest.raises(exception) as ei:
    fn(*args, msg=msg, exception=exception)

  return ei.value


def _expect_ok(fn, *args, **kwargs):
  """Runs a check expecting success (no exception raised)."""
  fn(*args, **kwargs)


def test_check_success():
  _expect_ok(tas.check, True)
  _expect_ok(tas.check, 1)
  _expect_ok(tas.check, 'non-empty')


def test_check_failure_message():
  # Called directly, so the reported location is this test function.
  with pytest.raises(CheckError) as ei:
    tas.check(False, exception=CheckError)

  msg = str(ei.value)
  assert msg.startswith('Check failed from ')
  assert 'test_assert_checks.py' in msg
  assert 'tas.check(False, exception=CheckError)' in msg


def test_check_msg():
  err = _get_fail(tas.check, False, msg='custom message')

  assert 'custom message' in str(err)


def test_check_custom_exception():
  err = _get_fail(tas.check, False, exception=ValueError)

  assert isinstance(err, ValueError)


def test_check_custom_level(caplog):
  with caplog.at_level(logging.WARNING), pytest.raises(CheckError):
    tas.check(False, level=logging.WARNING, exception=CheckError)

  assert caplog.records[-1].levelname == 'WARNING'
  assert 'Check failed from' in caplog.records[-1].getMessage()


def test_check_fn_success():
  _expect_ok(tas.check_fn, lambda x: x > 0, 5)
  _expect_ok(tas.check_fn, lambda: True)


def test_check_fn_failure():
  err = _get_fail(tas.check_fn, lambda x: x > 0, -1)

  # The predicate is used as the operation in the failure message.
  assert '<lambda>(-1) failed from' in str(err)


def test_check_fnres_eq_success():
  _expect_ok(tas.check_fnres_eq, 10, lambda x: x * 2, 5)


def test_check_fnres_eq_failure():
  err = _get_fail(tas.check_fnres_eq, 10, lambda: 3)

  assert '<lambda>() == 10 failed from' in str(err)


def test_check_fnres_ne_success():
  _expect_ok(tas.check_fnres_ne, 7, lambda x: x * 2, 5)


def test_check_fnres_ne_failure():
  err = _get_fail(tas.check_fnres_ne, 7, lambda: 7)

  assert '<lambda>() != 7' in str(err)
  assert ' 7 failed from' in str(err)


def test_check_is_none():
  _expect_ok(tas.check_is_none, None)

  err = _get_fail(tas.check_is_none, 1)

  assert '1 == None' in str(err)


def test_check_is_not_none():
  _expect_ok(tas.check_is_not_none, 1)
  _expect_ok(tas.check_is_not_none, 0)  # Falsy but not None: passes.

  err = _get_fail(tas.check_is_not_none, None)

  assert 'None != None' in str(err)


def test_check_in():
  _expect_ok(tas.check_in, 2, [1, 2, 3])
  _expect_ok(tas.check_in, 'a', {'a': 1})

  err = _get_fail(tas.check_in, 4, [1, 2, 3])

  assert '4 in [1, 2, 3]' in str(err)


def test_check_eq_ne_success():
  _expect_ok(tas.check_eq, 'x', 'x')
  _expect_ok(tas.check_ne, 'x', 'y')


def test_check_eq_failure():
  err = _get_fail(tas.check_eq, 'x', 'y')

  assert "x == y" in str(err)


def test_check_ne_failure():
  err = _get_fail(tas.check_ne, 'x', 'x')

  assert 'x != x' in str(err)


def test_check_le_ge_lt_gt_success():
  _expect_ok(tas.check_le, 1, 2)
  _expect_ok(tas.check_le, 2, 2)  # Inclusive.
  _expect_ok(tas.check_ge, 2, 2)  # Inclusive.
  _expect_ok(tas.check_lt, 1, 3)
  _expect_ok(tas.check_gt, 3, 1)


def test_check_le_ge_lt_gt_failures():
  err = _get_fail(tas.check_le, 3, 2)
  assert '3 <= 2' in str(err)

  err = _get_fail(tas.check_ge, 1, 2)
  assert '1 >= 2' in str(err)

  err = _get_fail(tas.check_lt, 3, 1)
  assert '3 < 1' in str(err)

  err = _get_fail(tas.check_gt, 2, 2)
  assert '2 > 2' in str(err)


def test_check_lt_gt_strict():
  # lt/gt are strict: equal values fail.
  err = _get_fail(tas.check_lt, 2, 2)
  assert '2 < 2' in str(err)

  err = _get_fail(tas.check_gt, 2, 2)
  assert '2 > 2' in str(err)
