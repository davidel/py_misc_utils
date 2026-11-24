"""Tests for py_misc_utils.infix_op.InfixOp.

Expectations pinned to the observed behavior (verified by probe scripts
before writing): `value | op` (via __ror__) and `value << op` (via
__rlshift__) return a NEW partially-applied InfixOp holding the left
operand; `partial | y` and `partial >> y` (via __or__/__rshift__) then
call the underlying function with (lhs, y). Direct __call__ takes both
operands. So the infix form is `x | op | y` / `x << op | y` /
`(x | op) >> y`.
"""

import pytest

from py_misc_utils import infix_op as iop


def test_or_chain():
  """`x | op | y` applies opfn(x, y)."""
  add = iop.InfixOp(lambda a, b: a + b)
  assert 5 | add | 3 == 8


def test_lshift_then_or():
  """`x << op | y` applies opfn(x, y) (<< binds tighter than |)."""
  sub = iop.InfixOp(lambda a, b: a - b)
  assert 5 << sub | 2 == 3


def test_or_then_rshift():
  """`(x | op) >> y` finishes the application as opfn(x, y)."""
  sub = iop.InfixOp(lambda a, b: a - b)
  assert (5 | sub) >> 2 == 3


def test_partial_is_infixop():
  """`value | op` returns an InfixOp instance, not a result."""
  add = iop.InfixOp(lambda a, b: a + b)
  partial = 5 | add
  assert isinstance(partial, iop.InfixOp)


def test_partial_finished_with_or():
  """A partially-applied InfixOp is finished with |, not by calling.

  The partial op holds a single-argument _opfn; __or__ passes the rhs
  as that single argument.
  """
  add = iop.InfixOp(lambda a, b: a + b)
  assert (5 | add) | 3 == 8
  assert (5 | add) >> 3 == 8


def test_direct_call():
  """Calling the wrapper directly passes both operands to opfn."""
  cat = iop.InfixOp(lambda a, b: a + b)
  assert cat('a', 'b') == 'ab'


def test_lshift_partial_is_infixop():
  """`value << op` also returns a partially-applied InfixOp."""
  add = iop.InfixOp(lambda a, b: a + b)
  partial = 5 << add
  assert isinstance(partial, iop.InfixOp)
  assert partial | 3 == 8


def test_partial_call_forwards_both_args():
  """Calling a partial op with two args forwards both to the single-arg
  lambda: value1 binds x, value2 binds self (report-only quirk - the
  partial wrapper is operator-finished only, and the second arg is
  treated as the wrapper itself)."""
  add = iop.InfixOp(lambda a, b: a + b)
  with pytest.raises(AttributeError):
    (5 | add)(1, 2)
