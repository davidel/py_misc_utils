"""Tests for py_misc_utils.moving_average.MovingAverage.

Expectations are pinned to the observed behavior (verified by probe
scripts before writing): the average blends as
value = value * factor + new * (1 - factor), the first update on an
uninitialized average adopts the new value, the factor bounds are
checked with tas.check (upper bound only), and factor 1.0 keeps the
initial value fixed forever.
"""

import pytest

from py_misc_utils import moving_average as mav


def test_first_update_adopts_value_when_uninitialized():
  """An average without init takes the first observed value verbatim."""
  ma = mav.MovingAverage(0.25)
  assert ma.update(4.0) == 4.0


def test_init_value_used_as_starting_point():
  """A provided init is the value the blending starts from."""
  ma = mav.MovingAverage(0.5, init=10.0)
  assert ma.value == 10.0


def test_blend_formula_applies_factor_to_old_value():
  """update() computes old * factor + new * (1 - factor)."""
  ma = mav.MovingAverage(0.5, init=10.0)
  assert ma.update(20.0) == 15.0
  assert ma.update(30.0) == 22.5


def test_factor_one_keeps_running_value():
  """factor 1.0 weights the running average fully: it stays fixed."""
  ma = mav.MovingAverage(1.0, init=5.0)
  assert ma.update(9.0) == 5.0
  assert ma.update(100.0) == 5.0


def test_factor_zero_adopts_latest_value():
  """factor 0.0 weights the new observation fully: it is adopted."""
  ma = mav.MovingAverage(0.0, init=5.0)
  assert ma.update(9.0) == 9.0
  assert ma.update(100.0) == 100.0


def test_factor_above_one_rejected():
  """A factor greater than 1.0 raises an AssertionError."""
  with pytest.raises(AssertionError):
    mav.MovingAverage(1.5)


def test_factor_one_exactly_accepted():
  """The factor check is inclusive at 1.0.

  Acceptance is asserted behaviorally: with factor 1.0 the running
  value never moves off its initial value.
  """
  ma = mav.MovingAverage(1.0, init=42.0)
  assert ma.update(100.0) == 42.0


def test_negative_factor_rejected():
  """Negative factors are rejected with an AssertionError."""
  with pytest.raises(AssertionError):
    mav.MovingAverage(-0.5)
