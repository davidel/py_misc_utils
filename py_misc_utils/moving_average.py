"""Exponentially weighted moving average computation.

This module provides the :class:`MovingAverage` class which maintains a running
average of values using an exponential smoothing factor. Each new value is
blended with the previous average according to the factor.
"""

from . import assert_checks as tas
from . import num_utils as nu


class MovingAverage:
  """Maintain an exponentially weighted moving average of values.

  Attributes:
    value: The current moving average value.
"""

  def __init__(self, factor, init=None):
    """Initialize the moving average with a smoothing factor.

    The average is blended as value = old * factor + new * (1 - factor),
    so a factor of 0.0 always adopts the latest observation, while a
    factor of 1.0 keeps the running value unchanged (and an average
    initialized with None stays None forever).

    Args:
      factor: Smoothing factor between 0.0 and 1.0 (inclusive on both
        ends).
      init: Optional initial value for the average.
    """
    tas.check(factor >= 0.0 and factor <= 1.0, msg=f'Invalid factor: {factor:.4e}')
    self._factor = factor
    self.value = init

  def update(self, value):
    """Incorporate a new value into the moving average.

    Args:
      value: The new observation to blend into the average.

    Returns:
      The updated moving average value.
    """
    if self.value is None:
      self.value = value
    else:
      self.value = nu.mix(self.value, value, self._factor)

    return self.value
