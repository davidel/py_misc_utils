from . import assert_checks as tas
from . import num_utils as nu


class MovingAverage:

  def __init__(self, factor, init=None):
    tas.check(factor >= 0.0 and factor <= 1.0, msg=f'Invalid factor: {factor:.4e}')
    self._factor = factor
    self.value = init

  def update(self, value):
    if self.value is None:
      self.value = value
    else:
      self.value = nu.mix(self.value, value, self._factor)

    return self.value

