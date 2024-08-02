from . import assert_checks as tas


class MovingAverage:

  def __init__(self, factor, init=None):
    tas.check(factor >= 0.0 and factor <= 1.0, msg=f'Invalid factor: {factor:.4e}')
    self._factor = factor
    self.value = init or 0

  def update(self, value):
    self.value = self.value * self._factor + value * (1.0 - self._factor)

    return self.value

