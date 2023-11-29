class MovingAverage(object):

  def __init__(self, factor):
    assert factor > 0.0 and factor < 1.0, f'{factor:.4e}'
    self._factor = factor
    self._value = 0

  def update(self, value):
    self._value = self._value * self._factor + value * (1.0 - self._factor)

  @property
  def value(self):
    return self._value
