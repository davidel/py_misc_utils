
class MovingAverage:

  def __init__(self, factor):
    assert factor > 0.0 and factor < 1.0, f'{factor:.4e}'
    self._factor = factor
    self.value = 0

  def update(self, value):
    self.value = self.value * self._factor + value * (1.0 - self._factor)

