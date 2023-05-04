
class InfixOp:
  def __init__(self, opfn):
    self._opfn = opfn

  def __ror__(self, lhs):
    return InfixOp(lambda x, self=self, lhs=lhs: self._opfn(lhs, x))

  def __or__(self, rhs):
    return self._opfn(rhs)

  def __rlshift__(self, lhs):
    return InfixOp(lambda x, self=self, lhs=lhs: self._opfn(lhs, x))

  def __rshift__(self, rhs):
    return self._opfn(rhs)

  def __call__(self, value1, value2):
    return self._opfn(value1, value2)

