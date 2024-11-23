from . import assert_checks as tas


class Pipeline:

  def __init__(self, *elems):
    self._elems = list(elems)

  def __len__(self):
    return len(self._elems)

  def __getitem__(self, i):
    return self._elems[i]

  def __iter__(self):
    return iter(self._elems)

  def add(self, elem):
    tas.check(callable(elem), msg=f'Pipeline elements must be callable: {type(elem)}')
    self._elems.append(elem)

    return len(self._elems) - 1

  def pop(self, i=None):
    return self._elems.pop(i) if i is not None else self._elems.pop()

  def elems(self):
    return tuple(self._elems)

  def __call__(self, x):
    for elem in self._elems:
      x = elem(x)

    return x

