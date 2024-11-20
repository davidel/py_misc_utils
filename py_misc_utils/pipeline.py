
class Pipeline:

  def __init__(self, init=None):
    self._elems = list(init) if init else []

  def __len__(self):
    return len(self._elems)

  def __getitem__(self, i):
    return self._elems[i]

  def __iter__(self):
    return iter(self._elems)

  def add(self, elem):
    self._elems.append(elem)

  def pop(self, i=None):
    return self._elems.pop(i) if i is not None else self._elems.pop()

  def __call__(self, x):
    for elem in self._elems:
      x = elem(x)

    return x

