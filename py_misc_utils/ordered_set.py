import copy


class OrderedSet:

  def __init__(self, init=None):
    self._data = dict()
    self._seqno = 0

    for value in init or ():
      self.add(value)

  def add(self, value):
    n = self._data.get(value, self)
    if n is self:
      self._data[value] = n = self._seqno
      self._seqno += 1

    return n

  def remove(self, value):
    self._data.pop(value)

  def discard(self, value):
    self._data.pop(value, None)

  def pop(self):
    value, n = self._data.popitem()

    return value

  def clear(self):
    self._data = dict()
    self._seqno = 0

  def __len__(self):
    return len(self._data)

  def values(self):
    return sorted(self._data.keys(), key=lambda x: self._data[x])

  def __iter__(self):
    return iter(self.values())

  def __contains__(self, value):
    return value in self._data

  def union(self, *others):
    nos = copy.copy(self)
    for other in others:
      for value in other:
        nos.add(value)

    return nos

  def intersection(self, *others):
    nos = OrderedSet()
    for value in self.values():
      if all(value in other for other in others):
        nos.add(value)

    return nos

  def difference(self, *others):
    nos = OrderedSet()
    for value in self.values():
      if not any(value in other for other in others):
        nos.add(value)

    return nos

