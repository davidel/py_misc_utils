import collections


IterData = collections.namedtuple('IterData', 'n, last, data')


class BufferedIterator:

  def __init__(self, data, buffer_size):
    self._data = data
    self._buffer_size = buffer_size

  def generate(self):
    queue, n = collections.deque(), 0
    for data in self._data:
      if len(queue) < self._buffer_size:
        queue.append(data)
      else:
        cdata = queue.popleft()
        queue.append(data)
        yield IterData(n=n, last=False, data=cdata)

      n += 1

    while queue:
      cdata = queue.popleft()
      yield IterData(n=n, last=len(queue) == 0, data=cdata)
      n += 1

  def __iter__(self):
    return iter(self.generate())


