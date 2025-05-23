import collections


IterData = collections.namedtuple('IterData', 'n, left, data')


class BufferedIterator:

  def __init__(self, data, buffer_size):
    self._data = data
    self._buffer_size = buffer_size

  def generate(self):
    queue, n = [], 0
    for data in self._data:
      if len(queue) < self._buffer_size:
        queue.append(data)
      else:
        cidx = n % len(queue)
        cdata = queue[cidx]
        queue[cidx] = data
        yield IterData(n=n, left=len(queue), data=cdata)

      n += 1

    for i in range(len(queue)):
      cidx = n % len(queue)
      cdata = queue[cidx]
      yield IterData(n=n, left=len(queue) - i - 1, data=cdata)
      n += 1

  def __iter__(self):
    return self.generate()


