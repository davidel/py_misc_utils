"""Buffered iterator with bounded-size reservoir sampling.

This module provides BufferedIterator, which wraps an iterable and yields
IterData namedtuples. It maintains a bounded buffer of the most recent items,
replacing older items probabilistically as new data arrives. This is useful
for streaming scenarios where you need to keep a fixed-size sample of recent
items while iterating.
"""

import collections

IterData = collections.namedtuple('IterData', 'n, left, data')
# Named tuple yielded by BufferedIterator.generate():
# - n: int, the total number of items seen so far.
# - left: int, the number of items remaining in the buffer at yield time.
# - data: the data item being yielded.


class BufferedIterator:
  """Iterator that maintains a bounded buffer of items from a data source.

  As items are drawn from the underlying iterable, they are added to a buffer
  up to buffer_size. Once full, new items replace existing ones at a
  pseudorandom position. Each yielded IterData includes the item, the count
  of items seen so far, and the number of items left in the buffer.

  Attributes:
    _data: The source iterable.
    _buffer_size: Maximum number of items to keep in the buffer.
  """

  def __init__(self, data, buffer_size):
    """Initialize the BufferedIterator.

    Args:
      data: An iterable of data items to iterate over.
      buffer_size: Maximum number of items to buffer at once.
    """
    self._data = data
    self._buffer_size = buffer_size

  def generate(self):
    """Yield IterData namedtuples for each item from the source iterable.

    Maintains a bounded buffer, replacing items as needed. After all source
    data is exhausted, yields the remaining buffered items.

    Yields:
      IterData namedtuples with fields n (item count), left (items remaining
      in buffer), and data (the yielded item).
    """
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
    """Return the iterator by delegating to generate().

    Returns:
      A generator from self.generate().
    """
    return self.generate()
