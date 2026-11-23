"""Tests for py_misc_utils.buffered_iterator.

The replacement index is n % len(queue) - fully deterministic, so the exact
yield sequences are pinned here. Note that n counts items drawn from the
source iterable, not yields: when the buffer is not yet full, items are
appended without a yield but still bump n.
"""

from py_misc_utils import buffered_iterator as bi


def test_bsize2_exact_sequence():
  out = list(bi.BufferedIterator([1, 2, 3, 4], buffer_size=2).generate())
  # Hand-computed: fills [1,2], then replaces 1, 2, then drains.
  got = [(od.n, od.left, od.data) for od in out]
  assert got == [
      (2, 2, 1),
      (3, 2, 2),
      (4, 1, 3),
      (5, 0, 4),
  ]


def test_bsize1_passthrough():
  out = list(bi.BufferedIterator([10, 20, 30], buffer_size=1).generate())
  got = [(od.n, od.left, od.data) for od in out]
  assert got == [
      (1, 1, 10),
      (2, 1, 20),
      (3, 0, 30),
  ]


def test_bsize_ge_len_preserves_order():
  out = list(bi.BufferedIterator([5, 6, 7], buffer_size=5).generate())
  got = [(od.n, od.left, od.data) for od in out]
  assert got == [
      (3, 2, 5),
      (4, 1, 6),
      (5, 0, 7),
  ]


def test_yield_count_equals_len_data():
  data = list(range(10))
  out = list(bi.BufferedIterator(data, buffer_size=3).generate())
  assert len(out) == 10


def test_yielded_multiset_equals_source():
  data = list(range(10))
  out = list(bi.BufferedIterator(data, buffer_size=3).generate())
  assert sorted(od.data for od in out) == data


def test_empty_source_yields_nothing():
  out = list(bi.BufferedIterator([], buffer_size=2).generate())
  assert out == []


def test_laziness():
  consumed = []

  def src():
    for i in range(5):
      consumed.append(i)
      yield i

  it = bi.BufferedIterator(src(), buffer_size=2).generate()
  # Creating the generator must not consume the source.
  assert consumed == []
  # The first yield requires filling the buffer (2 items) plus drawing the
  # item which replaces the first slot.
  next(it)
  assert consumed == [0, 1, 2]
  next(it)
  assert consumed == [0, 1, 2, 3]


def test_iter_delegates():
  data = [1, 2, 3]
  bit = bi.BufferedIterator(data, buffer_size=2)
  got = [od.data for od in bit]
  assert sorted(got) == data


def test_buffer_size_zero_raises():
  # Report-only pin: degenerate configuration raises ZeroDivisionError
  # (n % len(queue) with an empty queue).
  import pytest
  with pytest.raises(ZeroDivisionError):
    list(bi.BufferedIterator([1, 2], buffer_size=0).generate())
