"""Tests for the py_misc_utils.pipeline callable chaining helpers.

Expectations follow the documented API contract: Pipeline applies its
elements sequentially (output of one is input to the next), iterators
propagate as iterators with IterElement elements consuming the whole
stream, HaltedPipeline stops element application, and clone() returns an
independent copy.
"""

import pytest

import py_misc_utils.pipeline as ppl


class _Chunker(ppl.IterElement):
  """Collects items in groups of size n until flushed."""

  def __init__(self, n):
    self._n = n
    self._buf = []

  def __call__(self, it):
    for x in it:
      self._buf.append(x)
      if len(self._buf) == self._n:
        out, self._buf = self._buf, []

        yield out

  def flush(self, _prev=None):
    """Return the buffered remainder, if any.

    Pipeline.flush() calls element flush functions with the previous
    flush result, so an argument must be accepted.
    """
    if self._buf:
      out, self._buf = self._buf, []

      return out

    return None


class _Halter(ppl.IterElement):
  """Stream element raising HaltedPipeline to end the data stream."""

  def __init__(self, max_items):
    self._left = max_items
    self._seen = 0

  def __call__(self, it):
    for x in it:
      self._seen += 1
      if self._seen > self._left:
        raise ppl.HaltedPipeline()
      yield x


def test_len_getitem_iter_and_elems():
  """len(), __getitem__, __iter__ and elems() expose the element list."""
  f1, f2, f3 = lambda x: x + 1, lambda x: x * 2, lambda x: -x
  p = ppl.Pipeline(f1, f2, f3)

  assert len(p) == 3
  assert p[0] is f1
  assert p[2] is f3
  assert list(p) == [f1, f2, f3]
  assert list(p.elems()) == [f1, f2, f3]
  assert p[-1] is f3
  assert p.elems() is not p._elems


def test_append_extend_pop_and_empty_pop_error():
  """append()/extend() grow the pipeline; pop() removes the last element."""
  f1, f2, f3 = lambda x: x + 1, lambda x: x * 2, lambda x: x - 3
  p = ppl.Pipeline(f1)

  p.append(f2)
  p.extend([f3])

  assert len(p) == 3

  assert p.pop() is f3
  assert p.pop() is f2
  assert len(p) == 1

  with pytest.raises(IndexError):
    ppl.Pipeline().pop()

  assert p.pop() is f1
  assert len(p) == 0


def test_call_composes_sequentially():
  """__call__ applies elements left to right: (x + 1) * 2 then negated."""
  p = ppl.Pipeline(lambda x: x + 1, lambda x: x * 2, lambda x: -x)

  assert p(5) == -12


def test_empty_pipeline_is_identity():
  """An empty pipeline returns its input unchanged."""
  p = ppl.Pipeline()
  data = object()

  assert p(data) is data


def test_call_with_iterator_propagates_streaming():
  """Iterator inputs stay iterators through plain callable elements."""
  p = ppl.Pipeline(lambda x: x * 10)

  out = p(iter([1, 2, 3]))
  assert hasattr(out, '__next__')
  assert list(out) == [10, 20, 30]


def test_iter_element_consumes_whole_stream_and_flush():
  """IterElement elements receive the full iterator and may buffer.

  flush() calls the elements' own flush() and feeds the previous flush
  result (here: the buffered remainder) through the following elements.
  """
  chunker = _Chunker(2)
  p = ppl.Pipeline(chunker, lambda x: sum(x))

  out = p(iter([1, 2, 3, 4, 5]))
  assert list(out) == [3, 7]

  # The chunker buffered the odd 5; flush() pushes it through the pipeline.
  assert p.flush() == 5


def test_halted_pipeline_surfaces_to_output_consumer():
  """HaltedPipeline propagates to whoever consumes the output iterator.

  The exception is raised lazily: __call__() returns the chained
  generator without consuming it, so the consumer sees the items
  emitted before the halt and then the exception itself.
  """
  p = ppl.Pipeline(_Halter(2), lambda x: x * 10)

  out = p(iter([1, 2, 3, 4]))

  assert next(out) == 10
  assert next(out) == 20
  with pytest.raises(ppl.HaltedPipeline):
    next(out)


def test_halted_pipeline_first_n_filter_pattern():
  """The first-N-filter use case: consumer catches HaltedPipeline."""
  p = ppl.Pipeline(_Halter(3))

  collected = []
  out = p(iter([1, 2, 3, 4, 5]))
  with pytest.raises(ppl.HaltedPipeline):
    collected.extend(item for item in out)

  assert collected == [1, 2, 3]


def test_clone_is_independent():
  """clone() copies the element list; later appends do not leak."""
  f = lambda x: x + 1
  p = ppl.Pipeline(f)
  c = p.clone()

  assert c.elems() == p.elems()

  c.append(lambda x: x * 2)
  p.append(lambda x: x * 3)

  assert len(c) == 2
  assert len(p) == 2
  assert c.elems()[1] is not p.elems()[1]
  assert c(1) == 4
  assert p(1) == 6


def test_repr_lists_numbered_elements():
  """__repr__ shows one numbered element per line with its repr."""
  p = ppl.Pipeline(lambda x: x + 1, lambda x: x * 2)

  lines = repr(p).split('\n')

  assert len(lines) == 2
  assert lines[0].startswith('[0] ')
  assert lines[1].startswith('[1] ')
  # Plain functions repr as their builtins form.
  assert 'function' in lines[0] and 'function' in lines[1]


def test_iter_process_applies_element_to_each_item():
  """_iter_process() maps elem over the input iterator lazily."""
  src = iter([1, 2, 3])
  gen = ppl._iter_process(lambda x: x + 100, src)

  assert next(gen) == 101
  assert next(gen) == 102
  assert next(gen) == 103
  with pytest.raises(StopIteration):
    next(gen)
