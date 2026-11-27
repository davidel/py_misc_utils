"""Pipeline processing utilities for composing callable transformations.

This module provides a Pipeline class for chaining callable elements,
with support for both single-value and iterator-based processing.
"""

from . import assert_checks as tas
from . import core_utils as cu


class Pipeline:
  """A pipeline of callable elements for data transformation.

  Elements are applied sequentially: output of one becomes input to the next.
  Supports both single-value processing and iterator/streaming processing.

  Elements can be plain callables (applied value-by-value) or IterElement
  subclasses (applied to the whole stream at once).

  Example:
    p = pipeline.Pipeline(lambda x: x + 1, lambda x: x * 2)
    p(5)  # 12
    list(p(iter([1, 2])))  # [4, 6] - iterators stay lazy streams.
  """

  def __init__(self, *elems):
    """Initialize a pipeline with callable elements.

    Args:
      *elems: Callable elements to apply in sequence.
    """
    self._elems = list(elems)

  def __len__(self):
    """Return the number of elements in the pipeline."""
    return len(self._elems)

  def __getitem__(self, i):
    """Get the pipeline element at index i."""
    return self._elems[i]

  def __iter__(self):
    """Iterate over pipeline elements."""
    return iter(self._elems)

  def append(self, elem):
    """Append a callable element to the pipeline (returns its index).

    Args:
      elem: Callable to append.

    Returns:
      Index of the appended element.

    Raises:
      AssertionError: If elem is not callable.
    """
    tas.check(callable(elem), msg=f'Pipeline elements must be callable: {type(elem)}')
    self._elems.append(elem)

    return len(self._elems) - 1

  def extend(self, elems):
    """Extend the pipeline with multiple callable elements.

    Args:
      elems: Iterable of callables to append.
    """
    for elem in elems:
      self.append(elem)

  def pop(self, i=None):
    """Remove and return an element from the pipeline.

    Args:
      i: Index to pop (default: last element).

    Returns:
      The removed element.
    """
    return self._elems.pop(i) if i is not None else self._elems.pop()

  def elems(self):
    """Get all pipeline elements as a tuple.

    Returns:
      Tuple of pipeline elements.
    """
    return tuple(self._elems)

  def _apply(self, elem, data):
    """Apply a single pipeline element to data.

    Handles iterator/streaming logic for IterElement subclasses: when data
    is an iterator, an IterElement consumes the whole stream, while a plain
    callable is mapped over it value-by-value.

    Args:
      elem: Pipeline element (callable or IterElement).
      data: Input data (single value or iterator).

    Returns:
      Result of applying elem to data.
    """
    if cu.is_iterator(data):
      if isinstance(elem, IterElement):
        return elem(data)
      else:
        return _iter_process(elem, data)
    elif isinstance(elem, IterElement):
      return elem([data])
    else:
      return elem(data)

  def __call__(self, x):
    """Apply the pipeline to input data.

    Args:
      x: Input data (single value or iterator).

    Returns:
      Result after applying all pipeline elements.
    """
    y = x
    for elem in self._elems:
      y = self._apply(elem, y)

    return y

  def __repr__(self):
    """Return a string representation of the pipeline."""
    return '\n'.join(f'[{i}] {elem!r}' for i, elem in enumerate(self._elems))

  def clone(self):
    """Create a copy of the pipeline, cloning stateful elements.

    Elements with a `clone()` method will be cloned; others are reused.

    Returns:
      New Pipeline instance with cloned elements.
    """
    # If a Pipeline elements has a state, it must implement the clone() API.
    elems = [cu.maybe_call_dv(elem, 'clone', elem) for elem in self._elems]

    return Pipeline(*elems)

  def flush(self):
    """Flush stateful pipeline elements.

    Calls `flush()` on elements that have it, passing the previous flush
    result - so that residual data (e.g. a partial batch held by a chunking
    element) is pushed through the remaining elements. Elements without a
    flush() method process the pending value as a regular call.

    Returns:
      Result of the last flush operation, or None.
    """
    y = None
    for elem in self._elems:
      flush_fn = getattr(elem, 'flush', None)
      if callable(flush_fn):
        y = flush_fn(y or ())
      elif y is not None:
        y = self._apply(elem, y)

    return y


class HaltedPipeline(Exception):
  """Exception thrown to stop the stream of data of an iterative pipeline.

  Members of iterative pipelines raise it when they want to stop the
  stream, signaling that nothing more will be allowed through it.
  Returning an empty iterator/generator will not cut it, as this is a
  legal return value in case of batching elements - think about a
  pipeline element which is a filter which should pass through the first
  N samples, for example.
  """


# The Pipeline can also be used with data which is returned as iterators, where
# there is not a 1:1 mapping between input and output.
# Think about a pipeline element which absorbs data, and return batches of it (or
# absorbs text and emits token indices).
# The non-iterator approach would not work, as for many inputs there are no ouputs
# at all (till the batch size is reached).
# When used in such fashion, pipeline elements whould inherit from IterElement.
class IterElement:
  """Base class for pipeline elements that process iterators directly.

  Unlike regular pipeline elements which receive single values,
  IterElement subclasses receive an iterator and must return an iterator.
  This is useful for elements that batch data, absorb multiple inputs
  before producing output, or transform streams.

  Subclasses must implement __call__(self, iterator) -> iterator. A
  flush() method may also be provided to emit residual data at the end of
  a stream (see Pipeline.flush()).
  """


def _iter_process(elem, data):
  """Apply a regular callable element to an iterator, yielding results.

  Args:
    elem: Callable element (not an IterElement).
    data: Input iterator.

  Yields:
    Results of applying elem to each item in data.
  """
  for x in data:
    yield elem(x)
