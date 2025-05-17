from . import assert_checks as tas
from . import core_utils as cu


class Pipeline:

  def __init__(self, *elems):
    self._elems = list(elems)

  def __len__(self):
    return len(self._elems)

  def __getitem__(self, i):
    return self._elems[i]

  def __iter__(self):
    return iter(self._elems)

  def append(self, elem):
    tas.check(callable(elem), msg=f'Pipeline elements must be callable: {type(elem)}')
    self._elems.append(elem)

    return len(self._elems) - 1

  def extend(self, elems):
    for elem in elems:
      self.append(elem)

  def pop(self, i=None):
    return self._elems.pop(i) if i is not None else self._elems.pop()

  def elems(self):
    return tuple(self._elems)

  def _apply(self, elem, data):
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
    y = x
    for elem in self._elems:
      y = self._apply(elem, y)

    return y

  def __repr__(self):
    return '\n'.join(f'[{i}] {repr(elem)}' for i, elem in enumerate(self._elems))

  def clone(self):
    # If a Pipeline elements has a state, it must implement the clone() API.
    elems = [cu.maybe_call_dv(elem, 'clone', elem) for elem in self._elems]

    return Pipeline(*elems)

  def flush(self):
    y = None
    for elem in self._elems:
      flush_fn = getattr(elem, 'flush', None)
      if flush_fn is not None:
        y = flush_fn(y or ())
      elif y is not None:
        y = self._apply(elem, y)

    return y


# Exception thrown by members of iterative pipelines when they want to stop the
# stream of data, signaling that nothing more will be allowed through it.
# Returning an empty iterator/generator will not cut it, as this is a legal
# return value in case of batching elements.
# Think about a pipeline element which is a filter which should pass through the
# first N samples, for example.
class HaltedPipeline(Exception):
  pass


# The Pipeline can also be used with data which is returned as iterators, where
# there is not a 1:1 mapping between input and output.
# Think about a pipeline element which absorbs data, and return batches of it (or
# absorbs text and emits token indices).
# The non-iterator approach would not work, as for many inputs there are no ouputs
# at all (till the batch size is reached).
# When used in such fashion, pipeline elements whould inherit from IterElement.
class IterElement:
  pass


def _iter_process(elem, data):
  for x in data:
    yield elem(x)

