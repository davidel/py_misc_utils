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

  def add(self, elem):
    tas.check(callable(elem), msg=f'Pipeline elements must be callable: {type(elem)}')
    self._elems.append(elem)

    return len(self._elems) - 1

  def pop(self, i=None):
    return self._elems.pop(i) if i is not None else self._elems.pop()

  def elems(self):
    return tuple(self._elems)

  def __call__(self, x):
    y = x
    for elem in self._elems:
      y = elem(y)

    return y

  def clone(self):
    elems = [cu.clone_or_self(elem) for elem in self._elems]

    return Pipeline(*elems)

  def flush(self):
    y = None
    for elem in self._elems:
      if y is not None:
        y = elem(y)

      flush_fn = getattr(elem, 'flush', None)
      if flush_fn is not None:
        y = flush_fn(y or ())

    return y

  def _try_call(self, name, *args, **kwargs):
    for elem in self._elems:
      cu.maybe_call(elem, name, *args, **kwargs)

  def reset(self):
    self._try_call('reset')


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
# When used in such fashion, pipeline elements whould inherit from IterElement and
# implement the _process() API.
class IterElement:

  def __call__(self, data):
    # Calls the _process() API making sure the input is an iterator.
    return self._process(cu.as_iterator(data))


# A simple IterElement that calls a function over the data. This is the same as
# the standard Pipeline use, but with support for iterator based pipelines.
class IterProcess(IterElement):

  def __init__(self, proc_fn, *args, **kwargs):
    super().__init__()
    self._proc_fn = proc_fn
    self._args = args
    self._kwargs = kwargs

  def _process(self, data):
    for value in data:
      yield self._proc_fn(*self._args, value, **self._kwargs)

