"""Index generation utilities for multi-dimensional array iteration.

This module provides generate(), which converts an index specification (int,
slice, list, ndarray, or tuple) into an iterator over index tuples
for a given array shape. It handles common NumPy-style indexing patterns
including integer indexing, slicing, fancy indexing, and ellipsis expansion
(Ellipsis is supported inside tuple specifications only).
"""

import itertools

import numpy as np


def generate(shape, idx):
  """Generate an iterator over index tuples for a multi-dimensional array shape.

  Converts an index specification into an iterator that yields tuples of indices,
  handling all common NumPy-style indexing patterns:
  - int: Selects a single element along one axis (requires 1-D shape)
  - slice: Selects a range along one axis (requires 1-D shape)
  - list/np.ndarray: Fancy indexing along one axis (requires 1-D shape)
  - tuple: Multi-axis indexing, mixing ints, slices, arrays, and Ellipsis
    (Ellipsis expands to fill remaining dimensions with full ranges)

  Args:
    shape: Tuple of integers representing the array shape.
    idx: Index specification. Can be an int, slice, list, numpy array, tuple,
      or Ellipsis.

  Returns:
    An itertools.product iterator yielding index tuples.

  Raises:
    AssertionError: If a 1-D-only index type is used with a multi-D shape.
    TypeError: If the index specification is not one of the supported types
      (a bare Ellipsis is only supported inside tuple specifications).
    ValueError: If the index specification is invalid for the given shape.
  """
  if isinstance(idx, int):
    assert len(shape) == 1, f'{shape}'

    gens = [(idx,)]
  elif isinstance(idx, slice):
    assert len(shape) == 1, f'{shape}'

    gens = [range(*idx.indices(shape[0]))]
  elif isinstance(idx, (list, np.ndarray)):
    assert len(shape) == 1, f'{shape}'

    gens = [idx]
  elif isinstance(idx, tuple):
    assert len(shape) >= len(idx), f'{shape} vs. {idx}'

    gens = [None] * len(shape)
    eidx = None
    for i, tidx in enumerate(idx):
      if isinstance(tidx, int):
        gens[i] = (tidx,)
      elif isinstance(tidx, slice):
        gens[i] = range(*tidx.indices(shape[i]))
      elif hasattr(tidx, '__iter__'):
        gens[i] = tidx
      elif isinstance(tidx, Ellipsis.__class__):
        eidx = i
        break

    if eidx is not None:
      for i in range(1, len(idx) - eidx):
        tidx = idx[-i]
        if isinstance(tidx, int):
          gens[-i] = (tidx,)
        elif isinstance(tidx, slice):
          gens[-i] = range(*tidx.indices(shape[-i]))
        elif hasattr(tidx, '__iter__'):
          gens[-i] = tidx
        elif isinstance(tidx, Ellipsis.__class__):
          raise ValueError(f'Wrong index {idx} for shape {shape}')

      for i in range(len(shape)):
        if gens[i] is None:
          gens[i] = range(shape[i])
  else:
    raise TypeError(f'Wrong index {idx} for shape {shape}')

  return itertools.product(*gens)
