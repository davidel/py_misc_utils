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
  """Generate an iterator of index tuples for a given array shape.

  Args:
    shape: The shape of the array to index - a sequence of dimension
      sizes.
    idx: The index specification:
      - an int: selects that position in a 1-D shape;
      - a slice: selects the positions in its range;
      - a list (or other iterable): selects the given positions (1-D
        shapes only);
      - an ndarray: as list, for any shape depth;
      - a tuple: one specification per dimension, applied right-to-left;
        dimensions not covered by the tuple iterate over their full
        range. An Ellipsis entry expands to the full range of one
        dimension (numpy-like auto-fill of trailing dimensions is NOT
        supported: a tuple shorter than the shape raises TypeError).

  Returns:
    An iterator of tuples - one per selected element - whose length
    matches the shape depth. Each tuple holds one index per dimension.

  Raises:
    AssertionError: If a 1-D-only index type (int, slice, list) is used
      with a multi-dimensional shape.
    TypeError: If the index specification is not one of the supported
      types (a bare Ellipsis is not supported outside tuples), or if a
      tuple specification does not cover all the shape dimensions.
    ValueError: If the index specification is invalid for the given
      shape.

  Example:
    list(gen_indices.generate((3,), 1))  # [(1,)]
    list(gen_indices.generate((3,), [0, 2]))  # [(0,), (2,)]
    list(gen_indices.generate((2, 3), (Ellipsis, 1)))
    # -> [(0, 1), (1, 1)] - full range on dim 0, fixed index 1 on dim 1.
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
