import itertools

import numpy as np


def generate(shape, idx):
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
    raise ValueError(f'Wrong index {idx} for shape {shape}')

  return itertools.product(*gens)

